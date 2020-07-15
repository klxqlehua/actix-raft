use tokio::io::AsyncWriteExt;

use crate::{AppData, AppDataResponse, AppError, RaftNetwork, RaftStorage};
use crate::error::Result;
use crate::raft::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::core::{TargetState, RaftCore, SnapshotState, UpdateCurrentLeader};
use crate::storage::SnapshotWriter;

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> RaftCore<D, R, E, N, S> {
    /// Invoked by leader to send chunks of a snapshot to a follower (§7).
    ///
    /// Leaders always send chunks in order. It is important to note that, according to the Raft spec,
    /// a log may only have one snapshot at any time. As snapshot contents are application specific,
    /// the Raft log will only store a pointer to the snapshot file along with the index & term.
    pub(super) async fn handle_install_snapshot_request(&mut self, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, E> {
        // If message's term is less than most recent term, then we do not honor the request.
        if &req.term < &self.current_term {
            return Ok(InstallSnapshotResponse{term: self.current_term});
        }

        // Update election timeout.
        self.update_next_election_timeout();

        // Update current term if needed.
        if &self.current_term != &req.term {
            self.update_current_term(req.term, None);
            self.save_hard_state().await?;
        }

        // Update current leader if needed.
        if self.current_leader.as_ref() != Some(&req.leader_id) {
            self.update_current_leader(UpdateCurrentLeader::OtherNode(req.leader_id));
        }

        // If not follower, become follower.
        if !self.target_state.is_follower() && !self.target_state.is_non_voter() {
            self.set_target_state(TargetState::Follower);
        }

        // Compare current snapshot state with received RPC and handle as needed.
        match self.snapshot_state.take() {
            None => Ok(self.begin_installing_snapshot(req).await?),
            Some(SnapshotState::Compacting) => todo!("cancel self compaction in favor of streaming snapshot from leader"),
            Some(SnapshotState::Streaming{snapshot, last_offset_start}) => Ok(self.continue_installing_snapshot(req, last_offset_start, snapshot).await?),
        }
    }

    async fn begin_installing_snapshot(&mut self, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, E> {
        // Create a new snapshot and begin writing its contents.
        let mut snapshot = self.storage.install_snapshot(req.last_included_index, req.last_included_term, self.membership.clone()).await
            .map_err(|err| self.map_fatal_storage_result(err))?;
        snapshot.as_mut().write_all(&req.data).await?;

        // If this was a small snapshot, and it is already done, then finish up.
        if req.done {
            self.finalize_snapshot_installation(req, snapshot.as_mut()).await?;
            return Ok(InstallSnapshotResponse{term: self.current_term});
        }

        // Else, retain snapshot components for later segments & respod.
        self.snapshot_state = Some(SnapshotState::Streaming{
            last_offset_start: req.offset,
            snapshot,
        });
        return Ok(InstallSnapshotResponse{term: self.current_term});
    }

    async fn continue_installing_snapshot(
        &mut self, req: InstallSnapshotRequest, mut last_offset_start: u64, mut snapshot: Box<dyn SnapshotWriter>,
    ) -> Result<InstallSnapshotResponse, E> {
        // If this is a re-transmitted old chunk, just respond ok.
        if &req.offset <= &last_offset_start {
            self.snapshot_state = Some(SnapshotState::Streaming{last_offset_start, snapshot});
            return Ok(InstallSnapshotResponse{term: self.current_term});
        }

        // Write the next segment & update offset.
        if let Err(err) = snapshot.as_mut().write_all(&req.data).await {
            self.snapshot_state = Some(SnapshotState::Streaming{last_offset_start, snapshot});
            return Err(err.into());
        }
        last_offset_start = req.offset;

        // If the snapshot stream is done, then finalize.
        if req.done {
            self.finalize_snapshot_installation(req, snapshot.as_mut()).await?;
        } else {
            self.snapshot_state = Some(SnapshotState::Streaming{last_offset_start, snapshot});
        }
        return Ok(InstallSnapshotResponse{term: self.current_term});
    }

    /// Finalize the installation of a new snapshot.
    ///
    /// Any errors which come up from this routine will cause the Raft node to go into shutdown.
    async fn finalize_snapshot_installation(&mut self, req: InstallSnapshotRequest, snapshot: &mut dyn SnapshotWriter) -> Result<(), E> {
        snapshot.shutdown().await.map_err(|err| self.map_fatal_storage_result(err))?;
        let delete_through = if &self.last_log_index > &req.last_included_index {
            Some(req.last_included_index)
        } else {
            None
        };
        self.storage.finalize_install_snapshot(req.last_included_index, req.last_included_term, delete_through).await
            .map_err(|err| self.map_fatal_storage_result(err))?;
        Ok(())
    }
}
