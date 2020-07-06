use tokio::sync::oneshot;

use crate::{AppData, AppDataResponse, AppError, NodeId, RaftNetwork, RaftStorage};
use crate::config::SnapshotPolicy;
use crate::error::{RaftError, Result};
use crate::core::{ConsensusState, LeaderState, TargetState, UpdateCurrentLeader};
use crate::storage::CurrentSnapshotData;

/// An event coming from a replication stream.
pub(crate) enum ReplicaEvent<E: AppError> {
    /// An event representing an update to the replication rate of a replication stream.
    RateUpdate{
        /// The ID of the Raft node to which this event relates.
        target: NodeId,
        /// A flag indicating if the corresponding target node is replicating at line rate.
        ///
        /// When replicating at line rate, the replication stream will receive log entires to
        /// replicate as soon as they are ready. When not running at line rate, the Raft node will
        /// only send over metadata without entries to replicate.
        is_line_rate: bool,
    },
    /// An event from a replication stream which updates the target node's match index.
    UpdateMatchIndex{
        /// The ID of the target node for which the match index is to be updated.
        target: NodeId,
        /// The index of the most recent log known to have been successfully replicated on the target.
        match_index: u64,
    },
    /// An event indicating that the Raft node needs to revert to follower state.
    RevertToFollower{
        /// The ID of the target node from which the new term was observed.
        target: NodeId,
        /// The new term observed.
        term: u64,
    },
    /// An event from a replication stream requesting snapshot info.
    NeedsSnapshot{
        /// The ID of the target node from which the event was sent.
        target: NodeId,
        /// The response channel for delivering the snapshot data.
        tx: oneshot::Sender<Result<CurrentSnapshotData, E>>,
    },
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> LeaderState<'a, D, R, E, N, S> {
    pub(super) async fn handle_replica_event(&mut self, event: ReplicaEvent<E>) -> Result<(), E> {
        match event {
            ReplicaEvent::RateUpdate{target, is_line_rate} => Ok(self.handle_rate_update(target, is_line_rate).await?),
            ReplicaEvent::RevertToFollower{target, term} => Ok(self.handle_revert_to_follower(target, term).await?),
            ReplicaEvent::UpdateMatchIndex{target, match_index} => Ok(self.handle_update_match_index(target, match_index).await?),
            ReplicaEvent::NeedsSnapshot{target, tx} => Ok(self.handle_needs_snapshot(target, tx).await?),
        }
    }

    /// Handle events from replication streams updating their replication rate tracker.
    async fn handle_rate_update(&mut self, target: NodeId, is_line_rate: bool) -> Result<(), E> {
        // Get a handle the target's replication stat & update it as needed.
        let repl_state = match self.nodes.get_mut(&target) {
            Some(repl_state) => repl_state,
            _ => return Ok(()),
        };
        repl_state.is_at_line_rate = is_line_rate;
        // If in joint consensus, and the target node was one of the new nodes, update
        // the joint consensus state to indicate that the target is up-to-date.
        if let ConsensusState::Joint{new_nodes_being_synced, is_committed} = &mut self.consensus_state {
            if let Some((idx, _)) = new_nodes_being_synced.iter().enumerate().find(|(_, e)| e == &&target) {
                new_nodes_being_synced.remove(idx);
            }
            // If there are no remaining nodes to sync, then finalize this joint consensus.
            if self.consensus_state.is_joint_consensus_safe_to_finalize() {
                self.finalize_joint_consensus().await?;
            }
        }

        Ok(())
    }

    /// Handle events from replication streams for when this node needs to revert to follower state.
    async fn handle_revert_to_follower(&mut self, target: NodeId, term: u64) -> Result<(), E> {
        if &term > &self.core.current_term {
            self.core.update_current_term(term, None);
            self.core.save_hard_state().await?;
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            self.core.set_target_state(TargetState::Follower);
        }
        Ok(())
    }

    /// Handle events from a replication stream which updates the target node's match index.
    #[tracing::instrument(skip(self))]
    async fn handle_update_match_index(&mut self, target: NodeId, match_index: u64) -> Result<(), E> {
        // Update target's match index & check if it is awaiting removal.
        let mut needs_removal = false;
        match self.nodes.get_mut(&target) {
            Some(replstate) => {
                replstate.match_index = match_index;
                if let Some(threshold) = &replstate.remove_after_commit {
                    if &match_index >= threshold {
                        needs_removal = true;
                    }
                }
            },
            _ => return Ok(()),
        }

        // Drop replication stream if needed.
        if needs_removal {
            tracing::debug!("node {} is dropping replication stream for target {}", &self.core.id, &target);
            self.nodes.remove(&target);
        }

        // Parse through each targets' match index, and update the value of `commit_index` based
        // on the highest value which has been replicated to a majority of the cluster
        // including the leader which created the entry.
        let mut indices: Vec<_> = self.nodes.values().map(|elem| elem.match_index).collect();
        indices.push(self.core.last_log_index);
        let new_commit_index = calculate_new_commit_index(indices, self.core.commit_index);
        let has_new_commit_index = &new_commit_index > &self.core.commit_index;

        // If a new commit index has been determined, update a few needed elements.
        if has_new_commit_index {
            self.core.commit_index = new_commit_index;

            // Update all replication streams based on new commit index.
            for node in self.nodes.iter() {
                // TODO: impl this.
                // let _ = node.1.addr.do_send(RSUpdateLineCommit(self.commit_index));
            }

            // Check if there are any pending requests which need to be processed.
            let filter = self.awaiting_committed.iter().enumerate()
                .take_while(|(_idx, elem)| &elem.entry.index <= &new_commit_index)
                .last()
                .map(|(idx, _)| idx);
            if let Some(offset) = filter {
                // Build a new ApplyLogsTask from each of the given client requests.
                for request in self.awaiting_committed.drain(..=offset).collect::<Vec<_>>() {
                    self.client_request_post_commit(request).await;
                }
            }
        }
        Ok(())
    }

    /// Handle events from replication streams requesting for snapshot info.
    async fn handle_needs_snapshot(&mut self, target: NodeId, tx: oneshot::Sender<Result<CurrentSnapshotData, E>>) -> Result<(), E> {
        // Ensure snapshotting is configured, else do nothing.
        let threshold = match &self.core.config.snapshot_policy {
            SnapshotPolicy::LogsSinceLast(threshold) => *threshold,
        };

        // Check for existence of current snapshot.
        let current_snapshot_opt = self.core.storage.get_current_snapshot().await
            .map_err(|err| self.core.map_fatal_storage_result(err))?;
        if let Some(snapshot) = current_snapshot_opt {
            // If snapshot exists, ensure its distance from the leader's last log index is <= half
            // of the configured snapshot threshold, else create a new snapshot.
            if snapshot_is_within_half_of_threshold(&snapshot, &self.core.last_log_index, &threshold) {
                let _ = tx.send(Ok(snapshot));
                return Ok(());
            }
        }
        // If snapshot is not within half of threshold, or if snapshot does not exist, create a new snapshot.
        // Create a new snapshot up through the committed index (to avoid jitter).
        let storage = self.core.storage.clone();
        let through_index = self.core.commit_index;
        tokio::spawn(async move {
            let res = storage.create_snapshot(through_index).await;
            let _ = tx.send(res.map_err(RaftError::from));
        });
        Ok(())
    }
}

/// Determine the value for `current_commit` based on all known indicies of the cluster members.
///
/// - `entries`: is a vector of all of the highest known index to be replicated on a target node,
/// one per node of the cluster, including the leader.
/// - `current_commit`: is the Raft node's `current_commit` value before invoking this function.
/// The output of this function will never be less than this value.
///
/// NOTE: there are a few edge cases accounted for in this routine which will never practically
/// be hit, but they are accounted for in the name of good measure.
fn calculate_new_commit_index(mut entries: Vec<u64>, current_commit: u64) -> u64 {
    // Handle cases where len < 2.
    let len = entries.len();
    if len == 0 {
        return current_commit;
    } else if len == 1 {
        let only_elem = entries[0];
        return if only_elem < current_commit { current_commit } else { only_elem };
    };

    // Calculate offset which will give the majority slice of high-end.
    entries.sort();
    let offset = if (len % 2) == 0 { (len/2)-1 } else { len/2 };
    let new_val = entries.get(offset).unwrap_or(&current_commit);
    if new_val < &current_commit {
        current_commit
    } else {
        *new_val
    }
}

/// Check if the given snapshot data is within half of the configured threshold.
fn snapshot_is_within_half_of_threshold(data: &CurrentSnapshotData, last_log_index: &u64, threshold: &u64) -> bool {
    // Calculate distance from actor's last log index.
    let distance_from_line = if &data.index > last_log_index { 0u64 } else { last_log_index - &data.index }; // Guard against underflow.
    let half_of_threshold = threshold / &2;
    distance_from_line <= half_of_threshold
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Unit Tests ////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::MembershipConfig;
    use crate::storage::SnapshotReader;

    impl SnapshotReader for tokio::io::Empty {}

    //////////////////////////////////////////////////////////////////////////
    // snapshot_is_within_half_of_threshold //////////////////////////////////

    mod snapshot_is_within_half_of_threshold {
        use super::*;

        macro_rules! test_snapshot_is_within_half_of_threshold {
            ({test=>$name:ident, data=>$data:expr, last_log_index=>$last_log:expr, threshold=>$thresh:expr, expected=>$exp:literal}) => {
                #[test]
                fn $name() {
                    let res = snapshot_is_within_half_of_threshold($data, $last_log, $thresh);
                    assert_eq!(res, $exp)
                }
            }
        }

        test_snapshot_is_within_half_of_threshold!({
            test=>happy_path_true_when_within_half_threshold,
            data=>&CurrentSnapshotData{
                term: 1, index: 50, membership: MembershipConfig{members: vec![], non_voters: vec![], removing: vec![], is_in_joint_consensus: false},
                snapshot: Box::new(tokio::io::empty()),
            },
            last_log_index=>&100, threshold=>&500, expected=>true
        });

        test_snapshot_is_within_half_of_threshold!({
            test=>happy_path_false_when_above_half_threshold,
            data=>&CurrentSnapshotData{
                term: 1, index: 1, membership: MembershipConfig{members: vec![], non_voters: vec![], removing: vec![], is_in_joint_consensus: false},
                snapshot: Box::new(tokio::io::empty()),
            },
            last_log_index=>&500, threshold=>&100, expected=>false
        });

        test_snapshot_is_within_half_of_threshold!({
            test=>guards_against_underflow,
            data=>&CurrentSnapshotData{
                term: 1, index: 200, membership: MembershipConfig{members: vec![], non_voters: vec![], removing: vec![], is_in_joint_consensus: false},
                snapshot: Box::new(tokio::io::empty()),
            },
            last_log_index=>&100, threshold=>&500, expected=>true
        });
    }

    //////////////////////////////////////////////////////////////////////////
    // calculate_new_commit_index ////////////////////////////////////////////

    mod calculate_new_commit_index {
        use super::*;

        macro_rules! test_calculate_new_commit_index {
            ($name:ident, $expected:literal, $current:literal, $entries:expr) => {
                #[test]
                fn $name() {
                    let mut entries = $entries;
                    let output = calculate_new_commit_index(entries.clone(), $current);
                    entries.sort();
                    assert_eq!(output, $expected, "Sorted values: {:?}", entries);
                }
            }
        }

        test_calculate_new_commit_index!(
            basic_values,
            10, 5, vec![20, 5, 0, 15, 10]
        );

        test_calculate_new_commit_index!(
            len_zero_should_return_current_commit,
            20, 20, vec![]
        );

        test_calculate_new_commit_index!(
            len_one_where_greater_than_current,
            100, 0, vec![100]
        );

        test_calculate_new_commit_index!(
            len_one_where_less_than_current,
            100, 100, vec![50]
        );

        test_calculate_new_commit_index!(
            even_number_of_nodes,
            0, 0, vec![0, 100, 0, 100, 0, 100]
        );

        test_calculate_new_commit_index!(
            majority_wins,
            100, 0, vec![0, 100, 0, 100, 0, 100, 100]
        );
    }
}
