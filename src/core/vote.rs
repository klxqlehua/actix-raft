use tokio::time::Instant;
use tokio::sync::mpsc;

use crate::{AppData, AppDataResponse, AppError, NodeId, RaftNetwork, RaftStorage};
use crate::error::Result;
use crate::core::{CandidateState, RaftCore, TargetState, UpdateCurrentLeader};
use crate::raft::{VoteRequest, VoteResponse};

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> RaftCore<D, R, E, N, S> {
    /// An RPC invoked by candidates to gather votes (§5.2).
    ///
    /// See `receiver implementation: RequestVote RPC` in raft-essentials.md in this repo.
    #[tracing::instrument(skip(self))]
    pub(super) async fn handle_vote_request(&mut self, msg: VoteRequest) -> Result<VoteResponse, E> {
        // Don't accept vote requests from unknown-cluster members.
        if !self.membership.contains(&msg.candidate_id) {
            return Ok(VoteResponse{term: self.current_term, vote_granted: false, is_candidate_unknown: true});
        }

        // If candidate's current term is less than this nodes current term, reject.
        if &msg.term < &self.current_term {
            return Ok(VoteResponse{term: self.current_term, vote_granted: false, is_candidate_unknown: false});
        }

        // Do not respond to the request if we've received a heartbeat within the election timeout minimum.
        if let Some(inst) = &self.last_heartbeat {
            let now = Instant::now();
            let delta = now.duration_since(*inst);
            if self.config.election_timeout_min >= (delta.as_millis() as u64) {
                return Ok(VoteResponse{term: self.current_term, vote_granted: false, is_candidate_unknown: false});
            }
        }

        // Per spec, if we observe a term greater than our own, we must update
        // term & immediately become follower, we still need to do vote checking after this.
        if &msg.term > &self.current_term {
            self.update_current_term(msg.term, None);
            self.set_target_state(TargetState::Follower);
            self.save_hard_state().await?;
        }

        // Check if candidate's log is at least as up-to-date as this node's.
        // If candidate's log is not at least as up-to-date as this node, then reject.
        let client_is_uptodate = (&msg.last_log_term >= &self.last_log_term) && (&msg.last_log_index >= &self.last_log_index);
        if !client_is_uptodate {
            return Ok(VoteResponse{term: self.current_term, vote_granted: false, is_candidate_unknown: false});
        }

        // Candidate's log is up-to-date so handle voting conditions.
        match &self.voted_for {
            // This node has already voted for the candidate.
            Some(candidate_id) if candidate_id == &msg.candidate_id => {
                Ok(VoteResponse{term: self.current_term, vote_granted: true, is_candidate_unknown: false})
            }
            // This node has already voted for a different candidate.
            Some(_) => Ok(VoteResponse{term: self.current_term, vote_granted: false, is_candidate_unknown: false}),
            // This node has not yet voted for the current term, so vote for the candidate.
            None => {
                self.voted_for = Some(msg.candidate_id);
                self.set_target_state(TargetState::Follower);
                self.save_hard_state().await?;
                Ok(VoteResponse{term: self.current_term, vote_granted: true, is_candidate_unknown: false})
            },
        }
    }
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> CandidateState<'a, D, R, E, N, S> {
    /// Handle response from a vote request sent to a peer.
    #[tracing::instrument(skip(self))]
    pub(super) async fn handle_vote_response(&mut self, res: VoteResponse, target: NodeId) -> Result<(), E> {
        // If responding node sees this node as being unknown to the cluster, and this node has been active,
        // then go into NonVoter state, as this typically means this node is being removed from the cluster.
        if res.is_candidate_unknown && self.core.last_log_index > 0 {
            tracing::trace!({id=self.core.id, target=target}, "target node considers this node to be unknown to the cluster, transitioning to non-voter");
            self.core.set_target_state(TargetState::NonVoter);
            return Ok(());
        }

        // If peer's term is greater than current term, revert to follower state.
        if res.term > self.core.current_term {
            self.core.update_current_term(res.term, None);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            self.core.set_target_state(TargetState::Follower);
            self.core.save_hard_state().await?;
            return Ok(());
        }

        // If peer granted vote, then update campaign state.
        if res.vote_granted {
            self.votes_granted += 1;
            if self.votes_granted >= self.votes_needed {
                // If the campaign was successful, go into leader state.
                self.core.set_target_state(TargetState::Leader);
                return Ok(());
            }
        }

        // Otherwise, we just return and let the candidate loop wait for more votes to come in.
        Ok(())
    }

    /// Build a future of vote requests sent to all peers.
    #[tracing::instrument(skip(self))]
    pub(super) fn spawn_parallel_vote_requests(&self) -> mpsc::Receiver<(VoteResponse, NodeId)> {
        let (tx, rx) = mpsc::channel(self.core.membership.members.len());
        for member in self.core.membership.members.iter().cloned().filter(|member| member != &self.core.id) {
            let rpc = VoteRequest::new(self.core.current_term, self.core.id, self.core.last_log_index, self.core.last_log_term);
            let (network, mut tx_inner) = (self.core.network.clone(), tx.clone());
            let _ = tokio::spawn(async move {
                match network.vote(member, rpc).await {
                    Ok(res) => {
                        let _ = tx_inner.send((res, member)).await;
                    }
                    Err(err) => tracing::error!({error=%err, peer=member}, "error while requesting vote from peer"),
                }
            });
        }
        rx
    }
}
