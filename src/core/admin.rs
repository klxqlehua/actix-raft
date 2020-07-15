use futures::future::{Future, FutureExt, TryFutureExt};
use tokio::sync::oneshot;

use crate::{AppData, AppDataResponse, AppError, RaftNetwork, RaftStorage};
use crate::error::{InitWithConfigError, ProposeConfigChangeError, RaftError};
use crate::raft::{ClientRequest, ClientResponse, InitWithConfig, MembershipConfig, ProposeConfigChange};
use crate::core::{ConsensusState, LeaderState, TargetState, UpdateCurrentLeader};
use crate::core::client::ClientRequestEntry;
use crate::metrics::State;
// use crate::replication::ReplicationStream;

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> LeaderState<'a, D, R, E, N, S> {
    /// An admin message handler invoked exclusively for cluster formation.
    ///
    /// This command will work for single-node or multi-node cluster formation. This command
    /// should be called with all discovered nodes which need to be part of cluster.
    ///
    /// This command will be rejected if the node is not at index 0 & in the NonVoter state, as
    /// either of those constraints being false indicates that the cluster is already formed
    /// and in motion.
    ///
    /// This routine will set the given config as the active config, only in memory, and will
    /// start an election.
    ///
    /// All nodes must issue this command at startup, as they will not be able to vote for other
    /// nodes until they appear in their config. This handler will ensure that it is safe to
    /// execute this command.
    ///
    /// Once a node becomes leader and detects that its index is 0, it will commit a new config
    /// entry (instead of the normal blank entry created by new leaders).
    ///
    /// If a race condition takes place where two nodes persist an initial config and start an
    /// election, whichever node becomes leader will end up committing its entries to the cluster.
    pub(super) async fn handle_init_with_config(&mut self, mut req: InitWithConfig) -> Result<(), InitWithConfigError<E>> {
        if self.core.last_log_index != 0 {
            return Err(InitWithConfigError::NotAllowed);
        }

        // Ensure given config is normalized and ready for use in the cluster.
        req = normalize_init_config(req);
        if !req.members.contains(&self.core.id) {
            req.members.push(self.core.id);
        }

        // Build a new membership config from given init data & assign it as the new cluster
        // membership config in memory only.
        self.core.membership = MembershipConfig{is_in_joint_consensus: false, members: req.members, non_voters: vec![], removing: vec![]};

        // Become a candidate and start campaigning for leadership. If this node is the only node
        // in the cluster, then become leader without holding an election.
        if self.core.membership.members.len() == 1 && &self.core.membership.members[0] == &self.core.id {
            self.core.current_term += 1;
            self.core.voted_for = Some(self.core.id);
            self.core.set_target_state(TargetState::Leader);
            self.core.save_hard_state().await?;
        } else {
            self.core.set_target_state(TargetState::Candidate);
        }

        Ok(())
    }

    /// An admin message handler invoked to trigger dynamic cluster configuration changes. See §6.
    pub(super) async fn handle_propose_config_change(
        &mut self, msg: ProposeConfigChange<D, R, E>,
    ) -> Result<impl Future<Output=Result<(), RaftError<E>>> + Send + Sync + 'static, ProposeConfigChangeError<E>> {
        // Normalize the proposed config to ensure everything is valid.
        let mut msg = normalize_proposed_config(msg, &self.core.membership)?;

        // Only allow config updates when currently in a uniform consensus state.
        match &mut self.consensus_state {
            ConsensusState::Joint{..} => return Err(ProposeConfigChangeError::AlreadyInJointConsensus),
            _ => self.consensus_state = ConsensusState::Joint{
                new_nodes_being_synced: msg.add_members.clone(),
                is_committed: false,
            },
        }

        // Update current config.
        self.core.membership.is_in_joint_consensus = true;
        self.core.membership.non_voters.append(&mut msg.add_members);
        self.core.membership.removing.append(&mut msg.remove_members);

        // Spawn new replication streams for new members. Track state as non voters so that they
        // can be updated to be normal members once all non-voters have been brought up-to-date.
        for target in msg.add_members {
            // TODO: impl this
            // // Build the replication stream for the target member.
            // let rs = ReplicationStream::new(
            //     self.id, target, self.current_term, self.config.clone(),
            //     self.last_log_index, self.last_log_term, self.commit_index,
            //     ctx.address(), self.network.clone(), self.storage.clone().recipient::<GetLogEntries<D, E>>(),
            // );
            // let addr = rs.start(); // Start the actor on the same thread.

            // // Retain the addr of the replication stream.
            // let state = ReplicationState{
            //     addr, match_index: self.last_log_index, remove_after_commit: None,
            //     is_at_line_rate: true, // Line rate is always initialize to true.
            // };
            // leader_state.nodes.insert(target, state);
        }

        // For any nodes being removed which are currently non-voters, immediately remove them.
        for node in msg.remove_members {
            if let Some((idx, _)) = self.core.membership.non_voters.iter().enumerate().find(|(_, e)| *e == &node) {
                // TODO: might need to come back to this, as the replication stream interface is currently in flux.
                self.nodes.remove(&node); // Dropping the replication stream's addr will kill it.
                self.core.membership.non_voters.remove(idx);
            }
        }
        self.core.report_metrics(State::Leader);

        // Propagate the command as any other client request.
        let payload = ClientRequest::<D, R, E>::new_config(self.core.membership.clone());
        let (tx_joint, rx_join) = oneshot::channel();
        let entry = self.append_payload_to_log(payload.entry).await?;
        let cr_entry = ClientRequestEntry::from_entry(entry, payload.response_mode, tx_joint);
        self.replicate_client_request(cr_entry).await;

        // Setup channels for eventual response to the 2-phase config change.
        let (tx_cfg_change, rx_cfg_change) = oneshot::channel();
        self.propose_config_change_cb = Some(tx_cfg_change); // Once the entire process is done, this is our response channel.
        self.joint_consensus_cb.push(rx_join); // Receiver for when the joint consensus is committed.

        Ok(rx_cfg_change
            .map_err(|_| RaftError::ShuttingDown)
            .into_future()
            .then(|res| futures::future::ready(match res {
                Ok(ok) => match ok {
                    Ok(ok) => Ok(ok),
                    Err(err) => Err(err),
                },
                Err(err) => Err(err),
            }))
        )
    }

    /// Handle the committment of a joint consensus cluster configuration.
    #[tracing::instrument(skip(self, _res))]
    pub(super) async fn handle_joint_consensus_committed(&mut self, _res: ClientResponse<R>) -> Result<(), RaftError<E>> {
        match &mut self.consensus_state {
            ConsensusState::Joint{is_committed, ..} => {
                *is_committed = true; // Mark as comitted.
            }
            _ => (),
        }
        // Only proceed to finalize this joint consensus if there are no remaining nodes being synced.
        if self.consensus_state.is_joint_consensus_safe_to_finalize() {
            self.finalize_joint_consensus().await?;
        }
        Ok(())
    }

    /// Finalize the comitted joint consensus.
    #[tracing::instrument(skip(self))]
    pub(super) async fn finalize_joint_consensus(&mut self) -> Result<(), RaftError<E>> {
        // Only proceed if it is safe to do so.
        if !self.consensus_state.is_joint_consensus_safe_to_finalize() {
            tracing::warn!("attempted to finalize joint consensus when it was not safe to do so");
            return Ok(());
        }

        // Update current config to prepare for exiting joint consensus.
        for node in self.core.membership.non_voters.drain(..) {
            self.core.membership.members.push(node);
        }
        for node in self.core.membership.removing.drain(..) {
            if let Some((idx, _)) = self.core.membership.members.iter().enumerate().find(|(_, e)| *e == &node) {
                self.core.membership.members.remove(idx);
            }
        }
        self.core.membership.is_in_joint_consensus = false;
        self.consensus_state = ConsensusState::Uniform;

        self.core.report_metrics(State::Leader);

        // Propagate the next command as any other client request.
        let payload = ClientRequest::<D, R, E>::new_config(self.core.membership.clone());
        let (tx_uniform, rx_uniform) = oneshot::channel();
        let entry = self.append_payload_to_log(payload.entry).await?;
        let cr_entry = ClientRequestEntry::from_entry(entry, payload.response_mode, tx_uniform);
        self.replicate_client_request(cr_entry).await;

        // Setup channel for eventual committment of the uniform consensus config.
        self.uniform_consensus_cb.push(rx_uniform); // Receiver for when the uniform consensus is committed.
        Ok(())
    }

    /// Handle the committment of a uniform consensus cluster configuration.
    #[tracing::instrument(skip(self, res))]
    pub(super) async fn handle_uniform_consensus_committed(&mut self, res: ClientResponse<R>) -> Result<(), RaftError<E>> {
        // Step down if needed.
        if !self.core.membership.contains(&self.core.id) {
            tracing::info!("raft node {} is stepping down", self.core.id);
            self.core.set_target_state(TargetState::NonVoter);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            return Ok(());
        }

        // Remove any replication streams which have replicated this config & which are no longer
        // cluster members. All other replication streams which are no longer cluster members, but
        // which have not yet replicated this config will be marked for removal.
        let membership = &self.core.membership;
        let nodes_to_remove: Vec<_> = self.nodes.iter_mut()
            .filter(|(id, _)| !membership.contains(id))
            .filter_map(|(idx, replstate)| {
                if replstate.match_index >= res.index() {
                    Some(idx.clone())
                } else {
                    replstate.remove_after_commit = Some(res.index());
                    None
                }
            }).collect();
        for node in nodes_to_remove {
            self.nodes.remove(&node);
        }

        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Utilities /////////////////////////////////////////////////////////////////////////////////////

// Ensure given config is normalized and ready for use in the cluster.
fn normalize_init_config(msg: InitWithConfig) -> InitWithConfig {
    let mut nodes = vec![];
    for node in msg.members {
        if !nodes.contains(&node) {
            nodes.push(node);
        }
    }

    InitWithConfig{members: nodes}
}

/// Check the proposed config changes with the current config to ensure changes are valid.
///
/// See the documentation on on `ProposeConfigChangeError` for the conditions which will cause
/// errors to be returned.
fn normalize_proposed_config<D: AppData, R: AppDataResponse, E: AppError>(mut msg: ProposeConfigChange<D, R, E>, current: &MembershipConfig) -> Result<ProposeConfigChange<D, R, E>, ProposeConfigChangeError<E>> {
    // Ensure no duplicates in adding new nodes & ensure the new
    // node is not also be requested for removal.
    let mut new_nodes = vec![];
    for node in msg.add_members {
        if !current.contains(&node) && !msg.remove_members.contains(&node) {
            new_nodes.push(node);
        }
    }

    // Ensure targets to remove exist in current config.
    let mut remove_nodes = vec![];
    for node in msg.remove_members {
        if current.contains(&node) && !current.removing.contains(&node) {
            remove_nodes.push(node);
        }
    }

    // Account for noop.
    if (new_nodes.len() == 0) && (remove_nodes.len() == 0) {
        return Err(ProposeConfigChangeError::Noop);
    }

    // Ensure cluster will have at least two nodes.
    let total_removing = current.removing.len() + remove_nodes.len();
    let count = current.members.len() + current.non_voters.len() + new_nodes.len();
    if total_removing >= count {
        return Err(ProposeConfigChangeError::InoperableConfig);
    } else if (count - total_removing) < 2 {
        return Err(ProposeConfigChangeError::InoperableConfig);
    }

    msg.add_members = new_nodes;
    msg.remove_members = remove_nodes;
    Ok(msg)
}
