//! The Raft actor's module and its associated logic.

mod admin;
mod append_entries;
mod apply_entries;
mod client;
mod install_snapshot;
mod replication;
mod vote;

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::prelude::*;
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Duration, delay_until};

use crate::{AppData, AppDataResponse, AppError, RaftNetwork, RaftStorage, NodeId};
use crate::error::{RaftError, Result};
use crate::config::Config;
use crate::raft::{ClientRequest, MembershipConfig, VoteRequest, VoteResponse};
use crate::metrics::{RaftMetrics, State};
use crate::core::client::ClientRequestEntry;
use crate::raft::{TxAppendEntriesResponse, TxVoteResponse, TxInstallSnapshotResponse, TxClientResponse};
use crate::raft::{RxChanAppendEntries, RxChanVote, RxChanInstallSnapshot, RxChanClient};
use crate::raft::ClientResponse;
// use crate::replication::{ReplicationStream, RSTerminate};
use crate::storage::{HardState, InitialState, SnapshotWriter};

//////////////////////////////////////////////////////////////////////////////////////////////////
// RaftCore //////////////////////////////////////////////////////////////////////////////////////

/// The core type implementing the Raft protocol.
///
/// For more information on the Raft protocol, see the specification here:
/// https://raft.github.io/raft.pdf (**pdf warning**).
///
/// The beginning of §5, the spec has a condensed summary of the Raft consensus algorithm. This
/// crate, and especially this actor, attempts to follow the terminology and nomenclature used
/// there as precisely as possible to aid in understanding this system.
///
/// ### api
/// This actor's API is broken up into 3 different layers, all based on message handling. In order
/// to effectively use this actor, only these 3 layers need to be considered.
///
/// #### network
/// The network interface of the parent application is responsible for providing a conduit to
/// exchange the various messages types defined in this system. The `RaftNetwork` trait defines
/// the interface needed for being able to allow Raft cluster members to be able to communicate
/// with each other. In addition to the `RaftNetwork` trait, applications are expected to provide
/// and interface for their clients to be able to submit data which needs to be managed by Raft.
///
/// ##### raft rpc messages
/// These are Raft request PRCs coming from other nodes of the cluster. They are defined in the
/// `messages` module of this crate. They are `AppendEntriesRequest`, `VoteRequest` &
/// `InstallSnapshotRequest`. This actor will use the `RaftNetwork` impl of the parent application
/// to send RPCs to other nodes.
///
/// The application's networking layer must decode these message types and pass them over to the
/// appropriate handler on this type, await the response, and then send the response back over the
/// wire to the caller.
///
/// ##### client request messages
/// These are messages coming from an application's clients, represented by the
/// `messages::ClientPayload` type. When the message type's handler is called a future will be
/// returned which will resolve with the appropriate response type. Only data mutating messages
/// should ever need to go through Raft. The contentsof these messages are entirely specific to
/// your application.
///
/// #### storage
/// The storage interface is typically going to be the most involved as this is where your
/// application really exists. SQL, NoSQL, mutable, immutable, KV, append only ... whatever your
/// application's data model, this is where it comes to life.
///
/// The storage interface is defined by the `RaftStorage` trait. Depending on the data storage
/// system being used, the actor my be sync or async. It just needs to implement handlers for
/// the needed actix message types.
///
/// #### admin
/// These are admin commands which may be issued to a Raft node in order to influence it in ways
/// outside of the normal Raft lifecycle. Dynamic membership changes and cluster initialization
/// are the main commands of this layer.
pub struct RaftCore<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    /// This node's ID.
    id: NodeId,
    /// This node's runtime config.
    config: Arc<Config>,
    /// The cluster's current membership configuration.
    membership: MembershipConfig,
    /// The address of the actor responsible for implementing the `RaftNetwork` interface.
    network: Arc<N>,
    /// The address of the actor responsible for implementing the `RaftStorage` interface.
    storage: Arc<S>,

    /// The target state of the system.
    target_state: TargetState,

    /// The index of the highest log entry known to be committed cluster-wide.
    ///
    /// The definition of a committed log is that the leader which has created the log has
    /// successfully replicated the log to a majority of the cluster. This value is updated via
    /// AppendEntries RPC from the leader, or if a node is the leader, it will update this value
    /// as new entries have been successfully replicated to a majority of the cluster.
    ///
    /// Is initialized to 0, and increases monotonically. This is always based on the leader's
    /// commit index which is communicated to other members via the AppendEntries protocol.
    commit_index: u64,
    /// The index of the highest log entry which has been applied to the local state machine.
    ///
    /// Is initialized to 0, increases monotonically following the `commit_index` as logs are
    /// applied to the state machine (via the storage interface).
    last_applied: u64,
    /// The current term.
    ///
    /// Is initialized to 0 on first boot, and increases monotonically. This is normally based on
    /// the leader's term which is communicated to other members via the AppendEntries protocol,
    /// but this may also be incremented when a follower becomes a candidate.
    current_term: u64,
    /// The ID of the current leader of the Raft cluster.
    ///
    /// This value is kept up-to-date based on a very simple algorithm, which is the only way to
    /// do so reasonably using only the canonical Raft RPCs described in the spec. When a new
    /// leader comes to power, it will send AppendEntries RPCs to establish its leadership. When
    /// such an RPC is observed with a newer term, this value will be updated. This value will be
    /// set to `None` when a newer term is observed in any other way.
    current_leader: Option<NodeId>,
    /// The ID of the candidate which received this node's vote for the current term.
    ///
    /// Each server will vote for at most one candidate in a given term, on a
    /// first-come-first-served basis. See §5.4.1 for additional restriction on votes.
    voted_for: Option<NodeId>,

    /// The index of the last entry to be appended to the log.
    last_log_index: u64,
    /// The term of the last entry to be appended to the log.
    last_log_term: u64,

    /// The node's current snapshot state.
    snapshot_state: Option<SnapshotState>,

    /// The last time a heartbeat was received.
    last_heartbeat: Option<Instant>,
    /// The duration until the next election timeout.
    next_election_timeout: Option<Instant>,

    rx_append_entries: RxChanAppendEntries<D, E>,
    rx_vote: RxChanVote<E>,
    rx_install_snapshot: RxChanInstallSnapshot<E>,
    rx_client: RxChanClient<D, R, E>,
    tx_metrics: watch::Sender<RaftMetrics>,
}

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> RaftCore<D, R, E, N, S> {
    pub fn spawn(
        id: NodeId, config: Config, network: Arc<N>, storage: Arc<S>,
        rx_append_entries: RxChanAppendEntries<D, E>,
        rx_vote: RxChanVote<E>,
        rx_install_snapshot: RxChanInstallSnapshot<E>,
        rx_client: RxChanClient<D, R, E>,
        tx_metrics: watch::Sender<RaftMetrics>,
    ) -> JoinHandle<Result<(), E>> {
        let config = Arc::new(config);
        let membership = MembershipConfig{is_in_joint_consensus: false, members: vec![id], non_voters: vec![], removing: vec![]};
        let this = Self{
            id, config, membership, network, storage,
            target_state: TargetState::Follower,
            commit_index: 0, last_applied: 0, current_term: 0, current_leader: None, voted_for: None,
            last_log_index: 0, last_log_term: 0,
            snapshot_state: None,
            last_heartbeat: None, next_election_timeout: None,
            rx_append_entries, rx_vote, rx_install_snapshot, rx_client, tx_metrics,
        };
        tokio::spawn(this.main())
    }

    /// The main loop of the Raft protocol.
    ///
    /// If this node has configuration present from being online previously, then this node will
    /// begin a standard lifecycle as a follower. If this node is pristine, then it will wait in
    /// standby mode. If this is the only member of a single-node cluster, then this node will
    /// immediately assume the Raft leader role.
    ///
    /// ### previous state | follower
    /// If the node has previous state, then there are a few cases to account for.
    ///
    /// If the node has been offline for some time and was removed from the cluster, no problem.
    /// Any RPCs sent from this node will be rejected until it is added to the cluster. Once it is
    /// added to the cluster again, the standard Raft protocol will resume as normal.
    ///
    /// If the node went down only very briefly, then it should immediately start receiving
    /// heartbeats and resume as normal, else it will start an election if it doesn't receive any
    /// heartbeats from the leader per normal Raft protocol.
    ///
    /// If the node was running standalone, it will win the election and resume as a standalone.
    ///
    /// ### pristine state | standby
    /// While in standby mode, the Raft leader of the current cluster may discover this node and
    /// add it to the cluster. In such a case, it will begin receiving heartbeats from the leader
    /// and business proceeds as usual.
    ///
    /// If there is no current cluster, while in standby mode, the node may receive an admin
    /// command instructing it to campaign with a specific config, or to begin operating as the
    /// leader of a standalone cluster.
    async fn main(mut self) -> Result<(), E> {
        let state = self.storage.get_initial_state().await?;
        self.last_log_index = state.last_log_index;
        self.last_log_term = state.last_log_term;
        self.current_term = state.hard_state.current_term;
        self.voted_for = state.hard_state.voted_for;
        self.membership = state.hard_state.membership;
        self.last_applied = state.last_applied_log;
        // NOTE: this is repeated here for clarity. It is unsafe to initialize the node's commit
        // index to any other value. The commit index must be determined by a leader after
        // successfully committing a new log to the cluster.
        self.commit_index = 0;

        // Set initial state based on state recovered from disk.
        let is_only_configured_member = self.membership.len() == 1 && self.membership.contains(&self.id);
        // If this is the only configured member and there is live state, then this is
        // a single-node cluster. Become leader.
        if is_only_configured_member && &self.last_log_index != &u64::min_value() {
            self.target_state = TargetState::Leader;
        }
        // Else if there are other members, that can only mean that state was recovered. Become follower.
        else if !is_only_configured_member {
            self.target_state = TargetState::Follower;
        }
        // Else, for any other condition, stay non-voter.
        else {
            self.target_state = TargetState::NonVoter;
        }

        // This is central loop of the system. The Raft core assumes a few different roles based
        // on cluster state. The Raft core will delegate control to the different state
        // controllers and simply awaits the delegated loop to return, which will only take place
        // if some error has been encountered, or if a state change is required.
        loop {
            match &self.target_state {
                TargetState::Leader => LeaderState::new(&mut self).run().await?,
                TargetState::Candidate => CandidateState::new(&mut self).run().await?,
                TargetState::Follower => FollowerState::new(&mut self).run().await?,
                TargetState::NonVoter => NonVoterState::new(&mut self).run().await?,
                TargetState::Shutdown => return Ok(()),
            }
        }
    }

    /// Report a metrics payload on the current state of the Raft node.
    fn report_metrics(&mut self, state: State) {
        let res = self.tx_metrics.broadcast(RaftMetrics{
            id: self.id, state, current_term: self.current_term,
            last_log_index: self.last_log_index,
            last_applied: self.last_applied,
            current_leader: self.current_leader,
            membership_config: self.membership.clone(),
        });
        if let Err(err) = res {
            tracing::error!({error=%err}, "error reporting metrics");
        }
    }

    /// Save the Raft node's current hard state to disk.
    async fn save_hard_state(&mut self) -> Result<(), E> {
        let hs = HardState{current_term: self.current_term, voted_for: self.voted_for, membership: self.membership.clone()};
        Ok(self.storage.save_hard_state(&hs).await.map_err(|err| self.map_fatal_storage_result(err))?)
    }

    /// Update core's target state, ensuring all invariants are upheld.
    fn set_target_state(&mut self, state: TargetState) {
        if &state == &TargetState::Follower && self.membership.non_voters.contains(&self.id) {
            self.target_state = TargetState::NonVoter;
        }
        self.target_state = state;
    }

    /// Get the next election timeout, generating a new value if not set.
    fn get_next_election_timeout(&mut self) -> Instant {
        match self.next_election_timeout {
            Some(inst) => inst.clone(),
            None => {
                let inst = Instant::now() + Duration::from_millis(self.config.new_rand_election_timeout());
                self.next_election_timeout = Some(inst.clone());
                inst
            }
        }
    }

    /// Set a value for the next election timeout.
    fn update_next_election_timeout(&mut self) {
        self.next_election_timeout = Some(Instant::now() + Duration::from_millis(self.config.new_rand_election_timeout()));
    }

    /// Update the value of the `current_leader` property.
    ///
    /// NOTE WELL: there was previously a bit of log encapsulated here related to forwarding
    /// requests to leaders and such. In order to more closely mirror the Raft spec and allow apps
    /// to determine how they want to handle forwarding client requests to leaders, that logic was
    /// removed and this handler has thus been greatly simplified. We are keeping it as is in case
    /// we need to add some additional logic here.
    fn update_current_leader(&mut self, update: UpdateCurrentLeader) {
        match update {
            UpdateCurrentLeader::ThisNode => {
                self.current_leader = Some(self.id);
            }
            UpdateCurrentLeader::OtherNode(target) => {
                self.current_leader = Some(target);
            }
            UpdateCurrentLeader::Unknown => {
                self.current_leader = None;
            },
        }
    }

    /// Encapsulate the process of updating the current term, as updating the `voted_for` state must also be updated.
    fn update_current_term(&mut self, new_term: u64, voted_for: Option<NodeId>) {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = voted_for;
        }
    }

    /// Trigger the shutdown sequence due to a non-recoverable error from the storage layer.
    ///
    /// This method assumes that a storage error observed here is non-recoverable. As such, the
    /// Raft node will be instructed to stop. If such behavior is not needed, then don't use this
    /// interface.
    fn map_fatal_storage_result<Err: std::error::Error>(&mut self, err: Err) -> Err {
        tracing::error!({error=%err}, "fatal storage error, shutting down");
        self.set_target_state(TargetState::Shutdown);
        err
    }

    /// Update the node's current membership config & save hard state.
    ///
    /// NOTE WELL: if a leader is stepping down, it should not call this method, as it will cause
    /// the node to transition out of leader state before it can commit the config entry.
    ///
    /// TODO: do some investigation to see if there is a better pattern so that we can get rid of this docs warning.
    async fn update_membership(&mut self, cfg: MembershipConfig) -> Result<(), E> {
        // If the given config does not contain this node's ID, it means one of the following:
        //
        // - the node is currently a non-voter and is replicating an old config to which it has
        // not yet been added.
        // - the node has been removed from the cluster. The parent application can observe the
        // transition to the non-voter state as a signal for when it is safe to shutdown a node
        // being removed.
        self.membership = cfg;
        if !self.membership.contains(&self.id) {
            self.set_target_state(TargetState::NonVoter);
        } else if &self.target_state == &TargetState::NonVoter && self.membership.members.contains(&self.id) {
            // The node is a NonVoter and the new config has it configured as a normal member.
            // Transition to follower.
            self.set_target_state(TargetState::Follower);
        }
        Ok(self.save_hard_state().await?)
    }
}

/// An enum describing the way the current leader property is to be updated.
pub(crate) enum UpdateCurrentLeader {
    Unknown,
    OtherNode(NodeId),
    ThisNode,
}

/// The current snapshot state of the Raft node.
pub(crate) enum SnapshotState {
    /// The Raft node is compacting itself.
    Compacting,
    /// The Raft node is streaming in a snapshot from the leader.
    Streaming {
        /// The start of the last offset written to the snapshot.
        last_offset_start: u64,
        /// A handle to the snapshot writer.
        snapshot: Box<dyn SnapshotWriter>,
    }
}

// /// The Raft node is streaming in a snapshot from the leader.
// pub(crate) struct SnapshotStateStreaming {
//     /// The start of the last offset written to the snapshot.
//     pub last_offset_start: u64,
//     /// A handle to the snapshot writer.
//     pub snapshot: Box<dyn SnapshotWriter>,
// }

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// The desired target state of a Raft node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetState {
    NonVoter,
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

impl TargetState {
    /// Check if currently in non-voter state.
    pub fn is_non_voter(&self) -> bool {
        if let Self::NonVoter = self { true } else { false }
    }

    /// Check if currently in follower state.
    pub fn is_follower(&self) -> bool {
        if let Self::Follower = self { true } else { false }
    }

    /// Check if currently in candidate state.
    pub fn is_candidate(&self) -> bool {
        if let Self::Candidate = self { true } else { false }
    }

    /// Check if currently in leader state.
    pub fn is_leader(&self) -> bool {
        if let Self::Leader = self { true } else { false }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to the Raft leader.
///
/// This state is reinitialized after an election.
struct LeaderState<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    pub core: &'a mut RaftCore<D, R, E, N, S>,
    /// A mapping of node IDs the replication state of the target node.
    pub nodes: BTreeMap<NodeId, ReplicationState<D, R, E, N, S>>,
    /// A buffer of client requests which have been appended locally and are awaiting to be committed to the cluster.
    pub awaiting_committed: Vec<ClientRequestEntry<D, R, E>>,
    /// A field tracking the cluster's current consensus state, which is used for dynamic membership.
    pub consensus_state: ConsensusState,
    /// An optional response channel for when a config change has been proposed, and is awaiting a response.
    pub propose_config_change_cb: Option<oneshot::Sender<Result<(), E>>>,
    /// An optional receiver for when a joint consensus config is committed.
    pub joint_consensus_cb: FuturesOrdered<oneshot::Receiver<Result<ClientResponse<R>, E>>>,
    /// An optional receiver for when a uniform consensus config is committed.
    pub uniform_consensus_cb: FuturesOrdered<oneshot::Receiver<Result<ClientResponse<R>, E>>>,
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> LeaderState<'a, D, R, E, N, S> {
    /// Create a new instance.
    pub(self) fn new(core: &'a mut RaftCore<D, R, E, N, S>) -> Self {
        let consensus_state = if core.membership.is_in_joint_consensus {
            ConsensusState::Joint{
                new_nodes_being_synced: core.membership.non_voters.clone(),
                is_committed: false,
            }
        } else {
            ConsensusState::Uniform
        };
        Self{
            core, nodes: BTreeMap::new(), awaiting_committed: Vec::new(), consensus_state,
            propose_config_change_cb: None, joint_consensus_cb: FuturesOrdered::new(), uniform_consensus_cb: FuturesOrdered::new(),
        }
    }

    /// Transition to the Raft leader state.
    #[tracing::instrument(skip(self))]
    pub(self) async fn run(mut self) -> Result<(), E> {
        // Spawn replication streams.
        let targets = self.core.membership.members.iter()
            .filter(|elem| *elem != &self.core.id)
            .chain(self.core.membership.non_voters.iter());
        for target in targets {
            // TODO: build this out.
            // // Build & spawn a replication stream for the target member.
            // let replstream = ReplicationStream::new(
            //     self.core.id, *target, self.core.current_term, self.core.config.clone(),
            //     self.core.last_log_index, self.core.last_log_term, self.core.commit_index,
            //     self.core.network.clone(), self.core.storage.clone(),
            // ).spawn();

            // // Retain the addr of the replication stream.
            // let state = ReplicationState{match_index: self.core.last_log_index, is_at_line_rate: true, replstream, remove_after_commit: None};
            // self.nodes.insert(*target, state);
        }

        // Setup state as leader.
        self.core.last_heartbeat = None;
        self.core.next_election_timeout = None;
        self.core.update_current_leader(UpdateCurrentLeader::ThisNode);
        self.core.report_metrics(State::Leader);

        // Per §8, commit an initial entry as part of becoming the cluster leader.
        self.commit_initial_leader_entry().await?;

        loop {
            if !self.core.target_state.is_leader() {
                return Ok(());
            }
            tokio::select!{
                Some((rpc, tx)) = self.core.rx_append_entries.recv() => {
                    let res = self.core.handle_append_entries_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_vote.recv() => {
                    let res = self.core.handle_vote_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_install_snapshot.recv() => {
                    let res = self.core.handle_install_snapshot_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_client.recv() => self.handle_client_request(rpc, tx).await,
                Some(Ok(res)) = self.joint_consensus_cb.next() => {
                    match res {
                        Ok(clientres) => self.handle_joint_consensus_committed(clientres).await?,
                        Err(err) => if let Some(cb) = self.propose_config_change_cb.take() {
                            let _ = cb.send(Err(err));
                        }
                    }
                }
                Some(Ok(res)) = self.uniform_consensus_cb.next() => {
                    match res {
                        Ok(clientres) => {
                            let final_res = self.handle_uniform_consensus_committed(clientres).await;
                            if let Some(cb) = self.propose_config_change_cb.take() {
                                let _ = cb.send(final_res);
                            }
                        }
                        Err(err) => if let Some(cb) = self.propose_config_change_cb.take() {
                            let _ = cb.send(Err(err));
                        }
                    }
                }
                // TODO: select over data channels coming from the replication streams.
            }
        }
    }
}

/// A struct tracking the state of a replication stream from the perspective of the Raft actor.
pub struct ReplicationState<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    pub match_index: u64,
    pub is_at_line_rate: bool,
    pub remove_after_commit: Option<u64>,
    // pub addr: ReplicationStream<D, R, E, N, S>,
    m0: std::marker::PhantomData<D>,
    m1: std::marker::PhantomData<R>,
    m2: std::marker::PhantomData<E>,
    m3: std::marker::PhantomData<N>,
    m4: std::marker::PhantomData<S>,
}

pub enum ConsensusState {
    /// The cluster consensus is uniform; not in a joint consensus state.
    Uniform,
    /// The cluster is in a joint consensus state and is syncing new nodes.
    Joint {
        /// The new nodes which are being synced.
        new_nodes_being_synced: Vec<NodeId>,
        /// A bool indicating if the associated config which started this join consensus has yet been comitted.
        ///
        /// NOTE: when a new leader is elected, it will initialize this value to false, and then
        /// update this value to true once the new leader's blank payload has been committed.
        is_committed: bool,
    }
}

impl ConsensusState {
    /// Check the current state to determine if it is in joint consensus, and if it is safe to finalize the joint consensus.
    ///
    /// The return value is based on:
    /// 1. if this object currently represents a joint consensus state.
    /// 2. and if the corresponding config for this consensus state has been committed to the cluster.
    /// 3. and if any new nodes being added to the cluster have been synced.
    pub fn is_joint_consensus_safe_to_finalize(&self) -> bool {
        match self {
            ConsensusState::Joint{is_committed, new_nodes_being_synced}
                if *is_committed && new_nodes_being_synced.len() == 0 => true,
            _ => false,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a Raft node in candidate state.
struct CandidateState<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    core: &'a mut RaftCore<D, R, E, N, S>,
    /// The number of votes which have been granted by peer nodes.
    votes_granted: u64,
    /// The number of votes needed in order to become the Raft leader.
    votes_needed: u64,
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> CandidateState<'a, D, R, E, N, S> {
    pub(self) fn new(core: &'a mut RaftCore<D, R, E, N, S>) -> Self {
        Self{core, votes_granted: 0, votes_needed: 0}
    }

    /// Run the candidate loop.
    #[tracing::instrument(skip(self))]
    pub(self) async fn run(mut self) -> Result<(), E> {
        // Each iteration of the outer loop represents a new term.
        loop {
            // Setup initial state per term.
            self.votes_granted = 1; // We must vote for ourselves per the Raft spec.
            self.votes_needed = ((self.core.membership.members.len() / 2) + 1) as u64; // Just need a majority.

            // Setup new term.
            self.core.update_next_election_timeout(); // Generates a new rand value within range.
            self.core.current_term += 1;
            self.core.voted_for = Some(self.core.id);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            self.core.save_hard_state().await?;
            self.core.report_metrics(State::Candidate);

            // Send RPCs to all members in parallel.
            let mut pending_votes = self.spawn_parallel_vote_requests();

            // Inner processing loop for this Raft state.
            loop {
                if !self.core.target_state.is_candidate() {
                    return Ok(());
                }

                let mut timeout_fut = delay_until(self.core.get_next_election_timeout());
                tokio::select!{
                    Some((rpc, tx)) = self.core.rx_append_entries.recv() => {
                        let res = self.core.handle_append_entries_request(rpc).await;
                        let _ = tx.send(res);
                    }
                    Some((rpc, tx)) = self.core.rx_vote.recv() => {
                        let res = self.core.handle_vote_request(rpc).await;
                        let _ = tx.send(res);
                    }
                    Some((rpc, tx)) = self.core.rx_install_snapshot.recv() => {
                        let res = self.core.handle_install_snapshot_request(rpc).await;
                        let _ = tx.send(res);
                    }
                    _ = &mut timeout_fut => break, // This election has timed-out. Break to outer loop, which starts a new term.
                    // Some((rpc, tx)) = self.core.rx_client.next() => self.core.forward_client_request(rpc, tx).await, // TODO: impl this.
                    Some((res, peer)) = pending_votes.recv() => self.handle_vote_response(res, peer).await?,
                }
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a Raft node in follower state.
pub struct FollowerState<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    core: &'a mut RaftCore<D, R, E, N, S>,
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> FollowerState<'a, D, R, E, N, S> {
    pub(self) fn new(core: &'a mut RaftCore<D, R, E, N, S>) -> Self {
        Self{core}
    }

    /// Run the follower loop.
    #[tracing::instrument(skip(self))]
    pub(self) async fn run(self) -> Result<(), E> {
        self.core.report_metrics(State::Follower);
        loop {
            if !self.core.target_state.is_follower() {
                return Ok(());
            }

            let mut election_timeout = delay_until(self.core.get_next_election_timeout()); // Value is updated as heartbeats are received.
            tokio::select!{
                // If an election timeout is hit, then we need to transition to candidate.
                _ = &mut election_timeout => self.core.set_target_state(TargetState::Candidate),
                Some((rpc, tx)) = self.core.rx_append_entries.recv() => {
                    let res = self.core.handle_append_entries_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_vote.recv() => {
                    let res = self.core.handle_vote_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_install_snapshot.recv() => {
                    let res = self.core.handle_install_snapshot_request(rpc).await;
                    let _ = tx.send(res);
                }
                // (rpc, tx) = self.core.rx_client.next() => self.core.forward_client_request(rpc, tx).await, // TODO: impl this.
                Some((rpc, tx)) = self.core.rx_client.recv() => todo!(), // TODO: impl this.
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a Raft node in non-voter state.
pub struct NonVoterState<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    core: &'a mut RaftCore<D, R, E, N, S>,
}

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> NonVoterState<'a, D, R, E, N, S> {
    pub(self) fn new(core: &'a mut RaftCore<D, R, E, N, S>) -> Self {
        Self{core}
    }

    /// Run the non-voter loop.
    #[tracing::instrument(skip(self))]
    pub(self) async fn run(self) -> Result<(), E> {
        self.core.report_metrics(State::NonVoter);
        loop {
            if !self.core.target_state.is_non_voter() {
                return Ok(());
            }
            tokio::select!{
                Some((rpc, tx)) = self.core.rx_append_entries.recv() => {
                    let res = self.core.handle_append_entries_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_vote.recv() => {
                    let res = self.core.handle_vote_request(rpc).await;
                    let _ = tx.send(res);
                }
                Some((rpc, tx)) = self.core.rx_install_snapshot.recv() => {
                    let res = self.core.handle_install_snapshot_request(rpc).await;
                    let _ = tx.send(res);
                }
                // (rpc, tx) = self.core.rx_client.next() => self.core.forward_client_request(rpc, tx).await, // TODO: impl this.
                Some((rpc, tx)) = self.core.rx_client.recv() => todo!(), // TODO: impl this.
            }
        }
    }
}
