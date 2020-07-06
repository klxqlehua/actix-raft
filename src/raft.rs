use std::sync::Arc;

use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::{AppData, AppDataResponse, AppError, NodeId, RaftNetwork, RaftStorage};
use crate::config::Config;
use crate::error::{RaftError, Result};
use crate::metrics::RaftMetrics;
use crate::core::RaftCore;

pub(crate) type TxAppendEntriesResponse<E> = oneshot::Sender<Result<AppendEntriesResponse, E>>;
pub(crate) type TxVoteResponse<E> = oneshot::Sender<Result<VoteResponse, E>>;
pub(crate) type TxInstallSnapshotResponse<E> = oneshot::Sender<Result<InstallSnapshotResponse, E>>;
pub(crate) type TxClientResponse<R, E> = oneshot::Sender<Result<ClientResponse<R>, E>>;

pub(crate) type RxChanAppendEntries<D, E> = mpsc::UnboundedReceiver<(AppendEntriesRequest<D>, TxAppendEntriesResponse<E>)>;
pub(crate) type RxChanVote<E> = mpsc::UnboundedReceiver<(VoteRequest, TxVoteResponse<E>)>;
pub(crate) type RxChanInstallSnapshot<E> = mpsc::UnboundedReceiver<(InstallSnapshotRequest, TxInstallSnapshotResponse<E>)>;
pub(crate) type RxChanClient<D, R, E> = mpsc::UnboundedReceiver<(ClientRequest<D, R, E>, TxClientResponse<R, E>)>;

pub(crate) type TxChanAppendEntries<D, E> = mpsc::UnboundedSender<(AppendEntriesRequest<D>, TxAppendEntriesResponse<E>)>;
pub(crate) type TxChanVote<E> = mpsc::UnboundedSender<(VoteRequest, TxVoteResponse<E>)>;
pub(crate) type TxChanInstallSnapshot<E> = mpsc::UnboundedSender<(InstallSnapshotRequest, TxInstallSnapshotResponse<E>)>;
pub(crate) type TxChanClient<D, R, E> = mpsc::UnboundedSender<(ClientRequest<D, R, E>, TxClientResponse<R, E>)>;

/// TODO: docs
pub struct Raft<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    tx_append_entries: TxChanAppendEntries<D, E>,
    tx_vote: TxChanVote<E>,
    tx_install_snapshot: TxChanInstallSnapshot<E>,
    tx_client: TxChanClient<D, R, E>,
    rx_metrics: watch::Receiver<RaftMetrics>,
    raft_handle: JoinHandle<Result<(), E>>,
    _marker_n: std::marker::PhantomData<N>,
    _marker_s: std::marker::PhantomData<S>,
}

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> Raft<D, R, E, N, S> {
    /// Spawn a new Raft instance.
    pub async fn spawn(id: NodeId, config: Config, network: Arc<N>, storage: Arc<S>) -> Self {
        let (tx_append_entries, rx_append_entries) = mpsc::unbounded_channel();
        let (tx_vote, rx_vote) = mpsc::unbounded_channel();
        let (tx_install_snapshot, rx_install_snapshot) = mpsc::unbounded_channel();
        let (tx_client, rx_client) = mpsc::unbounded_channel();
        let (tx_metrics, rx_metrics) = watch::channel(RaftMetrics::initial(id, MembershipConfig::new_initial(id)));
        let raft_handle = RaftCore::spawn(
            id, config, network, storage,
            rx_append_entries, rx_vote, rx_install_snapshot, rx_client, tx_metrics,
        );
        Self{
            tx_append_entries, tx_vote, tx_install_snapshot, tx_client, rx_metrics, raft_handle,
            _marker_n: std::marker::PhantomData, _marker_s: std::marker::PhantomData,
        }
    }

    /// An RPC invoked by the leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
    pub async fn append_entries(&self, msg: AppendEntriesRequest<D>) -> Result<AppendEntriesResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_append_entries.send((msg, tx)).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// An RPC invoked by candidates to gather votes (§5.2).
    pub async fn vote(&self, msg: VoteRequest) -> Result<VoteResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_vote.send((msg, tx)).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// An RPC invoked by the Raft leader to send chunks of a snapshot to a follower (§7).
    pub async fn install_snapshot(&self, msg: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_install_snapshot.send((msg, tx)).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// An RPC invoked by an external client to update the state of the system (§5.1).
    pub async fn client(&self, msg: ClientRequest<D, R, E>) -> Result<ClientResponse<R>, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_client.send((msg, tx)).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// Get a handle to the metrics channel.
    pub fn metrics(&self) -> watch::Receiver<RaftMetrics> {
        self.rx_metrics.clone()
    }

    /// Get the Raft core JoinHandle, consuming this interface.
    pub fn core_handle(self) -> tokio::task::JoinHandle<Result<(), E>> {
        self.raft_handle
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC invoked by the leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesRequest<D: AppData> {
    /// The leader's current term.
    pub term: u64,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: u64,
    /// The index of the log entry immediately preceding the new entries.
    pub prev_log_index: u64,
    /// The term of the `prev_log_index` entry.
    pub prev_log_term: u64,
    /// The new log entries to store.
    ///
    /// This may be empty when the leader is sending heartbeats. Entries
    /// may be batched for efficiency.
    #[serde(bound="D: AppData")]
    pub entries: Vec<Entry<D>>,
    /// The leader's commit index.
    pub leader_commit: u64,
}

/// An RPC response to an `AppendEntriesRequest` message.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The responding node's current term, for leader to update itself.
    pub term: u64,
    /// Will be true if follower contained entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,
    /// A value used to implement the _conflicting term_ optimization outlined in §5.3.
    ///
    /// This value will only be present, and should only be considered, when `success` is `false`.
    pub conflict_opt: Option<ConflictOpt>,
}

/// A struct used to implement the _conflicting term_ optimization outlined in §5.3 for log replication.
///
/// This value will only be present, and should only be considered, when an `AppendEntriesResponse`
/// object has a `success` value of `false`.
///
/// This implementation of Raft uses this value to more quickly synchronize a leader with its
/// followers which may be some distance behind in replication, may have conflicting entries, or
/// which may be new to the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictOpt {
    /// The term of the most recent entry which does not conflict with the received request.
    pub term: u64,
    /// The index of the most recent entry which does not conflict with the received request.
    pub index: u64,
}

/// A Raft log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry<D: AppData> {
    /// This entry's term.
    pub term: u64,
    /// This entry's index.
    pub index: u64,
    /// This entry's payload.
    #[serde(bound="D: AppData")]
    pub payload: EntryPayload<D>,
}

impl<D: AppData> Entry<D> {
    /// Create a new snapshot pointer from the given data.
    pub fn new_snapshot_pointer(pointer: EntrySnapshotPointer, index: u64, term: u64) -> Self {
        Entry{term, index, payload: EntryPayload::SnapshotPointer(pointer)}
    }
}

/// Log entry payload variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload<D: AppData> {
    /// An empty payload committed by a new cluster leader.
    Blank,
    /// A normal log entry.
    #[serde(bound="D: AppData")]
    Normal(EntryNormal<D>),
    /// A config change log entry.
    ConfigChange(EntryConfigChange),
    /// An entry which points to a snapshot.
    SnapshotPointer(EntrySnapshotPointer),
}

/// A normal log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryNormal<D: AppData> {
    /// The contents of this entry.
    #[serde(bound="D: AppData")]
    pub data: D,
}

/// A log entry holding a config change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryConfigChange {
    /// The full list of node IDs to be considered cluster members as part of this config change.
    pub membership: MembershipConfig,
}

/// A log entry pointing to a snapshot.
///
/// This will only be present when read from storage. An entry of this type will never be
/// transmitted from a leader during replication, an `InstallSnapshotRequest`
/// RPC will be sent instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntrySnapshotPointer {
    /// The ID of the snapshot, which is application specific, and probably only meaningful to the storage layer.
    pub id: String,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// A model of the membership configuration of the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// A flag indicating if the system is currently in a joint consensus state.
    pub is_in_joint_consensus: bool,
    /// Voting members of the Raft cluster.
    pub members: Vec<NodeId>,
    /// Non-voting members of the cluster.
    ///
    /// These nodes are being brought up-to-speed by the leader and will be transitioned over to
    /// being standard members once they are up-to-date.
    pub non_voters: Vec<NodeId>,
    /// The set of nodes which are to be removed after joint consensus is complete.
    pub removing: Vec<NodeId>,
}

impl MembershipConfig {
    /// Create a new initial config containing only the given node ID.
    pub(crate) fn new_initial(id: NodeId) -> Self {
        Self{is_in_joint_consensus: false, members: vec![id], non_voters: vec![], removing: vec![]}
    }

    /// Check if the given NodeId exists in this membership config.
    ///
    /// This checks only the contents of `members` & `non_voters`.
    pub fn contains(&self, x: &NodeId) -> bool {
        self.members.contains(x) || self.non_voters.contains(x)
    }

    /// Get an iterator over all nodes in the current config.
    pub fn all_nodes(&self) -> impl Iterator<Item=&NodeId> {
        self.members.iter().chain(self.non_voters.iter())
    }

    /// Get the length of the members & non_voters vectors.
    pub fn len(&self) -> usize {
        self.members.len() + self.non_voters.len()
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC invoked by candidates to gather votes (§5.2).
#[derive(Debug, Serialize, Deserialize)]
pub struct VoteRequest {
    /// The candidate's current term.
    pub term: u64,
    /// The candidate's ID.
    pub candidate_id: u64,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: u64,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: u64,
}

impl VoteRequest {
    /// Create a new instance.
    pub fn new(term: u64, candidate_id: u64, last_log_index: u64, last_log_term: u64) -> Self {
        Self{term, candidate_id, last_log_index, last_log_term}
    }
}

/// An RPC response to an `VoteResponse` message.
#[derive(Debug, Serialize, Deserialize)]
pub struct VoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: u64,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
    /// Will be true if the candidate is unknown to the responding node's config.
    ///
    /// If this field is true, and the sender's (the candidate's) index is greater than 0, then it
    /// should revert to the NonVoter state; if the sender's index is 0, then resume campaigning.
    pub is_candidate_unknown: bool,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC invoked by the Raft leader to send chunks of a snapshot to a follower (§7).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// The leader's current term.
    pub term: u64,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: u64,
    /// The snapshot replaces all log entries up through and including this index.
    pub last_included_index: u64,
    /// The term of the `last_included_index`.
    pub last_included_term: u64,
    /// The byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// The raw Vec<u8> of the snapshot chunk, starting at `offset`.
    pub data: Vec<u8>,
    /// Will be `true` if this is the last chunk in the snapshot.
    pub done: bool,
}

/// An RPC response to an `InstallSnapshotResponse` message.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// The receiving node's current term, for leader to update itself.
    pub term: u64,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC invoked by some external client to update the state of the system (§5.1).
///
/// The entries of this payload will be appended to the Raft log and then applied to the Raft state
/// machine according to the Raft protocol.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRequest<D: AppData, R: AppDataResponse, E: AppError> {
    /// The application specific contents of this client request.
    #[serde(bound="D: AppData")]
    pub(crate) entry: EntryPayload<D>,
    /// The response mode needed by this request.
    pub(crate) response_mode: ResponseMode,
    #[serde(skip)]
    marker0: std::marker::PhantomData<R>,
    #[serde(skip)]
    marker1: std::marker::PhantomData<E>,
}

impl<D: AppData, R: AppDataResponse, E: AppError> ClientRequest<D, R, E> {
    /// Create a new client payload instance with a normal entry type.
    pub fn new(entry: EntryNormal<D>, response_mode: ResponseMode) -> Self {
        Self::new_base(EntryPayload::Normal(entry), response_mode)
    }

    /// Create a new instance.
    pub(crate) fn new_base(entry: EntryPayload<D>, response_mode: ResponseMode) -> Self {
        Self{entry, response_mode, marker0: std::marker::PhantomData, marker1: std::marker::PhantomData}
    }

    /// Generate a new payload holding a config change.
    pub(crate) fn new_config(membership: MembershipConfig) -> Self {
        Self::new_base(EntryPayload::ConfigChange(EntryConfigChange{membership}), ResponseMode::Committed)
    }

    /// Generate a new blank payload.
    ///
    /// This is used by new leaders when first coming to power.
    pub(crate) fn new_blank_payload() -> Self {
        Self::new_base(EntryPayload::Blank, ResponseMode::Committed)
    }
}

/// The desired response mode for a client request.
///
/// This value specifies when a client request desires to receive its response from Raft. When
/// `Comitted` is chosen, the client request will receive a response after the request has been
/// successfully replicated to at least half of the nodes in the cluster. This is what the Raft
/// protocol refers to as being comitted.
///
/// When `Applied` is chosen, the client request will receive a response after the request has
/// been successfully committed and successfully applied to the state machine.
///
/// The choice between these two options depends on the requirements related to the request. If
/// the data of the client request payload will need to be read immediately after the response is
/// received, then `Applied` must be used. If there is no requirement that the data must be
/// immediately read after receiving a response, then `Committed` may be used to speed up
/// response times.
#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMode {
    /// A response will be returned after the request has been committed to the cluster.
    Committed,
    /// A response will be returned after the request has been applied to the leader's state machine.
    Applied,
}

/// A response to a client payload proposed to the Raft system.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientResponse<R: AppDataResponse> {
    /// A client response issued just after the request was committed to the cluster.
    Committed {
        /// The log index of the successfully processed client request.
        index: u64,
    },
    Applied {
        /// The log index of the successfully processed client request.
        index: u64,
        /// Application specific response data.
        #[serde(bound="R: AppDataResponse")]
        data: R,
    },
}

impl<R: AppDataResponse> ClientResponse<R> {
    /// The index of the log entry corresponding to this response object.
    pub fn index(&self) -> u64 {
        match self {
            Self::Committed{index} => *index,
            Self::Applied{index, ..} => *index,
        }
    }

    /// The response data payload, if this is an `Applied` client response.
    pub fn data(&self) -> Option<&R> {
        match &self {
            Self::Committed{..} => None,
            Self::Applied{data, ..} => Some(data),
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// Initialize a pristine Raft node with the given config & start a campaign to become leader.
pub struct InitWithConfig {
    /// All currently known members to initialize the new cluster with.
    ///
    /// If the ID of the node this command is being submitted to is not present it will be added.
    /// If there are duplicates, they will be filtered out to ensure config is proper.
    pub members: Vec<NodeId>,
}

impl InitWithConfig {
    /// Construct a new instance.
    pub fn new(members: Vec<NodeId>) -> Self {
        Self{members}
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// Propose a new membership config change to a running cluster.
///
/// There are a few invariants which must be upheld here:
///
/// - if the node this command is sent to is not the leader of the cluster, it will be rejected.
/// - if the given changes would leave the cluster in an inoperable state, it will be rejected.
pub struct ProposeConfigChange<D: AppData, R: AppDataResponse, E: AppError> {
    /// New members to be added to the cluster.
    pub(crate) add_members: Vec<NodeId>,
    /// Members to be removed from the cluster.
    pub(crate) remove_members: Vec<NodeId>,
    marker_data: std::marker::PhantomData<D>,
    marker_res: std::marker::PhantomData<R>,
    marker_error: std::marker::PhantomData<E>,
}

impl<D: AppData, R: AppDataResponse, E: AppError> ProposeConfigChange<D, R, E> {
    /// Create a new instance.
    ///
    /// If there are duplicates in either of the givenn vectors, they will be filtered out to
    /// ensure config is proper.
    pub fn new(add_members: Vec<NodeId>, remove_members: Vec<NodeId>) -> Self {
        Self{
            add_members, remove_members,
            marker_data: std::marker::PhantomData,
            marker_res: std::marker::PhantomData,
            marker_error: std::marker::PhantomData,
        }
    }
}
