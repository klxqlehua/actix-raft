//! The Raft storage interface and data types.

use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{AppData, AppDataResponse, AppError, NodeId};
use crate::raft::{Entry, MembershipConfig};

/// A trait representing an `AsyncWrite` handle to a snapshot.
pub trait SnapshotWriter: AsyncWrite + Send + Sync + Unpin + 'static {}
/// A trait representing an `AsyncRead` handle to a snapshot.
pub trait SnapshotReader: AsyncRead + Send + Sync + Unpin + 'static {}

/// The data associated with the current snapshot.
pub struct CurrentSnapshotData {
    /// The snapshot entry's term.
    pub term: u64,
    /// The snapshot entry's index.
    pub index: u64,
    /// The latest membership configuration covered by the snapshot.
    pub membership: MembershipConfig,
    /// The snapshot entry's pointer to the snapshot file.
    pub snapshot: Box<dyn SnapshotReader>,
}

/// A record holding the hard state of a Raft node.
///
/// This model derives serde's traits for easily (de)serializing this
/// model for storage & retrieval.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HardState {
    /// The last recorded term observed by this system.
    pub current_term: u64,
    /// The ID of the node voted for in the `current_term`.
    pub voted_for: Option<NodeId>,
    /// The cluster membership configuration.
    pub membership: MembershipConfig,
}

/// A struct used to represent the initial state which a Raft node needs when first starting.
#[derive(Clone, Debug)]
pub struct InitialState {
    /// The index of the last entry.
    pub last_log_index: u64,
    /// The term of the last log entry.
    pub last_log_term: u64,
    /// The index of the last log applied to the state machine.
    pub last_applied_log: u64,
    /// The saved hard state of the node.
    pub hard_state: HardState,
}

/// A trait defining the interface for a Raft storage system.
///
/// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#InstallSnapshot)
/// for details and discussion on this trait and how to implement it.
#[async_trait]
pub trait RaftStorage<D, R, E>: Send + Sync + 'static
    where
        D: AppData,
        R: AppDataResponse,
        E: AppError,
{
    /// A request from Raft to get Raft's state information from storage.
    ///
    /// When the Raft actor is first started, it will call this interface on the storage system to
    /// fetch the last known state from stable storage. If no such entry exists due to being the
    /// first time the node has come online, then the default value for `InitialState` should be used.
    ///
    /// ### pro tip
    /// The storage impl may need to look in a few different places to accurately respond to this
    /// request: the last entry in the log for `last_log_index` & `last_log_term`; the node's hard
    /// state record; and the index of the last log applied to the state machine.
    async fn get_initial_state(&self) -> Result<InitialState, E>;

    /// A request from Raft to save its hard state.
    async fn save_hard_state(&self, hs: &HardState) -> Result<(), E>;

    /// A request from Raft to get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start, stop)`.
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<D>>, E>;

    /// Delete all logs starting from `start` and stopping at `stop`, else continuing to the end
    /// of the log if `stop` is `None`.
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<(), E>;

    /// A request from Raft to append a new entry to the log.
    ///
    /// These requests come about via client requests, and as such, this is the only RaftStorage
    /// interface which is allowed to return errors which will not cause Raft to shutdown. Application
    /// errors coming from this interface will be sent back as-is to the call point where your
    /// application originally presented the client request to Raft.
    ///
    /// This property of error handling allows you to keep your application logic as close to the
    /// storage layer as needed.
    async fn append_entry_to_log(&self, entry: &Entry<D>) -> Result<(), E>;

    /// A request from Raft to replicate a payload of entries to the log.
    ///
    /// These requests come about via the Raft leader's replication process. An error coming from this
    /// interface will cause Raft to shutdown, as this is not where application logic should be
    /// returning application specific errors. Application specific constraints may only be enforced
    /// in the `AppendEntryToLog` handler.
    ///
    /// Though the entries will always be presented in order, each entry's index should be used to
    /// determine its location to be written in the log, as logs may need to be overwritten under
    /// some circumstances.
    async fn replicate_to_log(&self, entries: &[Entry<D>]) -> Result<(), E>;

    /// A request from Raft to apply the given log entry to the state machine.
    ///
    /// This handler is called as part of the client request path. Client requests which are
    /// configured to respond after they have been `Applied` will wait until after this handler
    /// returns before issuing a response to the client request.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a majority of the cluster, will be applied to the state machine.
    async fn apply_entry_to_state_machine(&self, payload: &Entry<D>) -> Result<R, E>;

    /// A request from Raft to apply the given payload of entries to the state machine, as part of replication.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a majority of the cluster, will be applied to the state machine.
    async fn replicate_to_state_machine(&self, payload: &[Entry<D>]) -> Result<(), E>;

    /// A request from Raft to have a new snapshot created.
    ///
    /// This request is initialized by the Raft node itself as part of the periodic log compaction
    /// process.
    ///
    /// ### through
    /// The new snapshot should start from entry `0` and should cover all entries through the
    /// index specified by `through`, inclusively.
    ///
    /// ### implementation guide
    /// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#CreateSnapshot)
    /// for details on how to implement this handler.
    async fn create_snapshot(&self, through: u64) -> Result<CurrentSnapshotData, E>;

    /// A request from the leader of the Raft cluster to install a new streaming snapshot.
    ///
    /// ### implementation guide
    /// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#InstallSnapshot)
    /// for details on how to implement this handler.
    async fn install_snapshot(&self, index: u64, term: u64, membership_config: MembershipConfig) -> Result<Box<dyn SnapshotWriter>, E>;

    /// Finalize the creation of a snapshot which was streamed from the cluster leader.
    ///
    /// Delete all entries in the log, stopping at `deleted_through`, unless `None`, in which case
    /// all entries of the log are to be deleted.
    ///
    /// Write a new snapshot pointer to the log at the given `index`. The snapshot pointer should be
    /// constructed via the `Entry::new_snapshot_pointer` constructor.
    ///
    /// All other snapshots should be deleted at this point.
    async fn finalize_install_snapshot(&self, index: u64, term: u64, delete_through: Option<u64>) -> Result<(), E>;

    /// A request from Raft to get a readable handle to the current snapshot, along with its metadata.
    ///
    /// ### implementation algorithm
    /// Implementation for this type's handler should be quite simple. Check the configured snapshot
    /// directory for any snapshot files. A proper implementation will only ever have one
    /// active snapshot, though another may exist while it is being created. As such, it is
    /// recommended to use a file naming pattern which will allow for easily distinguishing between
    /// the current live snapshot, and any new snapshot which is being created.
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData>, E>;
}
