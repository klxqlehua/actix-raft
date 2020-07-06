//! The Raft network interface.

use async_trait::async_trait;

use crate::{AppData, AppError};
use crate::raft::{AppendEntriesRequest, AppendEntriesResponse};
use crate::raft::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::raft::{VoteRequest, VoteResponse};

/// A trait defining the interface for a Raft network between cluster members.
///
/// See the [network chapter of the guide](https://railgun-rs.github.io/actix-raft/network.html)
/// for details and discussion on this trait and how to implement it.
#[async_trait]
pub trait RaftNetwork<D, E>: Send + Sync + 'static
    where
        D: AppData,
        E: AppError,
{
    /// An RPC invoked by the leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
    async fn append_entries(&self, target: u64, msg: AppendEntriesRequest<D>) -> Result<AppendEntriesResponse, E>;

    /// Invoked by the Raft leader to send chunks of a snapshot to a follower (§7).
    async fn install_snapshot(&self, target: u64, msg: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, E>;

    /// An RPC invoked by candidates to gather votes (§5.2).
    async fn vote(&self, target: u64, msg: VoteRequest) -> Result<VoteResponse, E>;
}
