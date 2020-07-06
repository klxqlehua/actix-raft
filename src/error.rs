use thiserror::Error;

use crate::{AppError, NodeId};

/// The result type used by Raft.
pub type Result<T, E> = std::result::Result<T, RaftError<E>>;

/// Error variants used by Raft.
#[derive(Debug, Error)]
pub enum RaftError<E: AppError> {
    /// An application error.
    #[error("{0}")]
    AppError(#[from] E),
    /// A configuration error indicating that the given values for election timeout min & max are invalid: max must be greater than min.
    #[error("given values for election timeout min & max are invalid: max must be greater than min")]
    InvalidElectionTimeoutMinMax,
    /// An internal Raft error indicating that Raft is shutting down.
    #[error("Raft is shutting down")]
    ShuttingDown,
    /// An IO error from tokio.
    #[error("{0}")]
    IO(#[from] tokio::io::Error)
}

/// Error variants related to configuration.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    /// A configuration error indicating that the given values for election timeout min & max are invalid: max must be greater than min.
    #[error("given values for election timeout min & max are invalid: max must be greater than min")]
    InvalidElectionTimeoutMinMax,
}

/// The set of errors which may take place when requesting to propose a config change.
#[derive(Debug, Error)]
pub enum InitWithConfigError<E: AppError> {
    /// An internal error has taken place.
    #[error("{0}")]
    RaftError(#[from] RaftError<E>),
    /// The requested action is not allowed due to the Raft node's current state.
    #[error("the requested action is not allowed due to the Raft node's current state")]
    NotAllowed,
}

/// The set of errors which may take place when requesting to propose a config change.
#[derive(Debug, Error)]
pub enum ProposeConfigChangeError<E: AppError> {
    /// An error related to the processing of the config change request.
    ///
    /// Errors of this type will only come about from the internals of applying the config change
    /// to the Raft log and the process related to that workflow.
    #[error("{0}")]
    RaftError(#[from] RaftError<E>),
    /// The cluster is already undergoing a configuration change.
    #[error("the cluster is already undergoing a configuration change")]
    AlreadyInJointConsensus,
    /// The given config would leave the cluster in an inoperable state.
    ///
    /// This error will be returned if the full set of changes, once fully applied, would leave
    /// the cluster with less than two members.
    #[error("the given config would leave the cluster in an inoperable state")]
    InoperableConfig,
    /// The node the config change proposal was sent to was not the leader of the cluster.
    ///
    /// If the current cluster leader is known, its ID will be wrapped in this variant.
    #[error("this node is not the Raft leader, current leader: {0:?}")]
    NodeNotLeader(Option<NodeId>),
    /// The proposed config changes would make no difference to the current config.
    ///
    /// This takes into account a current joint consensus and the end result of the config.
    ///
    /// This error will be returned if the proposed add & remove elements are empty; all of the
    /// entries to be added already exist in the current config and/or all of the entries to be
    /// removed have already been scheduled for removal and/or do not exist in the current config.
    #[error("the proposed config change would have no effect, this is a no-op")]
    Noop,
}
