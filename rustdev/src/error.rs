//! Core error types for EloqStore

use std::io;
use thiserror::Error;
use crate::api::error::ApiError;

/// Main error type for EloqStore operations
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error from system operations
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Key not found in the database
    #[error("Key not found")]
    NotFound,

    /// Invalid argument provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Operation would block (non-blocking mode)
    #[error("Operation would block")]
    WouldBlock,

    /// Out of memory
    #[error("Out of memory")]
    OutOfMemory,

    /// Storage is full
    #[error("Storage full")]
    StorageFull,

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Data corruption detected
    #[error("Corruption detected: {0}")]
    Corruption(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Operation timed out
    #[error("Timeout")]
    Timeout,

    /// System is shutting down
    #[error("Shutdown in progress")]
    ShuttingDown,

    /// io_uring specific error
    #[error("io_uring error: {0}")]
    IoUring(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// API layer error
    #[error("API error: {0}")]
    Api(#[from] ApiError),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Invalid input error
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Queue is full
    #[error("Queue full")]
    QueueFull,

    /// Page is full
    #[error("Page full")]
    PageFull,

    /// Operation cancelled
    #[error("Cancelled")]
    Cancelled,

    /// Generic internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Not implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// I/O error wrapper (without auto From impl)
    #[error("I/O error: {0}")]
    IoError(std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// KV error codes matching C++ KvError enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvError {
    NoError = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IoError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    OutOfMemory = 14,
    OutOfRange = 15,
    NoSpace = 16,
    OpenFileLimit = 17,
    NotRunning = 18,
    InternalError = 19,
    NotImplemented = 20,
}

impl From<KvError> for Error {
    fn from(err: KvError) -> Self {
        match err {
            KvError::NoError => Error::Internal("NoError converted to Error".into()),
            KvError::NotFound => Error::NotFound,
            KvError::Corruption => Error::Corruption("Unknown".into()),
            KvError::NotSupported => Error::NotSupported("Unknown".into()),
            KvError::InvalidArgument => Error::InvalidArgument("Unknown".into()),
            KvError::IoError => Error::Io(io::Error::new(io::ErrorKind::Other, "Unknown")),
            KvError::ShutdownInProgress => Error::ShuttingDown,
            KvError::TimedOut => Error::Timeout,
            KvError::Aborted => Error::Cancelled,
            KvError::Busy => Error::WouldBlock,
            KvError::TryAgain => Error::WouldBlock,
            KvError::OutOfMemory => Error::OutOfMemory,
            KvError::NoSpace => Error::StorageFull,
            _ => Error::Internal(format!("KvError {:?}", err)),
        }
    }
}

impl KvError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KvError::TryAgain | KvError::TimedOut | KvError::Busy
        )
    }
}

impl Error {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::WouldBlock | Error::Timeout | Error::OutOfMemory
        )
    }

    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::Corruption(_) | Error::ShuttingDown | Error::Internal(_)
        )
    }
}