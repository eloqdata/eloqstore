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
}

pub type Result<T> = std::result::Result<T, Error>;

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