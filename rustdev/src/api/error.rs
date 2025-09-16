//! Error types for the API layer

use thiserror::Error;

/// API-level errors
#[derive(Debug, Error)]
pub enum ApiError {
    /// Key not found in the database
    #[error("Key not found")]
    NotFound,

    /// Invalid request parameters
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Request timed out
    #[error("Request timed out")]
    Timeout,

    /// Database is shutting down
    #[error("Database is shutting down")]
    Shutdown,

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceExhausted(String),

    /// Internal error from the storage layer
    #[error("Storage error: {0}")]
    StorageError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Serialization/deserialization error
    #[error("Codec error: {0}")]
    CodecError(String),

    /// Corruption detected
    #[error("Data corruption detected: {0}")]
    Corruption(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Transaction conflict
    #[error("Transaction conflict")]
    Conflict,

    /// Generic internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl ApiError {
    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ApiError::Timeout | ApiError::ResourceExhausted(_) | ApiError::Conflict
        )
    }

    /// Check if the error indicates corruption
    pub fn is_corruption(&self) -> bool {
        matches!(self, ApiError::Corruption(_))
    }
}