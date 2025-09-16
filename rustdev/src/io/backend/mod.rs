//! I/O backend abstraction layer
//!
//! This module provides a unified API for different I/O implementations,
//! allowing seamless switching between sync I/O, async I/O, io_uring, etc.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::FileId;
use crate::page::Page;
use crate::Result;

pub mod sync_backend;
pub mod tokio_backend;
pub mod thread_pool_backend;
// #[cfg(target_os = "linux")]
// pub mod uring_backend;  // TODO: Fix thread safety issues with tokio-uring
pub mod factory;

#[cfg(test)]
mod tests;

pub use factory::{IoBackendFactory, IoBackendType, IoBackendConfig};

/// File handle abstraction
#[async_trait]
pub trait FileHandle: Send + Sync {
    /// Read data at offset
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;

    /// Write data at offset
    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize>;

    /// Read exact bytes at offset
    async fn read_exact(&self, offset: u64, len: usize) -> Result<Bytes> {
        let data = self.read_at(offset, len).await?;
        if data.len() != len {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Expected {} bytes, got {}", len, data.len())
            )));
        }
        Ok(data)
    }

    /// Sync data to disk
    async fn sync(&self) -> Result<()>;

    /// Sync data only (not metadata)
    async fn sync_data(&self) -> Result<()>;

    /// Get file size
    async fn file_size(&self) -> Result<u64>;

    /// Truncate file
    async fn truncate(&self, size: u64) -> Result<()>;

    /// Pre-allocate space
    async fn allocate(&self, offset: u64, len: u64) -> Result<()>;
}

/// I/O backend trait
#[async_trait]
pub trait IoBackend: Send + Sync {
    /// Backend type identifier
    fn backend_type(&self) -> IoBackendType;

    /// Open or create a file
    async fn open_file(&self, path: &Path, create: bool) -> Result<Arc<dyn FileHandle>>;

    /// Delete a file
    async fn delete_file(&self, path: &Path) -> Result<()>;

    /// Check if file exists
    async fn file_exists(&self, path: &Path) -> Result<bool>;

    /// Create directory
    async fn create_dir(&self, path: &Path) -> Result<()>;

    /// Create directory and all parents
    async fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// List directory entries
    async fn read_dir(&self, path: &Path) -> Result<Vec<String>>;

    /// Get file metadata
    async fn metadata(&self, path: &Path) -> Result<FileMetadata>;

    /// Rename file
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Read page (convenience method)
    async fn read_page(&self, file: &dyn FileHandle, offset: u64, page_size: usize) -> Result<Page> {
        let data = file.read_exact(offset, page_size).await?;
        Ok(Page::from_bytes(data))
    }

    /// Write page (convenience method)
    async fn write_page(&self, file: &dyn FileHandle, offset: u64, page: &Page) -> Result<()> {
        file.write_at(offset, page.as_bytes()).await?;
        Ok(())
    }

    /// Check if backend supports async operations
    fn is_async(&self) -> bool;

    /// Get backend statistics
    fn stats(&self) -> IoStats;

    /// Shutdown the backend
    async fn shutdown(&self) -> Result<()>;
}

/// File metadata
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// File size in bytes
    pub size: u64,
    /// Is directory
    pub is_dir: bool,
    /// Is regular file
    pub is_file: bool,
    /// Creation time (if available)
    pub created: Option<std::time::SystemTime>,
    /// Last modified time
    pub modified: Option<std::time::SystemTime>,
    /// Last accessed time
    pub accessed: Option<std::time::SystemTime>,
}

/// I/O statistics
#[derive(Debug, Default, Clone)]
pub struct IoStats {
    /// Total reads
    pub reads: u64,
    /// Total writes
    pub writes: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total sync operations
    pub syncs: u64,
    /// Average read latency in microseconds
    pub avg_read_latency_us: u64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: u64,
}

/// Batch I/O operations for efficiency
#[derive(Debug)]
pub struct BatchIo {
    /// Operations to perform
    pub operations: Vec<IoOperation>,
}

/// Individual I/O operation
#[derive(Debug)]
pub enum IoOperation {
    /// Read operation
    Read {
        /// File handle
        file: Arc<dyn FileHandle>,
        /// Offset in file
        offset: u64,
        /// Length to read
        len: usize,
    },
    /// Write operation
    Write {
        /// File handle
        file: Arc<dyn FileHandle>,
        /// Offset in file
        offset: u64,
        /// Data to write
        data: Bytes,
    },
    /// Sync operation
    Sync {
        /// File handle
        file: Arc<dyn FileHandle>,
    },
}

/// Batch I/O result
#[derive(Debug)]
pub struct BatchIoResult {
    /// Results for each operation
    pub results: Vec<IoResult>,
}

/// Individual I/O result
#[derive(Debug)]
pub enum IoResult {
    /// Read result
    Read(Result<Bytes>),
    /// Write result
    Write(Result<usize>),
    /// Sync result
    Sync(Result<()>),
}

/// Extension trait for batch operations
#[async_trait]
pub trait BatchIoExt: IoBackend {
    /// Execute batch I/O operations
    async fn execute_batch(&self, batch: BatchIo) -> BatchIoResult {
        let mut results = Vec::with_capacity(batch.operations.len());

        for op in batch.operations {
            let result = match op {
                IoOperation::Read { file, offset, len } => {
                    IoResult::Read(file.read_at(offset, len).await)
                }
                IoOperation::Write { file, offset, data } => {
                    IoResult::Write(file.write_at(offset, &data).await)
                }
                IoOperation::Sync { file } => {
                    IoResult::Sync(file.sync().await)
                }
            };
            results.push(result);
        }

        BatchIoResult { results }
    }
}