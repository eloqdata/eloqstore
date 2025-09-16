//! Synchronous I/O backend implementation
//!
//! This backend uses standard blocking I/O operations.
//! All operations complete immediately when the function returns.

use std::fs::{File, OpenOptions, create_dir, create_dir_all, read_dir, remove_file, rename};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use crate::Result;
use crate::error::Error;

use super::{IoBackend, FileHandle, FileMetadata, IoStats, IoBackendType};

/// Synchronous file handle
pub struct SyncFileHandle {
    /// File path
    path: std::path::PathBuf,
    /// File handle
    file: Arc<Mutex<File>>,
    /// Statistics
    stats: Arc<SyncStats>,
}

impl SyncFileHandle {
    /// Create a new sync file handle
    fn new(path: std::path::PathBuf, file: File, stats: Arc<SyncStats>) -> Self {
        Self {
            path,
            file: Arc::new(Mutex::new(file)),
            stats,
        }
    }
}

#[async_trait]
impl FileHandle for SyncFileHandle {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let start = std::time::Instant::now();

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;

        let mut buffer = vec![0u8; len];
        let bytes_read = file.read(&mut buffer).map_err(Error::Io)?;
        buffer.truncate(bytes_read);

        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.record_read(bytes_read, elapsed);

        Ok(Bytes::from(buffer))
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let start = std::time::Instant::now();

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
        let bytes_written = file.write(data).map_err(Error::Io)?;

        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.record_write(bytes_written, elapsed);

        Ok(bytes_written)
    }

    async fn sync(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_all().map_err(Error::Io)?;
        self.stats.record_sync();
        Ok(())
    }

    async fn sync_data(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_data().map_err(Error::Io)?;
        self.stats.record_sync();
        Ok(())
    }

    async fn file_size(&self) -> Result<u64> {
        let file = self.file.lock().unwrap();
        let metadata = file.metadata().map_err(Error::Io)?;
        Ok(metadata.len())
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.set_len(size).map_err(Error::Io)?;
        Ok(())
    }

    async fn allocate(&self, _offset: u64, _len: u64) -> Result<()> {
        // Standard file API doesn't have fallocate
        // Could use platform-specific APIs if needed
        Ok(())
    }
}

/// Synchronous I/O backend
pub struct SyncBackend {
    /// Statistics
    stats: Arc<SyncStats>,
}

impl SyncBackend {
    /// Create a new sync backend
    pub fn new() -> Self {
        Self {
            stats: Arc::new(SyncStats::new()),
        }
    }
}

impl Default for SyncBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IoBackend for SyncBackend {
    fn backend_type(&self) -> IoBackendType {
        IoBackendType::Sync
    }

    async fn open_file(&self, path: &Path, create: bool) -> Result<Arc<dyn FileHandle>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);

        if create {
            options.create(true);
        }

        let file = options.open(path).map_err(Error::Io)?;
        let handle = SyncFileHandle::new(path.to_path_buf(), file, self.stats.clone());

        Ok(Arc::new(handle))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        remove_file(path).map_err(Error::Io)?;
        Ok(())
    }

    async fn file_exists(&self, path: &Path) -> Result<bool> {
        Ok(path.exists())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        create_dir(path).map_err(Error::Io)?;
        Ok(())
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        create_dir_all(path).map_err(Error::Io)?;
        Ok(())
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        let entries = read_dir(path).map_err(Error::Io)?;
        let mut names = Vec::new();

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }

        Ok(names)
    }

    async fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        let metadata = std::fs::metadata(path).map_err(Error::Io)?;

        Ok(FileMetadata {
            size: metadata.len(),
            is_dir: metadata.is_dir(),
            is_file: metadata.is_file(),
            created: metadata.created().ok(),
            modified: metadata.modified().ok(),
            accessed: metadata.accessed().ok(),
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        rename(from, to).map_err(Error::Io)?;
        Ok(())
    }

    fn is_async(&self) -> bool {
        false // Synchronous backend
    }

    fn stats(&self) -> IoStats {
        self.stats.to_io_stats()
    }

    async fn shutdown(&self) -> Result<()> {
        // Nothing to shutdown for sync backend
        Ok(())
    }
}

/// Statistics for sync backend
struct SyncStats {
    reads: AtomicU64,
    writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
    total_read_latency: AtomicU64,
    total_write_latency: AtomicU64,
}

impl SyncStats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            total_read_latency: AtomicU64::new(0),
            total_write_latency: AtomicU64::new(0),
        }
    }

    fn record_read(&self, bytes: usize, latency_us: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_read_latency.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_write(&self, bytes: usize, latency_us: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_write_latency.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_sync(&self) {
        self.syncs.fetch_add(1, Ordering::Relaxed);
    }

    fn to_io_stats(&self) -> IoStats {
        let reads = self.reads.load(Ordering::Relaxed);
        let writes = self.writes.load(Ordering::Relaxed);

        IoStats {
            reads,
            writes,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            syncs: self.syncs.load(Ordering::Relaxed),
            avg_read_latency_us: if reads > 0 {
                self.total_read_latency.load(Ordering::Relaxed) / reads
            } else {
                0
            },
            avg_write_latency_us: if writes > 0 {
                self.total_write_latency.load(Ordering::Relaxed) / writes
            } else {
                0
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sync_backend_basic() {
        let backend = SyncBackend::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.dat");

        // Open file
        let file = backend.open_file(&file_path, true).await.unwrap();

        // Write data
        let data = b"Hello, sync backend!";
        let written = file.write_at(0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read data back
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(read_data.as_ref(), data);

        // Check file size
        let size = file.file_size().await.unwrap();
        assert_eq!(size, data.len() as u64);

        // Sync
        file.sync().await.unwrap();

        // Check stats
        let stats = backend.stats();
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.bytes_written, data.len() as u64);
    }

    #[tokio::test]
    async fn test_sync_backend_directory() {
        let backend = SyncBackend::new();
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("subdir");

        // Create directory
        backend.create_dir(&dir_path).await.unwrap();
        assert!(backend.file_exists(&dir_path).await.unwrap());

        // Check metadata
        let metadata = backend.metadata(&dir_path).await.unwrap();
        assert!(metadata.is_dir);
        assert!(!metadata.is_file);
    }
}