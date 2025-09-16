//! Thread pool I/O backend implementation
//!
//! This backend executes blocking I/O operations in a dedicated thread pool,
//! preventing blocking of async runtime threads.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::task;

use crate::Result;
use crate::error::Error;

use super::{IoBackend, FileHandle, FileMetadata, IoStats, IoBackendType};

/// Thread pool file handle
pub struct ThreadPoolFileHandle {
    /// File path
    path: std::path::PathBuf,
    /// File handle
    file: Arc<Mutex<File>>,
    /// Statistics
    stats: Arc<ThreadPoolStats>,
}

impl ThreadPoolFileHandle {
    /// Create a new thread pool file handle
    fn new(path: std::path::PathBuf, file: File, stats: Arc<ThreadPoolStats>) -> Self {
        Self {
            path,
            file: Arc::new(Mutex::new(file)),
            stats,
        }
    }
}

#[async_trait]
impl FileHandle for ThreadPoolFileHandle {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let file = self.file.clone();
        let stats = self.stats.clone();

        task::spawn_blocking(move || {
            let start = std::time::Instant::now();

            let mut file = file.lock().unwrap();
            file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;

            let mut buffer = vec![0u8; len];
            let bytes_read = file.read(&mut buffer).map_err(Error::Io)?;
            buffer.truncate(bytes_read);

            let elapsed = start.elapsed().as_micros() as u64;
            stats.record_read(bytes_read, elapsed);

            Ok(Bytes::from(buffer))
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let file = self.file.clone();
        let stats = self.stats.clone();
        let data = data.to_vec();

        task::spawn_blocking(move || {
            let start = std::time::Instant::now();

            let mut file = file.lock().unwrap();
            file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
            let bytes_written = file.write(&data).map_err(Error::Io)?;

            let elapsed = start.elapsed().as_micros() as u64;
            stats.record_write(bytes_written, elapsed);

            Ok(bytes_written)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn sync(&self) -> Result<()> {
        let file = self.file.clone();
        let stats = self.stats.clone();

        task::spawn_blocking(move || {
            let file = file.lock().unwrap();
            file.sync_all().map_err(Error::Io)?;
            stats.record_sync();
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn sync_data(&self) -> Result<()> {
        let file = self.file.clone();
        let stats = self.stats.clone();

        task::spawn_blocking(move || {
            let file = file.lock().unwrap();
            file.sync_data().map_err(Error::Io)?;
            stats.record_sync();
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn file_size(&self) -> Result<u64> {
        let file = self.file.clone();

        task::spawn_blocking(move || {
            let file = file.lock().unwrap();
            let metadata = file.metadata().map_err(Error::Io)?;
            Ok(metadata.len())
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        let file = self.file.clone();

        task::spawn_blocking(move || {
            let file = file.lock().unwrap();
            file.set_len(size).map_err(Error::Io)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn allocate(&self, _offset: u64, _len: u64) -> Result<()> {
        // Standard file API doesn't have fallocate
        Ok(())
    }
}

/// Thread pool I/O backend
pub struct ThreadPoolBackend {
    /// Statistics
    stats: Arc<ThreadPoolStats>,
}

impl ThreadPoolBackend {
    /// Create a new thread pool backend
    pub fn new() -> Self {
        Self {
            stats: Arc::new(ThreadPoolStats::new()),
        }
    }
}

impl Default for ThreadPoolBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IoBackend for ThreadPoolBackend {
    fn backend_type(&self) -> IoBackendType {
        IoBackendType::ThreadPool
    }

    async fn open_file(&self, path: &Path, create: bool) -> Result<Arc<dyn FileHandle>> {
        let path_buf = path.to_path_buf();
        let stats = self.stats.clone();

        let file = task::spawn_blocking(move || {
            let mut options = OpenOptions::new();
            options.read(true).write(true);

            if create {
                options.create(true);
            }

            options.open(&path_buf).map_err(Error::Io)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))??;

        let handle = ThreadPoolFileHandle::new(path.to_path_buf(), file, stats);

        Ok(Arc::new(handle))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            std::fs::remove_file(&path_buf).map_err(Error::Io)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn file_exists(&self, path: &Path) -> Result<bool> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            Ok(path_buf.exists())
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            std::fs::create_dir(&path_buf).map_err(Error::Io)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            std::fs::create_dir_all(&path_buf).map_err(Error::Io)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            let entries = std::fs::read_dir(&path_buf).map_err(Error::Io)?;
            let mut names = Vec::new();

            for entry in entries {
                let entry = entry.map_err(Error::Io)?;
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }

            Ok(names)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        let path_buf = path.to_path_buf();

        task::spawn_blocking(move || {
            let metadata = std::fs::metadata(&path_buf).map_err(Error::Io)?;

            Ok(FileMetadata {
                size: metadata.len(),
                is_dir: metadata.is_dir(),
                is_file: metadata.is_file(),
                created: metadata.created().ok(),
                modified: metadata.modified().ok(),
                accessed: metadata.accessed().ok(),
            })
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from_buf = from.to_path_buf();
        let to_buf = to.to_path_buf();

        task::spawn_blocking(move || {
            std::fs::rename(&from_buf, &to_buf).map_err(Error::Io)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    fn is_async(&self) -> bool {
        true // Operations don't block the runtime
    }

    fn stats(&self) -> IoStats {
        self.stats.to_io_stats()
    }

    async fn shutdown(&self) -> Result<()> {
        // Nothing to shutdown for thread pool backend
        // The runtime manages the thread pool
        Ok(())
    }
}

/// Statistics for thread pool backend
struct ThreadPoolStats {
    reads: AtomicU64,
    writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
    total_read_latency: AtomicU64,
    total_write_latency: AtomicU64,
}

impl ThreadPoolStats {
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
    async fn test_thread_pool_backend() {
        let backend = ThreadPoolBackend::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.dat");

        // Open file
        let file = backend.open_file(&file_path, true).await.unwrap();

        // Write data
        let data = b"Hello from thread pool!";
        let written = file.write_at(0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read data back
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(read_data.as_ref(), data);

        // Sync
        file.sync().await.unwrap();

        // Check stats
        let stats = backend.stats();
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.syncs, 1);
    }
}