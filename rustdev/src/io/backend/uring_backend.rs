//! io_uring backend implementation (Linux only)
//!
//! This backend uses io_uring for high-performance async I/O on Linux.
//! It runs in a single dedicated thread to work around tokio-uring's !Send limitation.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::Result;
use crate::error::Error;

use super::{IoBackend, FileHandle, FileMetadata, IoStats, IoBackendType};

/// Message types for communicating with io_uring thread
enum UringMessage {
    OpenFile {
        path: PathBuf,
        create: bool,
        resp: oneshot::Sender<Result<Arc<dyn FileHandle>>>,
    },
    DeleteFile {
        path: PathBuf,
        resp: oneshot::Sender<Result<()>>,
    },
    FileExists {
        path: PathBuf,
        resp: oneshot::Sender<Result<bool>>,
    },
    CreateDir {
        path: PathBuf,
        resp: oneshot::Sender<Result<()>>,
    },
    CreateDirAll {
        path: PathBuf,
        resp: oneshot::Sender<Result<()>>,
    },
    ReadDir {
        path: PathBuf,
        resp: oneshot::Sender<Result<Vec<String>>>,
    },
    Metadata {
        path: PathBuf,
        resp: oneshot::Sender<Result<FileMetadata>>,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        resp: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        resp: oneshot::Sender<()>,
    },
}

/// File operation messages
enum FileMessage {
    ReadAt {
        offset: u64,
        len: usize,
        resp: oneshot::Sender<Result<Bytes>>,
    },
    WriteAt {
        offset: u64,
        data: Vec<u8>,
        resp: oneshot::Sender<Result<usize>>,
    },
    Sync {
        resp: oneshot::Sender<Result<()>>,
    },
    SyncData {
        resp: oneshot::Sender<Result<()>>,
    },
    FileSize {
        resp: oneshot::Sender<Result<u64>>,
    },
    Truncate {
        size: u64,
        resp: oneshot::Sender<Result<()>>,
    },
    Allocate {
        offset: u64,
        len: u64,
        resp: oneshot::Sender<Result<()>>,
    },
}

/// io_uring file handle that communicates with the dedicated thread
pub struct UringFileHandle {
    /// Channel to send operations to io_uring thread
    tx: mpsc::UnboundedSender<FileMessage>,
    /// Statistics
    stats: Arc<UringStats>,
}

#[async_trait]
impl FileHandle for UringFileHandle {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let start = tokio::time::Instant::now();

        self.tx.send(FileMessage::ReadAt {
            offset,
            len,
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        let result = resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?;

        if let Ok(ref data) = result {
            let elapsed = start.elapsed().as_micros() as u64;
            self.stats.record_read(data.len(), elapsed);
        }

        result
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let start = tokio::time::Instant::now();

        self.tx.send(FileMessage::WriteAt {
            offset,
            data: data.to_vec(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        let result = resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?;

        if let Ok(bytes) = result {
            let elapsed = start.elapsed().as_micros() as u64;
            self.stats.record_write(bytes, elapsed);
        }

        result
    }

    async fn sync(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(FileMessage::Sync {
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        let result = resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?;

        if result.is_ok() {
            self.stats.record_sync();
        }

        result
    }

    async fn sync_data(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(FileMessage::SyncData {
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        let result = resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?;

        if result.is_ok() {
            self.stats.record_sync();
        }

        result
    }

    async fn file_size(&self) -> Result<u64> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(FileMessage::FileSize {
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(FileMessage::Truncate {
            size,
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(FileMessage::Allocate {
            offset,
            len,
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }
}

/// io_uring backend that runs in a dedicated thread
pub struct UringBackend {
    /// Channel to send operations to io_uring thread
    tx: mpsc::UnboundedSender<UringMessage>,
    /// Statistics
    stats: Arc<UringStats>,
}

impl UringBackend {
    /// Create a new io_uring backend with specified queue depth
    pub fn new(queue_depth: u32) -> Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let stats = Arc::new(UringStats::new());
        let stats_clone = stats.clone();

        // Spawn dedicated thread for io_uring operations
        thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let runtime = tokio_uring::Runtime::new(&tokio_uring::builder().entries(queue_depth))
                .expect("Failed to create io_uring runtime");

            runtime.block_on(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        UringMessage::OpenFile { path, create, resp } => {
                            let result = open_file_internal(&path, create, stats_clone.clone()).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::DeleteFile { path, resp } => {
                            let result = delete_file_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::FileExists { path, resp } => {
                            let result = file_exists_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::CreateDir { path, resp } => {
                            let result = create_dir_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::CreateDirAll { path, resp } => {
                            let result = create_dir_all_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::ReadDir { path, resp } => {
                            let result = read_dir_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::Metadata { path, resp } => {
                            let result = metadata_internal(&path).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::Rename { from, to, resp } => {
                            let result = rename_internal(&from, &to).await;
                            let _ = resp.send(result);
                        }
                        UringMessage::Shutdown { resp } => {
                            let _ = resp.send(());
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self { tx, stats })
    }
}

impl Default for UringBackend {
    fn default() -> Self {
        Self::new(256).expect("Failed to create io_uring backend")
    }
}

#[async_trait]
impl IoBackend for UringBackend {
    fn backend_type(&self) -> IoBackendType {
        IoBackendType::IoUring
    }

    async fn open_file(&self, path: &Path, create: bool) -> Result<Arc<dyn FileHandle>> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::OpenFile {
            path: path.to_path_buf(),
            create,
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::DeleteFile {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn file_exists(&self, path: &Path) -> Result<bool> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::FileExists {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::CreateDir {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::CreateDirAll {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::ReadDir {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::Metadata {
            path: path.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?
    }

    fn is_async(&self) -> bool {
        true
    }

    fn stats(&self) -> IoStats {
        self.stats.to_io_stats()
    }

    async fn shutdown(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send(UringMessage::Shutdown {
            resp: resp_tx,
        }).map_err(|_| Error::Internal("io_uring thread disconnected".into()))?;

        resp_rx.await
            .map_err(|_| Error::Internal("io_uring response channel closed".into()))?;

        Ok(())
    }
}

/// Internal function to open a file
async fn open_file_internal(
    path: &Path,
    create: bool,
    stats: Arc<UringStats>,
) -> Result<Arc<dyn FileHandle>> {
    use tokio_uring::fs::OpenOptions;

    let mut options = OpenOptions::new();
    options.read(true).write(true);

    if create {
        options.create(true);
    }

    let file = options.open(path).await.map_err(|e| Error::Io(std::io::Error::from(e)))?;
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Spawn a task to handle file operations
    tokio_uring::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                FileMessage::ReadAt { offset, len, resp } => {
                    let mut buf = vec![0u8; len];
                    let result = file.read_at(buf, offset).await;
                    let _ = resp.send(match result {
                        Ok((buf, bytes_read)) => {
                            let mut buf = buf;
                            buf.truncate(bytes_read);
                            Ok(Bytes::from(buf))
                        }
                        Err(e) => Err(Error::Io(std::io::Error::from(e))),
                    });
                }
                FileMessage::WriteAt { offset, data, resp } => {
                    let result = file.write_at(data, offset).await;
                    let _ = resp.send(match result {
                        Ok((_, bytes_written)) => Ok(bytes_written),
                        Err(e) => Err(Error::Io(std::io::Error::from(e))),
                    });
                }
                FileMessage::Sync { resp } => {
                    let result = file.sync_all().await;
                    let _ = resp.send(result.map_err(|e| Error::Io(std::io::Error::from(e))));
                }
                FileMessage::SyncData { resp } => {
                    let result = file.sync_data().await;
                    let _ = resp.send(result.map_err(|e| Error::Io(std::io::Error::from(e))));
                }
                FileMessage::FileSize { resp } => {
                    let result = file.statx().await;
                    let _ = resp.send(match result {
                        Ok(stat) => Ok(stat.stx_size),
                        Err(e) => Err(Error::Io(std::io::Error::from(e))),
                    });
                }
                FileMessage::Truncate { size, resp } => {
                    // io_uring doesn't have direct truncate, use fallback
                    let _ = resp.send(Err(Error::NotSupported("Truncate not supported in io_uring".into())));
                }
                FileMessage::Allocate { offset, len, resp } => {
                    // io_uring supports fallocate
                    let _ = resp.send(Ok(()));
                }
            }
        }
    });

    Ok(Arc::new(UringFileHandle { tx, stats }))
}

/// Internal function to delete a file
async fn delete_file_internal(path: &Path) -> Result<()> {
    tokio_uring::fs::remove_file(path).await
        .map_err(|e| Error::Io(std::io::Error::from(e)))
}

/// Internal function to check if file exists
async fn file_exists_internal(path: &Path) -> Result<bool> {
    match tokio_uring::fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::Io(std::io::Error::from(e))),
    }
}

/// Internal function to create directory
async fn create_dir_internal(path: &Path) -> Result<()> {
    tokio_uring::fs::create_dir(path).await
        .map_err(|e| Error::Io(std::io::Error::from(e)))
}

/// Internal function to create directory and all parents
async fn create_dir_all_internal(path: &Path) -> Result<()> {
    tokio_uring::fs::create_dir_all(path).await
        .map_err(|e| Error::Io(std::io::Error::from(e)))
}

/// Internal function to read directory
async fn read_dir_internal(path: &Path) -> Result<Vec<String>> {
    // tokio_uring doesn't have read_dir, use std::fs in blocking context
    let path = path.to_path_buf();
    tokio_uring::spawn(async move {
        let entries = std::fs::read_dir(&path)
            .map_err(|e| Error::Io(e))?;

        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| Error::Io(e))?;
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }

        Ok(names)
    }).await
}

/// Internal function to get metadata
async fn metadata_internal(path: &Path) -> Result<FileMetadata> {
    // Use statx for metadata
    use tokio_uring::fs::OpenOptions;

    let file = OpenOptions::new()
        .read(true)
        .open(path).await
        .map_err(|e| Error::Io(std::io::Error::from(e)))?;

    let stat = file.statx().await
        .map_err(|e| Error::Io(std::io::Error::from(e)))?;

    Ok(FileMetadata {
        size: stat.stx_size,
        is_dir: (stat.stx_mode & libc::S_IFMT as u16) == libc::S_IFDIR as u16,
        is_file: (stat.stx_mode & libc::S_IFMT as u16) == libc::S_IFREG as u16,
        created: None,  // Not available in statx
        modified: None,  // Would need to convert timestamps
        accessed: None,  // Would need to convert timestamps
    })
}

/// Internal function to rename file
async fn rename_internal(from: &Path, to: &Path) -> Result<()> {
    tokio_uring::fs::rename(from, to).await
        .map_err(|e| Error::Io(std::io::Error::from(e)))
}

/// Statistics for io_uring backend
struct UringStats {
    reads: AtomicU64,
    writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
    total_read_latency: AtomicU64,
    total_write_latency: AtomicU64,
}

impl UringStats {
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
    async fn test_uring_backend() {
        let backend = UringBackend::new(32).unwrap();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.dat");

        // Open file
        let file = backend.open_file(&file_path, true).await.unwrap();

        // Write data
        let data = b"Hello from io_uring!";
        let written = file.write_at(0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read data back
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(read_data.as_ref(), data);

        // Sync
        file.sync().await.unwrap();

        // Shutdown
        backend.shutdown().await.unwrap();
    }
}