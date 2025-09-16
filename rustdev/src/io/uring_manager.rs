//! Async I/O manager using io_uring via tokio-uring

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, RwLock};
use tokio_uring::fs::{File, OpenOptions};

use crate::types::{FileId, FilePageId, TableIdent};
use crate::page::Page;
use crate::Result;
use crate::error::Error;

/// io_uring configuration
#[derive(Debug, Clone)]
pub struct UringConfig {
    /// Queue depth for submission queue
    pub queue_depth: u32,
    /// Enable kernel polling
    pub kernel_poll: bool,
    /// Enable submission queue polling
    pub sq_poll: bool,
    /// CPU affinity for SQ poll thread
    pub sq_poll_cpu: Option<u32>,
    /// Idle time in ms before kernel thread sleeps
    pub sq_poll_idle: u32,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            kernel_poll: false,
            sq_poll: false,
            sq_poll_cpu: None,
            sq_poll_idle: 1000,
        }
    }
}

/// Async file handle
struct AsyncFileHandle {
    /// File ID
    file_id: FileId,
    /// File path
    path: PathBuf,
    /// Async file handle
    file: Arc<File>,
    /// File size
    size: Arc<RwLock<u64>>,
}

impl AsyncFileHandle {
    /// Open or create a file
    async fn open(file_id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        let metadata = file.statx().await?;
        let size = metadata.stx_size;

        Ok(Self {
            file_id,
            path,
            file: Arc::new(file),
            size: Arc::new(RwLock::new(size)),
        })
    }

    /// Read data at offset
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> io::Result<Vec<u8>> {
        let (result, read_buf) = self.file.read_at(buf, offset).await;
        result?;
        Ok(read_buf)
    }

    /// Write data at offset
    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> io::Result<usize> {
        let len = buf.len();
        let submission = self.file.write_at(buf, offset);
        let (result, _) = submission.submit().await;
        let written = result?;

        // Update size if needed
        let new_end = offset + written as u64;
        let mut size = self.size.write().await;
        if new_end > *size {
            *size = new_end;
        }

        Ok(written)
    }

    /// Sync data to disk
    async fn sync(&self) -> io::Result<()> {
        self.file.sync_all().await
    }

    /// Get current file size
    async fn size(&self) -> u64 {
        *self.size.read().await
    }
}

/// Async I/O manager using io_uring
pub struct UringManager {
    /// Configuration
    config: UringConfig,
    /// Page size
    page_size: usize,
    /// Open file handles
    handles: Arc<RwLock<HashMap<FileId, Arc<AsyncFileHandle>>>>,
    /// Maximum open files
    max_open_files: usize,
}

impl UringManager {
    /// Create a new io_uring manager
    pub fn new(config: UringConfig, page_size: usize, max_open_files: usize) -> Self {
        Self {
            config,
            page_size,
            handles: Arc::new(RwLock::new(HashMap::new())),
            max_open_files,
        }
    }

    /// Initialize the manager
    pub async fn init(&self) -> Result<()> {
        // io_uring is initialized per-thread by tokio-uring
        Ok(())
    }

    /// Open or create a file
    pub async fn open_file(&self, file_id: FileId, path: impl AsRef<Path>) -> Result<()> {
        let mut handles = self.handles.write().await;

        if handles.contains_key(&file_id) {
            return Ok(()); // Already open
        }

        // Check if we need to evict files
        if handles.len() >= self.max_open_files {
            // Simple eviction - remove first entry
            if let Some(&evict_id) = handles.keys().next() {
                handles.remove(&evict_id);
            }
        }

        let handle = AsyncFileHandle::open(file_id, path)
            .await
            .map_err(Error::Io)?;

        handles.insert(file_id, Arc::new(handle));

        Ok(())
    }

    /// Close a file
    pub async fn close_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().await;
        handles.remove(&file_id);
        Ok(())
    }

    /// Read a page from file
    pub async fn read_page(&self, file_id: FileId, page_offset: u64) -> Result<Page> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        let offset = page_offset * self.page_size as u64;
        let buf = vec![0u8; self.page_size];

        let data = handle.read_at(buf, offset)
            .await
            .map_err(Error::Io)?;

        Ok(Page::from_bytes(Bytes::from(data)))
    }

    /// Write a page to file
    pub async fn write_page(&self, file_id: FileId, page_offset: u64, page: &Page) -> Result<()> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        let offset = page_offset * self.page_size as u64;
        let data = page.as_bytes().to_vec();

        handle.write_at(data, offset)
            .await
            .map_err(Error::Io)?;

        Ok(())
    }

    /// Read multiple pages
    pub async fn read_pages(&self, file_id: FileId, offsets: Vec<u64>) -> Result<Vec<Page>> {
        let mut pages = Vec::with_capacity(offsets.len());

        // TODO: Optimize with vectored I/O or parallel reads
        for offset in offsets {
            pages.push(self.read_page(file_id, offset).await?);
        }

        Ok(pages)
    }

    /// Write multiple pages
    pub async fn write_pages(&self, file_id: FileId, pages: Vec<(u64, Page)>) -> Result<()> {
        // TODO: Optimize with vectored I/O or parallel writes
        for (offset, page) in pages {
            self.write_page(file_id, offset, &page).await?;
        }

        Ok(())
    }

    /// Sync file to disk
    pub async fn sync_file(&self, file_id: FileId) -> Result<()> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        handle.sync()
            .await
            .map_err(Error::Io)?;

        Ok(())
    }

    /// Sync all files
    pub async fn sync_all(&self) -> Result<()> {
        let handles = self.handles.read().await;

        for handle in handles.values() {
            handle.sync()
                .await
                .map_err(Error::Io)?;
        }

        Ok(())
    }

    /// Get file size
    pub async fn file_size(&self, file_id: FileId) -> Result<u64> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        Ok(handle.size().await)
    }
}

/// Batch I/O request
pub struct BatchIoRequest {
    /// File ID
    pub file_id: FileId,
    /// Operations
    pub ops: Vec<IoOperation>,
}

/// I/O operation
pub enum IoOperation {
    /// Read operation
    Read {
        /// Page offset
        offset: u64,
    },
    /// Write operation
    Write {
        /// Page offset
        offset: u64,
        /// Page data
        page: Page,
    },
}

/// Batch I/O result
pub struct BatchIoResult {
    /// Results for each operation
    pub results: Vec<IoResult>,
}

/// Individual I/O result
pub enum IoResult {
    /// Read result
    Read(Result<Page>),
    /// Write result
    Write(Result<()>),
}

impl UringManager {
    /// Execute batch I/O operations
    pub async fn batch_io(&self, request: BatchIoRequest) -> BatchIoResult {
        let mut results = Vec::with_capacity(request.ops.len());

        // Execute operations
        // TODO: Optimize with parallel execution
        for op in request.ops {
            let result = match op {
                IoOperation::Read { offset } => {
                    IoResult::Read(self.read_page(request.file_id, offset).await)
                }
                IoOperation::Write { offset, page } => {
                    IoResult::Write(self.write_page(request.file_id, offset, &page).await)
                }
            };
            results.push(result);
        }

        BatchIoResult { results }
    }

    /// Prefetch pages (non-blocking)
    pub async fn prefetch(&self, file_id: FileId, offsets: Vec<u64>) -> Result<()> {
        // Start async reads but don't wait for completion
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?
            .clone();

        // Spawn background prefetch tasks
        for offset in offsets {
            let handle = handle.clone();
            let page_size = self.page_size;

            tokio_uring::spawn(async move {
                let buf = vec![0u8; page_size];
                let file_offset = offset * page_size as u64;
                let _ = handle.read_at(buf, file_offset).await;
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[ignore]
    #[tokio::test]
    async fn test_uring_manager_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manager = UringManager::new(UringConfig::default(), 4096, 10);

        manager.init().await.unwrap();

        let file_path = temp_dir.path().join("test.dat");
        manager.open_file(1, &file_path).await.unwrap();

        // Write a page
        let mut page = Page::new(4096);
        page.set_content_length(100);
        page.update_checksum();

        manager.write_page(1, 0, &page).await.unwrap();

        // Read it back
        let read_page = manager.read_page(1, 0).await.unwrap();
        assert_eq!(read_page.content_length(), 100);
        assert!(read_page.verify_checksum());

        // Sync
        manager.sync_file(1).await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_batch_io() {
        let temp_dir = TempDir::new().unwrap();
        let manager = UringManager::new(UringConfig::default(), 4096, 10);

        let file_path = temp_dir.path().join("batch.dat");
        manager.open_file(2, &file_path).await.unwrap();

        // Create batch request
        let mut ops = Vec::new();

        // Write operations
        for i in 0..5 {
            let mut page = Page::new(4096);
            page.set_content_length(i * 10);
            page.update_checksum();

            ops.push(IoOperation::Write {
                offset: i as u64,
                page,
            });
        }

        // Read operations
        for i in 0..5 {
            ops.push(IoOperation::Read {
                offset: i as u64,
            });
        }

        let request = BatchIoRequest {
            file_id: 2,
            ops,
        };

        let result = manager.batch_io(request).await;

        // Check results
        assert_eq!(result.results.len(), 10);

        // First 5 should be write results
        for i in 0..5 {
            match &result.results[i] {
                IoResult::Write(Ok(())) => {},
                _ => panic!("Expected successful write"),
            }
        }

        // Next 5 should be read results
        for i in 5..10 {
            match &result.results[i] {
                IoResult::Read(Ok(page)) => {
                    assert_eq!(page.content_length(), ((i - 5) * 10) as u16);
                },
                _ => panic!("Expected successful read"),
            }
        }
    }
}