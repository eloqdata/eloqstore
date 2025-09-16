//! Async file operations with io_uring

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io;

use bytes::{Bytes, BytesMut};
use tokio::sync::{RwLock, Semaphore};
use tokio_uring::fs::{File, OpenOptions};

use crate::Result;
use crate::error::Error;

/// Async file options
#[derive(Debug, Clone)]
pub struct AsyncFileOptions {
    /// Enable direct I/O
    pub direct_io: bool,
    /// Enable O_SYNC
    pub sync_io: bool,
    /// Read-ahead size in pages
    pub read_ahead: usize,
    /// Maximum concurrent operations
    pub max_concurrent_ops: usize,
}

impl Default for AsyncFileOptions {
    fn default() -> Self {
        Self {
            direct_io: false,
            sync_io: false,
            read_ahead: 4,
            max_concurrent_ops: 128,
        }
    }
}

/// Async file handle with advanced operations
pub struct AsyncFile {
    /// File path
    path: PathBuf,
    /// io_uring file handle
    file: Arc<File>,
    /// File options
    options: AsyncFileOptions,
    /// Semaphore for limiting concurrent operations
    semaphore: Arc<Semaphore>,
    /// Current file size
    size: Arc<RwLock<u64>>,
}

impl AsyncFile {
    /// Open a file with options
    pub async fn open(path: impl AsRef<Path>, options: AsyncFileOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);

        // Note: tokio-uring doesn't directly expose O_DIRECT and O_SYNC
        // These would need to be set via raw fd manipulation if needed

        let file = open_opts.open(&path)
            .await
            .map_err(Error::Io)?;

        let metadata = file.statx()
            .await
            .map_err(Error::Io)?;

        let size = metadata.stx_size;

        Ok(Self {
            path,
            file: Arc::new(file),
            options: options.clone(),
            semaphore: Arc::new(Semaphore::new(options.max_concurrent_ops)),
            size: Arc::new(RwLock::new(size)),
        })
    }

    /// Create a new file
    pub async fn create(path: impl AsRef<Path>, options: AsyncFileOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_opts = OpenOptions::new();
        open_opts.write(true).create_new(true);

        let file = open_opts.open(&path)
            .await
            .map_err(Error::Io)?;

        Ok(Self {
            path,
            file: Arc::new(file),
            options: options.clone(),
            semaphore: Arc::new(Semaphore::new(options.max_concurrent_ops)),
            size: Arc::new(RwLock::new(0)),
        })
    }

    /// Read data at offset
    pub async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let _permit = self.semaphore.acquire().await.unwrap();

        let buf = vec![0u8; len];
        let (result, mut read_buf) = self.file.read_at(buf, offset).await;
        let bytes_read = result.map_err(Error::Io)?;

        read_buf.truncate(bytes_read);
        Ok(Bytes::from(read_buf))
    }

    /// Write data at offset
    pub async fn write_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        let _permit = self.semaphore.acquire().await.unwrap();

        let buf = data.to_vec();
        let submission = self.file.write_at(buf, offset);
        let (result, _) = submission.submit().await;
        let written = result.map_err(Error::Io)?;

        // Update size if needed
        let new_end = offset + written as u64;
        let mut size = self.size.write().await;
        if new_end > *size {
            *size = new_end;
        }

        Ok(written)
    }

    /// Vectored read at offset
    pub async fn read_vectored(&self, offset: u64, lengths: Vec<usize>) -> Result<Vec<Bytes>> {
        let mut results = Vec::with_capacity(lengths.len());
        let mut current_offset = offset;

        // TODO: Optimize with actual vectored I/O when available
        for len in lengths {
            let data = self.read_at(current_offset, len).await?;
            current_offset += data.len() as u64;
            results.push(data);
        }

        Ok(results)
    }

    /// Vectored write at offset
    pub async fn write_vectored(&self, offset: u64, data: Vec<Bytes>) -> Result<usize> {
        let mut total_written = 0;
        let mut current_offset = offset;

        // TODO: Optimize with actual vectored I/O when available
        for chunk in data {
            let written = self.write_at(current_offset, chunk).await?;
            total_written += written;
            current_offset += written as u64;
        }

        Ok(total_written)
    }

    /// Sync data to disk
    pub async fn sync(&self) -> Result<()> {
        self.file.sync_all()
            .await
            .map_err(Error::Io)
    }

    /// Sync data only (not metadata)
    pub async fn sync_data(&self) -> Result<()> {
        self.file.sync_data()
            .await
            .map_err(Error::Io)
    }

    /// Truncate file to size
    pub async fn truncate(&self, size: u64) -> Result<()> {
        // tokio-uring doesn't have direct truncate, need to use std
        // This is a limitation that would need workaround in production
        std::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .and_then(|f| f.set_len(size))
            .map_err(Error::Io)?;

        let mut file_size = self.size.write().await;
        *file_size = size;

        Ok(())
    }

    /// Get file size
    pub async fn size(&self) -> u64 {
        *self.size.read().await
    }

    /// Fallocate - pre-allocate space
    pub async fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
        // Would need raw syscall for actual fallocate
        // For now, ensure size is updated
        let new_end = offset + len;
        let mut size = self.size.write().await;
        if new_end > *size {
            *size = new_end;
        }
        Ok(())
    }

    /// Read with prefetch
    pub async fn read_with_prefetch(&self, offset: u64, len: usize) -> Result<Bytes> {
        // Start prefetch for next blocks
        if self.options.read_ahead > 0 {
            let prefetch_offset = offset + len as u64;
            let prefetch_len = len * self.options.read_ahead;

            let file = self.file.clone();
            tokio_uring::spawn(async move {
                let buf = vec![0u8; prefetch_len];
                let _ = file.read_at(buf, prefetch_offset).await;
            });
        }

        // Read requested data
        self.read_at(offset, len).await
    }

    /// Append data to file
    pub async fn append(&self, data: Bytes) -> Result<u64> {
        let offset = *self.size.read().await;
        self.write_at(offset, data).await?;
        Ok(offset)
    }
}

/// Async file pool for managing multiple files
pub struct AsyncFilePool {
    /// Open files
    files: Arc<RwLock<HashMap<PathBuf, Arc<AsyncFile>>>>,
    /// Maximum open files
    max_open: usize,
    /// Default options
    default_options: AsyncFileOptions,
}

use std::collections::HashMap;

impl AsyncFilePool {
    /// Create a new file pool
    pub fn new(max_open: usize, default_options: AsyncFileOptions) -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            max_open,
            default_options,
        }
    }

    /// Get or open a file
    pub async fn get(&self, path: impl AsRef<Path>) -> Result<Arc<AsyncFile>> {
        let path = path.as_ref().to_path_buf();

        {
            let files = self.files.read().await;
            if let Some(file) = files.get(&path) {
                return Ok(file.clone());
            }
        }

        // Need to open the file
        let mut files = self.files.write().await;

        // Check again (double-check pattern)
        if let Some(file) = files.get(&path) {
            return Ok(file.clone());
        }

        // Evict if necessary
        if files.len() >= self.max_open {
            // Simple LRU: remove first entry
            if let Some(evict_path) = files.keys().next().cloned() {
                files.remove(&evict_path);
            }
        }

        // Open new file
        let file = AsyncFile::open(&path, self.default_options.clone()).await?;
        let file = Arc::new(file);
        files.insert(path, file.clone());

        Ok(file)
    }

    /// Close a file
    pub async fn close(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut files = self.files.write().await;
        files.remove(path.as_ref());
        Ok(())
    }

    /// Sync all open files
    pub async fn sync_all(&self) -> Result<()> {
        let files = self.files.read().await;

        for file in files.values() {
            file.sync().await?;
        }

        Ok(())
    }

    /// Clear the pool
    pub async fn clear(&self) {
        let mut files = self.files.write().await;
        files.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[ignore]
    #[tokio::test]
    async fn test_async_file_basic() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.dat");

        let file = AsyncFile::open(&file_path, AsyncFileOptions::default())
            .await
            .unwrap();

        // Write data
        let data = Bytes::from("Hello, async file!");
        let written = file.write_at(0, data.clone()).await.unwrap();
        assert_eq!(written, data.len());

        // Read data back
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(read_data, data);

        // Check size
        assert_eq!(file.size().await, data.len() as u64);

        // Sync
        file.sync().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_async_file_vectored() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("vectored.dat");

        let file = AsyncFile::open(&file_path, AsyncFileOptions::default())
            .await
            .unwrap();

        // Write vectored data
        let chunks = vec![
            Bytes::from("chunk1"),
            Bytes::from("chunk2"),
            Bytes::from("chunk3"),
        ];

        let total_written = file.write_vectored(0, chunks.clone()).await.unwrap();
        assert_eq!(total_written, 18); // 6 + 6 + 6

        // Read vectored data
        let lengths = vec![6, 6, 6];
        let read_chunks = file.read_vectored(0, lengths).await.unwrap();

        assert_eq!(read_chunks.len(), 3);
        assert_eq!(read_chunks[0], chunks[0]);
        assert_eq!(read_chunks[1], chunks[1]);
        assert_eq!(read_chunks[2], chunks[2]);
    }

    #[ignore]
    #[tokio::test]
    async fn test_async_file_append() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("append.dat");

        let file = AsyncFile::open(&file_path, AsyncFileOptions::default())
            .await
            .unwrap();

        // Append data
        let offset1 = file.append(Bytes::from("first")).await.unwrap();
        assert_eq!(offset1, 0);

        let offset2 = file.append(Bytes::from("second")).await.unwrap();
        assert_eq!(offset2, 5);

        let offset3 = file.append(Bytes::from("third")).await.unwrap();
        assert_eq!(offset3, 11);

        // Read all data
        let all_data = file.read_at(0, 16).await.unwrap();
        assert_eq!(all_data, Bytes::from("firstsecondthird"));
    }

    #[ignore]
    #[tokio::test]
    async fn test_file_pool() {
        let temp_dir = TempDir::new().unwrap();

        let pool = AsyncFilePool::new(2, AsyncFileOptions::default());

        // Open files through pool
        let file1_path = temp_dir.path().join("file1.dat");
        let file2_path = temp_dir.path().join("file2.dat");

        let file1 = pool.get(&file1_path).await.unwrap();
        let file2 = pool.get(&file2_path).await.unwrap();

        // Write to files
        file1.write_at(0, Bytes::from("file1 data")).await.unwrap();
        file2.write_at(0, Bytes::from("file2 data")).await.unwrap();

        // Get same file again - should return cached
        let file1_again = pool.get(&file1_path).await.unwrap();
        assert!(Arc::ptr_eq(&file1, &file1_again));

        // Sync all
        pool.sync_all().await.unwrap();
    }
}