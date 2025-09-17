//! Async file manager using the I/O backend abstraction
//!
//! This module provides an async file manager that uses the pluggable
//! I/O backend system for all file operations.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::types::{FileId, TableIdent};
use crate::page::Page;
use crate::Result;
use crate::error::Error;
use crate::io::backend::{IoBackend, FileHandle};

/// Async file metadata
#[derive(Debug, Clone)]
pub struct AsyncFileMetadata {
    /// File ID
    pub file_id: FileId,
    /// File path
    pub path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Number of pages
    pub page_count: u64,
    /// Page size
    pub page_size: usize,
    /// Creation timestamp
    pub created_at: Option<std::time::SystemTime>,
    /// Last modified timestamp
    pub modified_at: Option<std::time::SystemTime>,
}

/// Async file handle wrapper
struct AsyncFileHandle {
    /// File ID
    file_id: FileId,
    /// Backend file handle
    handle: Arc<dyn FileHandle>,
    /// File metadata
    metadata: AsyncFileMetadata,
}

impl std::fmt::Debug for AsyncFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFileHandle")
            .field("file_id", &self.file_id)
            .field("handle", &"<dyn FileHandle>")
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl AsyncFileHandle {
    /// Read a page at the given offset
    async fn read_page(&self, page_offset: u64, page_size: usize) -> Result<Page> {
        let file_offset = page_offset * page_size as u64;
        let data = self.handle.read_exact(file_offset, page_size).await?;
        Ok(Page::from_bytes(data))
    }

    /// Write a page at the given offset
    async fn write_page(&mut self, page_offset: u64, page: &Page) -> Result<()> {
        let file_offset = page_offset * page.size() as u64;
        self.handle.write_at(file_offset, page.as_bytes()).await?;

        // CRITICAL: Sync immediately for durability without WAL
        // This matches C++ behavior where writes must be durable before returning
        self.handle.sync_data().await?;

        // Update metadata
        let new_size = (page_offset + 1) * page.size() as u64;
        if new_size > self.metadata.size {
            self.metadata.size = new_size;
            self.metadata.page_count = new_size / self.metadata.page_size as u64;
        }

        Ok(())
    }

    /// Sync data to disk
    async fn sync(&self) -> Result<()> {
        self.handle.sync().await
    }

    /// Sync data only (not metadata)
    async fn sync_data(&self) -> Result<()> {
        self.handle.sync_data().await
    }

    /// Truncate file
    async fn truncate(&mut self, size: u64) -> Result<()> {
        self.handle.truncate(size).await?;
        self.metadata.size = size;
        self.metadata.page_count = size / self.metadata.page_size as u64;
        Ok(())
    }

    /// Get file size
    async fn file_size(&self) -> Result<u64> {
        self.handle.file_size().await
    }
}

/// Async file manager for managing data files
pub struct AsyncFileManager {
    /// Base directory for data files
    base_dir: PathBuf,
    /// Page size
    page_size: usize,
    /// I/O backend
    backend: Arc<dyn IoBackend>,
    /// Open file handles
    handles: Arc<RwLock<HashMap<FileId, AsyncFileHandle>>>,
    /// Next file ID
    next_file_id: Arc<RwLock<FileId>>,
    /// Maximum open files
    max_open_files: usize,
}

impl std::fmt::Debug for AsyncFileManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFileManager")
            .field("base_dir", &self.base_dir)
            .field("page_size", &self.page_size)
            .field("backend", &"<dyn IoBackend>")
            .field("max_open_files", &self.max_open_files)
            .finish()
    }
}

impl AsyncFileManager {
    /// Create a new async file manager
    pub fn new(
        base_dir: impl AsRef<Path>,
        page_size: usize,
        max_open_files: usize,
        backend: Arc<dyn IoBackend>,
    ) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            page_size,
            backend,
            handles: Arc::new(RwLock::new(HashMap::new())),
            next_file_id: Arc::new(RwLock::new(0)),
            max_open_files,
        }
    }

    /// Initialize the file manager
    pub async fn init(&self) -> Result<()> {
        // Create base directory if it doesn't exist
        self.backend.create_dir_all(&self.base_dir).await?;

        // Open existing data files
        self.open_existing_files().await
    }

    /// Open existing data files in the directory
    async fn open_existing_files(&self) -> Result<()> {
        use tokio::fs;

        tracing::debug!("Scanning directory {:?} for existing files", self.base_dir);

        let mut entries = fs::read_dir(&self.base_dir).await?;
        let mut max_file_id = 0u64;
        let mut opened_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(file_name) = path.file_name() {
                let name = file_name.to_string_lossy();
                tracing::debug!("Found file: {}", name);

                // Look for data files with pattern: table_name_XXXXXX.dat
                if name.ends_with(".dat") {
                    // Extract file_id from name
                    if let Some(underscore_pos) = name.rfind('_') {
                        if let Some(dot_pos) = name.rfind('.') {
                            if underscore_pos < dot_pos {
                                let id_str = &name[underscore_pos + 1..dot_pos];
                                if let Ok(file_id) = id_str.parse::<u64>() {
                                    tracing::info!("Opening existing file: {} with id={}", name, file_id);
                                    opened_count += 1;

                                    // Open the file
                                    let handle = self.backend.open_file(&path, false).await?;

                                    // Get file metadata
                                    let metadata = self.backend.metadata(&path).await?;

                                    let async_metadata = AsyncFileMetadata {
                                        file_id,
                                        path: path.clone(),
                                        size: metadata.size,
                                        page_count: metadata.size / self.page_size as u64,
                                        page_size: self.page_size,
                                        created_at: metadata.created,
                                        modified_at: metadata.modified,
                                    };

                                    let async_handle = AsyncFileHandle {
                                        file_id,
                                        handle,
                                        metadata: async_metadata,
                                    };

                                    // Store in handles map
                                    let mut handles = self.handles.write().await;
                                    handles.insert(file_id, async_handle);

                                    max_file_id = max_file_id.max(file_id);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update next_file_id to be after the max existing file
        let mut next_id = self.next_file_id.write().await;
        if opened_count > 0 {
            *next_id = max_file_id + 1;
        } else {
            *next_id = 0; // Start from 0 if no existing files
        }

        tracing::info!("Opened {} existing files, next file_id will be {}",
                      opened_count, *next_id);

        Ok(())
    }

    /// Create a new file
    pub async fn create_file(&self, table_id: &TableIdent) -> Result<FileId> {
        let mut next_id = self.next_file_id.write().await;
        let file_id = *next_id;
        *next_id += 1;

        let file_name = format!("{}_{:06}.dat", table_id.to_string(), file_id);
        let file_path = self.base_dir.join(&file_name);

        // Open/create the file
        let handle = self.backend.open_file(&file_path, true).await?;

        // Get file metadata
        let metadata = self.backend.metadata(&file_path).await?;

        let async_metadata = AsyncFileMetadata {
            file_id,
            path: file_path,
            size: metadata.size,
            page_count: metadata.size / self.page_size as u64,
            page_size: self.page_size,
            created_at: metadata.created,
            modified_at: metadata.modified,
        };

        let async_handle = AsyncFileHandle {
            file_id,
            handle,
            metadata: async_metadata,
        };

        let mut handles = self.handles.write().await;

        // Check if we need to evict files
        if handles.len() >= self.max_open_files {
            // Simple LRU eviction - remove the first entry
            if let Some(&evict_id) = handles.keys().next() {
                handles.remove(&evict_id);
            }
        }

        handles.insert(file_id, async_handle);

        Ok(file_id)
    }

    /// Open an existing file
    pub async fn open_file(&self, file_id: FileId, path: impl AsRef<Path>) -> Result<()> {
        let mut handles = self.handles.write().await;

        if handles.contains_key(&file_id) {
            return Ok(()); // Already open
        }

        // Check if we need to evict files
        if handles.len() >= self.max_open_files {
            if let Some(&evict_id) = handles.keys().next() {
                handles.remove(&evict_id);
            }
        }

        // Open the file
        let handle = self.backend.open_file(path.as_ref(), false).await?;

        // Get file metadata
        let metadata = self.backend.metadata(path.as_ref()).await?;

        let async_metadata = AsyncFileMetadata {
            file_id,
            path: path.as_ref().to_path_buf(),
            size: metadata.size,
            page_count: metadata.size / self.page_size as u64,
            page_size: self.page_size,
            created_at: metadata.created,
            modified_at: metadata.modified,
        };

        let async_handle = AsyncFileHandle {
            file_id,
            handle,
            metadata: async_metadata,
        };

        handles.insert(file_id, async_handle);

        Ok(())
    }

    /// Close a file
    pub async fn close_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().await;
        handles.remove(&file_id);
        Ok(())
    }

    /// Read a page from a file
    pub async fn read_page(&self, file_id: FileId, page_offset: u64) -> Result<Page> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        handle.read_page(page_offset, self.page_size).await
    }

    /// Write a page to a file
    pub async fn write_page(&self, file_id: FileId, page_offset: u64, page: &Page) -> Result<()> {
        let mut handles = self.handles.write().await;

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.write_page(page_offset, page).await
    }

    /// Read multiple pages
    pub async fn read_pages(&self, file_id: FileId, offsets: &[u64]) -> Result<Vec<Page>> {
        let mut pages = Vec::with_capacity(offsets.len());

        for &offset in offsets {
            pages.push(self.read_page(file_id, offset).await?);
        }

        Ok(pages)
    }

    /// Write multiple pages
    pub async fn write_pages(&self, file_id: FileId, pages: &[(u64, &Page)]) -> Result<()> {
        for (offset, page) in pages {
            self.write_page(file_id, *offset, page).await?;
        }
        Ok(())
    }

    /// Sync a file to disk
    pub async fn sync_file(&self, file_id: FileId) -> Result<()> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        handle.sync().await
    }

    /// Sync all files to disk
    pub async fn sync_all(&self) -> Result<()> {
        let handles = self.handles.read().await;

        for handle in handles.values() {
            handle.sync().await?;
        }

        Ok(())
    }

    /// Delete a file
    pub async fn delete_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().await;

        if let Some(handle) = handles.remove(&file_id) {
            // Delete the file
            self.backend.delete_file(&handle.metadata.path).await?;
        }

        Ok(())
    }

    /// Get file metadata
    pub async fn get_metadata(&self, file_id: FileId) -> Result<AsyncFileMetadata> {
        let handles = self.handles.read().await;

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        Ok(handle.metadata.clone())
    }

    /// List all files for a table
    pub async fn list_files(&self, table_id: &TableIdent) -> Result<Vec<PathBuf>> {
        let prefix = format!("{}_", table_id.to_string());
        let mut files = Vec::new();

        let entries = self.backend.read_dir(&self.base_dir).await?;

        for entry in entries {
            if entry.starts_with(&prefix) && entry.ends_with(".dat") {
                files.push(self.base_dir.join(entry));
            }
        }

        files.sort();
        Ok(files)
    }

    /// Truncate a file
    pub async fn truncate_file(&self, file_id: FileId, size: u64) -> Result<()> {
        let mut handles = self.handles.write().await;

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.truncate(size).await
    }

    /// Get the base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Get the page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Get I/O statistics
    pub fn io_stats(&self) -> crate::io::backend::IoStats {
        self.backend.stats()
    }

    /// Shutdown the file manager
    pub async fn shutdown(&self) -> Result<()> {
        // Sync all files
        self.sync_all().await?;

        // Clear all handles
        self.handles.write().await.clear();

        // Shutdown the backend
        self.backend.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::types::DEFAULT_PAGE_SIZE;
    use crate::io::backend::factory::IoBackendFactory;
    use crate::io::backend::IoBackendType;

    #[tokio::test]
    async fn test_async_file_manager_create() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();
        let manager = AsyncFileManager::new(
            temp_dir.path(),
            DEFAULT_PAGE_SIZE,
            10,
            backend,
        );

        manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).await.unwrap();

        assert_eq!(file_id, 0);

        // Check file exists
        let files = manager.list_files(&table_id).await.unwrap();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_async_file_manager_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();
        let manager = AsyncFileManager::new(
            temp_dir.path(),
            DEFAULT_PAGE_SIZE,
            10,
            backend,
        );

        manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).await.unwrap();

        // Write a page
        let mut page = Page::new(DEFAULT_PAGE_SIZE);
        page.set_content_length(100);
        page.update_checksum();

        manager.write_page(file_id, 0, &page).await.unwrap();

        // Read it back
        let read_page = manager.read_page(file_id, 0).await.unwrap();
        assert_eq!(read_page.content_length(), 100);
        assert!(read_page.verify_checksum());
    }

    #[tokio::test]
    async fn test_async_file_manager_multiple_backends() {
        // Test with different backends
        let backends = vec![
            IoBackendType::Sync,
            IoBackendType::Tokio,
            IoBackendType::ThreadPool,
        ];

        for backend_type in backends {
            let temp_dir = TempDir::new().unwrap();
            let backend = IoBackendFactory::create_default(backend_type).unwrap();
            let manager = AsyncFileManager::new(
                temp_dir.path(),
                DEFAULT_PAGE_SIZE,
                10,
                backend,
            );

            manager.init().await.unwrap();

            let table_id = TableIdent::new("test", 1);
            let file_id = manager.create_file(&table_id).await.unwrap();

            // Write and read
            let mut page = Page::new(DEFAULT_PAGE_SIZE);
            page.set_content_length(42);
            page.update_checksum();

            manager.write_page(file_id, 0, &page).await.unwrap();
            let read_page = manager.read_page(file_id, 0).await.unwrap();

            assert_eq!(read_page.content_length(), 42);
            assert!(read_page.verify_checksum());

            // Verify stats
            let stats = manager.io_stats();
            assert!(stats.reads > 0);
            assert!(stats.writes > 0);
        }
    }
}