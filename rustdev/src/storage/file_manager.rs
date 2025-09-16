//! File manager for handling file operations

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use bytes::{Bytes, BytesMut};

use crate::types::{FileId, FilePageId, TableIdent, DEFAULT_PAGE_SIZE};
use crate::page::Page;
use crate::Result;
use crate::error::Error;

/// File metadata
#[derive(Debug, Clone)]
pub struct FileMetadata {
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
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
}

/// File handle wrapper
struct FileHandle {
    /// File ID
    file_id: FileId,
    /// Underlying file
    file: File,
    /// File metadata
    metadata: FileMetadata,
}

impl FileHandle {
    /// Create a new file handle
    fn new(file_id: FileId, path: PathBuf, page_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let file_metadata = file.metadata()?;
        let size = file_metadata.len();

        let metadata = FileMetadata {
            file_id,
            path,
            size,
            page_count: size / page_size as u64,
            page_size,
            created_at: 0, // TODO: Get actual timestamps
            modified_at: 0,
        };

        Ok(Self {
            file_id,
            file,
            metadata,
        })
    }

    /// Read a page at the given offset
    fn read_page(&mut self, page_offset: u64, page_size: usize) -> io::Result<Page> {
        let mut buffer = BytesMut::zeroed(page_size);
        let file_offset = page_offset * page_size as u64;

        self.file.seek(SeekFrom::Start(file_offset))?;
        self.file.read_exact(&mut buffer)?;

        Ok(Page::from_bytes(buffer.freeze()))
    }

    /// Write a page at the given offset
    fn write_page(&mut self, page_offset: u64, page: &Page) -> io::Result<()> {
        let file_offset = page_offset * page.size() as u64;

        self.file.seek(SeekFrom::Start(file_offset))?;
        self.file.write_all(page.as_bytes())?;

        // Update metadata
        let new_size = (page_offset + 1) * page.size() as u64;
        if new_size > self.metadata.size {
            self.metadata.size = new_size;
            self.metadata.page_count = new_size / self.metadata.page_size as u64;
        }

        Ok(())
    }

    /// Sync data to disk
    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Truncate file to specified size
    fn truncate(&mut self, size: u64) -> io::Result<()> {
        self.file.set_len(size)?;
        self.metadata.size = size;
        self.metadata.page_count = size / self.metadata.page_size as u64;
        Ok(())
    }
}

/// File manager for managing data files
pub struct FileManager {
    /// Base directory for data files
    base_dir: PathBuf,
    /// Page size
    page_size: usize,
    /// Open file handles
    handles: Arc<RwLock<HashMap<FileId, FileHandle>>>,
    /// Next file ID
    next_file_id: Arc<RwLock<FileId>>,
    /// Maximum open files
    max_open_files: usize,
}

impl FileManager {
    /// Create a new file manager
    pub fn new(base_dir: impl AsRef<Path>, page_size: usize, max_open_files: usize) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            page_size,
            handles: Arc::new(RwLock::new(HashMap::new())),
            next_file_id: Arc::new(RwLock::new(0)),
            max_open_files,
        }
    }

    /// Initialize the file manager
    pub fn init(&self) -> Result<()> {
        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&self.base_dir)
            .map_err(|e| Error::Io(e))?;
        Ok(())
    }

    /// Create a new file
    pub fn create_file(&self, table_id: &TableIdent) -> Result<FileId> {
        let mut next_id = self.next_file_id.write().unwrap();
        let file_id = *next_id;
        *next_id += 1;

        let file_name = format!("{}_{:06}.dat", table_id.to_string(), file_id);
        let file_path = self.base_dir.join(&file_name);

        let handle = FileHandle::new(file_id, file_path, self.page_size)
            .map_err(|e| Error::Io(e))?;

        let mut handles = self.handles.write().unwrap();

        // Check if we need to evict files
        if handles.len() >= self.max_open_files {
            // Simple LRU eviction - remove the first entry
            if let Some(&evict_id) = handles.keys().next() {
                handles.remove(&evict_id);
            }
        }

        handles.insert(file_id, handle);

        Ok(file_id)
    }

    /// Open an existing file
    pub fn open_file(&self, file_id: FileId, path: impl AsRef<Path>) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        if handles.contains_key(&file_id) {
            return Ok(()); // Already open
        }

        // Check if we need to evict files
        if handles.len() >= self.max_open_files {
            if let Some(&evict_id) = handles.keys().next() {
                handles.remove(&evict_id);
            }
        }

        let handle = FileHandle::new(file_id, path.as_ref().to_path_buf(), self.page_size)
            .map_err(|e| Error::Io(e))?;

        handles.insert(file_id, handle);

        Ok(())
    }

    /// Close a file
    pub fn close_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().unwrap();
        handles.remove(&file_id);
        Ok(())
    }

    /// Read a page from a file
    pub fn read_page(&self, file_id: FileId, page_offset: u64) -> Result<Page> {
        let mut handles = self.handles.write().unwrap();

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.read_page(page_offset, self.page_size)
            .map_err(|e| Error::Io(e))
    }

    /// Write a page to a file
    pub fn write_page(&self, file_id: FileId, page_offset: u64, page: &Page) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.write_page(page_offset, page)
            .map_err(|e| Error::Io(e))
    }

    /// Read multiple pages
    pub fn read_pages(&self, file_id: FileId, offsets: &[u64]) -> Result<Vec<Page>> {
        let mut pages = Vec::with_capacity(offsets.len());

        for &offset in offsets {
            pages.push(self.read_page(file_id, offset)?);
        }

        Ok(pages)
    }

    /// Write multiple pages
    pub fn write_pages(&self, file_id: FileId, pages: &[(u64, &Page)]) -> Result<()> {
        for (offset, page) in pages {
            self.write_page(file_id, *offset, page)?;
        }
        Ok(())
    }

    /// Sync a file to disk
    pub fn sync_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.sync()
            .map_err(|e| Error::Io(e))
    }

    /// Sync all files to disk
    pub fn sync_all(&self) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        for handle in handles.values_mut() {
            handle.sync()
                .map_err(|e| Error::Io(e))?;
        }

        Ok(())
    }

    /// Delete a file
    pub fn delete_file(&self, file_id: FileId) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        if let Some(handle) = handles.remove(&file_id) {
            // Close and delete the file
            drop(handle.file);
            std::fs::remove_file(&handle.metadata.path)
                .map_err(|e| Error::Io(e))?;
        }

        Ok(())
    }

    /// Get file metadata
    pub fn get_metadata(&self, file_id: FileId) -> Result<FileMetadata> {
        let handles = self.handles.read().unwrap();

        let handle = handles.get(&file_id)
            .ok_or(Error::NotFound)?;

        Ok(handle.metadata.clone())
    }

    /// List all files for a table
    pub fn list_files(&self, table_id: &TableIdent) -> Result<Vec<PathBuf>> {
        let prefix = format!("{}_", table_id.to_string());
        let mut files = Vec::new();

        let entries = std::fs::read_dir(&self.base_dir)
            .map_err(|e| Error::Io(e))?;

        for entry in entries {
            let entry = entry.map_err(|e| Error::Io(e))?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            if file_name_str.starts_with(&prefix) && file_name_str.ends_with(".dat") {
                files.push(entry.path());
            }
        }

        files.sort();
        Ok(files)
    }

    /// Truncate a file
    pub fn truncate_file(&self, file_id: FileId, size: u64) -> Result<()> {
        let mut handles = self.handles.write().unwrap();

        let handle = handles.get_mut(&file_id)
            .ok_or(Error::NotFound)?;

        handle.truncate(size)
            .map_err(|e| Error::Io(e))
    }

    /// Get the base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Get the page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_manager_create() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);

        manager.init().unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).unwrap();

        assert_eq!(file_id, 0);

        // Check file exists
        let files = manager.list_files(&table_id).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_file_manager_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);

        manager.init().unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).unwrap();

        // Write a page
        let mut page = Page::new(DEFAULT_PAGE_SIZE);
        page.set_content_length(100);
        page.update_checksum();

        manager.write_page(file_id, 0, &page).unwrap();

        // Read it back
        let read_page = manager.read_page(file_id, 0).unwrap();
        assert_eq!(read_page.content_length(), 100);
        assert!(read_page.verify_checksum());
    }

    #[test]
    fn test_file_manager_multiple_pages() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);

        manager.init().unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).unwrap();

        // Write multiple pages
        let mut pages_to_write = Vec::new();
        for i in 0..5 {
            let mut page = Page::new(DEFAULT_PAGE_SIZE);
            page.set_content_length(i * 10);
            page.update_checksum();
            pages_to_write.push((i as u64, page));
        }

        let pages_refs: Vec<(u64, &Page)> = pages_to_write.iter()
            .map(|(offset, page)| (*offset, page))
            .collect();

        manager.write_pages(file_id, &pages_refs).unwrap();

        // Read them back
        let offsets: Vec<u64> = (0..5).collect();
        let read_pages = manager.read_pages(file_id, &offsets).unwrap();

        assert_eq!(read_pages.len(), 5);
        for (i, page) in read_pages.iter().enumerate() {
            assert_eq!(page.content_length(), (i * 10) as u16);
            assert!(page.verify_checksum());
        }
    }

    #[test]
    fn test_file_manager_delete() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);

        manager.init().unwrap();

        let table_id = TableIdent::new("test", 1);
        let file_id = manager.create_file(&table_id).unwrap();

        // Write a page
        let page = Page::new(DEFAULT_PAGE_SIZE);
        manager.write_page(file_id, 0, &page).unwrap();

        // Delete the file
        manager.delete_file(file_id).unwrap();

        // Check file is gone
        let files = manager.list_files(&table_id).unwrap();
        assert_eq!(files.len(), 0);

        // Reading should fail
        assert!(manager.read_page(file_id, 0).is_err());
    }
}