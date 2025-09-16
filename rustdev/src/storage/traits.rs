//! Storage traits and abstractions

use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

use crate::types::{FileId, FilePageId, TableIdent};
use crate::page::{Page, MappingSnapshot};
use crate::Result;

/// Storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Initialize the storage backend
    async fn init(&self) -> Result<()>;

    /// Create a new file
    async fn create_file(&self, table_id: &TableIdent) -> Result<FileId>;

    /// Delete a file
    async fn delete_file(&self, file_id: FileId) -> Result<()>;

    /// Read a page
    async fn read_page(&self, file_id: FileId, page_offset: u64) -> Result<Page>;

    /// Write a page
    async fn write_page(&self, file_id: FileId, page_offset: u64, page: &Page) -> Result<()>;

    /// Read multiple pages
    async fn read_pages(&self, file_id: FileId, offsets: Vec<u64>) -> Result<Vec<Page>>;

    /// Write multiple pages
    async fn write_pages(&self, file_id: FileId, pages: Vec<(u64, Page)>) -> Result<()>;

    /// Sync data to persistent storage
    async fn sync(&self, file_id: FileId) -> Result<()>;

    /// Sync all files
    async fn sync_all(&self) -> Result<()>;

    /// List all files for a table
    async fn list_files(&self, table_id: &TableIdent) -> Result<Vec<FileId>>;

    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total bytes used
    pub bytes_used: u64,
    /// Total bytes available
    pub bytes_available: u64,
    /// Number of files
    pub file_count: usize,
    /// Number of pages
    pub page_count: u64,
    /// Read operations count
    pub reads: u64,
    /// Write operations count
    pub writes: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
}

/// Metadata store trait for persistent metadata
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Save a mapping snapshot
    async fn save_snapshot(&self, table_id: &TableIdent, snapshot: &MappingSnapshot) -> Result<()>;

    /// Load the latest snapshot
    async fn load_snapshot(&self, table_id: &TableIdent) -> Result<Option<MappingSnapshot>>;

    /// List all snapshots for a table
    async fn list_snapshots(&self, table_id: &TableIdent) -> Result<Vec<u64>>;

    /// Delete old snapshots
    async fn cleanup_snapshots(&self, table_id: &TableIdent, keep_latest: usize) -> Result<()>;

    /// Save table metadata
    async fn save_table_metadata(&self, table_id: &TableIdent, metadata: &TableMetadata) -> Result<()>;

    /// Load table metadata
    async fn load_table_metadata(&self, table_id: &TableIdent) -> Result<Option<TableMetadata>>;
}

/// Table metadata
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Table identifier
    pub table_id: TableIdent,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
    /// Number of keys
    pub key_count: u64,
    /// Total data size
    pub data_size: u64,
    /// Compression enabled
    pub compression: bool,
    /// Custom metadata
    pub custom: Vec<(String, String)>,
}

/// Cloud storage trait for tiered storage
#[async_trait]
pub trait CloudStorage: Send + Sync {
    /// Upload a file to cloud storage
    async fn upload(&self, local_path: &Path, remote_key: &str) -> Result<()>;

    /// Download a file from cloud storage
    async fn download(&self, remote_key: &str, local_path: &Path) -> Result<()>;

    /// Delete a file from cloud storage
    async fn delete(&self, remote_key: &str) -> Result<()>;

    /// List files in cloud storage
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if a file exists
    async fn exists(&self, remote_key: &str) -> Result<bool>;

    /// Get file metadata
    async fn metadata(&self, remote_key: &str) -> Result<CloudFileMetadata>;
}

/// Cloud file metadata
#[derive(Debug, Clone)]
pub struct CloudFileMetadata {
    /// File key
    pub key: String,
    /// File size
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: u64,
    /// Storage class
    pub storage_class: String,
    /// ETag/checksum
    pub etag: String,
}

/// Cache trait for hot data
#[async_trait]
pub trait Cache: Send + Sync {
    /// Get a value from cache
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Put a value into cache
    async fn put(&self, key: &[u8], value: Bytes) -> Result<()>;

    /// Delete a value from cache
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Check if key exists in cache
    async fn contains(&self, key: &[u8]) -> Result<bool>;

    /// Clear the cache
    async fn clear(&self) -> Result<()>;

    /// Get cache statistics
    async fn stats(&self) -> Result<CacheStats>;
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of entries
    pub entries: usize,
    /// Total size in bytes
    pub bytes_used: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Evictions
    pub evictions: u64,
}

/// Compaction strategy trait
pub trait CompactionStrategy: Send + Sync {
    /// Check if compaction is needed
    fn should_compact(&self, stats: &CompactionStats) -> bool;

    /// Select files for compaction
    fn select_files(&self, files: &[FileInfo]) -> Vec<FileId>;

    /// Get compaction priority
    fn priority(&self, file: &FileInfo) -> u32;
}

/// File information for compaction
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// File ID
    pub file_id: FileId,
    /// File size
    pub size: u64,
    /// Number of entries
    pub entries: u64,
    /// Number of deleted entries
    pub deleted_entries: u64,
    /// Creation time
    pub created_at: u64,
    /// Level (for LSM-style compaction)
    pub level: u32,
}

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total file count
    pub file_count: usize,
    /// Total size
    pub total_size: u64,
    /// Space that can be reclaimed
    pub reclaimable_space: u64,
    /// Number of deleted entries
    pub deleted_entries: u64,
    /// Last compaction time
    pub last_compaction: u64,
}