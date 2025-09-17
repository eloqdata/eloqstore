//! Core types used throughout EloqStore

use std::fmt;
use std::path::PathBuf;
use bytes::Bytes;

/// Key type
pub type Key = Bytes;

/// Value type
pub type Value = Bytes;

/// Page identifier (32-bit)
pub type PageId = u32;

/// Maximum valid page ID
pub const MAX_PAGE_ID: PageId = u32::MAX;

// FilePageId module and re-exports
pub mod file_page_id;
pub use file_page_id::{FilePageId, MAX_FILE_PAGE_ID};

/// File identifier
pub type FileId = u64;

/// Maximum valid file ID
pub const MAX_FILE_ID: FileId = u64::MAX;

/// Default page size (4KB)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Maximum number of overflow pointers in a value
pub const MAX_OVERFLOW_POINTERS: usize = 8;

/// File name constants
pub const FILE_NAME_SEPARATOR: char = '_';
pub const FILE_NAME_DATA: &str = "data";
pub const FILE_NAME_MANIFEST: &str = "manifest";
pub const TMP_SUFFIX: &str = ".tmp";

/// Table identifier with partition information
#[derive(Debug, Clone, Hash, PartialEq, Eq, Default)]
pub struct TableIdent {
    /// Table name
    pub table_name: String,
    /// Partition ID
    pub partition_id: u32,
}

impl TableIdent {
    /// Create a new table identifier
    pub fn new(table_name: impl Into<String>, partition_id: u32) -> Self {
        Self {
            table_name: table_name.into(),
            partition_id,
        }
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        format!("{}.{}", self.table_name, self.partition_id)
    }

    /// Parse from string representation
    pub fn from_string(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            return None;
        }
        let partition_id = parts[1].parse().ok()?;
        Some(Self {
            table_name: parts[0].to_string(),
            partition_id,
        })
    }

    /// Get shard index for this table partition
    pub fn shard_index(&self, num_shards: u16) -> u16 {
        (self.partition_id as u16) % num_shards
    }

    /// Get disk index for this table partition
    pub fn disk_index(&self, num_disks: u8) -> u8 {
        (self.partition_id as u8) % num_disks
    }

    /// Get storage path for this table partition
    pub fn storage_path(&self, data_dirs: &[PathBuf]) -> PathBuf {
        let disk_idx = self.disk_index(data_dirs.len() as u8);
        data_dirs[disk_idx as usize].join(self.to_string())
    }

    /// Check if this identifier is valid
    pub fn is_valid(&self) -> bool {
        !self.table_name.is_empty()
    }
}

impl fmt::Display for TableIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table_name, self.partition_id)
    }
}

/// File key for file operations
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FileKey {
    /// Table identifier
    pub table_id: TableIdent,
    /// File name
    pub filename: String,
}

/// Key-value entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    /// Key
    pub key: Vec<u8>,
    /// Value
    pub value: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Expiration timestamp (0 means no expiration)
    pub expire_ts: u64,
}

/// Write operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// Insert or update
    Upsert = 0,
    /// Delete
    Delete = 1,
}

/// Write data entry
#[derive(Debug, Clone)]
pub struct WriteDataEntry {
    /// Key
    pub key: Vec<u8>,
    /// Value
    pub value: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Operation type
    pub op: WriteOp,
    /// Expiration timestamp
    pub expire_ts: u64,
}

impl WriteDataEntry {
    /// Create a new upsert entry
    pub fn upsert(key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            value,
            timestamp,
            op: WriteOp::Upsert,
            expire_ts: 0,
        }
    }

    /// Create a new delete entry
    pub fn delete(key: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            value: Vec::new(),
            timestamp,
            op: WriteOp::Delete,
            expire_ts: 0,
        }
    }

    /// Set expiration timestamp
    pub fn with_expire(mut self, expire_ts: u64) -> Self {
        self.expire_ts = expire_ts;
        self
    }
}

/// Page type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Data page
    Data = 1,
    /// Index page
    Index = 2,
    /// Overflow page
    Overflow = 3,
    /// Meta page
    Meta = 4,
    /// Leaf index page
    LeafIndex = 5,
    /// Non-leaf index page
    NonLeafIndex = 6,
}

impl PageType {
    /// Convert from u8
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(PageType::Data),
            2 => Some(PageType::Index),
            3 => Some(PageType::Overflow),
            4 => Some(PageType::Meta),
            5 => Some(PageType::LeafIndex),
            6 => Some(PageType::NonLeafIndex),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_ident() {
        let tid = TableIdent::new("users", 42);
        assert_eq!(tid.to_string(), "users.42");

        let parsed = TableIdent::from_string("users.42").unwrap();
        assert_eq!(parsed, tid);

        assert_eq!(tid.shard_index(4), 2);
        assert_eq!(tid.disk_index(3), 0);
    }

    #[test]
    fn test_write_data_entry() {
        let entry = WriteDataEntry::upsert(
            b"key".to_vec(),
            b"value".to_vec(),
            12345
        ).with_expire(67890);

        assert_eq!(entry.op, WriteOp::Upsert);
        assert_eq!(entry.expire_ts, 67890);
    }
}