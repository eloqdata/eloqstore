//! Request types for database operations

use bytes::Bytes;
use std::time::Duration;

/// Table identifier
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TableIdent {
    pub table_name: String,
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

    /// Get shard index for this table partition
    pub fn shard_index(&self, num_shards: u16) -> u16 {
        (self.partition_id as u16) % num_shards
    }

    /// Get disk index for this table partition
    pub fn disk_index(&self, num_disks: u8) -> u8 {
        (self.partition_id as u8) % num_disks
    }
}

/// Write operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// Insert or update a key
    Upsert,
    /// Delete a key
    Delete,
}

/// A single write entry
#[derive(Debug, Clone)]
pub struct WriteEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub op: WriteOp,
    pub timestamp: u64,
    pub expire_ts: Option<u64>,
}

/// Request types
#[derive(Debug)]
pub enum Request {
    /// Read a single key
    Read(ReadRequest),
    /// Find the floor key (largest key <= search key)
    Floor(FloorRequest),
    /// Scan a range of keys
    Scan(ScanRequest),
    /// Batch write operations
    BatchWrite(BatchWriteRequest),
    /// Truncate a table
    Truncate(TruncateRequest),
    /// Archive old data
    Archive(ArchiveRequest),
    /// Compact data files
    Compact(CompactRequest),
    /// Clean expired keys
    CleanExpired(CleanExpiredRequest),
}

/// Read request
#[derive(Debug)]
pub struct ReadRequest {
    pub table_id: TableIdent,
    pub key: Bytes,
    pub timeout: Option<Duration>,
}

/// Floor request - find the largest key <= search key
#[derive(Debug)]
pub struct FloorRequest {
    pub table_id: TableIdent,
    pub key: Bytes,
    pub timeout: Option<Duration>,
}

/// Scan request - iterate over a range of keys
#[derive(Debug)]
pub struct ScanRequest {
    pub table_id: TableIdent,
    pub start_key: Bytes,
    pub end_key: Option<Bytes>,
    pub limit: Option<usize>,
    pub reverse: bool,
    pub timeout: Option<Duration>,
}

/// Batch write request
#[derive(Debug)]
pub struct BatchWriteRequest {
    pub table_id: TableIdent,
    pub entries: Vec<WriteEntry>,
    pub sync: bool,
    pub timeout: Option<Duration>,
}

/// Truncate request - delete all data in a table
#[derive(Debug)]
pub struct TruncateRequest {
    pub table_id: TableIdent,
}

/// Archive request - move old data to archive storage
#[derive(Debug)]
pub struct ArchiveRequest {
    pub table_id: TableIdent,
    pub before_timestamp: u64,
}

/// Compact request - compact data files
#[derive(Debug)]
pub struct CompactRequest {
    pub table_id: TableIdent,
    pub level: Option<u32>,
}

/// Clean expired request - remove expired keys
#[derive(Debug)]
pub struct CleanExpiredRequest {
    pub table_id: TableIdent,
    pub current_time: u64,
}

/// Request builder for fluent API
pub struct RequestBuilder;

impl RequestBuilder {
    /// Create a new read request builder
    pub fn read(table_id: TableIdent, key: impl Into<Bytes>) -> ReadRequestBuilder {
        ReadRequestBuilder {
            table_id,
            key: key.into(),
            timeout: None,
        }
    }

    /// Create a new batch write request builder
    pub fn batch_write(table_id: TableIdent) -> BatchWriteRequestBuilder {
        BatchWriteRequestBuilder {
            table_id,
            entries: Vec::new(),
            sync: false,
            timeout: None,
        }
    }

    /// Create a new scan request builder
    pub fn scan(table_id: TableIdent, start_key: impl Into<Bytes>) -> ScanRequestBuilder {
        ScanRequestBuilder {
            table_id,
            start_key: start_key.into(),
            end_key: None,
            limit: None,
            reverse: false,
            timeout: None,
        }
    }
}

/// Read request builder
pub struct ReadRequestBuilder {
    table_id: TableIdent,
    key: Bytes,
    timeout: Option<Duration>,
}

impl ReadRequestBuilder {
    /// Set timeout for the request
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Build the read request
    pub fn build(self) -> ReadRequest {
        ReadRequest {
            table_id: self.table_id,
            key: self.key,
            timeout: self.timeout,
        }
    }
}

/// Batch write request builder
pub struct BatchWriteRequestBuilder {
    table_id: TableIdent,
    entries: Vec<WriteEntry>,
    sync: bool,
    timeout: Option<Duration>,
}

impl BatchWriteRequestBuilder {
    /// Add an upsert operation
    pub fn upsert(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>, timestamp: u64) -> Self {
        self.entries.push(WriteEntry {
            key: key.into(),
            value: value.into(),
            op: WriteOp::Upsert,
            timestamp,
            expire_ts: None,
        });
        self
    }

    /// Add a delete operation
    pub fn delete(mut self, key: impl Into<Bytes>, timestamp: u64) -> Self {
        self.entries.push(WriteEntry {
            key: key.into(),
            value: Bytes::new(),
            op: WriteOp::Delete,
            timestamp,
            expire_ts: None,
        });
        self
    }

    /// Enable sync writes
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Build the batch write request
    pub fn build(self) -> BatchWriteRequest {
        BatchWriteRequest {
            table_id: self.table_id,
            entries: self.entries,
            sync: self.sync,
            timeout: self.timeout,
        }
    }
}

/// Scan request builder
pub struct ScanRequestBuilder {
    table_id: TableIdent,
    start_key: Bytes,
    end_key: Option<Bytes>,
    limit: Option<usize>,
    reverse: bool,
    timeout: Option<Duration>,
}

impl ScanRequestBuilder {
    /// Set end key for the scan
    pub fn end_key(mut self, key: impl Into<Bytes>) -> Self {
        self.end_key = Some(key.into());
        self
    }

    /// Set limit for the scan
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Enable reverse scan
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Build the scan request
    pub fn build(self) -> ScanRequest {
        ScanRequest {
            table_id: self.table_id,
            start_key: self.start_key,
            end_key: self.end_key,
            limit: self.limit,
            reverse: self.reverse,
            timeout: self.timeout,
        }
    }
}