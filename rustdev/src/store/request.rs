//! Request types for EloqStore operations

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use parking_lot::Mutex;

use crate::types::{TableIdent, Key, Value, KvEntry, WriteDataEntry};
use crate::error::{Error, KvError};
use crate::Result;

/// Request type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Read,
    Floor,
    Scan,
    BatchWrite,
    Truncate,
    Archive,
    Compact,
    CleanExpired,
}

/// Base trait for all request types
pub trait KvRequest: Send + Sync {
    /// Get the request type
    fn request_type(&self) -> RequestType;

    /// Get the table identifier
    fn table_id(&self) -> &TableIdent;

    /// Get the error if any
    fn error(&self) -> Option<KvError>;

    /// Check if error is retryable
    fn is_retryable_error(&self) -> bool;

    /// Get user data
    fn user_data(&self) -> u64;

    /// Check if request is done
    fn is_done(&self) -> bool;

    /// Wait for request completion
    fn wait(&self);

    /// Set request as done with error
    fn set_done(&self, error: Option<KvError>);

    /// As any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Base request implementation
pub struct RequestBase {
    pub table_id: TableIdent,
    pub user_data: u64,
    pub callback: Arc<Mutex<Option<Box<dyn FnOnce(&dyn KvRequest) + Send>>>>,
    pub done: AtomicBool,
    pub error: Mutex<Option<KvError>>,
    pub waker: Mutex<Option<Waker>>,
}

impl RequestBase {
    pub fn new(table_id: TableIdent) -> Self {
        Self {
            table_id,
            user_data: 0,
            callback: Arc::new(Mutex::new(None)),
            done: AtomicBool::new(false),
            error: Mutex::new(None),
            waker: Mutex::new(None),
        }
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub fn set_done(&self, error: Option<KvError>) {
        *self.error.lock() = error;
        self.done.store(true, Ordering::Release);

        // Wake up any waiters
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }

        // Call callback if set
        // Note: This requires careful handling in actual implementation
    }

    pub fn wait(&self) {
        while !self.is_done() {
            std::thread::yield_now();
        }
    }
}

/// Read request
pub struct ReadRequest {
    pub base: RequestBase,
    pub key: Key,
    // Output
    pub value: Option<Value>,
    pub timestamp: u64,
    pub expire_ts: u64,
}

impl ReadRequest {
    pub fn new(table_id: TableIdent, key: Key) -> Self {
        Self {
            base: RequestBase::new(table_id),
            key,
            value: None,
            timestamp: 0,
            expire_ts: 0,
        }
    }
}

impl KvRequest for ReadRequest {
    fn request_type(&self) -> RequestType {
        RequestType::Read
    }

    fn table_id(&self) -> &TableIdent {
        &self.base.table_id
    }

    fn error(&self) -> Option<KvError> {
        *self.base.error.lock()
    }

    fn is_retryable_error(&self) -> bool {
        matches!(self.error(), Some(err) if err.is_retryable())
    }

    fn user_data(&self) -> u64 {
        self.base.user_data
    }

    fn is_done(&self) -> bool {
        self.base.is_done()
    }

    fn wait(&self) {
        self.base.wait()
    }

    fn set_done(&self, error: Option<KvError>) {
        self.base.set_done(error)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Floor request - find the largest key not greater than the search key
pub struct FloorRequest {
    pub base: RequestBase,
    pub key: Key,
    // Output
    pub floor_key: Option<Key>,
    pub value: Option<Value>,
    pub timestamp: u64,
    pub expire_ts: u64,
}

impl FloorRequest {
    pub fn new(table_id: TableIdent, key: Key) -> Self {
        Self {
            base: RequestBase::new(table_id),
            key,
            floor_key: None,
            value: None,
            timestamp: 0,
            expire_ts: 0,
        }
    }
}

impl KvRequest for FloorRequest {
    fn request_type(&self) -> RequestType {
        RequestType::Floor
    }

    fn table_id(&self) -> &TableIdent {
        &self.base.table_id
    }

    fn error(&self) -> Option<KvError> {
        *self.base.error.lock()
    }

    fn is_retryable_error(&self) -> bool {
        matches!(self.error(), Some(err) if err.is_retryable())
    }

    fn user_data(&self) -> u64 {
        self.base.user_data
    }

    fn is_done(&self) -> bool {
        self.base.is_done()
    }

    fn wait(&self) {
        self.base.wait()
    }

    fn set_done(&self, error: Option<KvError>) {
        self.base.set_done(error)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Scan request
pub struct ScanRequest {
    pub base: RequestBase,
    pub begin_key: Key,
    pub end_key: Key,
    pub begin_inclusive: bool,
    pub max_entries: usize,
    pub max_size: usize,
    // Output
    pub entries: Vec<KvEntry>,
    pub has_more: bool,
    pub last_key: Option<Key>,
}

impl ScanRequest {
    pub fn new(table_id: TableIdent, begin_key: Key, end_key: Key, begin_inclusive: bool) -> Self {
        Self {
            base: RequestBase::new(table_id),
            begin_key,
            end_key,
            begin_inclusive,
            max_entries: 1000,
            max_size: 4 * 1024 * 1024, // 4MB default
            entries: Vec::new(),
            has_more: false,
            last_key: None,
        }
    }

    pub fn set_pagination(&mut self, max_entries: usize, max_size: usize) {
        self.max_entries = max_entries;
        self.max_size = max_size;
    }
}

impl KvRequest for ScanRequest {
    fn request_type(&self) -> RequestType {
        RequestType::Scan
    }

    fn table_id(&self) -> &TableIdent {
        &self.base.table_id
    }

    fn error(&self) -> Option<KvError> {
        *self.base.error.lock()
    }

    fn is_retryable_error(&self) -> bool {
        matches!(self.error(), Some(err) if err.is_retryable())
    }

    fn user_data(&self) -> u64 {
        self.base.user_data
    }

    fn is_done(&self) -> bool {
        self.base.is_done()
    }

    fn wait(&self) {
        self.base.wait()
    }

    fn set_done(&self, error: Option<KvError>) {
        self.base.set_done(error)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Batch write request
pub struct BatchWriteRequest {
    pub base: RequestBase,
    pub entries: Vec<WriteDataEntry>,
    pub sync: bool,
    // Output
    pub written_count: usize,
}

impl BatchWriteRequest {
    pub fn new(table_id: TableIdent, entries: Vec<WriteDataEntry>) -> Self {
        Self {
            base: RequestBase::new(table_id),
            entries,
            sync: false,
            written_count: 0,
        }
    }
}

impl KvRequest for BatchWriteRequest {
    fn request_type(&self) -> RequestType {
        RequestType::BatchWrite
    }

    fn table_id(&self) -> &TableIdent {
        &self.base.table_id
    }

    fn error(&self) -> Option<KvError> {
        *self.base.error.lock()
    }

    fn is_retryable_error(&self) -> bool {
        matches!(self.error(), Some(err) if err.is_retryable())
    }

    fn user_data(&self) -> u64 {
        self.base.user_data
    }

    fn is_done(&self) -> bool {
        self.base.is_done()
    }

    fn wait(&self) {
        self.base.wait()
    }

    fn set_done(&self, error: Option<KvError>) {
        self.base.set_done(error)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Truncate request
pub struct TruncateRequest {
    pub base: RequestBase,
    pub position: Key,
}

impl TruncateRequest {
    pub fn new(table_id: TableIdent, position: Key) -> Self {
        Self {
            base: RequestBase::new(table_id),
            position,
        }
    }
}

impl KvRequest for TruncateRequest {
    fn request_type(&self) -> RequestType {
        RequestType::Truncate
    }

    fn table_id(&self) -> &TableIdent {
        &self.base.table_id
    }

    fn error(&self) -> Option<KvError> {
        *self.base.error.lock()
    }

    fn is_retryable_error(&self) -> bool {
        matches!(self.error(), Some(err) if err.is_retryable())
    }

    fn user_data(&self) -> u64 {
        self.base.user_data
    }

    fn is_done(&self) -> bool {
        self.base.is_done()
    }

    fn wait(&self) {
        self.base.wait()
    }

    fn set_done(&self, error: Option<KvError>) {
        self.base.set_done(error)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}