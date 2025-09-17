//! Core task traits and types

use std::any::Any;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent};
use crate::Result;

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// Critical priority - system tasks
    Critical = 0,
    /// High priority - user requests
    High = 1,
    /// Normal priority - standard operations
    Normal = 2,
    /// Low priority - background tasks
    Low = 3,
}

/// Task type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    /// Read operation
    Read,
    /// Batch read operation
    BatchRead,
    /// Write operation
    Write,
    /// Delete operation
    Delete,
    /// Batch write operation
    BatchWrite,
    /// Scan operation
    Scan,
    /// Background write task (compaction/archiving)
    BackgroundWrite,
    /// Compaction task
    Compaction,
    /// Garbage collection
    GarbageCollection,
    /// File garbage collection
    FileGC,
    /// Flush task
    Flush,
    /// Checkpoint task
    Checkpoint,
}

/// Task execution context
#[derive(Clone)]
pub struct TaskContext {
    /// Shard ID
    pub shard_id: u32,
    /// Options
    pub options: std::sync::Arc<crate::config::KvOptions>,
}

impl Default for TaskContext {
    fn default() -> Self {
        Self {
            shard_id: 0,
            options: std::sync::Arc::new(crate::config::KvOptions::default()),
        }
    }
}

/// Result of task execution
#[derive(Debug)]
pub enum TaskResult {
    /// Read result
    Read(Option<Bytes>),
    /// Batch read result
    BatchRead(Vec<Option<Bytes>>),
    /// Write result
    Write(()),
    /// Delete result (true if deleted)
    Delete(bool),
    /// Scan result with key-value pairs
    Scan(Vec<(Bytes, Bytes)>),
    /// Batch write result with number of written items
    BatchWrite(usize),
    /// Background task result with statistics
    Background(Box<dyn Any + Send + Sync>),
    /// File GC result
    FileGC,
}

/// Core task trait
#[async_trait]
pub trait Task: Send + Sync + Debug {
    /// Get task type
    fn task_type(&self) -> TaskType;

    /// Get task priority
    fn priority(&self) -> TaskPriority;

    /// Execute the task
    async fn execute(&self, context: &TaskContext) -> Result<TaskResult>;

    /// Cancel the task
    fn cancel(&mut self) {
        // Default implementation - no-op
    }

    /// Check if task is cancelable
    fn is_cancelable(&self) -> bool {
        true
    }

    /// Check if this task can be merged with another
    fn can_merge(&self, other: &dyn Task) -> bool {
        false
    }

    /// Merge another task into this one
    fn merge(&mut self, other: Box<dyn Task>) -> Result<()> {
        Err(crate::error::Error::InvalidState("Task merging not supported".into()))
    }

    /// Estimate task cost (for scheduling)
    fn estimated_cost(&self) -> usize {
        1
    }

    /// Get the task as Any for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Task factory for creating tasks
pub trait TaskFactory: Send + Sync {
    /// Create a read task
    fn create_read_task(&self, key: Key) -> Box<dyn Task>;

    /// Create a write task
    fn create_write_task(&self, key: Key, value: Value) -> Box<dyn Task>;

    /// Create a batch write task
    fn create_batch_write_task(&self, items: Vec<(Key, Value)>) -> Box<dyn Task>;

    /// Create a scan task
    fn create_scan_task(&self, start: Option<Key>, end: Option<Key>, limit: usize) -> Box<dyn Task>;
}

/// Task statistics
#[derive(Debug, Default, Clone)]
pub struct TaskStats {
    /// Total tasks submitted
    pub submitted: u64,
    /// Total tasks completed
    pub completed: u64,
    /// Total tasks failed
    pub failed: u64,
    /// Total tasks cancelled
    pub cancelled: u64,
    /// Average execution time in microseconds
    pub avg_exec_time_us: u64,
    /// Maximum execution time in microseconds
    pub max_exec_time_us: u64,
}

/// Task handle for tracking task execution
pub struct TaskHandle {
    /// Task ID
    pub task_id: u64,
    /// Result receiver
    pub result: tokio::sync::oneshot::Receiver<Result<TaskResult>>,
    /// Cancellation token
    pub cancel_token: tokio_util::sync::CancellationToken,
}