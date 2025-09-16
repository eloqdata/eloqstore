//! Work queue implementation for shards

use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::cmp::Ordering as CmpOrdering;

use tokio::sync::{mpsc, oneshot, Mutex};
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent};
use crate::Result;
use crate::error::Error;

/// Work priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkPriority {
    /// Critical priority - system operations
    Critical = 0,
    /// High priority - user reads
    High = 1,
    /// Normal priority - standard operations
    Normal = 2,
    /// Low priority - background tasks
    Low = 3,
}

/// Work item types
#[derive(Debug, Clone)]
pub enum WorkItemType {
    /// Read operation
    Read { key: Key },
    /// Write operation
    Write { key: Key, value: Value },
    /// Delete operation
    Delete { key: Key },
    /// Scan operation
    Scan { start: Option<Key>, end: Option<Key>, limit: usize },
    /// Batch write
    BatchWrite { items: Vec<(Key, Value)> },
    /// Flush operation
    Flush,
    /// Compaction
    Compaction,
    /// Custom operation
    Custom(Box<dyn CustomWork>),
}

/// Custom work trait
pub trait CustomWork: Send + Sync {
    /// Execute the work
    fn execute(&self) -> Result<WorkResult>;

    /// Clone the work
    fn clone_box(&self) -> Box<dyn CustomWork>;
}

impl Clone for Box<dyn CustomWork> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl std::fmt::Debug for Box<dyn CustomWork> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomWork")
    }
}

/// Work result
#[derive(Debug, Clone)]
pub enum WorkResult {
    /// Read result
    Read(Option<Bytes>),
    /// Write result
    Write(()),
    /// Delete result
    Delete(bool),
    /// Scan result
    Scan(Vec<(Bytes, Bytes)>),
    /// Batch write result
    BatchWrite(usize),
    /// Flush result
    Flush(()),
    /// Compaction result
    Compaction(CompactionStats),
    // Custom variant removed - Box<dyn Any> can't implement Clone
}

/// Compaction statistics
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Pages compacted
    pub pages_compacted: usize,
    /// Bytes saved
    pub bytes_saved: u64,
    /// Duration
    pub duration: Duration,
}

/// Work item in the queue
#[derive(Clone)]
pub struct WorkItem {
    /// Unique ID
    pub id: u64,
    /// Table identifier
    pub table: TableIdent,
    /// Work type
    pub work_type: WorkItemType,
    /// Priority
    pub priority: WorkPriority,
    /// Submit time
    pub submit_time: Instant,
    /// Deadline (if any)
    pub deadline: Option<Instant>,
    /// Result sender
    pub result_sender: Option<Arc<Mutex<Option<oneshot::Sender<Result<WorkResult>>>>>>,
}

impl WorkItem {
    /// Create a new work item
    pub fn new(
        id: u64,
        table: TableIdent,
        work_type: WorkItemType,
        priority: WorkPriority,
    ) -> Self {
        Self {
            id,
            table,
            work_type,
            priority,
            submit_time: Instant::now(),
            deadline: None,
            result_sender: None,
        }
    }

    /// Set deadline
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set result sender
    pub fn with_result_sender(mut self, sender: oneshot::Sender<Result<WorkResult>>) -> Self {
        self.result_sender = Some(Arc::new(Mutex::new(Some(sender))));
        self
    }

    /// Check if deadline has passed
    pub fn is_expired(&self) -> bool {
        self.deadline.map_or(false, |d| Instant::now() > d)
    }

    /// Get age of the work item
    pub fn age(&self) -> Duration {
        self.submit_time.elapsed()
    }
}

impl PartialEq for WorkItem {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for WorkItem {}

impl PartialOrd for WorkItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Higher priority first (lower value = higher priority)
        other.priority.cmp(&self.priority)
            .then_with(|| {
                // Earlier deadline first
                match (self.deadline, other.deadline) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    (Some(_), None) => CmpOrdering::Less,
                    (None, Some(_)) => CmpOrdering::Greater,
                    (None, None) => {
                        // Earlier submit time first
                        self.submit_time.cmp(&other.submit_time)
                    }
                }
            })
    }
}

/// Work queue for managing work items
pub struct WorkQueue {
    /// Queue capacity
    capacity: usize,
    /// Priority queue
    queue: Arc<Mutex<BinaryHeap<WorkItem>>>,
    /// Next work ID
    next_id: AtomicUsize,
    /// Current queue size
    size: AtomicUsize,
    /// Statistics
    stats: Arc<QueueStats>,
}

impl WorkQueue {
    /// Create a new work queue
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Arc::new(Mutex::new(BinaryHeap::with_capacity(capacity))),
            next_id: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
            stats: Arc::new(QueueStats::new()),
        }
    }

    /// Enqueue a work item
    pub async fn enqueue(&self, mut work: WorkItem) -> Result<u64> {
        // Check capacity
        if self.size.load(Ordering::Relaxed) >= self.capacity {
            self.stats.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Error::QueueFull);
        }

        // Assign ID if not set
        if work.id == 0 {
            work.id = self.next_id.fetch_add(1, Ordering::SeqCst) as u64;
        }

        let work_id = work.id;

        // Add to queue
        {
            let mut queue = self.queue.lock().await;
            queue.push(work);
        }

        self.size.fetch_add(1, Ordering::Relaxed);
        self.stats.enqueued.fetch_add(1, Ordering::Relaxed);

        Ok(work_id)
    }

    /// Dequeue a work item
    pub async fn dequeue(&self) -> Option<WorkItem> {
        let mut queue = self.queue.lock().await;

        // Remove expired items
        while let Some(work) = queue.peek() {
            if work.is_expired() {
                let expired = queue.pop().unwrap();
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);

                // Send timeout error
                if let Some(sender) = expired.result_sender {
                    if let Ok(mut sender_guard) = sender.try_lock() {
                        if let Some(sender) = sender_guard.take() {
                            let _ = sender.send(Err(Error::Timeout));
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Get next work item
        if let Some(work) = queue.pop() {
            self.size.fetch_sub(1, Ordering::Relaxed);
            self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
            Some(work)
        } else {
            None
        }
    }

    /// Peek at the next work item without removing it
    pub async fn peek(&self) -> Option<WorkItem> {
        let queue = self.queue.lock().await;
        queue.peek().cloned()
    }

    /// Get current queue size
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Check if queue is full
    pub fn is_full(&self) -> bool {
        self.size() >= self.capacity
    }

    /// Clear the queue
    pub async fn clear(&self) {
        let mut queue = self.queue.lock().await;

        // Send cancellation to all pending work
        while let Some(work) = queue.pop() {
            if let Some(sender) = work.result_sender {
                if let Ok(mut sender_guard) = sender.try_lock() {
                    if let Some(sender) = sender_guard.take() {
                        let _ = sender.send(Err(Error::Cancelled));
                    }
                }
            }
        }

        self.size.store(0, Ordering::Relaxed);
        self.stats.cleared.fetch_add(1, Ordering::Relaxed);
    }

    /// Get queue statistics
    pub fn stats(&self) -> QueueStatsSnapshot {
        self.stats.snapshot()
    }

    /// Requeue a work item (for retry)
    pub async fn requeue(&self, mut work: WorkItem) -> Result<()> {
        // Update priority for requeued items (lower priority)
        if work.priority < WorkPriority::Low {
            work.priority = match work.priority {
                WorkPriority::Critical => WorkPriority::High,
                WorkPriority::High => WorkPriority::Normal,
                WorkPriority::Normal => WorkPriority::Low,
                WorkPriority::Low => WorkPriority::Low,
            };
        }

        self.enqueue(work).await?;
        self.stats.requeued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Queue statistics
struct QueueStats {
    /// Total enqueued
    enqueued: AtomicUsize,
    /// Total dequeued
    dequeued: AtomicUsize,
    /// Total rejected (queue full)
    rejected: AtomicUsize,
    /// Total expired
    expired: AtomicUsize,
    /// Total requeued
    requeued: AtomicUsize,
    /// Total cleared
    cleared: AtomicUsize,
}

impl QueueStats {
    fn new() -> Self {
        Self {
            enqueued: AtomicUsize::new(0),
            dequeued: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            expired: AtomicUsize::new(0),
            requeued: AtomicUsize::new(0),
            cleared: AtomicUsize::new(0),
        }
    }

    fn snapshot(&self) -> QueueStatsSnapshot {
        QueueStatsSnapshot {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            dequeued: self.dequeued.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            requeued: self.requeued.load(Ordering::Relaxed),
            cleared: self.cleared.load(Ordering::Relaxed),
        }
    }
}

/// Queue statistics snapshot
#[derive(Debug, Clone)]
pub struct QueueStatsSnapshot {
    /// Total enqueued
    pub enqueued: usize,
    /// Total dequeued
    pub dequeued: usize,
    /// Total rejected
    pub rejected: usize,
    /// Total expired
    pub expired: usize,
    /// Total requeued
    pub requeued: usize,
    /// Total cleared
    pub cleared: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_work_queue_priority() {
        let queue = WorkQueue::new(10);

        // Add items with different priorities
        let work1 = WorkItem::new(1, TableIdent::default(),
            WorkItemType::Read { key: Bytes::from("key1") },
            WorkPriority::Low);

        let work2 = WorkItem::new(2, TableIdent::default(),
            WorkItemType::Read { key: Bytes::from("key2") },
            WorkPriority::Critical);

        let work3 = WorkItem::new(3, TableIdent::default(),
            WorkItemType::Read { key: Bytes::from("key3") },
            WorkPriority::Normal);

        queue.enqueue(work1).await.unwrap();
        queue.enqueue(work2).await.unwrap();
        queue.enqueue(work3).await.unwrap();

        // Should dequeue in priority order
        let first = queue.dequeue().await.unwrap();
        assert_eq!(first.id, 2); // Critical

        let second = queue.dequeue().await.unwrap();
        assert_eq!(second.id, 3); // Normal

        let third = queue.dequeue().await.unwrap();
        assert_eq!(third.id, 1); // Low
    }

    #[tokio::test]
    async fn test_work_queue_capacity() {
        let queue = WorkQueue::new(2);

        let work1 = WorkItem::new(1, TableIdent::default(),
            WorkItemType::Flush, WorkPriority::Normal);

        let work2 = WorkItem::new(2, TableIdent::default(),
            WorkItemType::Flush, WorkPriority::Normal);

        let work3 = WorkItem::new(3, TableIdent::default(),
            WorkItemType::Flush, WorkPriority::Normal);

        assert!(queue.enqueue(work1).await.is_ok());
        assert!(queue.enqueue(work2).await.is_ok());
        assert!(queue.enqueue(work3).await.is_err()); // Queue full

        assert_eq!(queue.size(), 2);
    }
}