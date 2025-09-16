//! I/O completion queue handling

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::Result;
use crate::error::Error;

/// I/O completion event
#[derive(Debug)]
pub struct CompletionEvent {
    /// Request ID
    pub request_id: u64,
    /// Operation type
    pub op_type: OpType,
    /// Result of the operation (Ok size or error string)
    pub result: std::result::Result<usize, String>,
    /// Time taken for the operation
    pub duration: Duration,
}

impl Clone for CompletionEvent {
    fn clone(&self) -> Self {
        Self {
            request_id: self.request_id,
            op_type: self.op_type,
            result: self.result.clone(),
            duration: self.duration,
        }
    }
}

use std::io;

/// Operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpType {
    Read,
    Write,
    Sync,
    Fallocate,
}

/// I/O request waiting for completion
struct PendingRequest {
    /// Request ID
    id: u64,
    /// Operation type
    op_type: OpType,
    /// Start time
    start_time: Instant,
    /// Completion notifier
    notifier: oneshot::Sender<CompletionEvent>,
}

/// Completion queue for managing I/O completions
pub struct CompletionQueue {
    /// Next request ID
    next_id: Arc<Mutex<u64>>,
    /// Pending requests
    pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
    /// Completion events channel
    events_tx: mpsc::UnboundedSender<CompletionEvent>,
    /// Completion events receiver
    events_rx: Arc<Mutex<mpsc::UnboundedReceiver<CompletionEvent>>>,
    /// Statistics
    stats: Arc<Mutex<CompletionStats>>,
}

use std::collections::HashMap;

/// Completion queue statistics
#[derive(Debug, Default, Clone)]
pub struct CompletionStats {
    /// Total operations submitted
    pub submitted: u64,
    /// Total operations completed
    pub completed: u64,
    /// Total operations failed
    pub failed: u64,
    /// Operations by type
    pub by_type: HashMap<OpType, u64>,
    /// Average completion time
    pub avg_duration_us: u64,
    /// Maximum completion time
    pub max_duration_us: u64,
}

impl CompletionQueue {
    /// Create a new completion queue
    pub fn new() -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        Self {
            next_id: Arc::new(Mutex::new(0)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            events_tx,
            events_rx: Arc::new(Mutex::new(events_rx)),
            stats: Arc::new(Mutex::new(CompletionStats::default())),
        }
    }

    /// Submit a request and get a completion receiver
    pub fn submit(&self, op_type: OpType) -> (u64, oneshot::Receiver<CompletionEvent>) {
        let mut next_id = self.next_id.lock();
        let id = *next_id;
        *next_id += 1;

        let (tx, rx) = oneshot::channel();

        let request = PendingRequest {
            id,
            op_type,
            start_time: Instant::now(),
            notifier: tx,
        };

        let mut pending = self.pending.lock();
        pending.insert(id, request);

        let mut stats = self.stats.lock();
        stats.submitted += 1;
        *stats.by_type.entry(op_type).or_insert(0) += 1;

        (id, rx)
    }

    /// Complete a request
    pub fn complete(&self, request_id: u64, result: io::Result<usize>) {
        let mut pending = self.pending.lock();

        if let Some(request) = pending.remove(&request_id) {
            let duration = request.start_time.elapsed();

            let event = CompletionEvent {
                request_id,
                op_type: request.op_type,
                result: result.as_ref().map(|v| *v).map_err(|e| e.to_string()),
                duration,
            };

            // Update stats
            let mut stats = self.stats.lock();
            stats.completed += 1;

            if result.is_err() {
                stats.failed += 1;
            }

            let duration_us = duration.as_micros() as u64;
            if stats.completed > 0 {
                stats.avg_duration_us =
                    (stats.avg_duration_us * (stats.completed - 1) + duration_us) / stats.completed;
            }
            stats.max_duration_us = stats.max_duration_us.max(duration_us);

            // Send completion event to waiting task
            let _ = request.notifier.send(event);

            // Create a copy for the event stream
            let stream_event = CompletionEvent {
                request_id,
                op_type: request.op_type,
                result: result.as_ref().map(|v| *v).map_err(|e| e.to_string()),
                duration,
            };
            let _ = self.events_tx.send(stream_event);
        }
    }

    /// Process completion events
    pub async fn process_events<F>(&self, mut handler: F)
    where
        F: FnMut(CompletionEvent),
    {
        let mut rx = self.events_rx.lock();

        while let Ok(event) = rx.try_recv() {
            handler(event);
        }
    }

    /// Get completion statistics
    pub fn stats(&self) -> CompletionStats {
        self.stats.lock().clone()
    }

    /// Clear statistics
    pub fn clear_stats(&self) {
        let mut stats = self.stats.lock();
        *stats = CompletionStats::default();
    }

    /// Get number of pending requests
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
}

/// Batch completion handler for processing multiple completions
pub struct BatchCompletionHandler {
    /// Completion queue
    queue: Arc<CompletionQueue>,
    /// Batch size
    batch_size: usize,
    /// Pending completions
    pending_completions: Arc<Mutex<VecDeque<(u64, io::Result<usize>)>>>,
    /// Notify for new completions
    notify: Arc<Notify>,
}

impl BatchCompletionHandler {
    /// Create a new batch completion handler
    pub fn new(queue: Arc<CompletionQueue>, batch_size: usize) -> Self {
        Self {
            queue,
            batch_size,
            pending_completions: Arc::new(Mutex::new(VecDeque::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Add a completion
    pub fn add_completion(&self, request_id: u64, result: io::Result<usize>) {
        let mut pending = self.pending_completions.lock();
        pending.push_back((request_id, result));

        if pending.len() >= self.batch_size {
            self.notify.notify_one();
        }
    }

    /// Process pending completions
    pub async fn process_batch(&self) {
        // Wait for batch to fill or timeout
        tokio::select! {
            _ = self.notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }

        let mut pending = self.pending_completions.lock();
        let batch: Vec<_> = pending.drain(..).collect();
        drop(pending);

        // Process completions
        for (request_id, result) in batch {
            self.queue.complete(request_id, result);
        }
    }

    /// Run the batch processor
    pub async fn run(&self) {
        loop {
            self.process_batch().await;
        }
    }
}

/// Priority completion queue with different priority levels
pub struct PriorityCompletionQueue {
    /// High priority queue
    high: Arc<CompletionQueue>,
    /// Normal priority queue
    normal: Arc<CompletionQueue>,
    /// Low priority queue
    low: Arc<CompletionQueue>,
}

/// Priority level for I/O operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Normal,
    Low,
}

impl PriorityCompletionQueue {
    /// Create a new priority completion queue
    pub fn new() -> Self {
        Self {
            high: Arc::new(CompletionQueue::new()),
            normal: Arc::new(CompletionQueue::new()),
            low: Arc::new(CompletionQueue::new()),
        }
    }

    /// Get queue for priority level
    pub fn get_queue(&self, priority: Priority) -> Arc<CompletionQueue> {
        match priority {
            Priority::High => self.high.clone(),
            Priority::Normal => self.normal.clone(),
            Priority::Low => self.low.clone(),
        }
    }

    /// Submit a request with priority
    pub fn submit(&self, op_type: OpType, priority: Priority) -> (u64, oneshot::Receiver<CompletionEvent>) {
        self.get_queue(priority).submit(op_type)
    }

    /// Process events from all queues
    pub async fn process_all_events<F>(&self, mut handler: F)
    where
        F: FnMut(CompletionEvent, Priority),
    {
        // Process high priority first
        self.high.process_events(|e| handler(e, Priority::High)).await;

        // Then normal
        self.normal.process_events(|e| handler(e, Priority::Normal)).await;

        // Finally low
        self.low.process_events(|e| handler(e, Priority::Low)).await;
    }

    /// Get total pending count across all queues
    pub fn total_pending(&self) -> usize {
        self.high.pending_count() +
        self.normal.pending_count() +
        self.low.pending_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_completion_queue() {
        let queue = CompletionQueue::new();

        // Submit a request
        let (id, rx) = queue.submit(OpType::Read);

        // Complete it
        queue.complete(id, Ok(1024));

        // Receive completion
        let event = rx.await.unwrap();
        assert_eq!(event.request_id, id);
        assert_eq!(event.op_type, OpType::Read);
        assert!(event.result.is_ok());

        // Check stats
        let stats = queue.stats();
        assert_eq!(stats.submitted, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn test_batch_completion() {
        let queue = Arc::new(CompletionQueue::new());
        let handler = BatchCompletionHandler::new(queue.clone(), 3);

        // Submit multiple requests
        let mut receivers = Vec::new();
        for _ in 0..3 {
            let (id, rx) = queue.submit(OpType::Write);
            receivers.push((id, rx));

            // Add completion
            handler.add_completion(id, Ok(512));
        }

        // Process batch
        handler.process_batch().await;

        // Check all completed
        for (id, rx) in receivers {
            let event = rx.await.unwrap();
            assert_eq!(event.request_id, id);
            assert!(event.result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_priority_queue() {
        let pq = PriorityCompletionQueue::new();

        // Submit requests with different priorities
        let (high_id, high_rx) = pq.submit(OpType::Read, Priority::High);
        let (normal_id, normal_rx) = pq.submit(OpType::Write, Priority::Normal);
        let (low_id, low_rx) = pq.submit(OpType::Sync, Priority::Low);

        // Complete them
        pq.get_queue(Priority::High).complete(high_id, Ok(100));
        pq.get_queue(Priority::Normal).complete(normal_id, Ok(200));
        pq.get_queue(Priority::Low).complete(low_id, Ok(300));

        // Check completions
        let high_event = high_rx.await.unwrap();
        assert_eq!(high_event.request_id, high_id);

        let normal_event = normal_rx.await.unwrap();
        assert_eq!(normal_event.request_id, normal_id);

        let low_event = low_rx.await.unwrap();
        assert_eq!(low_event.request_id, low_id);

        assert_eq!(pq.total_pending(), 0);
    }

    #[test]
    fn test_completion_stats() {
        let queue = CompletionQueue::new();

        // Submit and complete multiple operations
        for i in 0..10 {
            let (id, _rx) = queue.submit(OpType::Read);
            if i < 8 {
                queue.complete(id, Ok(1024));
            } else {
                queue.complete(id, Err(io::Error::new(io::ErrorKind::Other, "test error")));
            }
        }

        let stats = queue.stats();
        assert_eq!(stats.submitted, 10);
        assert_eq!(stats.completed, 10);
        assert_eq!(stats.failed, 2);
        assert_eq!(*stats.by_type.get(&OpType::Read).unwrap(), 10);
    }
}