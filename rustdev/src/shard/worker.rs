//! Shard worker implementation

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

use crate::Result;
use crate::error::Error;

use super::shard::ShardId;
use super::queue::{WorkQueue, WorkItem, WorkResult, WorkItemType};

/// Worker configuration
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Worker ID
    pub id: usize,
    /// Shard ID
    pub shard_id: ShardId,
    /// Batch size for processing
    pub batch_size: usize,
    /// Idle timeout
    pub idle_timeout: Duration,
    /// Maximum retries
    pub max_retries: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            id: 0,
            shard_id: 0,
            batch_size: 10,
            idle_timeout: Duration::from_millis(100),
            max_retries: 3,
        }
    }
}

/// Shard worker
pub struct ShardWorker {
    /// Configuration
    config: WorkerConfig,
    /// Work queue
    queue: Arc<WorkQueue>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<WorkerStats>,
    /// Worker handle
    handle: Option<JoinHandle<()>>,
}

impl ShardWorker {
    /// Create a new worker
    pub fn new(config: WorkerConfig, queue: Arc<WorkQueue>) -> Self {
        Self {
            config,
            queue,
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(WorkerStats::new()),
            handle: None,
        }
    }

    /// Start the worker
    pub fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Err(Error::InvalidState("Worker already running".into()));
        }

        self.running.store(true, Ordering::Relaxed);

        let config = self.config.clone();
        let queue = self.queue.clone();
        let running = self.running.clone();
        let stats = self.stats.clone();

        let handle = tokio::spawn(async move {
            Self::worker_loop(config, queue, running, stats).await;
        });

        self.handle = Some(handle);
        Ok(())
    }

    /// Stop the worker
    pub async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            handle.await.map_err(|e| Error::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Worker main loop
    async fn worker_loop(
        config: WorkerConfig,
        queue: Arc<WorkQueue>,
        running: Arc<AtomicBool>,
        stats: Arc<WorkerStats>,
    ) {
        let mut idle_start = None;

        while running.load(Ordering::Relaxed) {
            // Try to get work
            if let Some(work) = queue.dequeue().await {
                idle_start = None;
                stats.record_work_start();

                // Process work item
                let result = Self::process_work(&config, work.clone()).await;

                // Update stats
                let success = result.is_ok();

                // Send result if needed
                if let Some(sender) = work.result_sender {
                    if let Ok(mut sender_guard) = sender.try_lock() {
                        if let Some(sender) = sender_guard.take() {
                            let _ = sender.send(result);
                        }
                    }
                }

                // Record stats
                if success {
                    stats.record_work_success()
                } else {
                    stats.record_work_failure()
                }
            } else {
                // No work available
                if idle_start.is_none() {
                    idle_start = Some(Instant::now());
                }

                // Check idle timeout
                if let Some(start) = idle_start {
                    if start.elapsed() > config.idle_timeout {
                        // Yield to other tasks
                        tokio::task::yield_now().await;
                        idle_start = None;
                    }
                }

                // Brief sleep
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Process a work item
    async fn process_work(config: &WorkerConfig, work: WorkItem) -> Result<WorkResult> {
        match work.work_type {
            WorkItemType::Read { ref key } => {
                // Simulate read operation
                tokio::time::sleep(Duration::from_micros(100)).await;
                Ok(WorkResult::Read(None))
            }
            WorkItemType::Write { ref key, ref value } => {
                // Simulate write operation
                tokio::time::sleep(Duration::from_micros(200)).await;
                Ok(WorkResult::Write(()))
            }
            WorkItemType::Delete { ref key } => {
                // Simulate delete operation
                tokio::time::sleep(Duration::from_micros(150)).await;
                Ok(WorkResult::Delete(true))
            }
            WorkItemType::Scan { .. } => {
                // Simulate scan operation
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok(WorkResult::Scan(Vec::new()))
            }
            WorkItemType::BatchWrite { ref items } => {
                // Simulate batch write
                tokio::time::sleep(Duration::from_micros(100 * items.len())).await;
                Ok(WorkResult::BatchWrite(items.len()))
            }
            WorkItemType::Flush => {
                // Simulate flush
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(WorkResult::Flush(()))
            }
            WorkItemType::Compaction => {
                // Simulate compaction
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(WorkResult::Compaction(super::queue::CompactionStats {
                    pages_compacted: 10,
                    bytes_saved: 1024,
                    duration: Duration::from_millis(100),
                }))
            }
            WorkItemType::Custom(ref work) => {
                work.execute()
            }
        }
    }

    /// Get worker statistics
    pub fn stats(&self) -> WorkerStatsSnapshot {
        self.stats.snapshot()
    }
}

/// Worker pool for managing multiple workers
pub struct WorkerPool {
    /// Workers
    workers: Vec<ShardWorker>,
    /// Shared work queue
    queue: Arc<WorkQueue>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(num_workers: usize, shard_id: ShardId, queue_capacity: usize) -> Self {
        let queue = Arc::new(WorkQueue::new(queue_capacity));
        let mut workers = Vec::with_capacity(num_workers);

        for id in 0..num_workers {
            let config = WorkerConfig {
                id,
                shard_id,
                ..Default::default()
            };

            workers.push(ShardWorker::new(config, queue.clone()));
        }

        Self { workers, queue }
    }

    /// Start all workers
    pub fn start_all(&mut self) -> Result<()> {
        for worker in &mut self.workers {
            worker.start()?;
        }
        Ok(())
    }

    /// Stop all workers
    pub async fn stop_all(&mut self) -> Result<()> {
        for worker in &mut self.workers {
            worker.stop().await?;
        }
        Ok(())
    }

    /// Submit work to the pool
    pub async fn submit(&self, work: WorkItem) -> Result<u64> {
        self.queue.enqueue(work).await
    }

    /// Get queue
    pub fn queue(&self) -> &Arc<WorkQueue> {
        &self.queue
    }

    /// Get combined statistics
    pub fn stats(&self) -> PoolStats {
        let mut pool_stats = PoolStats::default();

        for worker in &self.workers {
            let worker_stats = worker.stats();
            pool_stats.total_processed += worker_stats.processed;
            pool_stats.total_succeeded += worker_stats.succeeded;
            pool_stats.total_failed += worker_stats.failed;
            pool_stats.total_time_ms += worker_stats.total_time_ms;
        }

        pool_stats.queue_size = self.queue.size();
        pool_stats.num_workers = self.workers.len();

        pool_stats
    }
}

/// Worker statistics
struct WorkerStats {
    /// Total work items processed
    processed: AtomicU64,
    /// Successful completions
    succeeded: AtomicU64,
    /// Failed completions
    failed: AtomicU64,
    /// Total processing time in milliseconds
    total_time_ms: AtomicU64,
    /// Current work start time
    current_start: RwLock<Option<Instant>>,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            processed: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            total_time_ms: AtomicU64::new(0),
            current_start: RwLock::new(None),
        }
    }

    fn record_work_start(&self) {
        if let Ok(mut start) = self.current_start.try_write() {
            *start = Some(Instant::now());
        }
    }

    fn record_work_success(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.succeeded.fetch_add(1, Ordering::Relaxed);
        self.record_work_end();
    }

    fn record_work_failure(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.record_work_end();
    }

    fn record_work_end(&self) {
        if let Ok(mut start) = self.current_start.try_write() {
            if let Some(start_time) = start.take() {
                let elapsed_ms = start_time.elapsed().as_millis() as u64;
                self.total_time_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> WorkerStatsSnapshot {
        WorkerStatsSnapshot {
            processed: self.processed.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            total_time_ms: self.total_time_ms.load(Ordering::Relaxed),
        }
    }
}

/// Worker statistics snapshot
#[derive(Debug, Clone)]
pub struct WorkerStatsSnapshot {
    /// Total processed
    pub processed: u64,
    /// Total succeeded
    pub succeeded: u64,
    /// Total failed
    pub failed: u64,
    /// Total time in milliseconds
    pub total_time_ms: u64,
}

/// Pool statistics
#[derive(Debug, Default, Clone)]
pub struct PoolStats {
    /// Number of workers
    pub num_workers: usize,
    /// Current queue size
    pub queue_size: usize,
    /// Total processed
    pub total_processed: u64,
    /// Total succeeded
    pub total_succeeded: u64,
    /// Total failed
    pub total_failed: u64,
    /// Total processing time
    pub total_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableIdent;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_worker_pool() {
        let mut pool = WorkerPool::new(2, 0, 100);

        // Start pool
        pool.start_all().unwrap();

        // Submit work
        let work = WorkItem::new(
            1,
            TableIdent::default(),
            WorkItemType::Read { key: Bytes::from("test") },
            super::super::queue::WorkPriority::Normal,
        );

        let work_id = pool.submit(work).await.unwrap();
        assert_eq!(work_id, 1);

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check stats
        let stats = pool.stats();
        assert_eq!(stats.num_workers, 2);

        // Stop pool
        pool.stop_all().await.unwrap();
    }
}