//! Task scheduler for managing task execution

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::cmp::Ordering as CmpOrdering;

use tokio::sync::{mpsc, oneshot, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext, TaskStats};

/// Task handle for tracking execution
pub struct TaskHandle {
    /// Task ID
    pub task_id: u64,
    /// Result receiver
    pub result: oneshot::Receiver<Result<TaskResult>>,
    /// Cancellation token
    pub cancel_token: CancellationToken,
}

/// Scheduled task wrapper
struct ScheduledTask {
    /// Task ID
    id: u64,
    /// Task to execute
    task: Box<dyn Task>,
    /// Priority
    priority: TaskPriority,
    /// Submit time
    submit_time: Instant,
    /// Result sender
    result_sender: oneshot::Sender<Result<TaskResult>>,
    /// Cancellation token
    cancel_token: CancellationToken,
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ScheduledTask {}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Higher priority first (lower value = higher priority)
        other.priority.cmp(&self.priority)
            .then_with(|| {
                // Earlier submit time first for same priority
                other.submit_time.cmp(&self.submit_time)
            })
    }
}

/// Task scheduler configuration
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum concurrent tasks
    pub max_concurrent: usize,
    /// Maximum queued tasks
    pub max_queued: usize,
    /// Task timeout
    pub task_timeout: Duration,
    /// Enable task stealing
    pub enable_stealing: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            max_queued: 10000,
            task_timeout: Duration::from_secs(60),
            enable_stealing: true,
        }
    }
}

/// Task scheduler
pub struct TaskScheduler {
    /// Configuration
    config: SchedulerConfig,
    /// Next task ID
    next_id: Arc<AtomicU64>,
    /// Task queue by priority
    queue: Arc<RwLock<BinaryHeap<ScheduledTask>>>,
    /// Semaphore for concurrency control
    semaphore: Arc<Semaphore>,
    /// Statistics
    stats: Arc<RwLock<TaskStats>>,
    /// Shutdown token
    shutdown: CancellationToken,
    /// Worker handles
    workers: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            next_id: Arc::new(AtomicU64::new(0)),
            queue: Arc::new(RwLock::new(BinaryHeap::new())),
            stats: Arc::new(RwLock::new(TaskStats::default())),
            shutdown: CancellationToken::new(),
            workers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the scheduler
    pub async fn start(&self, num_workers: usize) -> Result<()> {
        let mut workers = self.workers.write().await;

        for worker_id in 0..num_workers {
            let queue = self.queue.clone();
            let semaphore = self.semaphore.clone();
            let stats = self.stats.clone();
            let shutdown = self.shutdown.clone();
            let timeout = self.config.task_timeout;

            let handle = tokio::spawn(async move {
                Self::worker_loop(worker_id, queue, semaphore, stats, shutdown, timeout).await;
            });

            workers.push(handle);
        }

        Ok(())
    }

    /// Worker loop
    async fn worker_loop(
        worker_id: usize,
        queue: Arc<RwLock<BinaryHeap<ScheduledTask>>>,
        semaphore: Arc<Semaphore>,
        stats: Arc<RwLock<TaskStats>>,
        shutdown: CancellationToken,
        timeout: Duration,
    ) {
        loop {
            // Check shutdown
            if shutdown.is_cancelled() {
                break;
            }

            // Get next task
            let task = {
                let mut queue = queue.write().await;
                queue.pop()
            };

            if let Some(mut task) = task {
                // Acquire permit
                let _permit = semaphore.acquire().await.unwrap();

                // Create context
                let context = TaskContext {
                    task_id: task.id,
                    table: Default::default(),
                    priority: task.priority,
                    timeout: Some(timeout),
                    cancel_token: task.cancel_token.clone(),
                };

                // Execute task
                let start_time = Instant::now();
                let result = tokio::time::timeout(timeout, task.task.execute(context)).await;

                let duration = start_time.elapsed();

                // Handle result
                let task_result = match result {
                    Ok(Ok(res)) => {
                        // Success
                        let mut stats = stats.write().await;
                        stats.completed += 1;
                        Self::update_timing_stats(&mut stats, duration);
                        Ok(res)
                    }
                    Ok(Err(e)) => {
                        // Task failed
                        let mut stats = stats.write().await;
                        stats.failed += 1;
                        Err(e)
                    }
                    Err(_) => {
                        // Timeout
                        let mut stats = stats.write().await;
                        stats.failed += 1;
                        Err(Error::Timeout)
                    }
                };

                // Send result
                let _ = task.result_sender.send(task_result);
            } else {
                // No task, sleep briefly
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Update timing statistics
    fn update_timing_stats(stats: &mut TaskStats, duration: Duration) {
        let duration_us = duration.as_micros() as u64;

        if stats.completed > 0 {
            stats.avg_exec_time_us =
                (stats.avg_exec_time_us * (stats.completed - 1) + duration_us) / stats.completed;
        } else {
            stats.avg_exec_time_us = duration_us;
        }

        stats.max_exec_time_us = stats.max_exec_time_us.max(duration_us);
    }

    /// Submit a task
    pub async fn submit(&self, mut task: Box<dyn Task>) -> Result<TaskHandle> {
        // Check queue size
        {
            let queue = self.queue.read().await;
            if queue.len() >= self.config.max_queued {
                return Err(Error::QueueFull);
            }
        }

        // Generate task ID
        let task_id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Create result channel
        let (tx, rx) = oneshot::channel();

        // Create cancellation token
        let cancel_token = CancellationToken::new();

        // Create scheduled task
        let scheduled = ScheduledTask {
            id: task_id,
            priority: task.priority(),
            task,
            submit_time: Instant::now(),
            result_sender: tx,
            cancel_token: cancel_token.clone(),
        };

        // Add to queue
        {
            let mut queue = self.queue.write().await;
            queue.push(scheduled);
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.submitted += 1;
        }

        Ok(TaskHandle {
            task_id,
            result: rx,
            cancel_token,
        })
    }

    /// Cancel a task
    pub fn cancel(&self, handle: &TaskHandle) {
        handle.cancel_token.cancel();

        // Update stats
        tokio::spawn({
            let stats = self.stats.clone();
            async move {
                let mut stats = stats.write().await;
                stats.cancelled += 1;
            }
        });
    }

    /// Get scheduler statistics
    pub async fn stats(&self) -> TaskStats {
        self.stats.read().await.clone()
    }

    /// Get queue size
    pub async fn queue_size(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Shutdown the scheduler
    pub async fn shutdown(&self) {
        self.shutdown.cancel();

        // Wait for workers to finish
        let mut workers = self.workers.write().await;
        for worker in workers.drain(..) {
            let _ = worker.await;
        }
    }
}

/// Work-stealing scheduler for better load balancing
pub struct WorkStealingScheduler {
    /// Number of workers
    num_workers: usize,
    /// Worker queues
    queues: Vec<Arc<RwLock<VecDeque<ScheduledTask>>>>,
    /// Next worker for round-robin
    next_worker: Arc<AtomicUsize>,
    /// Base scheduler for each worker
    schedulers: Vec<Arc<TaskScheduler>>,
}

impl WorkStealingScheduler {
    /// Create a new work-stealing scheduler
    pub fn new(num_workers: usize, config: SchedulerConfig) -> Self {
        let mut queues = Vec::with_capacity(num_workers);
        let mut schedulers = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            queues.push(Arc::new(RwLock::new(VecDeque::new())));
            schedulers.push(Arc::new(TaskScheduler::new(config.clone())));
        }

        Self {
            num_workers,
            queues,
            next_worker: Arc::new(AtomicUsize::new(0)),
            schedulers,
        }
    }

    /// Start all workers
    pub async fn start(&self) -> Result<()> {
        for (i, scheduler) in self.schedulers.iter().enumerate() {
            scheduler.start(1).await?;

            // Start stealing task for this worker
            let queue = self.queues[i].clone();
            let other_queues: Vec<_> = self.queues.iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, q)| q.clone())
                .collect();

            tokio::spawn(Self::stealing_loop(queue, other_queues));
        }

        Ok(())
    }

    /// Work stealing loop
    async fn stealing_loop(
        own_queue: Arc<RwLock<VecDeque<ScheduledTask>>>,
        other_queues: Vec<Arc<RwLock<VecDeque<ScheduledTask>>>>,
    ) {
        loop {
            // Check if own queue is empty
            let should_steal = {
                let queue = own_queue.read().await;
                queue.is_empty()
            };

            if should_steal {
                // Try to steal from other queues
                for other_queue in &other_queues {
                    let stolen = {
                        let mut other = other_queue.write().await;
                        if other.len() > 1 {
                            // Steal half of the tasks
                            let steal_count = other.len() / 2;
                            let mut stolen = Vec::new();
                            for _ in 0..steal_count {
                                if let Some(task) = other.pop_back() {
                                    stolen.push(task);
                                }
                            }
                            stolen
                        } else {
                            Vec::new()
                        }
                    };

                    if !stolen.is_empty() {
                        let mut own = own_queue.write().await;
                        for task in stolen {
                            own.push_back(task);
                        }
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Submit a task using round-robin
    pub async fn submit(&self, task: Box<dyn Task>) -> Result<TaskHandle> {
        // Select worker using round-robin
        let worker_id = self.next_worker.fetch_add(1, Ordering::SeqCst) % self.num_workers;
        self.schedulers[worker_id].submit(task).await
    }

    /// Shutdown all workers
    pub async fn shutdown(&self) {
        for scheduler in &self.schedulers {
            scheduler.shutdown().await;
        }
    }
}