//! Core shard implementation

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent};
use crate::task::{Task, TaskHandle, TaskScheduler};
use crate::page::{PageCache, PageMapper};
use crate::storage::FileManager;
use crate::Result;
use crate::error::Error;

/// Shard identifier
pub type ShardId = u16;

/// Shard configuration
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Shard ID
    pub id: ShardId,
    /// Number of worker threads
    pub num_workers: usize,
    /// Queue capacity
    pub queue_capacity: usize,
    /// Page cache size
    pub cache_size: usize,
    /// Enable compression
    pub compression: bool,
    /// Batch size for writes
    pub batch_size: usize,
    /// Flush interval
    pub flush_interval: Duration,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            id: 0,
            num_workers: 4,
            queue_capacity: 10000,
            cache_size: 256 * 1024 * 1024, // 256MB
            compression: true,
            batch_size: 100,
            flush_interval: Duration::from_secs(1),
        }
    }
}

/// Shard state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Shard is initializing
    Initializing,
    /// Shard is running
    Running,
    /// Shard is paused
    Paused,
    /// Shard is stopping
    Stopping,
    /// Shard is stopped
    Stopped,
}

/// Shard implementation
pub struct Shard {
    /// Shard ID
    id: ShardId,
    /// Configuration
    config: ShardConfig,
    /// Current state
    state: Arc<RwLock<ShardState>>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<FileManager>,
    /// Task scheduler
    scheduler: Arc<TaskScheduler>,
    /// Statistics
    stats: Arc<ShardStats>,
    /// Worker handles
    workers: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown: tokio_util::sync::CancellationToken,
}

impl Shard {
    /// Create a new shard
    pub async fn new(config: ShardConfig) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(Default::default()));
        let page_mapper = Arc::new(PageMapper::new());
        // Create file manager with default configuration
        let base_dir = std::path::PathBuf::from(format!("/tmp/shard_{}", config.id));
        let file_manager = Arc::new(FileManager::new(base_dir, 4096, 100));
        let scheduler = Arc::new(TaskScheduler::new(Default::default()));

        Ok(Self {
            id: config.id,
            config,
            state: Arc::new(RwLock::new(ShardState::Initializing)),
            page_cache,
            page_mapper,
            file_manager,
            scheduler,
            stats: Arc::new(ShardStats::new()),
            workers: Arc::new(RwLock::new(Vec::new())),
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Start the shard
    pub async fn start(&self) -> Result<()> {
        // Update state
        {
            let mut state = self.state.write().await;
            if *state != ShardState::Initializing && *state != ShardState::Stopped {
                return Err(Error::InvalidState("Shard already started".into()));
            }
            *state = ShardState::Running;
        }

        // Start scheduler
        self.scheduler.start(self.config.num_workers).await?;

        // Start workers
        let mut workers = self.workers.write().await;
        for worker_id in 0..self.config.num_workers {
            let shard_id = self.id;
            let shutdown = self.shutdown.clone();
            let stats = self.stats.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(shard_id, worker_id, shutdown, stats).await;
            });

            workers.push(handle);
        }

        Ok(())
    }

    /// Stop the shard
    pub async fn stop(&self) -> Result<()> {
        // Update state
        {
            let mut state = self.state.write().await;
            if *state != ShardState::Running {
                return Ok(());
            }
            *state = ShardState::Stopping;
        }

        // Signal shutdown
        self.shutdown.cancel();

        // Wait for workers to finish
        let mut workers = self.workers.write().await;
        for worker in workers.drain(..) {
            let _ = worker.await;
        }

        // Shutdown scheduler
        self.scheduler.shutdown().await;

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ShardState::Stopped;
        }

        Ok(())
    }

    /// Pause the shard
    pub async fn pause(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state == ShardState::Running {
            *state = ShardState::Paused;
            Ok(())
        } else {
            Err(Error::InvalidState("Shard not running".into()))
        }
    }

    /// Resume the shard
    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state == ShardState::Paused {
            *state = ShardState::Running;
            Ok(())
        } else {
            Err(Error::InvalidState("Shard not paused".into()))
        }
    }

    /// Get shard ID
    pub fn id(&self) -> ShardId {
        self.id
    }

    /// Get current state
    pub async fn state(&self) -> ShardState {
        *self.state.read().await
    }

    /// Submit a task to the shard
    pub async fn submit_task(&self, task: Box<dyn Task>) -> Result<TaskHandle> {
        // Check state
        let state = self.state.read().await;
        if *state != ShardState::Running {
            return Err(Error::InvalidState("Shard not running".into()));
        }

        // Submit to scheduler
        self.scheduler.submit(task).await
    }

    /// Get shard statistics
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }

    /// Worker loop
    async fn worker_loop(
        shard_id: ShardId,
        worker_id: usize,
        shutdown: tokio_util::sync::CancellationToken,
        stats: Arc<ShardStats>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Process work items
                    stats.record_heartbeat();
                }
            }
        }
    }

    /// Check if shard owns a key
    pub fn owns_key(&self, key: &Key, total_shards: u16) -> bool {
        let hash = Self::hash_key(key);
        (hash % total_shards as u64) as u16 == self.id
    }

    /// Hash a key
    fn hash_key(key: &Key) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

/// Shard statistics
pub struct ShardStats {
    /// Total requests processed
    pub requests: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Current queue depth
    pub queue_depth: AtomicUsize,
    /// Last heartbeat time
    last_heartbeat: RwLock<Instant>,
}

impl ShardStats {
    /// Create new stats
    pub fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            last_heartbeat: RwLock::new(Instant::now()),
        }
    }

    /// Record a heartbeat
    pub fn record_heartbeat(&self) {
        if let Ok(mut last) = self.last_heartbeat.try_write() {
            *last = Instant::now();
        }
    }

    /// Get time since last heartbeat
    pub async fn time_since_heartbeat(&self) -> Duration {
        let last = self.last_heartbeat.read().await;
        last.elapsed()
    }

    /// Increment request count
    pub fn inc_requests(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Add bytes read
    pub fn add_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add bytes written
    pub fn add_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Update queue depth
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }
}