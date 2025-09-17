//! Core shard implementation following C++ shard.cpp

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, mpsc};

use crate::config::KvOptions;
use crate::task::{Task, TaskScheduler, ReadTask, TaskResult};
use crate::page::{PageCache, PageMapper};
use crate::storage::{AsyncFileManager, ManifestData, ManifestFile};
use crate::io::backend::{IoBackendFactory, IoBackendType};
use crate::index::IndexPageManager;
use crate::types::KvEntry;
use crate::Result;
use crate::error::KvError;

/// Shard identifier
pub type ShardId = usize;

/// Request queue item
/// Wrapper for raw pointer to make it Send
/// SAFETY: We ensure this is only used in a safe context
struct RequestPtr(*mut dyn crate::store::KvRequest);
unsafe impl Send for RequestPtr {}
unsafe impl Sync for RequestPtr {}

pub struct RequestItem {
    pub request: Box<dyn crate::store::KvRequest>,
    pub timestamp: Instant,
    /// Raw pointer to original request for setting done status
    /// SAFETY: Only valid during exec_sync lifetime
    pub original: Option<RequestPtr>,
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

/// Shard statistics
pub struct ShardStats {
    /// Read operations
    pub reads: AtomicU64,
    /// Write operations
    pub writes: AtomicU64,
    /// Scan operations
    pub scans: AtomicU64,
    /// Errors
    pub errors: AtomicU64,
    /// Request latency (microseconds)
    pub latency_us: AtomicU64,
}

impl ShardStats {
    pub fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_us: AtomicU64::new(0),
        }
    }
}

/// Shard implementation (following C++ Shard class)
pub struct Shard {
    /// Shard ID
    id: usize,
    /// KV options
    options: Arc<KvOptions>,
    /// File descriptor limit for this shard
    fd_limit: u32,
    /// Current state
    state: Arc<RwLock<ShardState>>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index page manager
    index_manager: Option<Arc<IndexPageManager>>,
    /// Task scheduler
    scheduler: Arc<TaskScheduler>,
    /// Request queue
    request_queue: mpsc::UnboundedSender<RequestItem>,
    request_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<RequestItem>>>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<ShardStats>,
}

impl Shard {
    /// Create a new shard (following C++ Shard constructor)
    pub fn new(id: usize, options: Arc<KvOptions>, fd_limit: u32) -> Self {
        let page_cache = Arc::new(PageCache::new(Default::default()));
        let page_mapper = Arc::new(PageMapper::new());

        // Create file manager with I/O backend
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();
        let base_dir = if !options.data_dirs.is_empty() {
            options.data_dirs[id % options.data_dirs.len()].clone()
        } else {
            std::path::PathBuf::from(format!("/tmp/shard_{}", id))
        };

        let file_manager = Arc::new(AsyncFileManager::new(
            base_dir,
            options.data_page_size,
            fd_limit as usize,
            backend,
        ));

        let scheduler = Arc::new(TaskScheduler::new(Default::default()));
        let (tx, rx) = mpsc::unbounded_channel();

        // Initialize index page manager here instead of in init()
        let index_manager = Some(Arc::new(IndexPageManager::new(
            file_manager.clone(),
            options.clone(),
        )));

        Self {
            id,
            options: options.clone(),
            fd_limit,
            state: Arc::new(RwLock::new(ShardState::Initializing)),
            page_cache,
            page_mapper,
            file_manager: file_manager.clone(),
            index_manager,
            scheduler,
            request_queue: tx,
            request_rx: Arc::new(RwLock::new(Some(rx))),
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(ShardStats::new()),
        }
    }

    /// Get the manifest file path for this shard
    fn get_manifest_path(&self) -> std::path::PathBuf {
        let base_dir = if !self.options.data_dirs.is_empty() {
            self.options.data_dirs[self.id % self.options.data_dirs.len()].clone()
        } else {
            std::path::PathBuf::from(format!("/tmp/shard_{}", self.id))
        };
        base_dir.join(format!("shard_{}_manifest.bin", self.id))
    }

    /// Initialize shard (following C++ Init)
    pub async fn init(&mut self) -> Result<()> {
        tracing::info!("Initializing shard {}", self.id);

        // Initialize file manager
        self.file_manager.init().await?;

        // Index manager is already initialized in new()

        // Load manifest if exists
        let manifest_path = self.get_manifest_path();
        if manifest_path.exists() {
            tracing::info!("Loading manifest from {:?}", manifest_path);

            match ManifestFile::load_from_file(&manifest_path).await {
                Ok(manifest) => {
                    // Apply manifest to restore state

                    // Restore page mappings
                    for (page_id, file_page_id) in &manifest.mappings {
                        self.page_mapper.update_mapping(*page_id, *file_page_id);
                    }
                    let num_mappings = manifest.mappings.len();
                    let num_roots = manifest.roots.len();
                    tracing::info!("Restored {} page mappings", num_mappings);

                    // Restore root metadata for each table
                    if let Some(index_mgr) = &self.index_manager {
                        index_mgr.restore_roots(manifest.roots);
                        tracing::info!("Restored {} root entries", num_roots);
                    }

                    tracing::info!("Manifest loaded successfully with {} mappings and {} roots",
                                 num_mappings, num_roots);
                }
                Err(e) => {
                    tracing::warn!("Failed to load manifest: {}. Starting fresh.", e);
                }
            }
        } else {
            tracing::info!("No manifest found at {:?}. Starting fresh.", manifest_path);
        }

        // Restore from checkpoint if available
        // TODO: Implement checkpoint restoration
        // The checkpoint contains:
        // - In-memory index pages
        // - Hot data pages in cache
        // - Transaction state

        // Update state
        let mut state = self.state.write().await;
        *state = ShardState::Running;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!("Shard {} initialized successfully", self.id);

        Ok(())
    }

    /// Add a KV request to the queue (following C++ AddKvRequest)
    pub async fn add_request(&self, request: &dyn crate::store::KvRequest) -> bool {
        if !self.running.load(Ordering::Relaxed) {
            tracing::warn!("Shard {} not running, rejecting request", self.id);
            return false;
        }

        use crate::store::{RequestType, ReadRequest, BatchWriteRequest, ScanRequest, FloorRequest};

        tracing::debug!("Shard {} adding request type {:?}", self.id, request.request_type());

        // Create a new boxed request based on the request type
        // We need to downcast and create new instances since KvRequest is not Clone
        let boxed_request: Box<dyn crate::store::KvRequest> = match request.request_type() {
            RequestType::Read => {
                if let Some(read_req) = request.as_any().downcast_ref::<ReadRequest>() {
                    // Create new request but share the value field
                    let mut new_req = ReadRequest::new(
                        read_req.base.table_id.clone(),
                        read_req.key.clone(),
                    );
                    // Share the Arc<Mutex<Option<Value>>> so updates are visible
                    new_req.value = read_req.value.clone();
                    Box::new(new_req)
                } else {
                    return false;
                }
            }
            RequestType::BatchWrite => {
                if let Some(write_req) = request.as_any().downcast_ref::<BatchWriteRequest>() {
                    Box::new(BatchWriteRequest::new(
                        write_req.base.table_id.clone(),
                        write_req.entries.clone(),
                    ))
                } else {
                    return false;
                }
            }
            RequestType::Scan => {
                if let Some(scan_req) = request.as_any().downcast_ref::<ScanRequest>() {
                    let mut new_req = ScanRequest::new(
                        scan_req.base.table_id.clone(),
                        scan_req.begin_key.clone(),
                        scan_req.end_key.clone(),
                        scan_req.begin_inclusive,
                    );
                    new_req.set_pagination(scan_req.max_entries, scan_req.max_size);
                    Box::new(new_req)
                } else {
                    return false;
                }
            }
            RequestType::Floor => {
                if let Some(floor_req) = request.as_any().downcast_ref::<FloorRequest>() {
                    Box::new(FloorRequest::new(
                        floor_req.base.table_id.clone(),
                        floor_req.key.clone(),
                    ))
                } else {
                    return false;
                }
            }
            _ => {
                // Unsupported request type
                return false;
            }
        };

        // Queue the request with a reference to the original
        let item = RequestItem {
            request: boxed_request,
            timestamp: Instant::now(),
            // Cast to mutable pointer - safe because exec_sync waits for completion
            original: Some(RequestPtr(request as *const dyn crate::store::KvRequest as *mut dyn crate::store::KvRequest)),
        };

        self.request_queue.send(item).is_ok()
    }

    /// Run the shard worker (following C++ Run coroutine)
    pub async fn run(&self) {
        self.running.store(true, Ordering::Relaxed);
        tracing::info!("Shard {} starting", self.id);

        // Take the receiver
        let rx = {
            let mut rx_lock = self.request_rx.write().await;
            rx_lock.take()
        };

        if let Some(mut rx) = rx {
            while self.running.load(Ordering::Relaxed) {
                // Process requests from queue
                tokio::select! {
                    Some(item) = rx.recv() => {
                        tracing::debug!("Shard {} received request from queue", self.id);
                        // Process the request
                        self.process_request(item).await;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Periodic maintenance tasks
                        self.maintenance().await;
                    }
                }
            }
        }

        tracing::info!("Shard {} stopped", self.id);
    }

    /// Helper to set done on the correct request
    fn set_request_done(original: Option<RequestPtr>, request: &Box<dyn crate::store::KvRequest>, error: Option<KvError>) {
        if let Some(RequestPtr(orig_ptr)) = original {
            unsafe {
                (*orig_ptr).set_done(error);
            }
        } else {
            request.set_done(error);
        }
    }

    /// Process a single request
    async fn process_request(&self, item: RequestItem) {
        use crate::store::RequestType;
        use crate::task::TaskContext;

        let start = Instant::now();
        let request = item.request;
        let original = item.original;

        tracing::debug!("Shard {} processing request type {:?}", self.id, request.request_type());

        match request.request_type() {
            RequestType::Read => {
                self.stats.reads.fetch_add(1, Ordering::Relaxed);

                if let Some(read_req) = request.as_any().downcast_ref::<crate::store::ReadRequest>() {
                    // Use simplified read for now
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                            return;
                        }
                    };

                    match crate::task::write_simple::simple_read(
                        &read_req.key,
                        &read_req.base.table_id,
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager,
                    ).await {
                        Ok(value) => {
                            // Set result on request BEFORE marking as done
                            *read_req.value.lock() = value;
                            // Mark as done AFTER setting the value
                            Self::set_request_done(original, &request, None);
                        }
                        Err(e) => {
                            tracing::error!("Read error: {:?}", e);
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                        }
                    }
                }
            }
            RequestType::BatchWrite => {
                self.stats.writes.fetch_add(1, Ordering::Relaxed);

                if let Some(write_req) = request.as_any().downcast_ref::<crate::store::BatchWriteRequest>() {
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                            return;
                        }
                    };

                    // Use simplified write for now - just write first entry for testing
                    let mut had_error = false;
                    for entry in &write_req.entries {
                        match crate::task::write_simple::simple_write(
                            &entry.key,
                            &entry.value,
                            &write_req.base.table_id,
                            self.page_cache.clone(),
                            self.page_mapper.clone(),
                            self.file_manager.clone(),
                            index_manager.clone(),
                        ).await {
                            Ok(_) => {
                                }
                            Err(e) => {
                                tracing::error!("Write error: {:?}", e);
                                had_error = true;
                                break;
                            }
                        }
                    }

                    if had_error {
                        self.stats.errors.fetch_add(1, Ordering::Relaxed);
                        Self::set_request_done(original, &request,Some(KvError::InternalError));
                    } else {
                        Self::set_request_done(original, &request,None);
                    }
                }
            }
            RequestType::Scan => {
                self.stats.scans.fetch_add(1, Ordering::Relaxed);
                if let Some(scan_req) = request.as_any().downcast_ref::<crate::store::ScanRequest>() {
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                            return;
                        }
                    };

                    let task = crate::task::ScanTask::new(
                        scan_req.begin_key.clone(),
                        Some(scan_req.end_key.clone()),
                        scan_req.max_entries,
                        scan_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager,
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(result) => {
                            // Set scan results on the original request
                            if let TaskResult::Scan(entries) = result {
                                if let Some(RequestPtr(orig_ptr)) = original {
                                    unsafe {
                                        // orig_ptr is already mutable
                                        if let Some(orig_scan) = (*orig_ptr).as_any().downcast_ref::<crate::store::ScanRequest>() {
                                            let mutable_scan = orig_scan as *const crate::store::ScanRequest as *mut crate::store::ScanRequest;
                                            (*mutable_scan).entries = entries.into_iter()
                                                .map(|(k, v)| KvEntry {
                                                    key: k.to_vec(),
                                                    value: v.to_vec(),
                                                    timestamp: 0,
                                                    expire_ts: 0,
                                                })
                                                .collect();
                                        }
                                    }
                                }
                            }
                            Self::set_request_done(original, &request,None);
                        }
                        Err(e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                        }
                    }
                } else {
                    Self::set_request_done(original, &request,Some(KvError::InvalidArgument));
                }
            }
            RequestType::Floor => {
                self.stats.reads.fetch_add(1, Ordering::Relaxed);

                if let Some(floor_req) = request.as_any().downcast_ref::<crate::store::ReadRequest>() {
                    let index_manager = match self.index_manager.as_ref() {
                        Some(im) => im,
                        None => {
                            tracing::error!("Index manager not initialized");
                            Self::set_request_done(original, &request,Some(KvError::NotFound));
                            return;
                        }
                    };
                    let task = ReadTask::floor(
                        floor_req.key.clone(),
                        floor_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager.clone(),
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(_result) => {
                            // TODO: Set result on request
                            Self::set_request_done(original, &request,None);
                        }
                        Err(_e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                        }
                    }
                } else {
                    Self::set_request_done(original, &request,Some(KvError::InvalidArgument));
                }
            }
            _ => {
                // Other request types like Archive, Compact, CleanExpired are background operations
                Self::set_request_done(original, &request,Some(KvError::NotSupported));
            }
        }

        // Update latency
        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.latency_us.store(elapsed, Ordering::Relaxed);
    }

    /// Periodic maintenance tasks
    async fn maintenance(&self) {
        // Check if maintenance is needed every few iterations
        use std::sync::atomic::AtomicUsize;
        static MAINTENANCE_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let count = MAINTENANCE_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Run maintenance every 1000 iterations
        if count % 1000 == 0 {
            // Evict cold pages from cache if needed
            // TODO: Implement cache eviction based on LRU or similar strategy

            // Trigger background compaction if needed
            if self.options.data_append_mode && self.index_manager.is_some() {
                // TODO: Check amplification factor and trigger compaction
                // For now, just log that we would compact
                tracing::debug!("Would trigger background compaction");
            }

            // Trigger file GC if configured
            if self.options.num_gc_threads > 0 {
                // TODO: Implement file GC triggering
                // Need to get retained files from index and mapping timestamp from manifest
                tracing::debug!("Would trigger file GC");
            }

            // NOTE: Removed dirty page flushing
            // Writes are now synchronous - pages are written immediately to disk
            // This ensures durability without WAL

            // Checkpoint every 10000 iterations (less frequent than other maintenance)
            if count % 10000 == 0 {
                tracing::debug!("Running periodic checkpoint");
                if let Err(e) = self.save_checkpoint().await {
                    tracing::error!("Failed to save periodic checkpoint: {}", e);
                }
            }
        }
    }

    /// Save manifest checkpoint
    async fn save_checkpoint(&self) -> Result<()> {
        let manifest_path = self.get_manifest_path();
        tracing::info!("Saving manifest checkpoint to {:?}", manifest_path);

        // Create manifest data
        let mut manifest = ManifestData::new();
        manifest.version = 1;
        manifest.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Collect page mappings from PageMapper
        let mappings = self.page_mapper.export_mappings();
        for (page_id, file_page_id) in mappings {
            manifest.mappings.insert(page_id, file_page_id);
        }
        tracing::debug!("Collected {} page mappings", manifest.mappings.len());

        // Collect root metadata from index manager
        if let Some(index_mgr) = &self.index_manager {
            manifest.roots = index_mgr.export_roots();
            tracing::debug!("Collected {} root entries", manifest.roots.len());
        }

        // Save the manifest
        ManifestFile::save_to_file(&manifest, &manifest_path).await?;

        tracing::info!("Manifest checkpoint saved with {} mappings and {} roots",
                      manifest.mappings.len(), manifest.roots.len());
        Ok(())
    }

    /// Stop the shard (following C++ Stop)
    pub async fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);

        // Update state
        let mut state = self.state.write().await;
        *state = ShardState::Stopping;
        drop(state); // Release lock before async operations

        // Save manifest checkpoint before shutdown
        if let Err(e) = self.save_checkpoint().await {
            tracing::error!("Failed to save manifest during shutdown: {}", e);
        }

        // Clear the page cache (no dirty pages to flush - writes are synchronous)
        // NOTE: Without WAL, all writes are flushed to disk immediately
        // so there are no dirty pages to flush at shutdown
        self.page_cache.clear().await;
        tracing::debug!("Cleared page cache");

        // 2. Sync all open files to disk
        if let Err(e) = self.file_manager.sync_all().await {
            tracing::error!("Failed to sync files during shutdown: {}", e);
        }

        // 3. Close all file handles (handled by drop)

        // Update final state
        let mut state = self.state.write().await;
        *state = ShardState::Stopped;

        tracing::info!("Shard {} stopped", self.id);
    }

    /// Get shard statistics
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }

    /// Get shard ID
    pub fn id(&self) -> usize {
        self.id
    }
}