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

                    // First restore roots with their COW mappers
                    let mut roots_with_mappings = manifest.roots.clone();

                    // Populate COW mappers with the saved mappings
                    for (_table_id, root_meta) in roots_with_mappings.iter_mut() {
                        if let Some(ref mut mapper) = root_meta.mapper {
                            // Clear any existing mappings and rebuild from manifest
                            for (page_id, file_page_id) in &manifest.mappings {
                                mapper.update_mapping(*page_id, *file_page_id);
                            }
                        }
                    }

                    // Restore page mappings to shard's page_mapper
                    for (page_id, file_page_id) in &manifest.mappings {
                        self.page_mapper.update_mapping(*page_id, *file_page_id);
                    }
                    let num_mappings = manifest.mappings.len();
                    let num_roots = roots_with_mappings.len();
                    tracing::info!("Restored {} page mappings", num_mappings);

                    // Restore root metadata for each table
                    if let Some(index_mgr) = &self.index_manager {
                        index_mgr.restore_roots(roots_with_mappings);
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

        // Checkpoint restoration is done lazily in C++ when FindRoot is called
        // The manifest loading above already handles the core restoration
        // Additional restoration (index pages, cache) happens on demand

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
                    // Use proper ReadTask following C++
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                            return;
                        }
                    };

                    // Create ReadTask following C++
                    let read_task = crate::task::read::ReadTask::new(
                        read_req.base.table_id.clone(),
                        read_req.key.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager,
                        self.options.clone(),
                    );

                    // Execute the task
                    let context = crate::task::traits::TaskContext {
                        shard_id: self.id as u32,
                        options: self.options.clone(),
                    };

                    match read_task.execute(&context).await {
                        Ok(crate::task::traits::TaskResult::Read(value)) => {
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
                        _ => {
                            tracing::warn!("Unexpected task result");
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                        }
                    }
                }
            }
            RequestType::BatchWrite => {
                self.stats.writes.fetch_add(1, Ordering::Relaxed);

                if let Some(write_req) = request.as_any().downcast_ref::<crate::store::BatchWriteRequest>() {
                    // Use proper BatchWriteTask implementation
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            Self::set_request_done(original, &request,Some(KvError::InternalError));
                            return;
                        }
                    };

                    // Convert entries to task::write::WriteDataEntry format
                    // Note: write_req.entries are already crate::types::WriteDataEntry
                    let entries: Vec<crate::task::write::WriteDataEntry> = write_req.entries.iter().map(|e| {
                        crate::task::write::WriteDataEntry {
                            key: crate::types::Key::from(e.key.clone()),
                            value: crate::types::Value::from(e.value.clone()),
                            op: match e.op {
                                crate::types::WriteOp::Upsert => crate::task::write::WriteOp::Upsert,
                                crate::types::WriteOp::Delete => crate::task::write::WriteOp::Delete,
                            },
                            timestamp: e.timestamp,
                            expire_ts: e.expire_ts,  // already u64
                        }
                    }).collect();

                    tracing::debug!("Creating BatchWriteTask with {} entries", entries.len());

                    // Create and execute BatchWriteTask
                    let batch_task = crate::task::write::BatchWriteTask::new(
                        entries,
                        write_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager.clone(),
                    );

                    let ctx = crate::task::TaskContext {
                        shard_id: self.id as u32,
                        options: self.options.clone(),
                    };

                    match batch_task.execute(&ctx).await {
                        Ok(crate::task::TaskResult::BatchWrite(count)) => {
                            tracing::debug!("BatchWriteTask completed successfully: {} entries", count);
                            self.stats.writes.fetch_add(count as u64, Ordering::Relaxed);
                            Self::set_request_done(original, &request, None);
                        }
                        Ok(other) => {
                            tracing::error!("Unexpected result from BatchWriteTask: {:?}", other);
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            Self::set_request_done(original, &request, Some(KvError::InternalError));
                        }
                        Err(e) => {
                            tracing::error!("BatchWriteTask error: {:?}", e);
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            Self::set_request_done(original, &request, Some(KvError::InternalError));
                        }
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
                    // Create ReadTask and call floor method
                    let read_task = crate::task::read::ReadTask::new(
                        floor_req.base.table_id.clone(),
                        floor_req.key.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager.clone(),
                        self.options.clone(),
                    );

                    // Call floor method directly
                    match read_task.floor().await {
                        Ok(Some((_floor_key, value, _timestamp, _expire_ts))) => {
                            // Set result on request
                            *floor_req.value.lock() = Some(value.into());
                            Self::set_request_done(original, &request,None);
                        }
                        Ok(None) => {
                            *floor_req.value.lock() = None;
                            Self::set_request_done(original, &request,None);
                        }
                        Err(e) => {
                            tracing::error!("Floor error: {:?}", e);
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
            // Cache eviction is handled by PageCache's background task which is started
            // when the cache is created. It monitors memory pressure and evicts pages
            // using LRU policy when needed.

            // Trigger background compaction if needed
            if self.options.data_append_mode && self.index_manager.is_some() {
                // Compaction is triggered by BatchWriteTask when amplification factor
                // exceeds threshold. See BatchWriteTask::compact_if_needed()
                tracing::debug!("Background compaction handled by write tasks");
            }

            // Trigger file GC if configured
            if self.options.num_gc_threads > 0 {
                // File GC is triggered by write tasks when needed
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

        // Collect root metadata from index manager (includes COW mappers)
        if let Some(index_mgr) = &self.index_manager {
            manifest.roots = index_mgr.export_roots();
            tracing::debug!("Collected {} root entries", manifest.roots.len());

            // Extract all page mappings from COW metadata mappers
            for (_table_id, root_meta) in &manifest.roots {
                if let Some(ref mapper) = root_meta.mapper {
                    // The mapper contains the page mappings for this table
                    let cow_mappings = mapper.export_mappings();
                    for (page_id, file_page_id) in cow_mappings {
                        manifest.mappings.insert(page_id, file_page_id);
                    }
                }
            }
        }

        // Also collect any mappings from the shard's page_mapper (for non-COW operations)
        let shard_mappings = self.page_mapper.export_mappings();
        for (page_id, file_page_id) in shard_mappings {
            // Only insert if not already present (COW mappings take precedence)
            manifest.mappings.entry(page_id).or_insert(file_page_id);
        }

        tracing::debug!("Collected {} total page mappings", manifest.mappings.len());

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