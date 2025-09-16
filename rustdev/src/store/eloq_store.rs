//! Main EloqStore implementation following C++ eloq_store.cpp

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::PathBuf;
use std::os::unix::io::IntoRawFd;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::KvOptions;
use crate::error::{Error, KvError};
use crate::shard::Shard;
use crate::Result;

use super::request;

/// Main EloqStore structure
pub struct EloqStore {
    /// Configuration options
    options: Arc<KvOptions>,

    /// Shards for parallel processing
    shards: Vec<Arc<Shard>>,

    /// Shard worker handles
    shard_handles: Vec<JoinHandle<()>>,

    /// Stopped flag
    stopped: AtomicBool,

    /// Root file descriptors for data directories
    root_fds: Vec<i32>,

    // Optional components (following C++)
    // file_gc: Option<FileGarbageCollector>,
    // archive_crond: Option<ArchiveCrond>,
    // obj_store: Option<ObjectStore>,
}

impl EloqStore {
    /// Create a new EloqStore instance
    pub fn new(options: KvOptions) -> Result<Self> {
        // Validate options (following C++ constructor)
        if (options.data_page_size & 0xFFF) != 0 {
            return Err(Error::InvalidConfig("data_page_size must be page aligned (4KB)".into()));
        }

        if options.overflow_pointers == 0 || options.overflow_pointers > crate::types::MAX_OVERFLOW_POINTERS {
            return Err(Error::InvalidConfig("Invalid overflow_pointers".into()));
        }

        if options.max_write_batch_pages == 0 {
            return Err(Error::InvalidConfig("Invalid max_write_batch_pages".into()));
        }

        // Cloud storage validation
        if let Some(ref cloud_path) = options.cloud_store_path {
            if !cloud_path.as_os_str().is_empty() {
                if options.num_gc_threads > 0 {
                    return Err(Error::InvalidConfig(
                        "num_gc_threads must be 0 when cloud store is enabled".into()
                    ));
                }
                if options.local_space_limit == 0 {
                    return Err(Error::InvalidConfig(
                        "Must set local_space_limit when cloud store is enabled".into()
                    ));
                }
            }
        }

        let options = Arc::new(options);

        Ok(Self {
            options,
            shards: Vec::new(),
            shard_handles: Vec::new(),
            stopped: AtomicBool::new(true),
            root_fds: Vec::new(),
        })
    }

    /// Start the store
    pub async fn start(&mut self) -> Result<()> {
        if !self.is_stopped() {
            return Ok(());
        }

        // Initialize store space (following C++ InitStoreSpace)
        self.init_store_space()?;

        // TODO: Initialize cloud storage if configured
        // if let Some(ref cloud_path) = self.options.cloud_store_path {
        //     self.obj_store = Some(ObjectStore::new(&self.options));
        // }

        // Calculate per-shard file descriptor limit
        let used_fd = Self::count_used_fd();
        let reserved_fd = 100; // Reserved FDs for system use
        let shard_fd_limit = if used_fd + reserved_fd < self.options.fd_limit as usize {
            (self.options.fd_limit as usize - used_fd - reserved_fd) / self.options.num_threads
        } else {
            0
        };

        // Create shards (following C++ shard creation)
        self.shards.clear();
        for i in 0..self.options.num_threads {
            let mut shard = Shard::new(
                i,
                self.options.clone(),
                shard_fd_limit as u32,
            );
            shard.init().await?;
            self.shards.push(Arc::new(shard));
        }

        // Mark as started
        self.stopped.store(false, Ordering::Relaxed);

        // Start optional components in append mode
        if self.options.data_append_mode {
            // TODO: Start file GC if configured
            // if self.options.num_gc_threads > 0 {
            //     self.file_gc = Some(FileGarbageCollector::new(&self.options));
            //     self.file_gc.as_mut().unwrap().start(self.options.num_gc_threads);
            // }

            // TODO: Start archive cron if configured
            // if self.options.num_retained_archives > 0 && self.options.archive_interval_secs > 0 {
            //     self.archive_crond = Some(ArchiveCrond::new(self));
            //     self.archive_crond.as_mut().unwrap().start();
            // }
        }

        // TODO: Start cloud storage if configured
        // if let Some(ref mut obj_store) = self.obj_store {
        //     obj_store.start();
        // }

        // Start shard workers
        for shard in &self.shards {
            let shard_clone = shard.clone();
            let handle = tokio::spawn(async move {
                shard_clone.run().await;
            });
            self.shard_handles.push(handle);
        }

        tracing::info!("EloqStore started with {} shards", self.shards.len());
        Ok(())
    }

    /// Stop the store
    pub async fn stop(&mut self) -> Result<()> {
        if self.is_stopped() {
            return Ok(());
        }

        // TODO: Stop archive cron if running
        // if let Some(ref mut archive_crond) = self.archive_crond {
        //     archive_crond.stop().await;
        // }

        // Mark as stopped
        self.stopped.store(true, Ordering::Relaxed);

        // Stop all shards
        for shard in &self.shards {
            shard.stop().await;
        }

        // Wait for shard workers to finish
        for handle in self.shard_handles.drain(..) {
            let _ = handle.await;
        }

        // TODO: Stop cloud storage if configured
        // if let Some(ref mut obj_store) = self.obj_store {
        //     obj_store.stop().await;
        // }

        // TODO: Stop file GC if running
        // if let Some(ref mut file_gc) = self.file_gc {
        //     file_gc.stop().await;
        // }

        // Clear resources
        self.shards.clear();

        // Close root file descriptors
        for fd in self.root_fds.drain(..) {
            unsafe {
                libc::close(fd);
            }
        }

        tracing::info!("EloqStore stopped");
        Ok(())
    }

    /// Execute request asynchronously with callback
    pub async fn exec_async<F>(&self, req: &dyn request::KvRequest, user_data: u64, callback: F) -> bool
    where
        F: FnOnce(&dyn request::KvRequest) + Send + 'static
    {
        // TODO: Set user_data and callback on request
        // req.base.user_data = user_data;
        // req.base.callback = Some(Box::new(callback));
        self.send_request(req).await
    }

    /// Execute request asynchronously without callback
    pub async fn exec_async_no_callback(&self, req: &dyn request::KvRequest) -> bool {
        self.send_request(req).await
    }

    /// Execute request synchronously
    pub fn exec_sync(&self, req: &dyn request::KvRequest) {
        if !futures::executor::block_on(self.send_request(req)) {
            req.set_done(Some(KvError::NotRunning));
        } else {
            req.wait();
        }
    }

    /// Send request to appropriate shard
    async fn send_request(&self, req: &dyn request::KvRequest) -> bool {
        if self.stopped.load(Ordering::Relaxed) {
            return false;
        }

        // Reset request state
        req.set_done(None);

        // Route to appropriate shard based on table ID
        let shard_index = req.table_id().shard_index(self.shards.len() as u16) as usize;
        let shard = &self.shards[shard_index];

        shard.add_request(req).await
    }

    /// Initialize store space (following C++ InitStoreSpace)
    fn init_store_space(&mut self) -> Result<()> {
        // Set file descriptor limit
        #[cfg(unix)]
        {
            use libc::{rlimit, getrlimit, setrlimit, RLIMIT_NOFILE};

            let mut fd_limit = rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };

            unsafe {
                if getrlimit(RLIMIT_NOFILE, &mut fd_limit) < 0 {
                    return Err(Error::Io(std::io::Error::last_os_error()));
                }

                if fd_limit.rlim_cur < self.options.fd_limit {
                    fd_limit.rlim_cur = self.options.fd_limit;
                    if setrlimit(RLIMIT_NOFILE, &fd_limit) != 0 {
                        return Err(Error::from(KvError::OpenFileLimit));
                    }
                }
            }
        }

        // Create data directories and open root FDs
        let cloud_store = self.options.cloud_store_path.is_some();

        for data_dir in &self.options.data_dirs {
            // Create directory if it doesn't exist
            if !data_dir.exists() {
                std::fs::create_dir_all(data_dir)?;
            }

            // Open directory for O_PATH access (following C++)
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                use std::fs::OpenOptions;

                let fd = OpenOptions::new()
                    .read(true)
                    .open(data_dir)?
                    .into_raw_fd();

                self.root_fds.push(fd);
            }

            // Set permissions if cloud storage is enabled
            if cloud_store {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = std::fs::metadata(data_dir)?.permissions();
                    perms.set_mode(0o755);
                    std::fs::set_permissions(data_dir, perms)?;
                }
            }
        }

        // Create archive directory if configured
        if let Some(ref archive_path) = self.options.archive_path {
            if !archive_path.as_os_str().is_empty() && !archive_path.exists() {
                std::fs::create_dir_all(archive_path)?;
            }
        }

        Ok(())
    }

    /// Count used file descriptors (approximation)
    fn count_used_fd() -> usize {
        #[cfg(unix)]
        {
            if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
                return entries.count();
            }
        }
        // Default estimate
        10
    }

    /// Check if the store is stopped
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    /// Get options
    pub fn options(&self) -> &KvOptions {
        &self.options
    }
}

impl Drop for EloqStore {
    fn drop(&mut self) {
        if !self.is_stopped() {
            // Use blocking stop in destructor
            let _ = futures::executor::block_on(self.stop());
        }
    }
}

// Note: Request types are re-exported from store/mod.rs