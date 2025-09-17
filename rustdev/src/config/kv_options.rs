//! KV options following C++ kv_options.h

use std::sync::Arc;
use std::path::PathBuf;

use crate::codec::Comparator;

/// KV options matching the C++ KvOptions structure
#[derive(Debug, Clone)]
pub struct KvOptions {
    /// Data page size (must be power of 2, min 512B)
    pub data_page_size: usize,
    /// Index page size
    pub index_page_size: usize,
    /// Maximum write batch pages
    pub max_write_batch_pages: usize,
    /// Number of overflow pointers per page
    pub overflow_pointers: usize,
    /// Maximum index pages in memory
    pub max_index_pages: usize,
    /// Index page restart interval for prefix compression
    pub index_page_restart_interval: u16,
    /// Data directories
    pub data_dirs: Vec<PathBuf>,
    /// Append-only mode
    pub data_append_mode: bool,
    /// Archive path for append mode
    pub archive_path: Option<PathBuf>,
    /// Pages per file (power of 2)
    pub pages_per_file: u32,
    /// File descriptor limit
    pub fd_limit: u64,
    /// Number of worker threads
    pub num_threads: usize,
    /// Cloud storage path
    pub cloud_store_path: Option<PathBuf>,
    /// Number of GC threads
    pub num_gc_threads: usize,
    /// Local space limit for cloud storage
    pub local_space_limit: u64,
    /// Number of retained archives
    pub num_retained_archives: usize,
    /// Archive interval in seconds
    pub archive_interval_secs: u64,
    /// Comparator for key comparison
    comparator: Arc<dyn Comparator>,
}

impl Default for KvOptions {
    fn default() -> Self {
        Self {
            data_page_size: 4096,
            index_page_size: 4096,
            max_write_batch_pages: 32,
            overflow_pointers: 8,
            max_index_pages: 10000,
            index_page_restart_interval: 16,
            data_dirs: vec![PathBuf::from("./data")],
            data_append_mode: false,
            archive_path: None,
            pages_per_file: 256,
            fd_limit: 4096,
            num_threads: 4,
            cloud_store_path: None,
            num_gc_threads: 0,
            local_space_limit: 0,
            num_retained_archives: 0,
            archive_interval_secs: 0,
            comparator: Arc::new(crate::codec::BytewiseComparator::new()),
        }
    }
}

impl KvOptions {
    /// Create new options with custom comparator
    pub fn new(comparator: Arc<dyn Comparator>) -> Self {
        Self {
            comparator,
            ..Default::default()
        }
    }

    /// Get comparator
    pub fn comparator(&self) -> Arc<dyn Comparator> {
        self.comparator.clone()
    }

    /// Get maximum index pages
    pub fn max_index_pages(&self) -> usize {
        self.max_index_pages
    }

    /// Validate options
    pub fn validate(&self) -> Result<(), String> {
        // Check page size is power of 2
        if !self.data_page_size.is_power_of_two() || self.data_page_size < 512 {
            return Err("Data page size must be power of 2 and at least 512".into());
        }

        if !self.index_page_size.is_power_of_two() || self.index_page_size < 512 {
            return Err("Index page size must be power of 2 and at least 512".into());
        }

        if self.data_dirs.is_empty() {
            return Err("At least one data directory must be specified".into());
        }

        if !self.pages_per_file.is_power_of_two() {
            return Err("Pages per file must be power of 2".into());
        }

        Ok(())
    }

    /// Get pages per file shift (for bit operations)
    pub fn pages_per_file_shift(&self) -> u8 {
        self.pages_per_file.trailing_zeros() as u8
    }
}