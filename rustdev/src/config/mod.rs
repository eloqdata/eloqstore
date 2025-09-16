//! Configuration management for EloqStore

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Runtime configuration
    pub runtime: RuntimeConfig,
    /// Storage configuration
    pub storage: StorageConfig,
}

/// Runtime configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Number of worker threads (shards)
    pub num_shards: u16,
    /// Number of I/O threads
    pub io_threads: u16,
    /// Maximum number of open files
    pub max_open_files: u32,
    /// Buffer pool size in MB
    pub buffer_pool_size_mb: usize,
}

/// Storage configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    /// Data directories
    pub data_dirs: Vec<PathBuf>,
    /// Page size in bytes
    pub page_size: usize,
    /// Maximum file size in MB
    pub max_file_size_mb: usize,
    /// Enable compression
    pub compression: bool,
    /// Storage mode (append-only or normal)
    pub append_mode: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime: RuntimeConfig {
                num_shards: 4,
                io_threads: 2,
                max_open_files: 1024,
                buffer_pool_size_mb: 256,
            },
            storage: StorageConfig {
                data_dirs: vec![PathBuf::from("./data")],
                page_size: 4096,
                max_file_size_mb: 1024,
                compression: false,
                append_mode: false,
            },
        }
    }
}