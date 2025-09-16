//! I/O backend factory for creating and configuring backends

use std::sync::Arc;

use crate::Result;
use crate::error::Error;

use super::{IoBackend, sync_backend::SyncBackend, tokio_backend::TokioBackend, thread_pool_backend::ThreadPoolBackend};
// #[cfg(target_os = "linux")]
// use super::uring_backend::UringBackend;

/// I/O backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackendType {
    /// Synchronous blocking I/O
    Sync,
    /// Tokio async I/O
    Tokio,
    /// Thread pool based I/O
    ThreadPool,
    /// io_uring (Linux only)
    #[cfg(target_os = "linux")]
    IoUring,
}

impl IoBackendType {
    /// Get backend name
    pub fn name(&self) -> &'static str {
        match self {
            IoBackendType::Sync => "sync",
            IoBackendType::Tokio => "tokio",
            IoBackendType::ThreadPool => "thread_pool",
            #[cfg(target_os = "linux")]
            IoBackendType::IoUring => "io_uring",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sync" | "blocking" => Some(IoBackendType::Sync),
            "tokio" | "async" => Some(IoBackendType::Tokio),
            "thread_pool" | "threadpool" => Some(IoBackendType::ThreadPool),
            #[cfg(target_os = "linux")]
            "io_uring" | "iouring" | "uring" => Some(IoBackendType::IoUring),
            _ => None,
        }
    }
}

/// I/O backend configuration
#[derive(Debug, Clone)]
pub struct IoBackendConfig {
    /// Backend type
    pub backend_type: IoBackendType,
    /// Thread pool size (for ThreadPool backend)
    pub thread_pool_size: Option<usize>,
    /// Queue depth (for io_uring backend)
    pub queue_depth: Option<u32>,
    /// Enable direct I/O
    pub direct_io: bool,
    /// Enable O_SYNC
    pub sync_io: bool,
    /// Buffer alignment (for direct I/O)
    pub alignment: usize,
}

impl Default for IoBackendConfig {
    fn default() -> Self {
        Self {
            backend_type: IoBackendType::Tokio,
            thread_pool_size: Some(4),
            queue_depth: Some(256),
            direct_io: false,
            sync_io: false,
            alignment: 512,
        }
    }
}

/// I/O backend factory
pub struct IoBackendFactory;

impl IoBackendFactory {
    /// Create a backend with default configuration
    pub fn create_default(backend_type: IoBackendType) -> Result<Arc<dyn IoBackend>> {
        let config = IoBackendConfig {
            backend_type,
            ..Default::default()
        };
        Self::create(config)
    }

    /// Create a backend with configuration
    pub fn create(config: IoBackendConfig) -> Result<Arc<dyn IoBackend>> {
        match config.backend_type {
            IoBackendType::Sync => {
                Ok(Arc::new(SyncBackend::new()))
            }
            IoBackendType::Tokio => {
                Ok(Arc::new(TokioBackend::new()))
            }
            IoBackendType::ThreadPool => {
                Ok(Arc::new(ThreadPoolBackend::new()))
            }
            #[cfg(target_os = "linux")]
            IoBackendType::IoUring => {
                // let queue_depth = config.queue_depth.unwrap_or(256);
                // Ok(Arc::new(UringBackend::new(queue_depth)?))
                Err(Error::NotSupported("IoUring backend temporarily disabled due to thread safety issues".into()))
            }
        }
    }

    /// Get available backends on this platform
    pub fn available_backends() -> Vec<IoBackendType> {
        let mut backends = vec![
            IoBackendType::Sync,
            IoBackendType::Tokio,
            IoBackendType::ThreadPool,
        ];

        #[cfg(target_os = "linux")]
        backends.push(IoBackendType::IoUring);

        backends
    }

    /// Check if a backend is available
    pub fn is_available(backend_type: IoBackendType) -> bool {
        Self::available_backends().contains(&backend_type)
    }

    /// Get recommended backend for the platform
    pub fn recommended_backend() -> IoBackendType {
        #[cfg(target_os = "linux")]
        {
            // Prefer io_uring on Linux when available
            if Self::is_io_uring_available() {
                return IoBackendType::IoUring;
            }
        }

        // Default to tokio for async operations
        IoBackendType::Tokio
    }

    #[cfg(target_os = "linux")]
    fn is_io_uring_available() -> bool {
        // Check kernel version or try to probe io_uring
        // For now, just return false until implemented
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_parsing() {
        assert_eq!(IoBackendType::from_str("sync"), Some(IoBackendType::Sync));
        assert_eq!(IoBackendType::from_str("tokio"), Some(IoBackendType::Tokio));
        assert_eq!(IoBackendType::from_str("ASYNC"), Some(IoBackendType::Tokio));
        assert_eq!(IoBackendType::from_str("invalid"), None);
    }

    #[tokio::test]
    async fn test_factory_creation() {
        // Test sync backend creation
        let sync_backend = IoBackendFactory::create_default(IoBackendType::Sync).unwrap();
        assert_eq!(sync_backend.backend_type(), IoBackendType::Sync);
        assert!(!sync_backend.is_async());

        // Test tokio backend creation
        let tokio_backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();
        assert_eq!(tokio_backend.backend_type(), IoBackendType::Tokio);
        assert!(tokio_backend.is_async());
    }

    #[test]
    fn test_available_backends() {
        let backends = IoBackendFactory::available_backends();
        assert!(backends.contains(&IoBackendType::Sync));
        assert!(backends.contains(&IoBackendType::Tokio));
    }
}