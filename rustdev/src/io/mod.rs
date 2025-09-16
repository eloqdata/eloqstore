//! Async I/O management

// Old io_uring based modules - being replaced with backend abstraction
// pub mod uring_manager;
// pub mod buffer_ring;
// pub mod async_file;
// pub mod completion;

// New backend abstraction
pub mod backend;

// Temporarily export these for compatibility
// pub use uring_manager::{UringManager, UringConfig, BatchIoRequest, BatchIoResult, IoOperation, IoResult};
// pub use buffer_ring::{BufferRing, BufferRingConfig, BufferPool, RingBuffer, ScopedBuffer, BufferChain};
// pub use async_file::{AsyncFile, AsyncFileOptions, AsyncFilePool};