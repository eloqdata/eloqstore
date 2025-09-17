//! EloqStore: High-performance key-value storage engine
//!
//! A Rust implementation of the EloqStore storage engine, providing
//! high-performance key-value storage with async I/O using io_uring.

// #![warn(missing_docs)]  // TODO: Add documentation later
#![warn(rust_2018_idioms)]
#![allow(dead_code)] // Temporary during development
#![allow(unused_variables)] // Temporary during development

// Global allocator for better performance
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod api;
pub mod codec;
pub mod config;
pub mod error;
pub mod index;
pub mod io;
pub mod page;
pub mod shard;
pub mod storage;
pub mod store;
pub mod task;
pub mod types;

#[cfg(feature = "ffi")]
pub mod ffi;


// Re-export main types
pub use error::{Error, Result};
pub use store::EloqStore;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");