# EloqStore Rust FFI

Rust bindings for [EloqStore](https://github.com/eloqdata/eloqstore), a high-performance embedded key-value database written in C++.

## Overview

EloqStore Rust FFI provides two ways to interact with EloqStore:

1. **RocksDB-style API** - Simple, familiar interface with `get()`, `put()`, `delete()`, etc.
2. **Request Trait API** - Type-safe, composable API using Rust traits to simulate C++ polymorphism

## Quick Start

```rust
use eloqstore::{EloqStore, Options, TableIdentifier};

fn main() -> Result<(), eloqstore::KvError> {
    // Configure the store
    let mut opts = Options::new()?;
    opts.set_num_threads(4)?;
    opts.add_store_path("/tmp/eloqstore_data");

    // Create and start the store
    let store = EloqStore::new(&opts)?;
    store.start()?;

    // Define a table
    let table = TableIdentifier::new("my_table", 0)?;

    // RocksDB-style API
    store.put(&table, b"key1", b"value1", 1000)?;
    let value = store.get(&table, b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));

    // Request Trait API
    use eloqstore::{ReadRequest, WriteRequest};

    let read_req = ReadRequest::new(table.clone(), b"key1");
    let resp = store.exec_sync(read_req)?;
    assert!(!resp.value.is_empty());

    let write_req = WriteRequest::new(table.clone())
        .put(b"key2", b"value2", 1001)
        .put(b"key3", b"value3", 1002);
    store.exec_sync(write_req)?;

    // Batch operations
    store.put_batch(
        &table,
        &[b"k1", b"k2", b"k3"],
        &[b"v1", b"v2", b"v3"],
        2000,
    )?;

    // Scan operations
    let entries = store.scan(&table, b"k1", b"k3")?;
    for entry in entries {
        println!("key: {:?}, value: {:?}", entry.key, entry.value);
    }

    // Floor operations
    if let Some((key, value)) = store.floor(&table, b"k2")? {
        println!("floor: key={:?}, value={:?}", key, value);
    }

    // Delete operations
    store.delete(&table, b"key1", 3000)?;

    Ok(())
}
```

## API Reference

### EloqStore

```rust
impl EloqStore {
    /// Create a new EloqStore instance with the given options
    pub fn new(opts: &Options) -> Result<Self, KvError>;

    /// Start the store
    pub fn start(&mut self) -> Result<(), KvError>;

    /// Stop the store
    pub fn stop(&mut self);

    /// Check if the store is stopped
    pub fn is_stopped(&self) -> bool;

    /// Execute a Request trait operation
    pub fn exec_sync<R: Request>(&self, req: R) -> Result<R::Response, KvError>;
}
```

### RocksDB-style Operations

```rust
impl EloqStore {
    /// Get a value by key
    pub fn get(&self, tbl: &TableIdentifier, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;

    /// Put a key-value pair
    pub fn put(&self, tbl: &TableIdentifier, key: &[u8], value: &[u8], ts: u64) -> Result<(), KvError>;

    /// Delete a key
    pub fn delete(&self, tbl: &TableIdentifier, key: &[u8], ts: u64) -> Result<(), KvError>;

    /// Put multiple key-value pairs
    pub fn put_batch(&self, tbl: &TableIdentifier, keys: &[&[u8]], values: &[&[u8]], ts: u64) -> Result<(), KvError>;

    /// Delete multiple keys
    pub fn delete_batch(&self, tbl: &TableIdentifier, keys: &[&[u8]], ts: u64) -> Result<(), KvError>;

    /// Find the floor entry (greatest key <= given key)
    pub fn floor(&self, tbl: &TableIdentifier, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvError>;

    /// Scan entries in a range
    pub fn scan(&self, tbl: &TableIdentifier, begin: &[u8], end: &[u8]) -> Result<Vec<KvEntry>, KvError>;
}
```

### Request Trait API

For advanced use cases, use the Request trait pattern:

```rust
use eloqstore::{Request, ReadRequest, WriteRequest, ScanRequest, FloorRequest};

// Read operations
let req = ReadRequest::new(table.clone(), b"key");
let resp = store.exec_sync(req)?;

// Write operations with chaining
let req = WriteRequest::new(table.clone())
    .put(b"k1", b"v1", 1000)
    .put(b"k2", b"v2", 1001)
    .delete(b"k3", 1002);
store.exec_sync(req)?;

// Scan with options
let req = ScanRequest::new(table.clone())
    .range(b"a", b"z", true)
    .pagination(100, 1024 * 1024);
let resp = store.exec_sync(req)?;

// Floor operation
let req = FloorRequest::new(table.clone(), b"key");
let resp = store.exec_sync(req)?;
```

## Options

```rust
let mut opts = Options::new()?;

// Configure thread count
opts.set_num_threads(4);

// Configure buffer pool size (in bytes)
opts.set_buffer_pool_size(1024 * 1024 * 256); // 256MB

// Configure data page size (in bytes)
opts.set_data_page_size(4096)?;

// Add store paths
opts.add_store_path("/path/to/data")?;

// Enable data append mode
opts.set_data_append_mode(true);

// Enable compression
opts.set_enable_compression(true);

// Validate options
assert!(opts.validate());
```

## Error Handling

```rust
use eloqstore::KvError;

match store.get(&table, b"key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Key not found"),
    Err(KvError::NotFound) => println!("Key not found"),
    Err(KvError::OutOfMem) => println!("Out of memory"),
    Err(e) => println!("Error: {:?}", e),
}
```

## Building

### Prerequisites

- Rust 1.70 or later
- CMake 3.16 or later
- C++ compiler with C++17 support
- Python 3 (for build scripts)

### Build

```bash
cargo build -p eloqstore
```

### Run Tests

```bash
# Tests must run serially due to C++ global pointer
cargo test -p eloqstore -- --test-threads=1
```

## Architecture

The project is structured as a Cargo workspace with two crates:

### eloqstore-sys

Low-level FFI bindings to the C++ EloqStore library. This crate:
- Contains only `extern "C"` declarations
- Uses CMake to build the C++ source
- Provides safe wrappers for C types

### eloqstore

High-level Rust API. This crate:
- Implements the `Request` and `Response` traits
- Provides both RocksDB-style and trait-based APIs
- Handles memory management and error conversion

## Limitations

- **Serial Execution**: Due to C++ global pointer usage, only one EloqStore instance can be active at a time. Tests must run with `--test-threads=1`.
- **Build Time**: Initial build requires compiling ~31MB of C++ source (Abseil, Boost, AWS SDK, etc.)

## License

EloqStore Rust FFI is licensed under the same license as EloqStore. See the [EloqStore repository](https://github.com/eloqdata/eloqstore) for details.
