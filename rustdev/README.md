# EloqStore Rust Implementation

A high-performance key-value storage engine built for EloqKV, ported from C++ to Rust with full binary compatibility.

## Overview

EloqStore is a sophisticated key-value storage engine that provides:
- **High Performance**: Asynchronous I/O with multiple backend options (tokio, sync, thread-pool)
- **Binary Compatibility**: Exact file format compatibility with the C++ implementation
- **Multi-Shard Architecture**: Parallel processing with configurable sharding
- **Copy-on-Write (COW)**: Efficient updates without in-place modifications
- **Overflow Handling**: Support for values larger than page size
- **TTL Support**: Time-to-live for automatic key expiration
- **Compression**: Optional LZ4 and Zstandard compression

## Architecture

### Core Components

#### 1. **Shard System**
The store divides data across multiple shards (threads), each managing its portion independently:
- Each shard runs in its own async runtime
- Requests are routed to shards based on key hash
- Shards process requests asynchronously using task queues
- No cross-shard synchronization needed for normal operations

#### 2. **Page Management**
Data is organized in pages with different types:

**DataPage** (4KB default):
- Stores key-value pairs with prefix compression
- Uses restart points for efficient binary search
- Format matches C++ exactly for compatibility
- Supports overflow pointers for large values

**IndexPage**:
- In-memory B-tree like index structure
- Hierarchical organization for fast lookups
- Copy-on-write updates maintain consistency

**OverflowPage**:
- Handles values exceeding page size
- Linked chain structure for arbitrary size values
- Transparent to upper layers

#### 3. **Storage Layout**

```
┌─────────────────────────────────────┐
│         Index Pages (COW)           │
│  ┌────────┐  ┌────────┐            │
│  │ Root   │→ │ Level1 │ → ...      │
│  └────────┘  └────────┘            │
└─────────────────────────────────────┘
                ↓
┌─────────────────────────────────────┐
│         Data Pages                  │
│  ┌──────────────────────────────┐  │
│  │ Header | KV Entries | Restart│  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
                ↓ (if overflow)
┌─────────────────────────────────────┐
│       Overflow Pages                │
│  ┌──────────────────────────────┐  │
│  │ Header | Data | Next Pointer │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

#### 4. **I/O Backend Abstraction**
Flexible I/O layer supporting multiple backends:
- **Sync**: Traditional blocking I/O
- **Tokio**: Async I/O using tokio runtime
- **ThreadPool**: Dedicated I/O thread pool
- **io_uring** (Linux): High-performance kernel bypass (experimental)

### Data Format (C++ Compatible)

#### Page Header (19 bytes)
```
Offset  Size  Field
0       8     Checksum (XXH3_64bits)
8       1     Page Type
9       2     Content Length
11      4     Previous Page ID
15      4     Next Page ID
```

#### Data Entry Encoding
Entries use variable-length encoding with prefix compression:
```
shared_len (varint32)      | Shared key prefix length
non_shared_len (varint32)  | Non-shared key suffix length
value_len_flags (varint32) | Value length + flags (overflow, expire)
key_data[non_shared_len]   | Key suffix bytes
value_data[value_len]      | Value bytes or overflow pointers
expire_ts (varint64)       | Optional expiration timestamp
timestamp_delta (varint64) | Delta from previous timestamp (zigzag encoded)
```

### Task System

Operations are implemented as async tasks:

1. **ReadTask**: Point lookups with overflow support
2. **BatchWriteTask**: Atomic multi-key updates with COW
3. **ScanTask**: Range queries with configurable limits
4. **FloorTask**: Find largest key ≤ target
5. **DeleteTask**: Key removal (implemented as tombstone write)
6. **BackgroundWriteTask**: Async persistence and compaction
7. **FileGarbageCollector**: Reclaim space from old COW pages

## Building

### Prerequisites
- Rust 1.70+ with cargo
- Linux/macOS/Windows (io_uring requires Linux 5.1+)
- Optional: io_uring support requires liburing

### Build Commands

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Run with specific I/O backend
cargo run -- --io-backend tokio
```

### Feature Flags
- `compression`: Enable LZ4/Zstandard compression (default: on)
- `cloud`: Enable cloud storage backends (default: on)

## Configuration

EloqStore is configured via `KvOptions`:

```rust
let mut options = KvOptions::default();
options.data_dirs = vec!["/path/to/data".into()];
options.shard_num = 4;              // Number of shards
options.data_page_size = 4096;      // Page size in bytes
options.write_buffer_size = 16384;  // Write buffer per shard
options.max_cache_size = 1 << 30;   // 1GB cache

let store = EloqStore::new(options)?;
store.start().await?;
```

## API Usage

### Basic Operations

```rust
use eloqstore::{EloqStore, KvOptions};
use eloqstore::types::{Key, Value, TableIdent};
use eloqstore::api::request::{ReadRequest, WriteEntry, BatchWriteRequest};

// Initialize store
let mut store = EloqStore::new(KvOptions::default())?;
store.start().await?;

let table = TableIdent::new("my_table", 0);

// Write
let write_req = BatchWriteRequest {
    table_id: table.clone(),
    entries: vec![WriteEntry {
        key: Key::from("user:123"),
        value: Value::from("John Doe"),
        op: WriteOp::Upsert,
        timestamp: 0,
        expire_ts: None,
    }],
    sync: true,
    timeout: None,
};
store.batch_write(write_req).await?;

// Read
let read_req = ReadRequest {
    table_id: table.clone(),
    key: Key::from("user:123"),
    timeout: None,
};
let result = store.read(read_req).await?;

// Scan range
let scan_req = ScanRequest {
    table_id: table.clone(),
    start_key: Key::from("user:"),
    end_key: Some(Key::from("user:~")),
    limit: Some(100),
    reverse: false,
    timeout: None,
};
let results = store.scan(scan_req).await?;
```

## C++ Compatibility

This Rust implementation maintains exact binary compatibility with the C++ version:

### What's Compatible
- ✅ Page format (header, data, index)
- ✅ Entry encoding (varint, prefix compression, zigzag)
- ✅ Checksum algorithm (XXH3_64bits)
- ✅ Manifest format for persistence
- ✅ Overflow page chains
- ✅ Index page structure
- ✅ Restart point mechanism

### Implementation Differences
- Async/await instead of coroutines
- Rust ownership instead of manual memory management
- Trait-based I/O abstraction
- Type-safe error handling

### Migration from C++
Data files created by the C++ version can be directly read by this Rust implementation and vice versa. No migration needed.

## Performance Characteristics

### Optimizations
- **Zero-copy reads**: Direct buffer references where possible
- **Prefix compression**: Reduces key storage overhead
- **Delta timestamps**: Compress temporal data
- **Restart points**: O(log n) searches within pages
- **COW updates**: Minimize write amplification
- **Async I/O**: Overlap computation and I/O

### Benchmarks
See `examples/benchmark.rs` for performance testing tools.

## Testing

### Unit Tests
```bash
# Run all unit tests
cargo test

# Run specific test module
cargo test page::tests

# Run with debug output
RUST_LOG=debug cargo test
```

### Integration Examples
See `examples/` directory:
- `eloqstore_example.rs`: Comprehensive API usage examples
- `benchmark.rs`: Performance benchmarking tool

## Limitations

### Current Limitations
- No Write-Ahead Log (WAL) - writes are synchronous
- io_uring backend experimental due to thread safety
- No hot backup support yet
- TTL cleanup requires manual compaction

### Known Issues
- Large overflow values may impact scan performance
- Memory usage scales with active page count
- Compaction not yet implemented

## Architecture Decisions

### Why Copy-on-Write?
COW enables consistent snapshots and reduces lock contention. When updating:
1. Allocate new page
2. Copy and modify data
3. Update parent pointers atomically
4. Old pages garbage collected later

### Why Sharding?
Sharding provides:
- Natural parallelism without fine-grained locking
- Independent failure domains
- Predictable latency (no cross-shard coordination)
- Linear scalability with CPU cores

### Why Multiple I/O Backends?
Different workloads benefit from different I/O patterns:
- **Sync**: Simple, predictable, good for debugging
- **Tokio**: Excellent for mixed CPU/IO workloads
- **ThreadPool**: Dedicated I/O threads prevent blocking
- **io_uring**: Maximum performance on modern Linux

## Contributing

Contributions welcome! Please ensure:
1. Code follows Rust idioms and passes clippy
2. Maintains C++ binary compatibility
3. Includes tests for new functionality
4. Updates documentation as needed

## License

MIT OR Apache-2.0 (same as Rust)

## Acknowledgments

This is a Rust port of the original C++ EloqStore implementation, maintaining full compatibility while leveraging Rust's safety and performance features.