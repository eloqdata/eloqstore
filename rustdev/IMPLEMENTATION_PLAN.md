# EloqStore Rust Implementation Plan

## Executive Summary
This document outlines a comprehensive plan to rewrite EloqStore from C++ to Rust, maintaining high performance while leveraging Rust's memory safety and modern async ecosystem. The implementation will be designed as if it were a native Rust project, following Rust idioms and best practices.

## 1. Architecture Overview

### Core Design Principles
1. **Zero-cost abstractions**: Use Rust's type system for compile-time guarantees without runtime overhead
2. **Async-first design**: Built on tokio with io_uring integration for maximum performance
3. **Memory safety by default**: Minimize unsafe code, clearly document and isolate unsafe sections
4. **Modular architecture**: Clean separation of concerns with well-defined module boundaries
5. **Type-driven development**: Leverage Rust's type system for correctness

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      API Layer                              │
│  (Request Types, Response Types, Public Interfaces)         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Store Core                               │
│  (EloqStore, Request Router, Shard Manager)                 │
└─────────────────────────────────────────────────────────────┘
                              │
┌──────────────────┬──────────────────┬──────────────────────┐
│   Shard Engine   │   Task System    │   Index Manager      │
│ (Worker threads) │ (Async tasks)    │ (In-memory indices)  │
└──────────────────┴──────────────────┴──────────────────────┘
                              │
┌──────────────────┬──────────────────┬──────────────────────┐
│   Page System    │   I/O Manager    │  Storage Backend     │
│ (Pages, Mapper)  │ (io_uring async) │ (Local/Cloud)        │
└──────────────────┴──────────────────┴──────────────────────┘
```

## 2. Directory Structure

```
eloqstore-rs/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── IMPLEMENTATION_PLAN.md
├── benches/
│   ├── load_bench.rs
│   └── comparison_bench.rs
├── examples/
│   ├── basic_usage.rs
│   └── concurrent_test.rs
├── src/
│   ├── lib.rs                 # Library entry point
│   ├── main.rs                 # Binary entry point (optional)
│   ├── api/
│   │   ├── mod.rs
│   │   ├── request.rs          # Request types (Read, Write, Scan, etc.)
│   │   ├── response.rs         # Response types
│   │   └── error.rs            # Error types
│   ├── store/
│   │   ├── mod.rs
│   │   ├── eloq_store.rs       # Main store implementation
│   │   ├── config.rs           # Configuration (KvOptions equivalent)
│   │   └── builder.rs          # Store builder pattern
│   ├── shard/
│   │   ├── mod.rs
│   │   ├── worker.rs           # Shard worker thread
│   │   ├── scheduler.rs        # Task scheduler
│   │   └── manager.rs          # Shard management
│   ├── task/
│   │   ├── mod.rs
│   │   ├── traits.rs           # Task trait definitions
│   │   ├── read_task.rs
│   │   ├── write_task.rs
│   │   ├── batch_write_task.rs
│   │   ├── scan_task.rs
│   │   ├── background_task.rs
│   │   └── manager.rs          # Task manager
│   ├── page/
│   │   ├── mod.rs
│   │   ├── data_page.rs        # Data page implementation
│   │   ├── index_page.rs       # Index page
│   │   ├── overflow_page.rs    # Overflow page
│   │   ├── builder.rs          # Page builder
│   │   ├── iterator.rs         # Page iterators
│   │   ├── mapper.rs           # Page mapping (logical -> physical)
│   │   └── pool.rs             # Page memory pool
│   ├── io/
│   │   ├── mod.rs
│   │   ├── uring_manager.rs    # io_uring async I/O manager
│   │   ├── file_manager.rs     # File operations
│   │   ├── buffer_ring.rs      # Zero-copy buffer management
│   │   └── completion.rs       # I/O completion handling
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── local.rs            # Local file storage
│   │   ├── cloud.rs            # Cloud storage integration
│   │   ├── archive.rs          # Archive management
│   │   └── manifest.rs         # Manifest file handling
│   ├── index/
│   │   ├── mod.rs
│   │   ├── mem_index.rs        # In-memory index
│   │   ├── manager.rs          # Index manager
│   │   └── cache.rs            # Index caching
│   ├── codec/
│   │   ├── mod.rs
│   │   ├── encoding.rs         # Key/value encoding
│   │   ├── compression.rs      # Compression support
│   │   └── checksum.rs         # CRC/checksum
│   ├── utils/
│   │   ├── mod.rs
│   │   ├── circular_queue.rs   # Lock-free circular queue
│   │   ├── comparator.rs       # Key comparators
│   │   └── metrics.rs          # Performance metrics
│   └── ffi/                    # Optional C FFI for compatibility
│       ├── mod.rs
│       └── bindings.rs
├── tests/
│   ├── integration/
│   │   ├── basic_ops.rs
│   │   ├── concurrent.rs
│   │   ├── persistence.rs
│   │   └── stress.rs
│   └── common/
│       ├── mod.rs
│       └── test_utils.rs
└── fuzz/                       # Fuzzing targets
    ├── Cargo.toml
    └── fuzz_targets/
        ├── write_fuzz.rs
        └── read_fuzz.rs
```

## 3. Module Organization and Dependencies

### Core Dependencies

```toml
[dependencies]
# Async runtime
tokio = { version = "1.40", features = ["full"] }
tokio-uring = "0.5"  # io_uring support

# Data structures
bytes = "1.7"
smallvec = "1.13"
dashmap = "6.0"  # Concurrent hashmap
crossbeam = "0.8"  # Lock-free data structures
parking_lot = "0.12"  # Better mutex/rwlock

# Serialization
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
postcard = "1.0"  # Efficient binary serialization

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Logging and metrics
tracing = "0.1"
tracing-subscriber = "0.3"
prometheus = "0.13"

# Configuration
config = "0.14"
clap = { version = "4.5", features = ["derive"] }

# Compression
lz4 = "1.28"
zstd = "0.13"

# Cloud storage
object_store = "0.11"  # S3, Azure, GCS support

[dev-dependencies]
criterion = "0.5"  # Benchmarking
proptest = "1.5"   # Property testing
tempfile = "3.12"
rand = "0.8"

[build-dependencies]
bindgen = "0.70"  # If FFI needed
```

### Module Hierarchy and Responsibilities

#### 1. **api** - Public API and Types
- Request/Response types with builder patterns
- Error types using `thiserror`
- Traits for extensibility

#### 2. **store** - Core Store Implementation
- Main `EloqStore` struct
- Request routing to shards
- Configuration management
- Store lifecycle (init, start, stop)

#### 3. **shard** - Sharding and Work Distribution
- Shard workers using tokio tasks
- Work stealing queue implementation
- Request distribution by consistent hashing

#### 4. **task** - Async Task System
- Trait-based task abstraction
- Task scheduling and execution
- Coroutine-style continuations using async/await

#### 5. **page** - Page Management
- Zero-copy page handling using `bytes::Bytes`
- Page builder with restart points
- Efficient binary search within pages
- Memory pool using `crossbeam::queue::ArrayQueue`

#### 6. **io** - Async I/O Layer
- `tokio-uring` for io_uring operations
- Buffer ring management for zero-copy I/O
- File descriptor caching and management
- Batch I/O operations

#### 7. **storage** - Storage Backends
- Trait-based storage abstraction
- Local filesystem implementation
- Cloud storage using `object_store`
- Append-mode optimizations

#### 8. **index** - In-Memory Indexing
- Lock-free skip list or B-tree
- Page-level index caching
- Bloom filters for optimization

#### 9. **codec** - Encoding/Decoding
- Varint encoding for space efficiency
- CRC32C checksums using SIMD
- Optional compression

## 4. Unsafe Code Sections

### Identified Unsafe Requirements

1. **io_uring Integration**
   - Direct memory buffer management
   - Submission/completion queue access
   - Buffer ring operations
   ```rust
   // src/io/uring_manager.rs
   unsafe {
       // Buffer registration with kernel
       // SQE/CQE manipulation
       // Memory pinning for DMA
   }
   ```

2. **Zero-Copy Page Operations**
   - Direct memory manipulation for pages
   - Pointer arithmetic for page layout
   ```rust
   // src/page/data_page.rs
   unsafe {
       // Direct byte manipulation for page headers
       // Unchecked slice operations for performance
   }
   ```

3. **Lock-Free Data Structures**
   - Custom atomic operations
   - Memory ordering guarantees
   ```rust
   // src/utils/circular_queue.rs
   unsafe {
       // Atomic pointer operations
       // Manual memory management for nodes
   }
   ```

4. **Memory Pool Management**
   - Custom allocator for page pool
   - Manual memory lifecycle management
   ```rust
   // src/page/pool.rs
   unsafe {
       // Raw allocation/deallocation
       // Memory reuse without zeroing
   }
   ```

5. **FFI Boundaries (if needed)**
   - C compatibility layer
   - Raw pointer conversions
   ```rust
   // src/ffi/bindings.rs
   unsafe {
       // C string handling
       // Struct layout guarantees
   }
   ```

### Safety Strategy
- Encapsulate all unsafe code in minimal, well-tested modules
- Use `#[safety]` documentation for all unsafe blocks
- Provide safe abstractions over unsafe internals
- Extensive testing including Miri and sanitizers
- Use `debug_assert!` for safety invariants

## 5. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up Rust project structure
- [ ] Implement basic types and errors
- [ ] Create configuration system
- [ ] Implement page structures and encoding
- [ ] Set up testing framework

### Phase 2: Core Storage (Weeks 3-4)
- [ ] Implement data page with restart points
- [ ] Create page builder and iterator
- [ ] Implement overflow page handling
- [ ] Build page mapper (logical to physical)
- [ ] Create basic file I/O manager

### Phase 3: Async I/O (Weeks 5-6)
- [ ] Integrate tokio-uring for io_uring
- [ ] Implement buffer ring management
- [ ] Create async file operations
- [ ] Build batch I/O operations
- [ ] Add I/O completion handling

### Phase 4: Task System (Weeks 7-8)
- [ ] Design task trait system
- [ ] Implement read task
- [ ] Implement write/batch write tasks
- [ ] Create scan task with iterators
- [ ] Build background tasks (compaction, GC)

### Phase 5: Shard System (Weeks 9-10)
- [ ] Implement shard worker threads
- [ ] Create work distribution system
- [ ] Build request routing
- [ ] Add task scheduling
- [ ] Implement backpressure mechanisms

### Phase 6: Index Management (Weeks 11-12)
- [ ] Design in-memory index structure
- [ ] Implement index page management
- [ ] Create index caching layer
- [ ] Add bloom filters
- [ ] Build index persistence

### Phase 7: Advanced Features (Weeks 13-14)
- [ ] Add cloud storage support
- [ ] Implement append-mode optimizations
- [ ] Create archive management
- [ ] Add compression support
- [ ] Build manifest handling

### Phase 8: Testing and Optimization (Weeks 15-16)
- [ ] Comprehensive unit tests
- [ ] Integration testing suite
- [ ] Stress testing framework
- [ ] Performance benchmarking
- [ ] Memory leak detection
- [ ] Fuzzing implementation

### Phase 9: Documentation and Polish (Week 17)
- [ ] API documentation
- [ ] Usage examples
- [ ] Performance tuning guide
- [ ] Migration guide from C++
- [ ] Deployment documentation

## 6. Testing Strategy

### Unit Testing
- Test each module in isolation
- Use property-based testing with `proptest`
- Mock external dependencies
- Achieve >90% code coverage

### Integration Testing
- End-to-end operation tests
- Multi-threaded stress tests
- Crash recovery tests
- Data consistency verification

### Performance Testing
- Benchmark against C++ version
- Use `criterion` for micro-benchmarks
- Load testing with various workloads
- Memory usage profiling

### Correctness Testing
- Fuzzing with `cargo-fuzz`
- Miri for undefined behavior detection
- AddressSanitizer/ThreadSanitizer
- Formal verification of critical algorithms

### Test Categories

1. **Functional Tests**
   ```rust
   #[test]
   fn test_basic_read_write() { ... }
   #[test]
   fn test_scan_operations() { ... }
   #[test]
   fn test_batch_writes() { ... }
   ```

2. **Concurrent Tests**
   ```rust
   #[test]
   fn test_concurrent_readers() { ... }
   #[test]
   fn test_reader_writer_fairness() { ... }
   ```

3. **Persistence Tests**
   ```rust
   #[test]
   fn test_crash_recovery() { ... }
   #[test]
   fn test_data_durability() { ... }
   ```

4. **Stress Tests**
   ```rust
   #[test]
   #[ignore] // Run with --ignored flag
   fn stress_test_high_load() { ... }
   ```

## 7. Migration Strategy

### Compatibility Layer
1. Provide C FFI for drop-in replacement
2. Maintain configuration file compatibility
3. Support existing data format
4. Implement protocol compatibility

### Incremental Migration
1. Start with read-only operations
2. Add write operations
3. Implement background tasks
4. Enable full feature parity

### Validation
1. Side-by-side testing with C++ version
2. Data consistency verification
3. Performance comparison
4. Production canary deployment

## 8. Key Design Decisions

### Async Runtime
- **Choice**: Tokio with io_uring
- **Rationale**: Best performance for I/O-heavy workloads
- **Alternative**: async-std (simpler but less features)

### Page Size
- **Choice**: Configurable, default 4KB
- **Rationale**: Matches OS page size, efficient I/O
- **Alternative**: Larger pages for sequential workloads

### Serialization
- **Choice**: Custom binary format with postcard fallback
- **Rationale**: Maximum performance, zero-copy where possible
- **Alternative**: Protobuf/MessagePack for flexibility

### Memory Management
- **Choice**: Arena allocators for pages
- **Rationale**: Reduced fragmentation, predictable performance
- **Alternative**: Standard allocator with pooling

### Concurrency Model
- **Choice**: Sharded architecture with work stealing
- **Rationale**: Scales linearly with cores
- **Alternative**: Single writer, multiple readers

## 9. Performance Goals

### Target Metrics
- **Read Latency**: < 100μs p99
- **Write Latency**: < 1ms p99
- **Throughput**: > 1M ops/sec on NVMe
- **Memory Usage**: < 2GB for 100M keys
- **CPU Efficiency**: > 80% useful work

### Optimization Strategies
1. Zero-copy I/O paths
2. Lock-free data structures where possible
3. SIMD for checksums and comparisons
4. Prefetching and cache-aware algorithms
5. Adaptive indexing based on workload

## 10. Risk Mitigation

### Technical Risks
1. **io_uring complexity**: Extensive testing, fallback to standard I/O
2. **Memory safety**: Minimize unsafe, use sanitizers
3. **Performance regression**: Continuous benchmarking
4. **Data corruption**: Checksums, atomic operations
5. **Deadlocks**: Careful lock ordering, timeout mechanisms

### Project Risks
1. **Scope creep**: Strict phase boundaries
2. **Testing gaps**: Comprehensive test plan from start
3. **Documentation lag**: Document as you code
4. **Integration issues**: Early FFI testing

## 11. Success Criteria

### Functional
- [ ] Feature parity with C++ version
- [ ] Pass all existing test suites
- [ ] No data corruption under stress
- [ ] Graceful error handling

### Performance
- [ ] Within 10% of C++ performance
- [ ] Linear scaling with cores
- [ ] Predictable latencies
- [ ] Efficient memory usage

### Quality
- [ ] No memory leaks
- [ ] No data races
- [ ] Comprehensive documentation
- [ ] Clean, idiomatic Rust code

## 12. Next Steps

1. **Review and approve this plan**
2. **Set up development environment**
3. **Create initial Rust project structure**
4. **Implement Phase 1 foundation**
5. **Establish CI/CD pipeline**
6. **Begin incremental implementation**

## Appendix A: C++ to Rust Mapping

| C++ Component | Rust Equivalent | Notes |
|--------------|-----------------|-------|
| boost::context | tokio tasks | Async/await instead of coroutines |
| liburing | tokio-uring | Safe wrapper over io_uring |
| std::variant | enum | More ergonomic in Rust |
| glog | tracing | Structured logging |
| abseil::flat_hash_map | dashmap/HashMap | Concurrent or standard |
| concurrentqueue | crossbeam::channel | MPMC queue |
| std::shared_ptr | Arc | Reference counting |
| std::unique_ptr | Box/ownership | RAII by default |

## Appendix B: Development Tools

### Required Tools
- Rust 1.75+ (latest stable)
- cargo-edit
- cargo-watch
- cargo-flamegraph
- cargo-criterion
- cargo-fuzz
- rust-analyzer

### Recommended IDE Setup
- VS Code with rust-analyzer
- IntelliJ with Rust plugin
- Neovim with rust-tools

### CI/CD Pipeline
- GitHub Actions / GitLab CI
- Automated testing on each commit
- Nightly benchmarks
- Security audits with cargo-audit
- Coverage reports with tarpaulin

---

This plan is a living document and will be updated as development progresses.