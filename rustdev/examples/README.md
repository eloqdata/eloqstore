# EloqStore Rust Examples

This directory contains example programs demonstrating how to use EloqStore.

## Available Examples

### 1. debug_test
Simple test program for debugging basic operations.

```bash
cargo run --example debug_test
```

Tests:
- Basic write and read operations
- Debug output and tracing

### 2. native_usage
Comprehensive demonstration of EloqStore features.

```bash
cargo run --example native_usage
```

Demonstrates:
- Store initialization and configuration
- Single key-value operations (PUT, GET, DELETE)
- Batch write operations
- Scanning (note: returns 0 results in current implementation)
- Performance benchmarking with 1000 random reads
- Proper shutdown procedures

### 3. complete_stress
Stress test with correctness verification.

```bash
# Run in release mode for best performance
cargo run --release --example complete_stress

# Run on ramdisk for optimal performance
cd /mnt/ramdisk && rm -rf stress_test
cargo run --release --example complete_stress
```

Features:
- 10 million operations on 1 million keys
- Operation mix: 40% puts, 30% gets, 10% deletes, 10% batch writes, 10% scans
- In-memory golden copy for correctness verification
- Real-time statistics and progress reporting
- Latency tracking and performance metrics
- Immediate failure on verification errors

## Building Examples

All examples can be built with:

```bash
cargo build --examples
```

For production/stress testing, use release mode:

```bash
cargo build --release --examples
```

## Notes

- The current implementation uses simplified indexing, so scan operations return 0 results
- Delete operations mark keys as deleted but don't immediately free space
- All examples use local file storage; for best performance use a ramdisk (/mnt/ramdisk)
- The store supports multiple shards/threads for parallel processing