# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Debug Build with Address Sanitizer
```bash
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_ASAN=ON
cmake --build . -j8
cd ..
```

### Release Build
```bash
mkdir Release
cd Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
cd ..
```

### Build Options
- `WITH_ASAN`: Enable Address Sanitizer for memory debugging
- `WITH_UNIT_TESTS`: Build unit tests (default ON)
- `WITH_EXAMPLE`: Build examples (default ON)
- `WITH_DB_STRESS`: Build stress testing tools (default ON)
- `WITH_BENCHMARK`: Build benchmark tools (default ON)
- `ELOQ_MODULE_ENABLED`: Enable EloqModule support

## Testing

### Run All Unit Tests
```bash
ctest --test-dir build/tests/
```

### Run Single Test
```bash
# Tests are built as individual executables using Catch2 framework
# Available test executables: scan, batch_write, delete, persist, manifest, concurrency, cloud
./build/tests/scan
./build/tests/batch_write
# Run specific test case with Catch2 syntax
./build/tests/scan -t "delete scan"
```

### Run Benchmark
```bash
./Release/benchmark/load_bench --kvoptions <path_to_config.ini>
```

### Run Stress Test
```bash
./build/db_stress/concurrent_test
./build/db_stress/test_client
```

## Code Style and Linting

### Format Code
The project uses clang-format with Google style base. Configuration is in `.clang-format`.
```bash
clang-format -i <file.cpp>
```

## Architecture Overview

EloqStore is a high-performance key-value storage engine built for EloqKV. It uses coroutines and io_uring for asynchronous I/O operations.

### Core Components

1. **Shard System**: The store is divided into multiple shards (threads), each managing its own portion of data. Shards are implemented using boost coroutines for cooperative multitasking.

2. **Task Management**: Operations are encapsulated as tasks (KvTask) with types including:
   - Read/Floor/Scan tasks for queries
   - BatchWrite for write operations
   - BackgroundWrite for async persistence
   - File eviction and garbage collection tasks

3. **Page Management**:
   - **DataPage**: Stores actual key-value pairs with restart points for efficient binary search
   - **IndexPage**: In-memory index structures for fast lookups
   - **OverflowPage**: Handles values that exceed page size limits
   - **PageMapper**: Maps logical pages to physical file locations

4. **Async I/O Manager**: Built on io_uring for high-performance asynchronous I/O:
   - Manages submission and completion queues
   - Handles file operations (open, close, read, write, sync)
   - Supports buffer rings for zero-copy operations

5. **Storage Modes**:
   - **Append Mode**: Optimized for sequential writes with archive support
   - **Non-Append Mode**: Standard read-write with in-place updates

6. **Cloud Storage Integration**: Supports tiered storage with cloud backends via rclone

### Key Classes and Files

- `eloq_store.h/cpp`: Main store interface and request handling
- `shard.h/cpp`: Shard implementation with coroutine scheduling
- `async_io_manager.h/cpp`: io_uring-based async I/O implementation
- `task.h/cpp`, `*_task.cpp`: Various task implementations
- `page_mapper.h/cpp`: Logical to physical page mapping
- `index_page_manager.h/cpp`: In-memory index management
- `batch_write_task.cpp`: Core write path implementation

### Configuration

The store is configured via INI files with two sections:
- `[run]`: Runtime parameters (threads, buffer sizes, file limits)
- `[permanent]`: Storage parameters (page size, data paths, storage mode)

Example configuration files are in `benchmark/opts_*.ini`.

### Dependencies

- C++20 compiler
- liburing for async I/O
- Boost.Context for coroutines
- glog for logging
- Catch2 for unit testing
- abseil for hash maps

## Integration with EloqKV

When integrated with EloqKV, EloqStore acts as the data store service. Key integration points:
- Set `WITH_DATA_STORE=ELOQDSS_ELOQSTORE` when building EloqKV
- Configure store parameters in `eloqkv.ini` under `[store]` section
- Worker threads must match `core_number` setting