# Phase 0: Baseline & Performance Benchmarking

**Date**: February 26, 2026  
**Status**: Completed

---

## Overview

This document establishes the baseline system configuration and current codebase state before implementing the Branch feature. The baseline serves as a reference point for measuring the impact of branch feature implementation on system performance.

---

## System Configuration

### Hardware Specifications

**CPU**:
- Model: AMD EPYC 7K62 48-Core Processor
- Cores: 16 (16 sockets × 1 core per socket)
- Threads per core: 1
- Total logical processors: 16

**Memory**:
- Total RAM: 47 GiB
- Available: ~30 GiB
- Swap: 0 B (disabled)

**Storage**:
- Type: SSD (mounted at /mnt/ssd)
- Filesystem: /dev/vdb
- Total capacity: 916 GB
- Used: 120 GB
- Available: 751 GB
- Usage: 14%

**Operating System**:
- OS: Ubuntu (Linux kernel 6.8.0-90-generic)
- Architecture: x86_64
- Kernel: 6.8.0-90-generic #91-Ubuntu SMP PREEMPT_DYNAMIC

---

## Codebase State

### Current File Naming Convention

**Data Files**:
- Format: `data_<file_id>_<term>`
- Example: `data_123_5` (file_id=123, term=5)
- No branch name in filename

**Manifest Files**:
- Format: `manifest_<term>`
- Example: `manifest_5` (term=5)
- Archive format: `manifest_<term>_<timestamp>`
- Example: `manifest_5_1234567890`
- No branch name in filename

**CURRENT_TERM File**:
- Format: `CURRENT_TERM` (single file, no branch differentiation)
- Contains current term as text

### Current Manifest Structure

From `include/storage/root_meta.h`:

**Manifest Header** (24 bytes):
```
[ Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
```

**Manifest Body**:
```
[ MaxFpId(8B) | DictLen(4B) | dict_bytes |
  mapping_bytes_len(4B) | mapping_tbl(varint64...) |
  FileIdTermMapping bytes(4B|varint64...) ]
```

**FileIdTermMapping** (`include/common.h` line 21):
```cpp
using FileIdTermMapping = absl::flat_hash_map<FileId, uint64_t>;  // file_id → term
```

- Maps each file_id to its term
- Size: O(N) where N = number of files
- No branch tracking

### Current Term Management

**Local Mode**:
- Term is always 0
- No term-based file versioning

**Cloud Mode**:
- Process term stored in `EloqStore::term_` and `CloudStoreMgr::process_term_`
- Single `CURRENT_TERM` file in S3
- CAS (Compare-And-Swap) operations for term coordination
- No per-branch term management

### Current File ID Allocation

**Allocator**: `AppendAllocator` and `FilePageAllocator`
- Location: `include/storage/page_mapper.h`
- Global file_id counter (not per-branch)
- Monotonically increasing across all writes
- No branch isolation

### Current GC Implementation

**File Classification**:
- Parses filenames: `data_<file_id>_<term>`, `manifest_<term>`, `manifest_<term>_<ts>`
- No branch name parsing

**Deletion Logic**:
- Builds reference set from current manifest
- Deletes data files not referenced
- Retains recent archives based on `num_retained_archives`
- No cross-branch reference tracking

### Test Infrastructure

**Test Framework**: Catch2
**Test Location**: `tests/` directory
**Test Files**:
- `filename_parsing.cpp` - File naming and parsing tests
- `manifest.cpp` - Manifest serialization tests
- `gc.cpp` - Garbage collection tests
- `cloud.cpp` - Cloud storage tests
- And 11 more test files

**Benchmark Tool**: `benchmark/simple_bench`
- Workloads: write, read, scan, write-read, write-scan
- Configurable batch size, partition count, KV size
- Metrics: latency, throughput

---

## Benchmark Setup

### Build Configuration

The project uses CMake build system:
```bash
# Debug build
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8

# Release build
mkdir Release
cd Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

### Benchmark Tool Location

- Executable: `build/benchmark/simple_bench`
- Configuration files:
  - `benchmark/opts_append.ini` - Append mode configuration
  - `benchmark/opts_local.ini` - Local mode configuration

### Sample Benchmark Command

```bash
# Load 10GB data (1K records)
./build/benchmark/simple_bench \
  --kvoptions=./benchmark/opts_append.ini \
  --workload=write-read \
  --kv_size=1024 \
  --batch_size=20000 \
  --max_key=10000000 \
  --read_per_part=4 \
  --partitions=1 \
  --load

# Run benchmark
./build/benchmark/simple_bench \
  --kvoptions=./benchmark/opts_append.ini \
  --workload=write-read \
  --kv_size=1024 \
  --batch_size=20000 \
  --max_key=10000000 \
  --read_per_part=4 \
  --partitions=1
```

---

## Baseline Metrics

### Note on Benchmark Execution

**Decision**: Defer comprehensive benchmark execution to post-implementation phase.

**Rationale**:
1. Current focus is on correct implementation of branch feature
2. Performance benchmarking will be more meaningful after implementation
3. We can compare pre/post performance when all phases are complete
4. No specific performance targets defined yet (per user clarification)

**Future Benchmarking Plan**:
- Run baseline benchmarks before Phase 7 (version headers)
- Run post-implementation benchmarks after Phase 8
- Focus areas:
  - Write throughput with branch isolation
  - Read latency for branch-specific data
  - GC performance with multiple branches (10, 50, 100+ branches)
  - Manifest loading time with BranchFileMapping
  - Archive creation and retention performance

---

## Key Files Inventory

### Files to be Modified in Implementation

**Phase 1 (Infrastructure)**:
- `include/common.h` - File naming and parsing functions
- `include/types.h` - Constants for branch names

**Phase 2 (Manifest Structure)**:
- `include/storage/root_meta.h` - BranchFileMapping, ManifestMetadata
- `src/storage/root_meta.cpp` - Serialization/deserialization

**Phase 3 (Branch Operations)**:
- `include/eloq_store.h` - Request classes
- `src/tasks/background_write.cpp` - CreateBranch, DeleteBranch
- `src/storage/shard.cpp` - Request handling
- `src/async_io_manager.cpp` - CURRENT_TERM management

**Phase 4 (Read/Write)**:
- `src/tasks/write_task.cpp` - Write path with branch support
- `include/storage/page_mapper.h` - File allocation

**Phase 5 (Archive)**:
- `src/tasks/background_write.cpp` - Archive with branch names
- `src/replayer.cpp` - Archive loading

**Phase 6 (GC)**:
- `src/file_gc.cpp` - Cross-branch GC

**Phase 7 (Version Headers)**:
- All header/source files with manifest and data file I/O

### Test Files to be Created

- `tests/branch_filename_parsing.cpp` (Phase 1)
- `tests/branch_file_mapping.cpp` (Phase 2)
- `tests/branch_manifest.cpp` (Phase 2)
- `tests/branch_operations.cpp` (Phase 3)
- `tests/branch_read_write.cpp` (Phase 4)
- `tests/branch_archive.cpp` (Phase 5)
- `tests/branch_gc.cpp` (Phase 6)
- `tests/version_headers.cpp` (Phase 7)

---

## Current Limitations (Pre-Branch Feature)

1. **No Data Isolation**: All data in single namespace, no logical separation
2. **Single Manifest**: One manifest per table, no branch-specific manifests
3. **Global File IDs**: File IDs are globally allocated, not per-branch
4. **Single Term Counter**: One term per table, no per-branch terms
5. **No Cross-Reference Tracking**: GC cannot handle shared data files across branches
6. **No Branch Metadata**: FileIdTermMapping doesn't track which branch owns which files

---

## Dependencies

### Required for Testing

- **MinIO**: S3-compatible object storage for testing
  - Download: https://dl.min.io/server/minio/release/linux-amd64/minio
  - Default port: 9900
  - Console port: 9901
  - Command: `./minio server /tmp/minio-data --address :9900 --console-address :9901`

### Build Dependencies

From CMakeLists.txt and README:
- CMake (build system)
- C++20 compiler
- io_uring support (Linux kernel 6.8+)
- External libraries (in `external/` directory):
  - Catch2 (testing framework)
  - glog (logging)
  - gflags (command-line flags)
  - bvar (performance metrics)
  - AWS SDK (S3 integration)
  - Others as specified in CMakeLists.txt

---

## Git Repository State

**Repository**: `/mnt/ssd/tianxj/workspace/eloqdata/eloqkv`
**Project Path**: `data_substrate/store_handler/eloq_data_store_service/eloqstore`
**Branch**: (to be checked before implementation)

---

## Summary

This baseline establishes:
1. ✅ System hardware and software configuration documented
2. ✅ Current file naming conventions captured
3. ✅ Current manifest structure documented
4. ✅ Current term and file_id management understood
5. ✅ Test infrastructure identified
6. ✅ Files to be modified listed
7. ⏸️ Performance benchmarking deferred to post-implementation

**Next Phase**: Phase 1 - Infrastructure (File Naming & Parsing)

---

## Appendix: File Counts

```
Total test files: 19
Total source files in src/: (to be counted)
Total header files in include/: (to be counted)
```

---

**Phase 0 Status**: ✅ COMPLETED  
**Ready to proceed**: Phase 1 - Infrastructure
