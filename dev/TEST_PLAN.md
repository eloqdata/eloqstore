Contin# Comprehensive Test Suite Plan for EloqStore

## Overview
This document outlines a detailed plan to create a comprehensive test suite for EloqStore database, focusing on validating correctness of all important interfaces and functions, with special emphasis on edge cases.

## Project Structure
All development will be contained within the `dev/` directory:

```
dev/
├── build/              # Build directory for dev tests
├── tests/              # All test files organized by category
│   ├── core/          # Core data structure tests
│   ├── fault/         # Fault injection tests
│   ├── integration/   # Integration tests
│   ├── integrity/     # Data integrity tests
│   ├── io/            # Async I/O tests
│   ├── performance/   # Performance benchmarks
│   ├── persistence/   # Recovery and persistence tests
│   ├── shard/         # Shard system tests
│   ├── storage/       # Storage layer tests
│   ├── stress/        # Stress and concurrency tests
│   ├── task/          # Task management tests
│   └── utils/         # Utility function tests
├── fixtures/          # Test fixtures and data generators
├── mocks/            # Mock objects for testing
├── scripts/          # Test runner and analysis scripts
├── CMakeLists.txt    # Build configuration
├── test_config.ini   # Test configuration
└── run_tests.sh      # Main test runner script
```

## Phase 1: Foundation Setup (Week 1)

### 1.1 Build System Setup
- Create CMakeLists.txt for dev/ directory
- Configure Catch2 integration
- Set up build targets for different test categories
- Enable coverage reporting with gcov/lcov
- Add sanitizers (ASAN, TSAN, UBSAN) configurations

### 1.2 Test Infrastructure
- Create base test fixtures for common setups
- Implement test data generators
- Create mock implementations for external dependencies
- Set up temporary file management for tests
- Implement test helpers for async operations

## Phase 2: Core Data Structure Tests (Week 2-3)

### 2.1 Coding Module (`coding.h/cpp`)
**Classes/Functions to test:**
- `EncodeFixed32`, `EncodeFixed64`
- `DecodeFixed32`, `DecodeFixed64`
- `EncodeVarint32`, `EncodeVarint64`
- `DecodeVarint32`, `DecodeVarint64`
- `PutLengthPrefixedSlice`, `GetLengthPrefixedSlice`

**Edge cases:**
- Maximum/minimum values for each integer type
- Zero values
- Buffer overflow scenarios
- Partial reads/writes
- Endianness consistency

### 2.2 Page Management (`page.h/cpp`, `data_page.h/cpp`)
**Classes to test:**
- `Page` base class
- `DataPage` - key-value storage
- `IndexPage` - index structures
- `OverflowPage` - large value handling

**Edge cases:**
- Empty pages
- Full pages (maximum capacity)
- Single entry pages
- Pages with maximum key/value sizes
- Corrupted page headers
- Invalid checksums
- Page boundary conditions

### 2.3 Data Page Builder (`data_page_builder.h/cpp`)
**Functions to test:**
- `Add()` - adding key-value pairs
- `Finish()` - finalizing page
- `EstimatedSize()`
- Restart point management

**Edge cases:**
- Adding entries that exceed page size
- Empty keys/values
- Duplicate keys
- Keys in non-sorted order
- Maximum restart points
- Building with single entry

### 2.4 Index Management (`mem_index_page.h/cpp`, `index_page_manager.h/cpp`)
**Classes to test:**
- `MemIndexPage` - in-memory index
- `IndexPageManager` - index cache management
- Index search operations
- LRU eviction

**Edge cases:**
- Empty index
- Single entry index
- Maximum capacity scenarios
- Cache thrashing
- Concurrent access patterns
- Index corruption recovery

## Phase 3: Storage Layer Tests (Week 3-4)

### 3.1 Page Mapper (`page_mapper.h/cpp`)
**Functions to test:**
- `LogicalToPhysical()` mapping
- `PhysicalToLogical()` reverse mapping
- `AllocatePage()`
- `FreePage()`
- Snapshot management

**Edge cases:**
- Mapping overflow scenarios
- Fragmented page allocation
- Maximum file limits
- Concurrent mapping updates
- Snapshot consistency
- Recovery after crash

### 3.2 File Management (`file_gc.h/cpp`)
**Classes to test:**
- `FileGC` - garbage collection
- File lifecycle management
- Space reclamation

**Edge cases:**
- No reclaimable space
- All files eligible for GC
- GC during active writes
- File descriptor exhaustion
- Disk full scenarios

### 3.3 Root Metadata (`root_meta.h/cpp`)
**Functions to test:**
- Metadata persistence
- Version management
- Schema evolution

**Edge cases:**
- Corrupted metadata
- Version mismatch
- Concurrent metadata updates
- Metadata recovery

## Phase 4: Async I/O Tests (Week 4-5)

### 4.1 AsyncIoManager (`async_io_manager.h/cpp`)
**Core operations to test:**
- `OpenFile()`, `CloseFile()`
- `ReadAsync()`, `WriteAsync()`
- `SyncFile()`
- Buffer ring management
- io_uring submission/completion

**Edge cases:**
- Queue overflow
- Partial I/O completion
- I/O cancellation
- File descriptor limits
- Zero-byte operations
- Very large I/O operations
- Concurrent operations on same file
- System call interruptions
- Memory pressure scenarios

### 4.2 I/O Error Handling
- Disk full
- Permission errors
- File not found
- I/O timeout
- Hardware errors simulation
- Network storage disconnection

## Phase 5: Task Management Tests (Week 5-6)

### 5.1 Task System (`task.h/cpp`, `task_manager.h/cpp`)
**Classes to test:**
- `KvTask` base class
- `TaskManager` scheduling
- Task state transitions
- Coroutine context switching

**Edge cases:**
- Task cancellation
- Deadlock scenarios
- Stack overflow in coroutines
- Maximum concurrent tasks
- Task priority inversions

### 5.2 Specific Task Types
**Read Task (`read_task.h/cpp`)**
- Key lookup operations
- Cache hits/misses
- Non-existent keys
- Concurrent reads

**Scan Task (`scan_task.h/cpp`)**
- Range queries
- Empty ranges
- Full table scans
- Scan with limits
- Reverse scans

**Write Task (`write_task.h/cpp`)**
- Single writes
- Write conflicts
- Write amplification

**Batch Write Task (`batch_write_task.h/cpp`)**
- Large batches
- Empty batches
- Partial failures
- Transaction semantics
- Memory limits

**Background Write (`background_write.h/cpp`)**
- Async persistence
- Write coalescing
- Crash recovery

## Phase 6: Sharding and Concurrency Tests (Week 6-7)

### 6.1 Shard System (`shard.h/cpp`)
**Components to test:**
- Shard assignment
- Request routing
- Load balancing
- Coroutine scheduling

**Edge cases:**
- Single shard operation
- Maximum shard count
- Uneven shard distribution
- Shard migration
- Hot shard scenarios

### 6.2 Concurrency Control
- Reader-writer conflicts
- Write-write conflicts
- Deadlock detection
- Lock timeout
- Optimistic concurrency control

### 6.3 Thread Safety Tests
- Concurrent operations on same keys
- Race condition detection (TSAN)
- Memory ordering violations
- Lock-free data structure validation

## Phase 7: Integration Tests (Week 7-8)

### 7.1 End-to-End Operations
- Complete read-write cycles
- Transaction workflows
- Backup and restore
- Migration scenarios

### 7.2 Storage Modes
**Append Mode Testing**
- Sequential write optimization
- Archive generation
- Archive rotation
- Recovery from archives

**Non-Append Mode Testing**
- In-place updates
- Space reclamation
- Fragmentation handling

### 7.3 Cloud Storage Integration
- Rclone interface testing
- Upload/download operations
- Tiering policies
- Network failure handling

## Phase 8: Stress and Performance Tests (Week 8-9)

### 8.1 Load Testing
- Sustained write load
- Read-heavy workloads
- Mixed read-write patterns
- Burst traffic handling

### 8.2 Stress Scenarios
- Memory pressure
- Disk space exhaustion
- File descriptor limits
- CPU saturation
- Network bandwidth limits

### 8.3 Longevity Tests
- Memory leak detection
- Resource leak detection
- Performance degradation over time
- GC effectiveness

## Phase 9: Edge Cases and Error Scenarios (Week 9-10)

### 9.1 Boundary Conditions
- Empty database operations
- Single entry database
- Maximum database size
- Maximum key/value sizes
- Maximum number of tables
- Zero-length keys/values

### 9.2 Error Injection
- Simulated disk failures
- Network partitions
- Process crashes
- Corrupted data handling
- Byzantine failures

### 9.3 Recovery Scenarios
- Crash recovery
- Partial write recovery
- Manifest corruption recovery
- Index rebuilding
- Data verification after recovery

## Phase 10: Specialized Tests (Week 10-11)

### 10.1 Comparator Testing (`comparator.h/cpp`)
- Custom comparator implementation
- Sort order validation
- Unicode handling
- Case sensitivity

### 10.2 Configuration Testing (`kv_options.h/cpp`)
- All configuration permutations
- Invalid configuration handling
- Runtime configuration changes
- Default value validation

### 10.3 Utility Functions (`utils.h`, `coding.h`)
- Hash functions
- CRC calculations
- Time utilities
- String manipulation

### 10.4 Kill Point Testing (`kill_point.h/cpp`)
- Fault injection points
- Crash testing
- Recovery validation

## Phase 11: Testing Tools and Automation (Week 11-12)

### 11.1 Test Runners
- Parallel test execution
- Test filtering and selection
- Retry mechanisms for flaky tests
- Test result aggregation

### 11.2 CI/CD Integration
- GitHub Actions workflows
- Pre-commit hooks
- Code coverage gates
- Performance regression detection

### 11.3 Testing Documentation
- Test case documentation
- Coverage reports
- Performance baselines
- Known issues tracking

## Test Implementation Guidelines

### Test Naming Convention
```cpp
TEST_CASE("ComponentName_FunctionName_Scenario_ExpectedResult", "[category]")
```

### Test Structure Template
```cpp
TEST_CASE("DataPage_Add_FullPage_ReturnsError", "[page][edge-case]")
{
    // Arrange
    DataPageBuilder builder;
    // Fill page to capacity

    // Act
    auto result = builder.Add(key, value);

    // Assert
    REQUIRE(result == KvError::PageFull);

    // Cleanup (if needed)
}
```

### Edge Case Categories
1. **Boundary Values**: min, max, zero, one, negative
2. **Capacity Limits**: empty, full, overflow
3. **Concurrency**: race conditions, deadlocks, synchronization
4. **Error Conditions**: I/O errors, corruption, timeouts
5. **Resource Constraints**: memory, disk, file descriptors
6. **State Transitions**: invalid sequences, partial operations

## Success Metrics

1. **Code Coverage**: Aim for >90% line coverage, >85% branch coverage
2. **Edge Case Coverage**: All identified edge cases have dedicated tests
3. **Performance**: No regression in key operations
4. **Reliability**: All tests pass consistently (no flaky tests)
5. **Documentation**: All public APIs have corresponding tests

## Deliverables

1. Complete test suite with 1000+ test cases
2. Automated test runner with CI integration
3. Coverage reports and dashboards
4. Performance baseline documentation
5. Test maintenance guide
6. Bug reports for issues discovered during testing

## Risk Mitigation

1. **Test Maintenance**: Keep tests simple and well-documented
2. **False Positives**: Implement proper test isolation
3. **Performance Impact**: Separate unit tests from integration tests
4. **Flaky Tests**: Add retry mechanisms and proper synchronization
5. **Test Data**: Use deterministic test data generation

## Timeline Summary

- **Weeks 1-2**: Foundation and core data structures
- **Weeks 3-4**: Storage layer
- **Weeks 5-6**: Async I/O and tasks
- **Weeks 7-8**: Sharding and integration
- **Weeks 9-10**: Stress testing and edge cases
- **Weeks 11-12**: Tools and documentation

Total estimated time: 12 weeks for comprehensive test suite implementation.