# EloqStore Comprehensive Test Suite

## Mission
Write a comprehensive test suite for the EloqStore C++ database that validates correctness, performance, and robustness of all important interfaces and functions. Focus on solid edge case testing to support future development with confidence.

## Core Principles

### 1. Directory Structure
- **ALL** modifications must be under the `dev/` directory
- **NO** modifications to code outside `dev/` unless absolutely necessary
- Build artifacts, tests, and utilities all reside in subdirectories under `dev/`

### 2. Test Coverage Goals
- Test **ALL** public interfaces and critical internal functions
- Minimum 80% code coverage for core components
- 100% coverage for critical paths (data integrity, transactions, persistence)
- Edge cases must have dedicated test scenarios

## Test Categories

### 1. Unit Tests
- Test individual functions/methods in isolation
- Use mocks for dependencies
- Fast execution (< 100ms per test)
- Categories:
  - Core data structures (pages, indices, entries)
  - Encoding/decoding functions
  - Memory management
  - Error handling

### 2. Integration Tests
- Test component interactions
- Real dependencies, no mocks
- Categories:
  - Read/Write/Scan operations
  - Transaction processing
  - Persistence and recovery
  - Async operations

### 3. Randomized Stress Tests
**REQUIREMENT**: All stress tests must support:
- **Configurable seed** via (in priority order):
  1. Command-line argument: `--seed=<value>`
  2. Environment variable: `ELOQ_TEST_SEED`
  3. Config file: `dev/test_config.ini` [stress] section
  4. Default: Current timestamp

- **Configurable duration/iterations**:
  1. Command-line: `--iterations=<n>` or `--duration=<seconds>`
  2. Environment: `ELOQ_TEST_ITERATIONS` or `ELOQ_TEST_DURATION`
  3. Config file: `iterations` or `duration_seconds`
  4. Default: 1000 iterations

- **Test patterns**:
  - Random key/value generation with controlled distributions
  - Mixed operation sequences (read/write/scan/delete)
  - Concurrent access patterns
  - Memory pressure scenarios
  - Large dataset handling (millions of keys)

### 4. Performance Tests
- Benchmark critical operations
- Track performance regressions
- Measure:
  - Throughput (ops/sec)
  - Latency (p50, p95, p99, p99.9)
  - Memory usage
  - I/O patterns

### 5. Fault Injection Tests
- Simulate failures:
  - Disk I/O errors
  - Memory allocation failures
  - Network interruptions (for cloud storage)
  - Process crashes
- Verify:
  - Data integrity maintained
  - Graceful degradation
  - Proper error reporting

### 6. Concurrency Tests
- Race condition detection
- Deadlock prevention
- Thread safety verification
- Scenarios:
  - Multiple readers/writers
  - Concurrent scans
  - Transaction conflicts
  - Async operation ordering

## Test Configuration

### Global Configuration File: `dev/test_config.ini`

```ini
[general]
verbose = false
output_dir = /mnt/ramdisk/test_results
temp_dir = /mnt/ramdisk/test_temp

[stress]
seed = 12345  # 0 for timestamp-based
iterations = 10000
duration_seconds = 0  # 0 for iteration-based
min_key_size = 8
max_key_size = 256
min_value_size = 16
max_value_size = 65536
thread_count = 4

[performance]
warmup_iterations = 100
measurement_iterations = 1000
report_percentiles = 50,95,99,99.9

[fault_injection]
io_error_rate = 0.001  # 0.1% chance
memory_fail_rate = 0.0001
crash_probability = 0.0
```

## Test Utilities Required

### 1. Random Data Generator
- Configurable distributions (uniform, normal, zipfian)
- Reproducible with seed
- Support for various data types
- Pattern generation (sequential, random, hotspot)

### 2. Test Harness
- Setup/teardown automation
- Resource cleanup
- Crash recovery
- Result collection and reporting

### 3. Verification Tools
- Data integrity checkers
- Memory leak detectors (valgrind integration)
- Thread sanitizer support
- Coverage report generation

## Success Criteria

1. **Correctness**: All tests pass consistently
2. **Coverage**: >80% line coverage, 100% for critical paths
3. **Performance**: No regression from baseline
4. **Stability**: 24-hour stress test passes without issues
5. **Memory**: No leaks detected under valgrind
6. **Concurrency**: No races detected by thread sanitizer

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)
- Test framework setup
- Basic unit tests
- Random data generators
- Configuration system

### Phase 2: Core Tests (Weeks 3-6)
- Comprehensive unit tests
- Integration tests
- Basic stress tests
- Performance baselines

### Phase 3: Robustness (Weeks 7-9)
- Fault injection
- Concurrency tests
- Extended stress tests
- Memory analysis

### Phase 4: Polish (Weeks 10-12)
- Test optimization
- Documentation
- CI/CD integration
- Coverage gaps

## Continuous Improvement
- Add tests for every bug found
- Update stress tests with production patterns
- Regular performance baseline updates
- Expand edge case coverage based on field issues