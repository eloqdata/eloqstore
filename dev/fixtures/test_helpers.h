#pragma once

#include <string>
#include <vector>
#include <map>
#include <functional>
#include <chrono>
#include <catch2/catch_test_macros.hpp>

#include "types.h"
#include "error.h"
#include "eloq_store.h"
#include "page.h"

namespace eloqstore::test {

// Timing utilities
class Timer {
public:
    Timer();
    void Start();
    void Stop();
    double ElapsedSeconds() const;
    double ElapsedMilliseconds() const;
    double ElapsedMicroseconds() const;

private:
    std::chrono::steady_clock::time_point start_;
    std::chrono::steady_clock::time_point end_;
    bool running_ = false;
};

// Memory tracking
class MemoryTracker {
public:
    MemoryTracker();
    ~MemoryTracker();

    size_t CurrentUsage() const;
    size_t PeakUsage() const;
    void Reset();

private:
    size_t initial_usage_;
    size_t peak_usage_;
};

// Test assertions
#define ASSERT_OK(err) REQUIRE((err) == eloqstore::KvError::NoError)
#define ASSERT_ERROR(err, expected) REQUIRE((err) == (expected))
#define ASSERT_KEY_VALUE(req, key, value) do { \
    REQUIRE((req)->key_ == (key)); \
    REQUIRE((req)->value_ == (value)); \
} while(0)

// Performance assertions
#define ASSERT_DURATION_LT(timer, max_ms) \
    REQUIRE((timer).ElapsedMilliseconds() < (max_ms))

#define ASSERT_MEMORY_LT(tracker, max_bytes) \
    REQUIRE((tracker).CurrentUsage() < (max_bytes))

// Test data validation
class DataValidator {
public:
    static bool ValidateKeyOrder(const std::vector<std::string>& keys);
    static bool ValidateDataIntegrity(const std::string& data, uint32_t expected_checksum);
    static uint32_t ComputeChecksum(const std::string& data);
    static bool CompareKeyValuePairs(const std::map<std::string, std::string>& expected,
                                     const std::map<std::string, std::string>& actual);
};

// Stress test helpers
class StressTestHelper {
public:
    struct Config {
        uint32_t duration_seconds = 10;
        uint32_t num_threads = 4;
        uint32_t operations_per_thread = 1000;
        double read_ratio = 0.5;
        double write_ratio = 0.3;
        double scan_ratio = 0.2;
        uint32_t key_space_size = 10000;
        uint32_t value_size_min = 10;
        uint32_t value_size_max = 1000;
    };

    static void RunStressTest(EloqStore* store,
                             const TableIdent& table,
                             const Config& config);

    static void GenerateWorkload(std::vector<std::function<void()>>& operations,
                                 const Config& config);

private:
    static void WorkerThread(EloqStore* store,
                            const TableIdent& table,
                            const Config& config,
                            std::atomic<bool>* stop_flag,
                            std::atomic<uint64_t>* op_count);
};

// Crash recovery helpers
class CrashTestHelper {
public:
    static void SimulateRandomCrash(std::function<void()> operation,
                                   double crash_probability);

    static void InjectRandomDelay(uint32_t min_ms, uint32_t max_ms);

    static void CorruptBytes(char* data, size_t size, double corruption_rate);

    static bool VerifyRecovery(EloqStore* store,
                              const TableIdent& table,
                              const std::map<std::string, std::string>& expected_data);
};

// Benchmark helpers
class BenchmarkHelper {
public:
    struct Result {
        double throughput_ops_per_sec;
        double latency_avg_ms;
        double latency_p50_ms;
        double latency_p95_ms;
        double latency_p99_ms;
        uint64_t total_operations;
        double duration_seconds;
    };

    static Result RunBenchmark(std::function<void()> operation,
                              uint32_t num_operations,
                              uint32_t num_threads = 1);

    static void PrintResults(const std::string& name, const Result& result);

    static Result MeasureReadPerformance(EloqStore* store,
                                        const TableIdent& table,
                                        const std::vector<std::string>& keys);

    static Result MeasureWritePerformance(EloqStore* store,
                                         const TableIdent& table,
                                         uint32_t num_writes,
                                         uint32_t value_size);
};

// Concurrency test helpers
class ConcurrencyTestHelper {
public:
    static void RunConcurrentTest(
        std::vector<std::function<void()>> operations,
        uint32_t num_threads);

    static bool DetectRaceCondition(
        std::function<void()> operation1,
        std::function<void()> operation2,
        uint32_t iterations = 1000);

    static void TestDeadlockFreedom(
        EloqStore* store,
        const std::vector<TableIdent>& tables,
        uint32_t num_threads,
        uint32_t duration_seconds);
};

// Debug helpers
class DebugHelper {
public:
    static void DumpStoreState(const EloqStore* store);
    static void DumpPageContent(const Page* page);
    static void DumpManifest(const std::string& manifest_path);
    static std::string HexDump(const void* data, size_t size);
    static void EnableVerboseLogging();
    static void DisableVerboseLogging();
};

} // namespace eloqstore::test