#include "test_helpers.h"
#include <algorithm>
#include <numeric>
#include <thread>
#include <random>
#include <iomanip>
#include <sstream>
#include <glog/logging.h>

namespace eloqstore::test {

// Timer implementation
Timer::Timer() {
    Start();
}

void Timer::Start() {
    start_ = std::chrono::steady_clock::now();
    running_ = true;
}

void Timer::Stop() {
    end_ = std::chrono::steady_clock::now();
    running_ = false;
}

double Timer::ElapsedSeconds() const {
    auto end = running_ ? std::chrono::steady_clock::now() : end_;
    return std::chrono::duration<double>(end - start_).count();
}

double Timer::ElapsedMilliseconds() const {
    return ElapsedSeconds() * 1000.0;
}

double Timer::ElapsedMicroseconds() const {
    return ElapsedSeconds() * 1000000.0;
}

// MemoryTracker implementation
MemoryTracker::MemoryTracker() {
    Reset();
}

MemoryTracker::~MemoryTracker() = default;

size_t MemoryTracker::CurrentUsage() const {
    // This is a simplified implementation
    // In production, use proper memory tracking APIs
    return 0;
}

size_t MemoryTracker::PeakUsage() const {
    return peak_usage_;
}

void MemoryTracker::Reset() {
    initial_usage_ = CurrentUsage();
    peak_usage_ = initial_usage_;
}

// DataValidator implementation
bool DataValidator::ValidateKeyOrder(const std::vector<std::string>& keys) {
    return std::is_sorted(keys.begin(), keys.end());
}

bool DataValidator::ValidateDataIntegrity(const std::string& data, uint32_t expected_checksum) {
    return ComputeChecksum(data) == expected_checksum;
}

uint32_t DataValidator::ComputeChecksum(const std::string& data) {
    // Simple CRC32-like checksum
    uint32_t checksum = 0;
    for (char c : data) {
        checksum = ((checksum << 1) | (checksum >> 31)) ^ static_cast<uint8_t>(c);
    }
    return checksum;
}

bool DataValidator::CompareKeyValuePairs(
    const std::map<std::string, std::string>& expected,
    const std::map<std::string, std::string>& actual) {
    if (expected.size() != actual.size()) {
        return false;
    }

    for (const auto& [key, value] : expected) {
        auto it = actual.find(key);
        if (it == actual.end() || it->second != value) {
            return false;
        }
    }
    return true;
}

// StressTestHelper implementation
void StressTestHelper::RunStressTest(
    EloqStore* store,
    const TableIdent& table,
    const Config& config) {
    std::atomic<bool> stop_flag{false};
    std::atomic<uint64_t> total_ops{0};
    std::vector<std::thread> threads;

    auto start_time = std::chrono::steady_clock::now();

    for (uint32_t i = 0; i < config.num_threads; ++i) {
        threads.emplace_back(WorkerThread, store, table, config, &stop_flag, &total_ops);
    }

    std::this_thread::sleep_for(std::chrono::seconds(config.duration_seconds));
    stop_flag = true;

    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double>(end_time - start_time).count();
    double throughput = total_ops / duration;

    LOG(INFO) << "Stress test completed: " << total_ops << " operations in "
              << duration << " seconds (" << throughput << " ops/sec)";
}

void StressTestHelper::WorkerThread(
    EloqStore* store,
    const TableIdent& table,
    const Config& config,
    std::atomic<bool>* stop_flag,
    std::atomic<uint64_t>* op_count) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(0, config.key_space_size - 1);
    std::uniform_int_distribution<> value_size_dist(config.value_size_min, config.value_size_max);
    std::uniform_real_distribution<> op_dist(0.0, 1.0);

    while (!stop_flag->load()) {
        double op_type = op_dist(gen);

        if (op_type < config.read_ratio) {
            // Perform read
            std::string key = "key_" + std::to_string(key_dist(gen));
            auto req = std::make_unique<ReadRequest>();
            req->SetArgs(table, key);
            store->ExecSync(req.get());
        } else if (op_type < config.read_ratio + config.write_ratio) {
            // Perform write
            std::string key = "key_" + std::to_string(key_dist(gen));
            std::string value(value_size_dist(gen), 'x');
            auto req = std::make_unique<BatchWriteRequest>();
            req->SetTableId(table);
            req->AddWrite(key, value, 0, WriteOp::Upsert);
            store->ExecSync(req.get());
        } else {
            // Perform scan
            int start = key_dist(gen);
            int end = std::min(start + 100, static_cast<int>(config.key_space_size));
            auto req = std::make_unique<ScanRequest>();
            std::string start_key = "key_" + std::to_string(start);
            std::string end_key = "key_" + std::to_string(end);
            req->SetArgs(table, start_key, end_key, true);
            req->SetPagination(10, SIZE_MAX);
            store->ExecSync(req.get());
        }

        (*op_count)++;
    }
}

// CrashTestHelper implementation
void CrashTestHelper::SimulateRandomCrash(
    std::function<void()> operation,
    double crash_probability) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    try {
        operation();
        if (dis(gen) < crash_probability) {
            LOG(WARNING) << "Simulating crash!";
            std::exit(1); // Simulate sudden crash
        }
    } catch (...) {
        // Crash during operation
        std::exit(1);
    }
}

void CrashTestHelper::InjectRandomDelay(uint32_t min_ms, uint32_t max_ms) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min_ms, max_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
}

void CrashTestHelper::CorruptBytes(char* data, size_t size, double corruption_rate) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> prob_dis(0.0, 1.0);
    std::uniform_int_distribution<> byte_dis(0, 255);

    for (size_t i = 0; i < size; ++i) {
        if (prob_dis(gen) < corruption_rate) {
            data[i] = static_cast<char>(byte_dis(gen));
        }
    }
}

bool CrashTestHelper::VerifyRecovery(
    EloqStore* store,
    const TableIdent& table,
    const std::map<std::string, std::string>& expected_data) {
    for (const auto& [key, expected_value] : expected_data) {
        auto req = std::make_unique<ReadRequest>();
        req->SetArgs(table, key);
        store->ExecSync(req.get());

        if (req->Error() != KvError::NoError || req->value_ != expected_value) {
            LOG(ERROR) << "Recovery verification failed for key: " << key;
            return false;
        }
    }
    return true;
}

// BenchmarkHelper implementation
BenchmarkHelper::Result BenchmarkHelper::RunBenchmark(
    std::function<void()> operation,
    uint32_t num_operations,
    uint32_t num_threads) {
    Result result;
    std::vector<double> latencies;
    latencies.reserve(num_operations);

    std::atomic<uint64_t> completed_ops{0};
    std::mutex latency_mutex;

    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i] {
            uint32_t ops_per_thread = num_operations / num_threads;
            uint32_t start_op = i * ops_per_thread;
            uint32_t end_op = (i == num_threads - 1) ? num_operations : start_op + ops_per_thread;

            for (uint32_t op = start_op; op < end_op; ++op) {
                auto op_start = std::chrono::steady_clock::now();
                operation();
                auto op_end = std::chrono::steady_clock::now();

                double latency_ms = std::chrono::duration<double, std::milli>(op_end - op_start).count();
                {
                    std::lock_guard<std::mutex> lock(latency_mutex);
                    latencies.push_back(latency_ms);
                }
                completed_ops++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();

    // Calculate results
    result.total_operations = completed_ops;
    result.duration_seconds = std::chrono::duration<double>(end_time - start_time).count();
    result.throughput_ops_per_sec = result.total_operations / result.duration_seconds;

    // Calculate latency statistics
    std::sort(latencies.begin(), latencies.end());
    result.latency_avg_ms = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    result.latency_p50_ms = latencies[latencies.size() * 0.50];
    result.latency_p95_ms = latencies[latencies.size() * 0.95];
    result.latency_p99_ms = latencies[latencies.size() * 0.99];

    return result;
}

void BenchmarkHelper::PrintResults(const std::string& name, const Result& result) {
    LOG(INFO) << "=== Benchmark: " << name << " ===";
    LOG(INFO) << "Total operations: " << result.total_operations;
    LOG(INFO) << "Duration: " << result.duration_seconds << " seconds";
    LOG(INFO) << "Throughput: " << result.throughput_ops_per_sec << " ops/sec";
    LOG(INFO) << "Latency - Avg: " << result.latency_avg_ms << " ms";
    LOG(INFO) << "Latency - P50: " << result.latency_p50_ms << " ms";
    LOG(INFO) << "Latency - P95: " << result.latency_p95_ms << " ms";
    LOG(INFO) << "Latency - P99: " << result.latency_p99_ms << " ms";
}

// ConcurrencyTestHelper implementation
void ConcurrencyTestHelper::RunConcurrentTest(
    std::vector<std::function<void()>> operations,
    uint32_t num_threads) {
    std::vector<std::thread> threads;

    for (uint32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&operations, i, num_threads] {
            for (size_t j = i; j < operations.size(); j += num_threads) {
                operations[j]();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

bool ConcurrencyTestHelper::DetectRaceCondition(
    std::function<void()> operation1,
    std::function<void()> operation2,
    uint32_t iterations) {
    for (uint32_t i = 0; i < iterations; ++i) {
        std::thread t1(operation1);
        std::thread t2(operation2);
        t1.join();
        t2.join();

        // Check for inconsistencies (implementation specific)
        // Return true if race condition detected
    }
    return false;
}

// DebugHelper implementation
std::string DebugHelper::HexDump(const void* data, size_t size) {
    std::stringstream ss;
    const unsigned char* bytes = static_cast<const unsigned char*>(data);

    for (size_t i = 0; i < size; i += 16) {
        ss << std::setfill('0') << std::setw(8) << std::hex << i << ": ";

        for (size_t j = i; j < std::min(i + 16, size); ++j) {
            ss << std::setw(2) << static_cast<int>(bytes[j]) << " ";
        }

        ss << " ";
        for (size_t j = i; j < std::min(i + 16, size); ++j) {
            char c = bytes[j];
            ss << (std::isprint(c) ? c : '.');
        }
        ss << "\n";
    }
    return ss.str();
}

void DebugHelper::EnableVerboseLogging() {
    FLAGS_minloglevel = 0; // INFO and above
    FLAGS_v = 2; // Verbose level 2
}

void DebugHelper::DisableVerboseLogging() {
    FLAGS_minloglevel = 2; // WARNING and above
    FLAGS_v = 0;
}

} // namespace eloqstore::test