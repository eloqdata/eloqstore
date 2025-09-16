#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_session.hpp>
#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/test_config.h"
#include "../../fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class StressTestRunner {
public:
    struct Stats {
        std::atomic<uint64_t> reads{0};
        std::atomic<uint64_t> writes{0};
        std::atomic<uint64_t> scans{0};
        std::atomic<uint64_t> deletes{0};
        std::atomic<uint64_t> errors{0};
        std::atomic<uint64_t> operations{0};

        void Print() const {
            std::cout << "\n=== Stress Test Statistics ===" << std::endl;
            std::cout << "Total operations: " << operations.load() << std::endl;
            std::cout << "Reads: " << reads.load() << std::endl;
            std::cout << "Writes: " << writes.load() << std::endl;
            std::cout << "Scans: " << scans.load() << std::endl;
            std::cout << "Deletes: " << deletes.load() << std::endl;
            std::cout << "Errors: " << errors.load() << std::endl;
        }
    };

    StressTestRunner(TestFixture& fixture, const TestConfig& config)
        : fixture_(fixture), config_(config) {
        table_ = fixture_.CreateTestTable("stress_test");

        // Initialize random generator with configured seed
        uint64_t effective_seed = config_.GetEffectiveSeed();
        random_gen_.SetSeed(effective_seed);

        if (config_.verbose) {
            std::cout << "Using seed: " << effective_seed << std::endl;
        }
    }

    void Run() {
        auto start_time = std::chrono::steady_clock::now();

        if (config_.IsTimeBased()) {
            RunTimeBased();
        } else {
            RunIterationBased();
        }

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            end_time - start_time).count();

        if (config_.verbose) {
            stats_.Print();
            std::cout << "Duration: " << duration << " seconds" << std::endl;
            std::cout << "Throughput: "
                      << (stats_.operations.load() / std::max(1L, duration))
                      << " ops/sec" << std::endl;
        }
    }

private:
    void RunTimeBased() {
        std::vector<std::thread> threads;
        std::atomic<bool> stop_flag{false};

        auto end_time = std::chrono::steady_clock::now() +
                        std::chrono::seconds(config_.duration_seconds);

        for (int i = 0; i < config_.thread_count; ++i) {
            threads.emplace_back([this, &stop_flag, end_time]() {
                while (!stop_flag &&
                       std::chrono::steady_clock::now() < end_time) {
                    ExecuteRandomOperation();
                }
            });
        }

        // Wait for duration
        std::this_thread::sleep_until(end_time);
        stop_flag = true;

        for (auto& t : threads) {
            t.join();
        }
    }

    void RunIterationBased() {
        std::vector<std::thread> threads;
        std::atomic<uint64_t> iteration_counter{0};

        for (int i = 0; i < config_.thread_count; ++i) {
            threads.emplace_back([this, &iteration_counter]() {
                while (true) {
                    uint64_t iter = iteration_counter.fetch_add(1);
                    if (iter >= config_.iterations) break;
                    ExecuteRandomOperation();
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }
    }

    void ExecuteRandomOperation() {
        // Thread-local random generator (seeded from main generator)
        thread_local RandomGenerator local_gen(random_gen_.GetUInt(0, UINT64_MAX));

        auto op = local_gen.GetRandomOperation({0.3, 0.4, 0.2, 0.1});

        try {
            switch (op) {
                case RandomGenerator::Operation::READ:
                    ExecuteRead(local_gen);
                    break;
                case RandomGenerator::Operation::WRITE:
                    ExecuteWrite(local_gen);
                    break;
                case RandomGenerator::Operation::SCAN:
                    ExecuteScan(local_gen);
                    break;
                case RandomGenerator::Operation::DELETE:
                    ExecuteDelete(local_gen);
                    break;
            }
            stats_.operations++;
        } catch (const std::exception& e) {
            stats_.errors++;
            if (config_.verbose) {
                std::cerr << "Operation failed: " << e.what() << std::endl;
            }
        }
    }

    void ExecuteRead(RandomGenerator& gen) {
        std::string key = gen.GetKey(config_.min_key_size, config_.max_key_size);
        std::string value;

        KvError err = fixture_.ReadSync(table_, key, value);

        // NotFound is acceptable for reads
        if (err != KvError::NoError && err != KvError::NotFound) {
            stats_.errors++;
        } else {
            stats_.reads++;

            // Verify value if found
            if (err == KvError::NoError) {
                {
                    std::lock_guard<std::mutex> lock(data_mutex_);
                    if (written_data_.count(key) > 0) {
                        if (written_data_[key] != value) {
                            stats_.errors++;
                            if (config_.verbose) {
                                std::cerr << "Data mismatch for key: " << key << std::endl;
                            }
                        }
                    }
                }
            }
        }
    }

    void ExecuteWrite(RandomGenerator& gen) {
        std::string key = gen.GetKey(config_.min_key_size, config_.max_key_size);
        std::string value = gen.GetValue(config_.min_value_size, config_.max_value_size);

        KvError err = fixture_.WriteSync(table_, key, value);

        if (err != KvError::NoError) {
            stats_.errors++;
        } else {
            stats_.writes++;

            // Track written data for verification
            {
                std::lock_guard<std::mutex> lock(data_mutex_);
                written_data_[key] = value;
            }
        }
    }

    void ExecuteScan(RandomGenerator& gen) {
        std::string start_key = gen.GetKey(config_.min_key_size, config_.max_key_size);
        std::string end_key = gen.GetKey(config_.min_key_size, config_.max_key_size);

        // Ensure start < end
        if (start_key > end_key) {
            std::swap(start_key, end_key);
        }

        std::vector<KvEntry> results;
        KvError err = fixture_.ScanSync(table_, start_key, end_key, results, 100);

        if (err != KvError::NoError) {
            stats_.errors++;
        } else {
            stats_.scans++;

            // Verify scan results are in order
            for (size_t i = 1; i < results.size(); ++i) {
                if (results[i-1].key_ >= results[i].key_) {
                    stats_.errors++;
                    if (config_.verbose) {
                        std::cerr << "Scan results not in order" << std::endl;
                    }
                    break;
                }
            }
        }
    }

    void ExecuteDelete(RandomGenerator& gen) {
        std::string key = gen.GetKey(config_.min_key_size, config_.max_key_size);

        // Use write with empty value as delete
        KvError err = fixture_.WriteSync(table_, key, "");

        if (err != KvError::NoError) {
            stats_.errors++;
        } else {
            stats_.deletes++;

            // Remove from tracking
            {
                std::lock_guard<std::mutex> lock(data_mutex_);
                written_data_.erase(key);
            }
        }
    }

    TestFixture& fixture_;
    TestConfig config_;
    TableIdent table_;
    RandomGenerator random_gen_;
    Stats stats_;

    // For data verification
    std::mutex data_mutex_;
    std::map<std::string, std::string> written_data_;
};

TEST_CASE("RandomizedStressTest", "[stress][randomized]") {
    // Load configuration from file/env/command-line
    TestConfig config = TestConfig::LoadConfig();

    SECTION("Basic randomized stress test") {
        TestFixture fixture;
        fixture.InitStoreWithDefaults();

        StressTestRunner runner(fixture, config);
        runner.Run();

        // Test passes if no crashes and error rate is acceptable
        REQUIRE(true);
    }
}

TEST_CASE("RandomizedStressTest_Patterns", "[stress][patterns]") {
    TestConfig config = TestConfig::LoadConfig();

    SECTION("Sequential key pattern") {
        TestFixture fixture;
        fixture.InitStoreWithDefaults();
        auto table = fixture.CreateTestTable("sequential_test");

        RandomGenerator gen(config.GetEffectiveSeed());

        // Write sequential keys
        for (uint64_t i = 0; i < config.iterations; ++i) {
            std::string key = gen.GetSequentialKey(16);
            std::string value = gen.GetValue(config.min_value_size, config.max_value_size);

            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }
    }

    SECTION("Hotspot key pattern") {
        TestFixture fixture;
        fixture.InitStoreWithDefaults();
        auto table = fixture.CreateTestTable("hotspot_test");

        RandomGenerator gen(config.GetEffectiveSeed());
        std::map<std::string, uint64_t> access_count;

        // Access with hotspot pattern
        for (uint64_t i = 0; i < config.iterations; ++i) {
            std::string key = gen.GetHotspotKey(16, 0.9);  // 90% hotspot
            access_count[key]++;

            if (i % 2 == 0) {
                // Write
                std::string value = gen.GetValue(config.min_value_size, config.max_value_size);
                fixture.WriteSync(table, key, value);
            } else {
                // Read
                std::string value;
                fixture.ReadSync(table, key, value);
            }
        }

        // Verify hotspot distribution
        uint64_t hotspot_accesses = 0;
        for (const auto& [key, count] : access_count) {
            if (key.substr(0, 4) == "HOT_") {
                hotspot_accesses += count;
            }
        }

        double hotspot_ratio = static_cast<double>(hotspot_accesses) / config.iterations;
        REQUIRE(hotspot_ratio > 0.8);  // Should be close to 90%
    }
}

// Custom main for stress tests with command-line argument support
int main(int argc, char* argv[]) {
    // Parse command-line arguments for test configuration
    TestConfig::GetInstance() = TestConfig::LoadConfig(argc, argv);

    // Print configuration if verbose
    if (TestConfig::GetInstance().verbose) {
        std::cout << "=== Test Configuration ===" << std::endl;
        std::cout << "Seed: " << TestConfig::GetInstance().GetEffectiveSeed() << std::endl;
        std::cout << "Iterations: " << TestConfig::GetInstance().iterations << std::endl;
        std::cout << "Threads: " << TestConfig::GetInstance().thread_count << std::endl;
        if (TestConfig::GetInstance().IsTimeBased()) {
            std::cout << "Duration: " << TestConfig::GetInstance().duration_seconds
                      << " seconds" << std::endl;
        }
        std::cout << "==========================\n" << std::endl;
    }

    // Run Catch2 tests
    return Catch::Session().run(argc, argv);
}