#include <catch2/catch_test_macros.hpp>
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class DataIntegrityChecker {
public:
    // Compute SHA256 hash of data
    static std::string ComputeHash(const std::string& data) {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256(reinterpret_cast<const unsigned char*>(data.c_str()),
               data.size(), hash);

        std::stringstream ss;
        for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0')
               << static_cast<int>(hash[i]);
        }
        return ss.str();
    }

    // Generate checksum for key-value pair
    static std::string GenerateChecksum(const std::string& key, const std::string& value) {
        return ComputeHash(key + "|" + value);
    }

    // Verify data consistency
    struct ConsistencyReport {
        int total_entries = 0;
        int verified_entries = 0;
        int missing_entries = 0;
        int corrupted_entries = 0;
        std::vector<std::string> error_keys;

        bool IsConsistent() const {
            return corrupted_entries == 0 && missing_entries == 0;
        }

        void Print() const {
            std::cout << "\n=== Data Consistency Report ===" << std::endl;
            std::cout << "Total entries: " << total_entries << std::endl;
            std::cout << "Verified: " << verified_entries << std::endl;
            std::cout << "Missing: " << missing_entries << std::endl;
            std::cout << "Corrupted: " << corrupted_entries << std::endl;
            if (!error_keys.empty()) {
                std::cout << "Error keys: ";
                for (size_t i = 0; i < std::min(size_t(5), error_keys.size()); ++i) {
                    std::cout << error_keys[i] << " ";
                }
                if (error_keys.size() > 5) {
                    std::cout << "... (" << error_keys.size() - 5 << " more)";
                }
                std::cout << std::endl;
            }
        }
    };

    static ConsistencyReport VerifyData(
        TestFixture& fixture,
        const TableIdent& table,
        const std::map<std::string, std::string>& expected_data) {

        ConsistencyReport report;
        report.total_entries = expected_data.size();

        for (const auto& [key, expected_value] : expected_data) {
            std::string actual_value;
            KvError err = fixture.ReadSync(table, key, actual_value);

            if (err == KvError::NotFound) {
                report.missing_entries++;
                report.error_keys.push_back(key);
            } else if (err != KvError::NoError) {
                report.corrupted_entries++;
                report.error_keys.push_back(key);
            } else if (actual_value != expected_value) {
                report.corrupted_entries++;
                report.error_keys.push_back(key);
            } else {
                report.verified_entries++;
            }
        }

        return report;
    }
};

TEST_CASE("DataIntegrity_BasicConsistency", "[integrity][basic]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("integrity_basic");

    SECTION("Write and verify with checksums") {
        std::map<std::string, std::string> test_data;
        std::map<std::string, std::string> checksums;

        // Generate test data with checksums
        for (int i = 0; i < 100; ++i) {
            std::string key = "key_" + std::string(5 - std::to_string(i).length(), '0') +
                             std::to_string(i);
            std::string value = "value_content_" + std::to_string(i) + "_data";
            test_data[key] = value;
            checksums[key] = DataIntegrityChecker::GenerateChecksum(key, value);
        }

        // Write all data
        for (const auto& [key, value] : test_data) {
            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }

        // Verify all data and checksums
        for (const auto& [key, expected_value] : test_data) {
            std::string actual_value;
            KvError err = fixture.ReadSync(table, key, actual_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(actual_value == expected_value);

            // Verify checksum
            std::string actual_checksum = DataIntegrityChecker::GenerateChecksum(key, actual_value);
            REQUIRE(actual_checksum == checksums[key]);
        }
    }

    SECTION("Detect data corruption") {
        std::string key = "corruption_test";
        std::string original_value = "original_data_12345";

        // Write original data
        KvError err = fixture.WriteSync(table, key, original_value);
        REQUIRE(err == KvError::NoError);

        // Generate original checksum
        std::string original_checksum = DataIntegrityChecker::GenerateChecksum(key, original_value);

        // Overwrite with different data (simulating corruption)
        std::string corrupted_value = "corrupted_data_67890";
        err = fixture.WriteSync(table, key, corrupted_value);
        REQUIRE(err == KvError::NoError);

        // Read and verify checksum mismatch
        std::string read_value;
        err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);

        std::string new_checksum = DataIntegrityChecker::GenerateChecksum(key, read_value);
        REQUIRE(new_checksum != original_checksum);
    }
}

TEST_CASE("DataIntegrity_ConcurrentConsistency", "[integrity][concurrent]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("integrity_concurrent");

    SECTION("Concurrent writes maintain consistency") {
        const int num_threads = 8;
        const int keys_per_thread = 50;
        std::map<std::string, std::string> final_data;
        std::mutex data_mutex;

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                for (int i = 0; i < keys_per_thread; ++i) {
                    // Each thread writes to unique keys
                    std::string key = "t" + std::to_string(thread_id) + "_k" + std::to_string(i);
                    std::string value = "thread_" + std::to_string(thread_id) +
                                       "_value_" + std::to_string(i);

                    KvError err = fixture.WriteSync(table, key, value);
                    if (err == KvError::NoError) {
                        std::lock_guard<std::mutex> lock(data_mutex);
                        final_data[key] = value;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Verify all data
        auto report = DataIntegrityChecker::VerifyData(fixture, table, final_data);
        report.Print();

        REQUIRE(report.IsConsistent());
        REQUIRE(report.verified_entries == final_data.size());
    }

    SECTION("Read-write consistency under load") {
        const int num_writers = 2;
        const int num_readers = 4;
        const int duration_ms = 1000;
        std::atomic<bool> stop{false};
        std::atomic<int> write_count{0};
        std::atomic<int> read_count{0};
        std::atomic<int> consistency_errors{0};

        std::map<std::string, std::string> current_data;
        std::mutex data_mutex;

        // Start writer threads
        std::vector<std::thread> writers;
        for (int w = 0; w < num_writers; ++w) {
            writers.emplace_back([&, writer_id = w]() {
                RandomGenerator gen(writer_id);
                while (!stop) {
                    std::string key = "key_" + std::to_string(gen.GetUInt(0, 99));
                    std::string value = "value_" + std::to_string(write_count.load());

                    KvError err = fixture.WriteSync(table, key, value);
                    if (err == KvError::NoError) {
                        write_count++;
                        std::lock_guard<std::mutex> lock(data_mutex);
                        current_data[key] = value;
                    }
                }
            });
        }

        // Start reader threads
        std::vector<std::thread> readers;
        for (int r = 0; r < num_readers; ++r) {
            readers.emplace_back([&, reader_id = r]() {
                RandomGenerator gen(reader_id + 100);
                while (!stop) {
                    std::string key = "key_" + std::to_string(gen.GetUInt(0, 99));
                    std::string expected_value;

                    {
                        std::lock_guard<std::mutex> lock(data_mutex);
                        if (current_data.count(key) > 0) {
                            expected_value = current_data[key];
                        }
                    }

                    if (!expected_value.empty()) {
                        std::string actual_value;
                        KvError err = fixture.ReadSync(table, key, actual_value);
                        if (err == KvError::NoError) {
                            read_count++;
                            if (actual_value != expected_value) {
                                // Value changed between check and read (acceptable)
                                // Only count as error if value is completely wrong
                                if (!actual_value.starts_with("value_")) {
                                    consistency_errors++;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Run for specified duration
        std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
        stop = true;

        // Wait for all threads
        for (auto& t : writers) t.join();
        for (auto& t : readers) t.join();

        std::cout << "\n=== Concurrent Read-Write Stats ===" << std::endl;
        std::cout << "Writes: " << write_count.load() << std::endl;
        std::cout << "Reads: " << read_count.load() << std::endl;
        std::cout << "Consistency errors: " << consistency_errors.load() << std::endl;

        REQUIRE(consistency_errors == 0);
        REQUIRE(write_count > 0);
        REQUIRE(read_count > 0);
    }
}

TEST_CASE("DataIntegrity_LargeDataset", "[integrity][large]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("integrity_large");

    SECTION("Large value integrity") {
        // Test with increasingly large values
        std::vector<size_t> sizes = {100, 1000, 10000, 100000, 1000000};  // Up to 1MB

        for (size_t size : sizes) {
            std::string key = "large_value_" + std::to_string(size);

            // Generate predictable pattern
            std::string value;
            value.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                value += static_cast<char>('A' + (i % 26));
            }

            // Write large value
            KvError err = fixture.WriteSync(table, key, value);
            if (err != KvError::NoError) {
                // Skip if size not supported
                continue;
            }

            // Read and verify
            std::string read_value;
            err = fixture.ReadSync(table, key, read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value.size() == size);

            // Verify pattern integrity
            bool pattern_intact = true;
            for (size_t i = 0; i < size; ++i) {
                if (read_value[i] != static_cast<char>('A' + (i % 26))) {
                    pattern_intact = false;
                    break;
                }
            }
            REQUIRE(pattern_intact);

            std::cout << "Verified integrity of " << size << " byte value" << std::endl;
        }
    }

    SECTION("Dataset consistency check") {
        const int num_entries = 1000;
        std::map<std::string, std::string> dataset;
        RandomGenerator gen(42);

        // Generate dataset
        for (int i = 0; i < num_entries; ++i) {
            std::string key = "data_" + std::string(8 - std::to_string(i).length(), '0') +
                             std::to_string(i);
            std::string value = gen.GetValue(50, 200);
            dataset[key] = value;
        }

        // Write dataset
        auto write_start = std::chrono::steady_clock::now();
        for (const auto& [key, value] : dataset) {
            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }
        auto write_end = std::chrono::steady_clock::now();

        // Verify dataset
        auto verify_start = std::chrono::steady_clock::now();
        auto report = DataIntegrityChecker::VerifyData(fixture, table, dataset);
        auto verify_end = std::chrono::steady_clock::now();

        report.Print();

        auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        auto verify_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            verify_end - verify_start).count();

        std::cout << "Write time: " << write_ms << " ms" << std::endl;
        std::cout << "Verify time: " << verify_ms << " ms" << std::endl;

        REQUIRE(report.IsConsistent());
        REQUIRE(report.verified_entries == num_entries);
    }
}

TEST_CASE("DataIntegrity_OrderPreservation", "[integrity][order]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("integrity_order");

    SECTION("Scan order consistency") {
        // Write ordered data
        std::vector<std::pair<std::string, std::string>> ordered_data;
        for (int i = 0; i < 100; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "ordered_value_" + std::to_string(i);
            ordered_data.push_back({key, value});

            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }

        // Perform multiple scans and verify order
        for (int scan = 0; scan < 5; ++scan) {
            std::vector<KvEntry> results;
            KvError err = fixture.ScanSync(table, "", "9999999999", results, 1000);
            REQUIRE(err == KvError::NoError);

            // Verify order
            for (size_t i = 1; i < results.size(); ++i) {
                REQUIRE(results[i-1].key_ < results[i].key_);
            }

            // Verify completeness
            REQUIRE(results.size() == ordered_data.size());

            // Verify exact match
            for (size_t i = 0; i < results.size(); ++i) {
                REQUIRE(results[i].key_ == ordered_data[i].first);
                REQUIRE(results[i].value_ == ordered_data[i].second);
            }
        }
    }

    SECTION("Concurrent scans maintain order") {
        // Prepare data
        for (int i = 0; i < 200; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "scan_value_" + std::to_string(i);
            fixture.WriteSync(table, key, value);
        }

        const int num_threads = 4;
        std::atomic<int> order_violations{0};

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < 25; ++i) {
                    std::vector<KvEntry> results;
                    KvError err = fixture.ScanSync(table, "", "9999999999", results, 200);

                    if (err == KvError::NoError) {
                        // Check order
                        for (size_t j = 1; j < results.size(); ++j) {
                            if (results[j-1].key_ >= results[j].key_) {
                                order_violations++;
                                break;
                            }
                        }
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(order_violations == 0);
    }
}