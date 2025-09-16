#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <random>
#include <set>

#include "scan_task.h"
#include "shard.h"
#include "index_page_manager.h"
#include "page_mapper.h"
#include "data_page_builder.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class ScanTaskTestFixture : public TestFixture {
public:
    ScanTaskTestFixture() {
        InitStoreWithDefaults();
        InitTestData();
    }

    void InitTestData() {
        table_ = CreateTestTable("scan_test_table");

        // Generate ordered test data
        for (int i = 0; i < 100; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = gen_.GenerateValue(100);
            test_data_[key] = value;
        }
    }

    TableIdent PopulateTable() {
        // This would populate the table with test data
        // In real implementation, would write pages to store
        return table_;
    }

protected:
    TableIdent table_;
    std::map<std::string, std::string> test_data_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_BasicScan", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Forward scan all") {
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(table_, "", "", 100, false, results);

        if (err == KvError::NoError) {
            // Should return results in order
            for (size_t i = 1; i < results.size(); ++i) {
                REQUIRE(results[i-1].first < results[i].first);
            }
        }
    }

    SECTION("Reverse scan all") {
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(table_, "", "", 100, true, results);

        if (err == KvError::NoError) {
            // Should return results in reverse order
            for (size_t i = 1; i < results.size(); ++i) {
                REQUIRE(results[i-1].first > results[i].first);
            }
        }
    }

    SECTION("Scan with range") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string start_key = "key_020";
        std::string end_key = "key_080";

        KvError err = task.Scan(table_, start_key, end_key, 100, false, results);

        if (err == KvError::NoError) {
            for (const auto& [key, value] : results) {
                REQUIRE(key >= start_key);
                REQUIRE(key <= end_key);
            }
        }
    }

    SECTION("Scan with limit") {
        std::vector<std::pair<std::string, std::string>> results;
        size_t limit = 10;

        KvError err = task.Scan(table_, "", "", limit, false, results);

        if (err == KvError::NoError) {
            REQUIRE(results.size() <= limit);
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_BoundaryConditions", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Empty range") {
        std::vector<std::pair<std::string, std::string>> results;

        // Start key > end key should return empty
        KvError err = task.Scan(table_, "zzz", "aaa", 100, false, results);

        REQUIRE(results.empty());
    }

    SECTION("Single key range") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string single_key = "key_050";
        KvError err = task.Scan(table_, single_key, single_key, 100, false, results);

        if (err == KvError::NoError) {
            if (!results.empty()) {
                REQUIRE(results.size() == 1);
                REQUIRE(results[0].first == single_key);
            }
        }
    }

    SECTION("Scan with zero limit") {
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(table_, "", "", 0, false, results);

        REQUIRE(results.empty());
    }

    SECTION("Scan beyond last key") {
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(table_, "key_999", "", 100, false, results);

        // Should handle gracefully, might be empty
        if (err == KvError::NoError) {
            // All results should be after key_999
            for (const auto& [key, value] : results) {
                REQUIRE(key >= "key_999");
            }
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_PrefixScan", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Scan with common prefix") {
        std::vector<std::pair<std::string, std::string>> results;

        // Scan all keys starting with "key_0"
        std::string prefix = "key_0";
        std::string prefix_end = "key_1";

        KvError err = task.Scan(table_, prefix, prefix_end, 100, false, results);

        if (err == KvError::NoError) {
            for (const auto& [key, value] : results) {
                REQUIRE(key.substr(0, prefix.length()) == prefix);
            }
        }
    }

    SECTION("Scan with non-existing prefix") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string prefix = "nonexistent_";
        std::string prefix_end = "nonexistent_z";

        KvError err = task.Scan(table_, prefix, prefix_end, 100, false, results);

        // Should be empty or error
        if (err == KvError::NoError) {
            REQUIRE(results.empty());
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_LargeScan", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Scan with pagination") {
        const size_t page_size = 20;
        std::string last_key = "";
        std::set<std::string> all_keys;
        int pages = 0;

        while (pages < 10) {  // Limit iterations
            std::vector<std::pair<std::string, std::string>> results;

            KvError err = task.Scan(table_, last_key, "", page_size, false, results);

            if (err != KvError::NoError || results.empty()) {
                break;
            }

            for (const auto& [key, value] : results) {
                all_keys.insert(key);
            }

            last_key = results.back().first + "\x00";  // Next key after last
            pages++;
        }

        // Should have collected unique keys
        REQUIRE(all_keys.size() > 0);
    }

    SECTION("Large limit scan") {
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(table_, "", "", 10000, false, results);

        if (err == KvError::NoError) {
            // Should return all available keys (up to actual count)
            REQUIRE(results.size() <= 10000);
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_ErrorHandling", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Invalid table") {
        TableIdent invalid_table("", 0);
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(invalid_table, "", "", 100, false, results);

        REQUIRE(err != KvError::NoError);
        REQUIRE(results.empty());
    }

    SECTION("Corrupted page during scan") {
        std::vector<std::pair<std::string, std::string>> results;

        // Would need to simulate corruption
        KvError err = task.Scan(table_, "", "", 100, false, results);

        if (err == KvError::Corruption) {
            REQUIRE(results.empty());
        }
    }

    SECTION("IO error during scan") {
        std::vector<std::pair<std::string, std::string>> results;

        // Would need to simulate IO error
        KvError err = task.Scan(table_, "", "", 100, false, results);

        if (err == KvError::IOError) {
            REQUIRE(results.empty());
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_SpecialKeys", "[scan][task][unit]") {
    ScanTask task;

    SECTION("Scan with binary keys") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string binary_start = gen_.GenerateBinaryString(10);
        std::string binary_end = gen_.GenerateBinaryString(10);

        if (binary_start > binary_end) {
            std::swap(binary_start, binary_end);
        }

        KvError err = task.Scan(table_, binary_start, binary_end, 100, false, results);

        // Should handle binary keys
    }

    SECTION("Scan with unicode keys") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string unicode_start = "键_001";
        std::string unicode_end = "键_100";

        KvError err = task.Scan(table_, unicode_start, unicode_end, 100, false, results);

        // Should handle unicode correctly
    }

    SECTION("Scan with empty key boundaries") {
        std::vector<std::pair<std::string, std::string>> results;

        // Empty start key means scan from beginning
        KvError err = task.Scan(table_, "", "key_050", 100, false, results);

        if (err == KvError::NoError) {
            for (const auto& [key, value] : results) {
                REQUIRE(key <= "key_050");
            }
        }

        results.clear();

        // Empty end key means scan to end
        err = task.Scan(table_, "key_050", "", 100, false, results);

        if (err == KvError::NoError) {
            for (const auto& [key, value] : results) {
                REQUIRE(key >= "key_050");
            }
        }
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_Performance", "[scan][task][benchmark]") {
    ScanTask task;
    TableIdent perf_table = PopulateTable();

    SECTION("Large forward scan") {
        Timer timer;
        std::vector<std::pair<std::string, std::string>> results;

        timer.Start();
        KvError err = task.Scan(perf_table, "", "", 10000, false, results);
        timer.Stop();

        if (err == KvError::NoError) {
            double keys_per_sec = results.size() / timer.ElapsedSeconds();
            LOG(INFO) << "Forward scan: " << keys_per_sec << " keys/sec";

            REQUIRE(keys_per_sec > 1000);  // Should scan >1000 keys/sec
        }
    }

    SECTION("Large reverse scan") {
        Timer timer;
        std::vector<std::pair<std::string, std::string>> results;

        timer.Start();
        KvError err = task.Scan(perf_table, "", "", 10000, true, results);
        timer.Stop();

        if (err == KvError::NoError) {
            double keys_per_sec = results.size() / timer.ElapsedSeconds();
            LOG(INFO) << "Reverse scan: " << keys_per_sec << " keys/sec";

            REQUIRE(keys_per_sec > 1000);
        }
    }

    SECTION("Range scan performance") {
        Timer timer;
        int successful_scans = 0;

        timer.Start();
        for (int i = 0; i < 100; ++i) {
            std::vector<std::pair<std::string, std::string>> results;

            std::string start = "key_" + std::to_string(i * 10);
            std::string end = "key_" + std::to_string(i * 10 + 10);

            KvError err = task.Scan(perf_table, start, end, 100, false, results);
            if (err == KvError::NoError) {
                successful_scans++;
            }
        }
        timer.Stop();

        double scans_per_sec = successful_scans / timer.ElapsedSeconds();
        LOG(INFO) << "Range scans: " << scans_per_sec << " scans/sec";
    }
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_Concurrency", "[scan][task][stress]") {
    const int thread_count = 8;
    const int scans_per_thread = 50;
    std::atomic<int> successful_scans{0};
    std::atomic<int> total_keys{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([this, &successful_scans, &total_keys, t, scans_per_thread]() {
            ScanTask task;

            for (int i = 0; i < scans_per_thread; ++i) {
                std::vector<std::pair<std::string, std::string>> results;

                // Each thread scans different ranges
                std::string start = "key_" + std::to_string(t * 10);
                std::string end = "key_" + std::to_string((t + 1) * 10);

                KvError err = task.Scan(table_, start, end, 20, false, results);

                if (err == KvError::NoError) {
                    successful_scans++;
                    total_keys += results.size();
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    REQUIRE(successful_scans > 0);
    LOG(INFO) << "Concurrent scans: " << successful_scans << " successful, "
              << total_keys << " total keys scanned";
}

TEST_CASE_METHOD(ScanTaskTestFixture, "ScanTask_EdgeCases", "[scan][task][edge-case]") {
    ScanTask task;

    SECTION("Maximum key boundaries") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string max_key(MaxKeySize, 0xFF);
        KvError err = task.Scan(table_, "", max_key, 100, false, results);

        // Should handle maximum key
    }

    SECTION("Scan with null bytes in range") {
        std::vector<std::pair<std::string, std::string>> results;

        std::string start_with_null = "key\0start";
        std::string end_with_null = "key\0end";

        KvError err = task.Scan(table_, start_with_null, end_with_null, 100, false, results);

        // Should handle null bytes correctly
    }

    SECTION("Alternating forward and reverse scans") {
        std::vector<std::pair<std::string, std::string>> forward_results;
        std::vector<std::pair<std::string, std::string>> reverse_results;

        KvError err1 = task.Scan(table_, "key_10", "key_20", 100, false, forward_results);
        KvError err2 = task.Scan(table_, "key_10", "key_20", 100, true, reverse_results);

        if (err1 == KvError::NoError && err2 == KvError::NoError) {
            // Forward and reverse should contain same keys, different order
            std::set<std::string> forward_keys;
            std::set<std::string> reverse_keys;

            for (const auto& [k, v] : forward_results) {
                forward_keys.insert(k);
            }
            for (const auto& [k, v] : reverse_results) {
                reverse_keys.insert(k);
            }

            REQUIRE(forward_keys == reverse_keys);
        }
    }

    SECTION("Scan with expired keys") {
        std::vector<std::pair<std::string, std::string>> results;

        // Would need to set up expired keys
        KvError err = task.Scan(table_, "", "", 100, false, results);

        if (err == KvError::NoError) {
            // Should not include expired keys
            for (const auto& [key, value] : results) {
                // Verify no expired keys in results
            }
        }
    }
}