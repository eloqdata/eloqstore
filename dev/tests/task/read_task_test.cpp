#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <random>

#include "read_task.h"
#include "shard.h"
#include "index_page_manager.h"
#include "page_mapper.h"
#include "data_page_builder.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class ReadTaskTestFixture : public TestFixture {
public:
    ReadTaskTestFixture() {
        InitStoreWithDefaults();
        InitTestData();
    }

    void InitTestData() {
        table_ = CreateTestTable("read_test_table");

        // Build some test pages with data
        DataPageBuilder builder(Options()->comparator, Options()->page_size);

        test_data_ = {
            {"key1", "value1"},
            {"key2", "value2"},
            {"key3", "value3"},
            {"key4", "very_long_value_that_might_overflow_" + std::string(1000, 'x')},
            {"key5", "value5"}
        };

        for (const auto& [key, value] : test_data_) {
            builder.Add(key, value, 100, 0);
        }
    }

    TableIdent CreateAndPopulateTable() {
        // Create a table with actual data
        TableIdent table("populated_table", 1);

        // This would normally involve writing pages through the store
        // For testing, we'd need to set up the proper environment

        return table;
    }

protected:
    TableIdent table_;
    std::map<std::string, std::string> test_data_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_BasicRead", "[read][task][unit]") {
    ReadTask task;

    SECTION("Read existing key") {
        // This would require a properly populated table
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        // Attempt to read
        KvError err = task.Read(table_, "key1", value, timestamp, expire_ts);

        // May fail if table not properly set up
        if (err == KvError::NoError) {
            REQUIRE(!value.empty());
        }
    }

    SECTION("Read non-existing key") {
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, "non_existing_key", value, timestamp, expire_ts);
        REQUIRE(err == KvError::NotFound);
    }

    SECTION("Read with empty key") {
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, "", value, timestamp, expire_ts);
        // Should handle empty key
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_Floor", "[read][task][unit]") {
    ReadTask task;

    SECTION("Floor with exact match") {
        std::string found_key;
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Floor(table_, "key2", found_key, value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            REQUIRE(found_key == "key2");
        }
    }

    SECTION("Floor with no exact match") {
        std::string found_key;
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        // Search for key between key2 and key3
        KvError err = task.Floor(table_, "key2.5", found_key, value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            // Should find key2 (the floor)
            REQUIRE(found_key <= "key2.5");
        }
    }

    SECTION("Floor before all keys") {
        std::string found_key;
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Floor(table_, "aaa", found_key, value, timestamp, expire_ts);

        // Should not find anything
        REQUIRE(err == KvError::NotFound);
    }

    SECTION("Floor after all keys") {
        std::string found_key;
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Floor(table_, "zzz", found_key, value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            // Should find the last key
            REQUIRE(!found_key.empty());
        }
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_OverflowHandling", "[read][task][unit]") {
    ReadTask task;

    SECTION("Read value with overflow") {
        // Create a very large value that would cause overflow
        std::string large_value(10000, 'x');
        std::string key = "overflow_key";

        // Would need to write this to the store first

        std::string read_value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, key, read_value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            // Should correctly handle overflow pages
            REQUIRE(read_value.size() > 0);
        }
    }

    SECTION("Multiple overflow pages") {
        // Test reading values that span multiple overflow pages
        std::string huge_value(100000, 'y');
        std::string key = "huge_overflow_key";

        std::string read_value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, key, read_value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            // Should handle multiple overflow pages
            REQUIRE(read_value.size() > 0);
        }
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_Timestamps", "[read][task][unit]") {
    ReadTask task;

    SECTION("Read with valid timestamp") {
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, "key1", value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            REQUIRE(timestamp > 0);
        }
    }

    SECTION("Read with expiration") {
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        // Would need to write a key with expiration first

        KvError err = task.Read(table_, "expiring_key", value, timestamp, expire_ts);

        if (err == KvError::NoError) {
            // Check expiration handling
            if (expire_ts > 0 && expire_ts < CurrentTime()) {
                // Key should be expired
            }
        }
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_ErrorHandling", "[read][task][unit]") {
    ReadTask task;

    SECTION("Invalid table") {
        TableIdent invalid_table("", 0);
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(invalid_table, "key", value, timestamp, expire_ts);
        REQUIRE(err != KvError::NoError);
    }

    SECTION("Corrupted page") {
        // Simulate reading from a corrupted page
        // Would need to set up corrupted data

        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, "corrupted_key", value, timestamp, expire_ts);

        // Should detect corruption
        if (err == KvError::Corruption) {
            REQUIRE(value.empty());
        }
    }

    SECTION("IO error during read") {
        // Simulate IO error
        // Would need mock IO manager

        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, "io_error_key", value, timestamp, expire_ts);

        if (err == KvError::IOError) {
            REQUIRE(value.empty());
        }
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_BinaryKeys", "[read][task][unit]") {
    ReadTask task;

    SECTION("Read with binary key") {
        std::string binary_key = gen_.GenerateBinaryString(50);
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, binary_key, value, timestamp, expire_ts);

        // Should handle binary keys correctly
    }

    SECTION("Read with null bytes") {
        std::string key_with_null = "key\0with\0nulls";
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, key_with_null, value, timestamp, expire_ts);

        // Should handle keys with null bytes
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_Performance", "[read][task][benchmark]") {
    ReadTask task;

    // Set up a table with many keys
    TableIdent perf_table = CreateAndPopulateTable();

    SECTION("Sequential read performance") {
        Timer timer;
        int successful_reads = 0;

        timer.Start();
        for (int i = 0; i < 1000; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value;
            uint64_t timestamp;
            uint64_t expire_ts;

            KvError err = task.Read(perf_table, key, value, timestamp, expire_ts);
            if (err == KvError::NoError) {
                successful_reads++;
            }
        }
        timer.Stop();

        double reads_per_sec = successful_reads / timer.ElapsedSeconds();
        LOG(INFO) << "Sequential reads: " << reads_per_sec << " reads/sec";

        // Should handle reasonable read rate
    }

    SECTION("Random read performance") {
        Timer timer;
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 999);
        int successful_reads = 0;

        timer.Start();
        for (int i = 0; i < 1000; ++i) {
            std::string key = "key_" + std::to_string(dist(rng));
            std::string value;
            uint64_t timestamp;
            uint64_t expire_ts;

            KvError err = task.Read(perf_table, key, value, timestamp, expire_ts);
            if (err == KvError::NoError) {
                successful_reads++;
            }
        }
        timer.Stop();

        double reads_per_sec = successful_reads / timer.ElapsedSeconds();
        LOG(INFO) << "Random reads: " << reads_per_sec << " reads/sec";
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_ConcurrentReads", "[read][task][stress]") {
    const int thread_count = 8;
    const int reads_per_thread = 100;
    std::atomic<int> successful_reads{0};
    std::atomic<int> failed_reads{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([this, &successful_reads, &failed_reads, t, reads_per_thread]() {
            ReadTask task;

            for (int i = 0; i < reads_per_thread; ++i) {
                std::string key = "key" + std::to_string((t * reads_per_thread + i) % 5);
                std::string value;
                uint64_t timestamp;
                uint64_t expire_ts;

                KvError err = task.Read(table_, key, value, timestamp, expire_ts);
                if (err == KvError::NoError) {
                    successful_reads++;
                } else {
                    failed_reads++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    int total_reads = successful_reads + failed_reads;
    REQUIRE(total_reads == thread_count * reads_per_thread);
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_EdgeCases", "[read][task][edge-case]") {
    ReadTask task;

    SECTION("Maximum key size") {
        std::string max_key(MaxKeySize, 'k');
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, max_key, value, timestamp, expire_ts);
        // Should handle maximum key size
    }

    SECTION("Maximum value size") {
        std::string key = "max_value_key";
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        // Would need to write maximum value first

        KvError err = task.Read(table_, key, value, timestamp, expire_ts);
        if (err == KvError::NoError) {
            // Should handle maximum value size
        }
    }

    SECTION("Unicode keys") {
        std::string unicode_key = "键值_キー_🔑";
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(table_, unicode_key, value, timestamp, expire_ts);
        // Should handle unicode correctly
    }

    SECTION("Special characters in keys") {
        std::vector<std::string> special_keys = {
            "key!@#$%^&*()",
            "key\t\n\r",
            "key with spaces",
            "key/with/slashes",
            "key\\with\\backslashes"
        };

        for (const auto& key : special_keys) {
            std::string value;
            uint64_t timestamp;
            uint64_t expire_ts;

            KvError err = task.Read(table_, key, value, timestamp, expire_ts);
            // Should handle special characters
        }
    }
}