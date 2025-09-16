#include <catch2/catch_test_macros.hpp>
#include <atomic>
#include <thread>
#include <chrono>
#include "../fixtures/test_fixtures.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("AsyncSimple_BasicOperations", "[async][simple]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("async_simple");

    SECTION("Async write and read") {
        std::atomic<bool> write_done{false};
        std::atomic<bool> read_done{false};
        std::string read_value;

        // Async write
        fixture.WriteAsync(table, "test_key", "test_value", [&write_done](KvError err) {
            REQUIRE(err == KvError::NoError);
            write_done = true;
        });

        // Wait for write
        while (!write_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Async read
        fixture.ReadAsync(table, "test_key", [&read_done, &read_value](KvError err, const std::string& value) {
            REQUIRE(err == KvError::NoError);
            REQUIRE(value == "test_value");
            read_value = value;
            read_done = true;
        });

        // Wait for read
        while (!read_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        REQUIRE(read_value == "test_value");
    }

    SECTION("Multiple async writes") {
        const int num_writes = 20;
        std::atomic<int> completed{0};

        // Issue multiple async writes
        for (int i = 0; i < num_writes; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);

            fixture.WriteAsync(table, key, value, [&completed](KvError err) {
                REQUIRE(err == KvError::NoError);
                completed++;
            });
        }

        // Wait for all writes
        while (completed < num_writes) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Verify with sync reads
        for (int i = 0; i < num_writes; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value;
            KvError err = fixture.ReadSync(table, key, value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(value == "value_" + std::to_string(i));
        }
    }

    SECTION("Async scan") {
        // First write some data
        for (int i = 0; i < 50; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "scan_" + std::to_string(i);
            fixture.WriteSync(table, key, value);
        }

        // Async scan
        std::atomic<bool> scan_done{false};
        std::vector<KvEntry> scan_results;

        fixture.ScanAsync(table, "0000000010", "0000000030",
            [&scan_done, &scan_results](KvError err, const std::vector<KvEntry>& results) {
                REQUIRE(err == KvError::NoError);
                scan_results = results;
                scan_done = true;
            }, 100);

        // Wait for scan
        while (!scan_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Verify results
        REQUIRE(scan_results.size() == 20); // Keys 10-29 inclusive
        for (size_t i = 0; i < scan_results.size(); ++i) {
            int key_num = 10 + i;
            std::string expected_value = "scan_" + std::to_string(key_num);
            REQUIRE(scan_results[i].value_ == expected_value);
        }
    }
}

TEST_CASE("AsyncSimple_MixedOperations", "[async][mixed]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("async_mixed");

    SECTION("Interleaved sync and async") {
        // Sync write
        KvError err = fixture.WriteSync(table, "sync1", "value1");
        REQUIRE(err == KvError::NoError);

        // Async write
        std::atomic<bool> async_done{false};
        fixture.WriteAsync(table, "async1", "value2", [&async_done](KvError err) {
            REQUIRE(err == KvError::NoError);
            async_done = true;
        });

        // Another sync write
        err = fixture.WriteSync(table, "sync2", "value3");
        REQUIRE(err == KvError::NoError);

        // Wait for async
        while (!async_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Verify all data
        std::string value;
        REQUIRE(fixture.ReadSync(table, "sync1", value) == KvError::NoError);
        REQUIRE(value == "value1");

        REQUIRE(fixture.ReadSync(table, "async1", value) == KvError::NoError);
        REQUIRE(value == "value2");

        REQUIRE(fixture.ReadSync(table, "sync2", value) == KvError::NoError);
        REQUIRE(value == "value3");
    }
}