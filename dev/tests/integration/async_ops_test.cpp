#include <catch2/catch_test_macros.hpp>
#include <atomic>
#include <vector>
#include <thread>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/data_generator.h"
#include "../../eloq_store.h"
#include "../../types.h"

using namespace eloqstore;
using namespace eloqstore::test;

class AsyncOpsTestFixture : public TestFixture {
public:
    AsyncOpsTestFixture() {
        InitStoreWithDefaults();
    }
};

TEST_CASE_METHOD(AsyncOpsTestFixture, "AsyncOps_BasicReadWrite", "[async][integration]") {
    auto table = CreateTestTable("async_test");

    SECTION("Async write followed by async read") {
        std::atomic<bool> write_done{false};
        std::atomic<bool> read_done{false};
        std::string read_value;

        // Async write
        WriteAsync(table, "key1", "value1", [&write_done](KvError err) {
            REQUIRE(err == KvError::NoError);
            write_done = true;
        });

        // Wait for write to complete
        while (!write_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Async read
        ReadAsync(table, "key1", [&read_done, &read_value](KvError err, const std::string& value) {
            REQUIRE(err == KvError::NoError);
            REQUIRE(value == "value1");
            read_value = value;
            read_done = true;
        });

        // Wait for read to complete
        while (!read_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        REQUIRE(read_value == "value1");
    }

    SECTION("Multiple async writes in parallel") {
        const int num_writes = 100;
        std::atomic<int> completed{0};
        std::vector<std::string> keys;
        std::vector<std::string> values;

        // Generate test data
        DataGenerator gen;
        for (int i = 0; i < num_writes; ++i) {
            keys.push_back(gen.GeneratePrefixedKey("key", i));
            values.push_back(gen.GenerateValue(100));
        }

        // Issue all writes async
        for (int i = 0; i < num_writes; ++i) {
            WriteAsync(table, keys[i], values[i], [&completed](KvError err) {
                REQUIRE(err == KvError::NoError);
                completed++;
            });
        }

        // Wait for all writes to complete
        while (completed < num_writes) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Verify all data with sync reads
        for (int i = 0; i < num_writes; ++i) {
            std::string read_value;
            auto err = ReadSync(table, keys[i], read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value == values[i]);
        }
    }
}

TEST_CASE_METHOD(AsyncOpsTestFixture, "AsyncOps_BatchOperations", "[async][integration]") {
    auto table = CreateTestTable("batch_async");

    SECTION("Issue multiple operations then wait") {
        // Create multiple requests
        std::vector<std::unique_ptr<BatchWriteRequest>> write_reqs;
        std::vector<std::unique_ptr<ReadRequest>> read_reqs;

        // Create batch write requests
        for (int batch = 0; batch < 5; ++batch) {
            auto req = std::make_unique<BatchWriteRequest>();
            req->SetTableId(table);
            for (int i = 0; i < 10; ++i) {
                std::string key = "batch" + std::to_string(batch) + "_key" + std::to_string(i);
                std::string value = "value_" + std::to_string(batch * 10 + i);
                req->AddWrite(key, value, 0, WriteOp::Upsert);
            }
            write_reqs.push_back(std::move(req));
        }

        // Submit all writes async
        std::vector<KvRequest*> pending_requests;
        for (auto& req : write_reqs) {
            store_->ExecAsyn(req.get(), 0, [](KvRequest* r) {
                REQUIRE(r->Error() == KvError::NoError);
            });
            pending_requests.push_back(req.get());
        }

        // Wait for all writes
        WaitForRequests(pending_requests);
        pending_requests.clear();

        // Now issue reads for verification
        for (int batch = 0; batch < 5; ++batch) {
            for (int i = 0; i < 10; ++i) {
                std::string key = "batch" + std::to_string(batch) + "_key" + std::to_string(i);
                auto req = std::make_unique<ReadRequest>();
                req->SetArgs(table, key);
                read_reqs.push_back(std::move(req));
            }
        }

        // Submit all reads async
        for (auto& req : read_reqs) {
            store_->ExecAsyn(req.get(), 0, [](KvRequest* r) {
                auto* read_req = static_cast<ReadRequest*>(r);
                REQUIRE(read_req->Error() == KvError::NoError);
                REQUIRE(!read_req->value_.empty());
            });
            pending_requests.push_back(req.get());
        }

        // Wait for all reads
        WaitForRequests(pending_requests);
    }
}

TEST_CASE_METHOD(AsyncOpsTestFixture, "AsyncOps_ScanOperations", "[async][integration]") {
    auto table = CreateTestTable("scan_async");

    SECTION("Async scan after writes") {
        // Write some data first
        for (int i = 0; i < 100; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            WriteSync(table, key, "value" + std::to_string(i));
        }

        // Async scan
        std::atomic<bool> scan_done{false};
        std::vector<KvEntry> scan_results;

        ScanAsync(table, "00000010", "00000050",
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
        REQUIRE(scan_results.size() == 40); // Keys 10-49 inclusive
        for (size_t i = 0; i < scan_results.size(); ++i) {
            int key_num = 10 + i;
            std::string expected_key = std::string(10 - std::to_string(key_num).length(), '0') +
                                       std::to_string(key_num);
            REQUIRE(scan_results[i].key_ == expected_key);
            REQUIRE(scan_results[i].value_ == "value" + std::to_string(key_num));
        }
    }
}

TEST_CASE("AsyncOps_MixedSyncAsync", "[async][integration]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("mixed_ops");

    SECTION("Interleaved sync and async operations") {
        // Sync write
        REQUIRE(fixture.WriteSync(table, "sync1", "value1") == KvError::NoError);

        // Async write
        std::atomic<bool> async_done{false};
        fixture.WriteAsync(table, "async1", "value2", [&async_done](KvError err) {
            REQUIRE(err == KvError::NoError);
            async_done = true;
        });

        // Another sync write while async is potentially in flight
        REQUIRE(fixture.WriteSync(table, "sync2", "value3") == KvError::NoError);

        // Wait for async to complete
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