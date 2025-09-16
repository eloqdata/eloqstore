#include <catch2/catch_test_macros.hpp>
#include <atomic>
#include <thread>
#include <chrono>
#include "../fixtures/test_fixtures.h"
#include "../fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("AsyncPattern_IssueAndWait", "[async][pattern]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("async_test");

    SECTION("Issue multiple async operations then wait") {
        const int num_writes = 50;
        std::vector<BatchWriteRequest> write_reqs(num_writes);

        // Issue all async writes first
        for (int i = 0; i < num_writes; ++i) {
            auto& req = write_reqs[i];
            req.SetTableId(table);

            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            req.AddWrite(key, value, 0, WriteOp::Upsert);

            // Use direct batch write for now (fixture doesn't expose ExecAsyn directly)
            fixture.WriteAsync(key, value, [](KvError err) {
                REQUIRE(err == KvError::NoError);
            });
        }

        // Now wait for all to complete
        for (auto& req : write_reqs) {
            while (!req.IsDone()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }

        // Verify all data with sync reads
        for (int i = 0; i < num_writes; ++i) {
            ReadRequest read_req;
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            read_req.SetArgs(table, key);

            std::string read_value;
            KvError err = fixture.ReadSync(table, key, read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value == "value_" + std::to_string(i));
        }
    }

    SECTION("Mixed async reads and writes") {
        // First write some data synchronously
        for (int i = 0; i < 20; ++i) {
            BatchWriteRequest req;
            req.SetTableId(table);
            std::string key = "key_" + std::to_string(i);
            std::string value = "initial_" + std::to_string(i);
            req.AddWrite(key, value, 0, WriteOp::Upsert);
            store->ExecSync(&req);
        }

        // Now issue mixed async operations
        std::vector<std::unique_ptr<KvRequest>> requests;

        for (int i = 0; i < 40; ++i) {
            if (i % 2 == 0) {
                // Async write
                auto req = std::make_unique<BatchWriteRequest>();
                req->SetTableId(table);
                std::string key = "key_" + std::to_string(i / 2);
                std::string value = "updated_" + std::to_string(i);
                req->AddWrite(key, value, 0, WriteOp::Upsert);
                requests.push_back(std::move(req));
            } else {
                // Async read
                auto req = std::make_unique<ReadRequest>();
                std::string key = "key_" + std::to_string(i / 2);
                req->SetArgs(table, key);
                requests.push_back(std::move(req));
            }
        }

        // Issue all operations asynchronously
        std::atomic<int> completed{0};
        for (auto& req : requests) {
            bool ok = store->ExecAsyn(req.get(), 0, [&completed](KvRequest* r) {
                completed++;
            });
            REQUIRE(ok);
        }

        // Wait for all to complete
        while (completed < static_cast<int>(requests.size())) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Verify no critical errors
        for (auto& req : requests) {
            if (req->Type() == RequestType::Read) {
                auto* read_req = static_cast<ReadRequest*>(req.get());
                REQUIRE(read_req->Error() == KvError::NoError);
            } else {
                REQUIRE(req->Error() == KvError::NoError);
            }
        }
    }
}

TEST_CASE("AsyncPattern_ConcurrentThreads", "[async][concurrent]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("async_test");

    SECTION("Multiple threads issuing async operations") {
        const int num_threads = 4;
        const int ops_per_thread = 25;
        std::atomic<int> total_completed{0};
        std::atomic<int> total_issued{0};

        std::vector<std::thread> threads;

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                std::vector<BatchWriteRequest> reqs(ops_per_thread);

                // Each thread issues its async operations
                for (int i = 0; i < ops_per_thread; ++i) {
                    auto& req = reqs[i];
                    req.SetTableId(table);

                    int key_num = thread_id * ops_per_thread + i;
                    std::string key = "tkey_" + std::to_string(key_num);
                    std::string value = "thread_" + std::to_string(thread_id) + "_val_" + std::to_string(i);
                    req.AddWrite(key, value, 0, WriteOp::Upsert);

                    bool ok = store->ExecAsyn(&req, 0, [&total_completed](KvRequest* r) {
                        REQUIRE(r->Error() == KvError::NoError);
                        total_completed++;
                    });
                    REQUIRE(ok);
                    total_issued++;
                }

                // Each thread waits for its own operations
                for (auto& req : reqs) {
                    while (!req.IsDone()) {
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                }
            });
        }

        // Wait for all threads
        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(total_issued == num_threads * ops_per_thread);
        REQUIRE(total_completed == num_threads * ops_per_thread);

        // Verify data integrity with a scan
        ScanRequest scan_req;
        scan_req.SetArgs(table, "tkey_", "tkey_~");
        store->ExecSync(&scan_req);

        REQUIRE(scan_req.Error() == KvError::NoError);
        REQUIRE(scan_req.Entries().size() == num_threads * ops_per_thread);
    }
}

TEST_CASE("AsyncPattern_AsyncScans", "[async][scan]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("async_test");

    // Prepare test data
    for (int i = 0; i < 200; ++i) {
        BatchWriteRequest req;
        req.SetTableId(table);
        std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
        std::string value = "scan_value_" + std::to_string(i);
        req.AddWrite(key, value, 0, WriteOp::Upsert);
        store->ExecSync(&req);
    }

    SECTION("Multiple async scans with overlapping ranges") {
        const int num_scans = 10;
        std::vector<ScanRequest> scan_reqs(num_scans);
        std::atomic<int> completed{0};

        // Issue all scans asynchronously
        for (int i = 0; i < num_scans; ++i) {
            auto& req = scan_reqs[i];

            int start = i * 15;  // Overlapping ranges
            int end = std::min(start + 30, 200);

            std::string start_key = std::string(10 - std::to_string(start).length(), '0') + std::to_string(start);
            std::string end_key = std::string(10 - std::to_string(end).length(), '0') + std::to_string(end);

            req.SetArgs(table, start_key, end_key);

            bool ok = store->ExecAsyn(&req, 0, [&completed, expected_size = end - start](KvRequest* r) {
                auto* scan = static_cast<ScanRequest*>(r);
                REQUIRE(scan->Error() == KvError::NoError);
                REQUIRE(scan->Entries().size() == expected_size);
                completed++;
            });
            REQUIRE(ok);
        }

        // Wait for all scans to complete
        while (completed < num_scans) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        REQUIRE(completed == num_scans);
    }
}