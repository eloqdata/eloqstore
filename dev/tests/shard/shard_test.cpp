#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>

#include "shard.h"
#include "kv_request.h"
#include "read_request.h"
#include "write_request.h"
#include "task_manager.h"
#include "async_io_manager.h"
#include "index_page_manager.h"
#include "pages_pool.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class ShardTestFixture : public TestFixture {
public:
    ShardTestFixture() {
        InitStoreWithDefaults();
    }

    std::unique_ptr<Shard> CreateShard(size_t shard_id = 0, uint32_t fd_limit = 100) {
        return std::make_unique<Shard>(store_.get(), shard_id, fd_limit);
    }

    std::unique_ptr<ReadRequest> CreateReadRequest(const TableIdent& table,
                                                  const std::string& key) {
        auto req = std::make_unique<ReadRequest>();
        req->table = table;
        req->key = key;
        return req;
    }

    std::unique_ptr<WriteRequest> CreateWriteRequest(const TableIdent& table,
                                                    const std::vector<WriteOp>& ops) {
        auto req = std::make_unique<WriteRequest>();
        req->table = table;
        req->ops = ops;
        return req;
    }

protected:
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(ShardTestFixture, "Shard_BasicOperations", "[shard][unit]") {
    auto shard = CreateShard();

    SECTION("Initialize shard") {
        KvError err = shard->Init();
        REQUIRE(err == KvError::NoError);

        // Should have initialized components
        REQUIRE(shard->IoManager() != nullptr);
        REQUIRE(shard->IndexManager() != nullptr);
        REQUIRE(shard->TaskMgr() != nullptr);
        REQUIRE(shard->PagePool() != nullptr);
    }

    SECTION("Start and stop shard") {
        shard->Init();

        shard->Start();
        // Shard should be running

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        shard->Stop();
        // Shard should stop gracefully
    }

    SECTION("Get shard options") {
        shard->Init();

        const KvOptions* opts = shard->Options();
        REQUIRE(opts != nullptr);
        REQUIRE(opts->page_size > 0);
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_RequestHandling", "[shard][unit]") {
    auto shard = CreateShard();
    shard->Init();
    shard->Start();

    TableIdent table("test_table", 1);

    SECTION("Add read request") {
        auto request = CreateReadRequest(table, "test_key");

        bool added = shard->AddKvRequest(request.get());
        REQUIRE(added == true);

        // Wait for processing
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    SECTION("Add write request") {
        std::vector<WriteOp> ops;
        WriteOp op;
        op.key = "test_key";
        op.value = "test_value";
        op.timestamp = 1000;
        ops.push_back(op);

        auto request = CreateWriteRequest(table, ops);

        bool added = shard->AddKvRequest(request.get());
        REQUIRE(added == true);
    }

    SECTION("Add multiple requests") {
        std::vector<std::unique_ptr<KvRequest>> requests;

        for (int i = 0; i < 10; ++i) {
            auto req = CreateReadRequest(table, "key_" + std::to_string(i));
            bool added = shard->AddKvRequest(req.get());
            REQUIRE(added == true);
            requests.push_back(std::move(req));
        }

        // All requests should be queued
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    shard->Stop();
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_CompactionManagement", "[shard][unit]") {
    auto shard = CreateShard();
    shard->Init();

    TableIdent table("compact_table", 1);

    SECTION("Add pending compaction") {
        REQUIRE(shard->HasPendingCompact(table) == false);

        shard->AddPendingCompact(table);
        REQUIRE(shard->HasPendingCompact(table) == true);
    }

    SECTION("Multiple compaction requests") {
        TableIdent table1("table1", 1);
        TableIdent table2("table2", 2);

        shard->AddPendingCompact(table1);
        shard->AddPendingCompact(table2);

        REQUIRE(shard->HasPendingCompact(table1) == true);
        REQUIRE(shard->HasPendingCompact(table2) == true);
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_TTLManagement", "[shard][unit]") {
    auto shard = CreateShard();
    shard->Init();

    TableIdent table("ttl_table", 1);

    SECTION("Add pending TTL cleanup") {
        REQUIRE(shard->HasPendingTTL(table) == false);

        shard->AddPendingTTL(table);
        REQUIRE(shard->HasPendingTTL(table) == true);
    }

    SECTION("TTL for multiple tables") {
        std::vector<TableIdent> tables;
        for (int i = 0; i < 5; ++i) {
            tables.emplace_back("ttl_table_" + std::to_string(i), i);
        }

        for (const auto& tbl : tables) {
            shard->AddPendingTTL(tbl);
        }

        for (const auto& tbl : tables) {
            REQUIRE(shard->HasPendingTTL(tbl) == true);
        }
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_ComponentAccess", "[shard][unit]") {
    auto shard = CreateShard();
    shard->Init();

    SECTION("IO Manager access") {
        AsyncIoManager* io_mgr = shard->IoManager();
        REQUIRE(io_mgr != nullptr);

        // Should be able to use IO manager
        REQUIRE(io_mgr->IsIdle() == true);
    }

    SECTION("Index Manager access") {
        IndexPageManager* idx_mgr = shard->IndexManager();
        REQUIRE(idx_mgr != nullptr);

        // Should have proper cache size
        TableIdent table("test", 1);
        auto [meta, err] = idx_mgr->FindRoot(table);
        // May not exist yet
    }

    SECTION("Task Manager access") {
        TaskManager* task_mgr = shard->TaskMgr();
        REQUIRE(task_mgr != nullptr);

        // Should track active tasks
        REQUIRE(task_mgr->NumActive() == 0);
    }

    SECTION("Page Pool access") {
        PagesPool* page_pool = shard->PagePool();
        REQUIRE(page_pool != nullptr);

        // Should be able to allocate pages
        Page page = page_pool->Get();
        REQUIRE(page.Data() != nullptr);
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_MultipleShards", "[shard][unit]") {
    const size_t num_shards = 4;
    std::vector<std::unique_ptr<Shard>> shards;

    SECTION("Create multiple shards") {
        for (size_t i = 0; i < num_shards; ++i) {
            auto shard = CreateShard(i);
            KvError err = shard->Init();
            REQUIRE(err == KvError::NoError);
            shards.push_back(std::move(shard));
        }

        REQUIRE(shards.size() == num_shards);

        // Each shard should have unique ID
        for (size_t i = 0; i < num_shards; ++i) {
            REQUIRE(shards[i]->shard_id_ == i);
        }
    }

    SECTION("Start all shards") {
        for (size_t i = 0; i < num_shards; ++i) {
            auto shard = CreateShard(i);
            shard->Init();
            shard->Start();
            shards.push_back(std::move(shard));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Stop all shards
        for (auto& shard : shards) {
            shard->Stop();
        }
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_RequestQueueing", "[shard][unit]") {
    auto shard = CreateShard();
    shard->Init();
    shard->Start();

    TableIdent table("queue_test", 1);

    SECTION("Queue saturation") {
        std::vector<std::unique_ptr<ReadRequest>> requests;

        // Add many requests quickly
        for (int i = 0; i < 1000; ++i) {
            auto req = CreateReadRequest(table, "key_" + std::to_string(i));
            bool added = shard->AddKvRequest(req.get());

            if (added) {
                requests.push_back(std::move(req));
            }
        }

        // Should handle queue saturation
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    SECTION("Mixed request types") {
        std::vector<std::unique_ptr<KvRequest>> requests;

        for (int i = 0; i < 100; ++i) {
            if (i % 2 == 0) {
                // Read request
                auto req = CreateReadRequest(table, "read_" + std::to_string(i));
                shard->AddKvRequest(req.get());
                requests.push_back(std::move(req));
            } else {
                // Write request
                std::vector<WriteOp> ops;
                WriteOp op;
                op.key = "write_" + std::to_string(i);
                op.value = gen_.GenerateValue(100);
                ops.push_back(op);

                auto req = CreateWriteRequest(table, ops);
                shard->AddKvRequest(req.get());
                requests.push_back(std::move(req));
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    shard->Stop();
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_Concurrency", "[shard][stress]") {
    auto shard = CreateShard();
    shard->Init();
    shard->Start();

    const int thread_count = 8;
    const int requests_per_thread = 100;
    std::atomic<int> successful_adds{0};

    SECTION("Concurrent request submission") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&shard, &successful_adds, t, requests_per_thread, this]() {
                TableIdent table("thread_" + std::to_string(t), t);

                for (int i = 0; i < requests_per_thread; ++i) {
                    auto req = CreateReadRequest(table, "key_" + std::to_string(i));

                    if (shard->AddKvRequest(req.get())) {
                        successful_adds++;
                    }

                    // Keep request alive
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(successful_adds > 0);
        LOG(INFO) << "Concurrent adds: " << successful_adds << " successful";
    }

    SECTION("Concurrent compaction triggers") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&shard, t]() {
                TableIdent table("compact_" + std::to_string(t), t);

                for (int i = 0; i < 10; ++i) {
                    shard->AddPendingCompact(table);
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));

                    bool has_pending = shard->HasPendingCompact(table);
                    (void)has_pending;  // Use to avoid warning
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }
    }

    shard->Stop();
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_ErrorHandling", "[shard][unit]") {
    SECTION("Init without store") {
        Shard shard(nullptr, 0, 100);

        // Should handle null store
        KvError err = shard.Init();
        REQUIRE(err != KvError::NoError);
    }

    SECTION("Add request to stopped shard") {
        auto shard = CreateShard();
        shard->Init();
        // Don't start the shard

        TableIdent table("test", 1);
        auto request = CreateReadRequest(table, "key");

        bool added = shard->AddKvRequest(request.get());
        // May or may not accept depending on implementation
    }

    SECTION("Invalid FD limit") {
        auto shard = CreateShard(0, 0);  // Zero FD limit

        KvError err = shard->Init();
        // Should handle invalid FD limit
    }
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_Performance", "[shard][benchmark]") {
    auto shard = CreateShard();
    shard->Init();
    shard->Start();

    TableIdent table("perf_test", 1);

    SECTION("Request throughput") {
        Timer timer;
        const int num_requests = 10000;
        std::vector<std::unique_ptr<ReadRequest>> requests;

        timer.Start();
        for (int i = 0; i < num_requests; ++i) {
            auto req = CreateReadRequest(table, "key_" + std::to_string(i));
            shard->AddKvRequest(req.get());
            requests.push_back(std::move(req));
        }
        timer.Stop();

        double requests_per_sec = num_requests / timer.ElapsedSeconds();
        LOG(INFO) << "Request submission: " << requests_per_sec << " req/sec";

        REQUIRE(requests_per_sec > 10000);  // Should handle >10K req/sec
    }

    SECTION("Task scheduling overhead") {
        Timer timer;
        const int num_cycles = 1000;

        timer.Start();
        for (int i = 0; i < num_cycles; ++i) {
            // Simulate task scheduling
            auto req = CreateReadRequest(table, "key");
            shard->AddKvRequest(req.get());
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
        timer.Stop();

        double cycles_per_sec = num_cycles / timer.ElapsedSeconds();
        LOG(INFO) << "Scheduling cycles: " << cycles_per_sec << " cycles/sec";
    }

    shard->Stop();
}

TEST_CASE_METHOD(ShardTestFixture, "Shard_EdgeCases", "[shard][edge-case]") {
    SECTION("Maximum shard ID") {
        auto shard = CreateShard(SIZE_MAX);
        KvError err = shard->Init();

        // Should handle maximum shard ID
        REQUIRE(shard->shard_id_ == SIZE_MAX);
    }

    SECTION("Rapid start-stop cycles") {
        auto shard = CreateShard();
        shard->Init();

        for (int i = 0; i < 10; ++i) {
            shard->Start();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            shard->Stop();
        }

        // Should handle rapid cycling
    }

    SECTION("Empty table identifier") {
        auto shard = CreateShard();
        shard->Init();
        shard->Start();

        TableIdent empty_table("", 0);
        auto request = CreateReadRequest(empty_table, "key");

        bool added = shard->AddKvRequest(request.get());
        // Should handle empty table

        shard->Stop();
    }

    SECTION("Null request") {
        auto shard = CreateShard();
        shard->Init();
        shard->Start();

        bool added = shard->AddKvRequest(nullptr);
        REQUIRE(added == false);

        shard->Stop();
    }
}