#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <atomic>
#include <random>
#include <chrono>
#include <vector>
#include <barrier>

#include "eloq_store.h"
#include "batch_write_task.h"
#include "read_task.h"
#include "scan_task.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class ConcurrentStressFixture : public MultiShardFixture {
public:
    ConcurrentStressFixture() : MultiShardFixture(8) {  // 8 shards for stress
        InitStoreWithDefaults();
    }

    void RunConcurrentTest(int num_threads, int duration_seconds,
                          std::function<void(int thread_id, std::atomic<bool>& stop)> worker) {
        std::atomic<bool> stop{false};
        std::vector<std::thread> threads;

        // Start worker threads
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker, i, std::ref(stop));
        }

        // Run for specified duration
        std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
        stop = true;

        // Wait for all threads
        for (auto& t : threads) {
            t.join();
        }
    }

protected:
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_MassiveConcurrentWrites", "[stress][concurrent]") {
    std::atomic<int64_t> total_writes{0};
    std::atomic<int64_t> failed_writes{0};
    TableIdent table = CreateTestTable("stress_write");

    auto writer = [this, &table, &total_writes, &failed_writes](int thread_id, std::atomic<bool>& stop) {
        BatchWriteTask task;
        std::mt19937 rng(thread_id);
        DataGenerator local_gen(thread_id);

        while (!stop) {
            auto request = std::make_unique<BatchWriteRequest>();
            request->table = table;

            // Generate batch of writes
            int batch_size = 10 + (rng() % 90);  // 10-100 writes per batch
            for (int i = 0; i < batch_size; ++i) {
                WriteOp op;
                op.key = local_gen.GenerateRandomKey(10, 50);
                op.value = local_gen.GenerateValue(100, 10000);
                op.timestamp = CurrentTime();
                request->ops.push_back(op);
            }

            KvError err = task.Execute(request.get());
            if (err == KvError::NoError) {
                total_writes += batch_size;
            } else {
                failed_writes += batch_size;
            }
        }
    };

    SECTION("Sustained write load") {
        RunConcurrentTest(16, 10, writer);

        LOG(INFO) << "Total writes: " << total_writes
                  << ", Failed: " << failed_writes
                  << ", Rate: " << (total_writes / 10) << " writes/sec";

        REQUIRE(total_writes > 10000);  // Should handle >1000 writes/sec
        REQUIRE(failed_writes < total_writes * 0.01);  // Less than 1% failure
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_MixedReadWriteLoad", "[stress][concurrent]") {
    std::atomic<int64_t> reads{0}, writes{0}, scans{0};
    std::atomic<int64_t> read_hits{0}, write_success{0}, scan_success{0};
    TableIdent table = CreateTestTable("stress_mixed");

    // Pre-populate some data
    BatchWriteTask init_task;
    auto init_req = std::make_unique<BatchWriteRequest>();
    init_req->table = table;

    for (int i = 0; i < 1000; ++i) {
        WriteOp op;
        op.key = "base_key_" + std::to_string(i);
        op.value = gen_.GenerateValue(100);
        op.timestamp = 1000;
        init_req->ops.push_back(op);
    }
    init_task.Execute(init_req.get());

    auto mixed_worker = [this, &table, &reads, &writes, &scans,
                        &read_hits, &write_success, &scan_success](int thread_id, std::atomic<bool>& stop) {
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> op_dist(0, 99);
        std::uniform_int_distribution<int> key_dist(0, 1999);
        DataGenerator local_gen(thread_id);

        while (!stop) {
            int op_type = op_dist(rng);

            if (op_type < 50) {  // 50% reads
                ReadTask read_task;
                std::string key = "base_key_" + std::to_string(key_dist(rng));
                std::string value;
                uint64_t timestamp, expire_ts;

                reads++;
                if (read_task.Read(table, key, value, timestamp, expire_ts) == KvError::NoError) {
                    read_hits++;
                }

            } else if (op_type < 80) {  // 30% writes
                BatchWriteTask write_task;
                auto request = std::make_unique<BatchWriteRequest>();
                request->table = table;

                WriteOp op;
                op.key = "dynamic_" + std::to_string(key_dist(rng));
                op.value = local_gen.GenerateValue(100);
                op.timestamp = CurrentTime();
                request->ops.push_back(op);

                writes++;
                if (write_task.Execute(request.get()) == KvError::NoError) {
                    write_success++;
                }

            } else {  // 20% scans
                ScanTask scan_task;
                std::vector<std::pair<std::string, std::string>> results;

                int start = key_dist(rng);
                std::string start_key = "base_key_" + std::to_string(start);
                std::string end_key = "base_key_" + std::to_string(start + 10);

                scans++;
                if (scan_task.Scan(table, start_key, end_key, 20, false, results) == KvError::NoError) {
                    scan_success++;
                }
            }

            // Small delay to prevent CPU saturation
            if (op_type % 10 == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }
    };

    SECTION("Mixed workload stress") {
        RunConcurrentTest(12, 15, mixed_worker);

        LOG(INFO) << "Operations - Reads: " << reads << " (hits: " << read_hits << ")"
                  << ", Writes: " << writes << " (success: " << write_success << ")"
                  << ", Scans: " << scans << " (success: " << scan_success << ")";

        double read_hit_rate = static_cast<double>(read_hits) / reads;
        double write_success_rate = static_cast<double>(write_success) / writes;
        double scan_success_rate = static_cast<double>(scan_success) / scans;

        REQUIRE(read_hit_rate > 0.1);  // Some reads should hit
        REQUIRE(write_success_rate > 0.95);  // Most writes should succeed
        REQUIRE(scan_success_rate > 0.90);  // Most scans should succeed
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_HotKeyContention", "[stress][concurrent]") {
    std::atomic<int64_t> updates{0};
    std::atomic<int64_t> conflicts{0};
    TableIdent table = CreateTestTable("stress_hotkey");

    // Define hot keys
    std::vector<std::string> hot_keys;
    for (int i = 0; i < 10; ++i) {
        hot_keys.push_back("hot_key_" + std::to_string(i));
    }

    auto hot_key_worker = [this, &table, &hot_keys, &updates, &conflicts](int thread_id, std::atomic<bool>& stop) {
        BatchWriteTask task;
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> key_dist(0, hot_keys.size() - 1);

        while (!stop) {
            auto request = std::make_unique<BatchWriteRequest>();
            request->table = table;

            // Update a hot key
            WriteOp op;
            op.key = hot_keys[key_dist(rng)];
            op.value = "thread_" + std::to_string(thread_id) + "_ts_" + std::to_string(CurrentTime());
            op.timestamp = CurrentTime();
            request->ops.push_back(op);

            KvError err = task.Execute(request.get());
            if (err == KvError::NoError) {
                updates++;
            } else {
                conflicts++;
            }

            // Brief pause to allow other threads
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    };

    SECTION("Hot key updates") {
        RunConcurrentTest(20, 10, hot_key_worker);

        LOG(INFO) << "Hot key updates: " << updates
                  << ", Conflicts: " << conflicts
                  << ", Conflict rate: " << (100.0 * conflicts / (updates + conflicts)) << "%";

        REQUIRE(updates > 1000);  // Should handle many updates
        // Some conflicts expected with high contention
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_LargeValueHandling", "[stress][concurrent]") {
    std::atomic<int64_t> large_writes{0};
    std::atomic<int64_t> large_reads{0};
    TableIdent table = CreateTestTable("stress_large");

    auto large_value_worker = [this, &table, &large_writes, &large_reads](int thread_id, std::atomic<bool>& stop) {
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> size_dist(10000, 1000000);  // 10KB to 1MB
        std::uniform_int_distribution<int> op_dist(0, 1);
        DataGenerator local_gen(thread_id);

        int key_counter = 0;

        while (!stop) {
            if (op_dist(rng) == 0) {
                // Write large value
                BatchWriteTask write_task;
                auto request = std::make_unique<BatchWriteRequest>();
                request->table = table;

                WriteOp op;
                op.key = "large_" + std::to_string(thread_id) + "_" + std::to_string(key_counter++);
                op.value = local_gen.GenerateValue(size_dist(rng));
                op.timestamp = CurrentTime();
                request->ops.push_back(op);

                if (write_task.Execute(request.get()) == KvError::NoError) {
                    large_writes++;
                }
            } else {
                // Read large value
                ReadTask read_task;
                std::string key = "large_" + std::to_string(thread_id) + "_" + std::to_string(rng() % std::max(1, key_counter));
                std::string value;
                uint64_t timestamp, expire_ts;

                if (read_task.Read(table, key, value, timestamp, expire_ts) == KvError::NoError) {
                    large_reads++;
                }
            }

            // Pace the operations
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    };

    SECTION("Large value stress") {
        RunConcurrentTest(8, 10, large_value_worker);

        LOG(INFO) << "Large value writes: " << large_writes
                  << ", Large value reads: " << large_reads;

        REQUIRE(large_writes > 100);  // Should handle large values
        REQUIRE(large_reads > 50);
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_RapidTableCreation", "[stress][concurrent]") {
    std::atomic<int> tables_created{0};
    std::atomic<int> operations_performed{0};

    auto table_creator = [this, &tables_created, &operations_performed](int thread_id, std::atomic<bool>& stop) {
        int table_counter = 0;

        while (!stop) {
            // Create new table
            TableIdent table("stress_table_" + std::to_string(thread_id) + "_" + std::to_string(table_counter),
                           thread_id * 1000 + table_counter);
            table_counter++;
            tables_created++;

            // Perform operations on the table
            BatchWriteTask write_task;
            auto request = std::make_unique<BatchWriteRequest>();
            request->table = table;

            for (int i = 0; i < 10; ++i) {
                WriteOp op;
                op.key = "key_" + std::to_string(i);
                op.value = "value_" + std::to_string(i);
                op.timestamp = CurrentTime();
                request->ops.push_back(op);
            }

            if (write_task.Execute(request.get()) == KvError::NoError) {
                operations_performed += 10;
            }

            // Brief pause
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };

    SECTION("Rapid table operations") {
        RunConcurrentTest(4, 5, table_creator);

        LOG(INFO) << "Tables created: " << tables_created
                  << ", Operations performed: " << operations_performed;

        REQUIRE(tables_created > 50);
        REQUIRE(operations_performed > 500);
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_MemoryPressure", "[stress][memory]") {
    std::atomic<int64_t> allocations{0};
    std::atomic<size_t> total_memory{0};
    TableIdent table = CreateTestTable("stress_memory");

    auto memory_stress_worker = [this, &table, &allocations, &total_memory](int thread_id, std::atomic<bool>& stop) {
        DataGenerator local_gen(thread_id);
        std::vector<std::string> cached_values;  // Hold values in memory

        while (!stop) {
            BatchWriteTask task;
            auto request = std::make_unique<BatchWriteRequest>();
            request->table = table;

            // Create batch with varying sizes
            int batch_size = 50;
            for (int i = 0; i < batch_size; ++i) {
                WriteOp op;
                op.key = local_gen.GenerateRandomKey(20, 100);
                std::string value = local_gen.GenerateValue(1000, 50000);

                // Cache some values to increase memory pressure
                if (cached_values.size() < 1000) {
                    cached_values.push_back(value);
                    total_memory += value.size();
                }

                op.value = value;
                op.timestamp = CurrentTime();
                request->ops.push_back(op);
            }

            task.Execute(request.get());
            allocations += batch_size;

            // Occasionally clear cache
            if (cached_values.size() > 900) {
                total_memory -= std::accumulate(cached_values.begin(), cached_values.end(), size_t(0),
                                              [](size_t sum, const std::string& s) { return sum + s.size(); });
                cached_values.clear();
            }
        }
    };

    SECTION("Memory intensive operations") {
        RunConcurrentTest(6, 8, memory_stress_worker);

        LOG(INFO) << "Total allocations: " << allocations
                  << ", Peak memory estimate: " << (total_memory / 1024 / 1024) << " MB";

        REQUIRE(allocations > 1000);
    }
}

TEST_CASE_METHOD(ConcurrentStressFixture, "Stress_RapidCompaction", "[stress][concurrent]") {
    std::atomic<int> compactions_triggered{0};
    std::atomic<int64_t> writes_between_compactions{0};
    TableIdent table = CreateTestTable("stress_compact");

    auto compaction_worker = [this, &table, &compactions_triggered, &writes_between_compactions](int thread_id, std::atomic<bool>& stop) {
        BatchWriteTask task;
        DataGenerator local_gen(thread_id);
        int write_count = 0;

        while (!stop) {
            // Write data
            auto request = std::make_unique<BatchWriteRequest>();
            request->table = table;

            for (int i = 0; i < 100; ++i) {
                WriteOp op;
                op.key = local_gen.GenerateSequentialKey(write_count++);
                op.value = local_gen.GenerateValue(1000);
                op.timestamp = CurrentTime();
                request->ops.push_back(op);
            }

            if (task.Execute(request.get()) == KvError::NoError) {
                writes_between_compactions += 100;
            }

            // Periodically trigger compaction
            if (write_count % 500 == 0) {
                for (auto& shard : GetShards()) {
                    shard->AddPendingCompact(table);
                }
                compactions_triggered++;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    };

    SECTION("Compaction under load") {
        RunConcurrentTest(4, 10, compaction_worker);

        LOG(INFO) << "Compactions triggered: " << compactions_triggered
                  << ", Writes: " << writes_between_compactions;

        REQUIRE(compactions_triggered > 5);
        REQUIRE(writes_between_compactions > 10000);
    }
}

TEST_CASE("Stress_MaximumLoad", "[stress][maximum]") {
    ConcurrentStressFixture fixture;
    const int num_threads = std::thread::hardware_concurrency() * 2;
    std::atomic<int64_t> total_operations{0};
    std::atomic<bool> stop{false};

    std::vector<std::thread> threads;

    // Launch maximum concurrent operations
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&fixture, &total_operations, &stop, i]() {
            TableIdent table = fixture.CreateTestTable("max_load_" + std::to_string(i));
            BatchWriteTask write_task;
            ReadTask read_task;
            DataGenerator gen(i);

            while (!stop) {
                // Rapid fire operations
                for (int j = 0; j < 10; ++j) {
                    auto request = std::make_unique<BatchWriteRequest>();
                    request->table = table;

                    WriteOp op;
                    op.key = gen.GenerateRandomKey(10, 30);
                    op.value = gen.GenerateValue(100);
                    op.timestamp = CurrentTime();
                    request->ops.push_back(op);

                    write_task.Execute(request.get());
                    total_operations++;
                }

                // No delay - maximum throughput
            }
        });
    }

    // Run for 5 seconds
    std::this_thread::sleep_for(std::chrono::seconds(5));
    stop = true;

    for (auto& t : threads) {
        t.join();
    }

    double ops_per_sec = total_operations / 5.0;
    LOG(INFO) << "Maximum load test: " << total_operations << " operations"
              << ", Rate: " << ops_per_sec << " ops/sec"
              << ", Threads: " << num_threads;

    REQUIRE(total_operations > 10000);
}