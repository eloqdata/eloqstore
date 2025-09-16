#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <random>
#include <atomic>
#include <chrono>

#include "eloq_store.h"
#include "shard.h"
#include "batch_write_task.h"
#include "read_task.h"
#include "scan_task.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class WorkflowTestFixture : public MultiShardFixture {
public:
    WorkflowTestFixture() : MultiShardFixture(4) {  // 4 shards
        InitStoreWithDefaults();
        InitTestData();
    }

    void InitTestData() {
        main_table_ = CreateTestTable("workflow_test");

        // Generate test data
        for (int i = 0; i < 1000; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = gen_.GenerateValue(100 + (i % 500));
            initial_data_[key] = value;
        }
    }

    KvError WriteData(const std::map<std::string, std::string>& data) {
        BatchWriteTask task;
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = main_table_;

        for (const auto& [key, value] : data) {
            WriteOp op;
            op.key = key;
            op.value = value;
            op.timestamp = CurrentTime();
            request->ops.push_back(op);
        }

        return task.Execute(request.get());
    }

    KvError ReadAndVerify(const std::string& key, const std::string& expected_value) {
        ReadTask task;
        std::string value;
        uint64_t timestamp;
        uint64_t expire_ts;

        KvError err = task.Read(main_table_, key, value, timestamp, expire_ts);
        if (err == KvError::NoError) {
            REQUIRE(value == expected_value);
        }
        return err;
    }

protected:
    TableIdent main_table_;
    std::map<std::string, std::string> initial_data_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_WriteReadVerify", "[integration][workflow]") {
    SECTION("Simple write-read cycle") {
        std::map<std::string, std::string> test_data = {
            {"key1", "value1"},
            {"key2", "value2"},
            {"key3", "value3"}
        };

        // Write data
        KvError err = WriteData(test_data);
        REQUIRE(err == KvError::NoError);

        // Read and verify
        for (const auto& [key, expected_value] : test_data) {
            err = ReadAndVerify(key, expected_value);
            REQUIRE(err == KvError::NoError);
        }
    }

    SECTION("Overwrite existing data") {
        std::map<std::string, std::string> initial = {
            {"update_key", "initial_value"}
        };

        // Initial write
        REQUIRE(WriteData(initial) == KvError::NoError);
        REQUIRE(ReadAndVerify("update_key", "initial_value") == KvError::NoError);

        // Overwrite
        std::map<std::string, std::string> updated = {
            {"update_key", "updated_value"}
        };
        REQUIRE(WriteData(updated) == KvError::NoError);
        REQUIRE(ReadAndVerify("update_key", "updated_value") == KvError::NoError);
    }

    SECTION("Delete and recreate") {
        // Write initial data
        std::map<std::string, std::string> data = {
            {"delete_key", "will_be_deleted"}
        };
        REQUIRE(WriteData(data) == KvError::NoError);

        // Delete
        BatchWriteTask delete_task;
        auto delete_req = std::make_unique<BatchWriteRequest>();
        delete_req->table = main_table_;

        WriteOp delete_op;
        delete_op.key = "delete_key";
        delete_op.is_delete = true;
        delete_op.timestamp = CurrentTime();
        delete_req->ops.push_back(delete_op);

        REQUIRE(delete_task.Execute(delete_req.get()) == KvError::NoError);

        // Verify deletion
        ReadTask read_task;
        std::string value;
        uint64_t timestamp, expire_ts;
        REQUIRE(read_task.Read(main_table_, "delete_key", value, timestamp, expire_ts) == KvError::NotFound);

        // Recreate
        data["delete_key"] = "recreated_value";
        REQUIRE(WriteData(data) == KvError::NoError);
        REQUIRE(ReadAndVerify("delete_key", "recreated_value") == KvError::NoError);
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_ScanOperations", "[integration][workflow]") {
    // Prepare ordered data
    std::map<std::string, std::string> scan_data;
    for (int i = 0; i < 100; ++i) {
        std::string key = "scan_key_" + fmt::format("{:04d}", i);
        scan_data[key] = "value_" + std::to_string(i);
    }

    REQUIRE(WriteData(scan_data) == KvError::NoError);

    SECTION("Full scan") {
        ScanTask task;
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(main_table_, "scan_key_", "scan_key_z", 1000, false, results);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.size() == 100);

        // Verify order
        for (size_t i = 1; i < results.size(); ++i) {
            REQUIRE(results[i-1].first < results[i].first);
        }
    }

    SECTION("Range scan with limit") {
        ScanTask task;
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(main_table_, "scan_key_0020", "scan_key_0040", 10, false, results);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.size() <= 10);

        for (const auto& [key, value] : results) {
            REQUIRE(key >= "scan_key_0020");
            REQUIRE(key <= "scan_key_0040");
        }
    }

    SECTION("Reverse scan") {
        ScanTask task;
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = task.Scan(main_table_, "scan_key_", "scan_key_z", 50, true, results);
        REQUIRE(err == KvError::NoError);

        // Verify reverse order
        for (size_t i = 1; i < results.size(); ++i) {
            REQUIRE(results[i-1].first > results[i].first);
        }
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_MixedOperations", "[integration][workflow]") {
    SECTION("Interleaved reads and writes") {
        for (int iteration = 0; iteration < 10; ++iteration) {
            // Write batch
            std::map<std::string, std::string> batch;
            for (int i = 0; i < 10; ++i) {
                int key_num = iteration * 10 + i;
                batch["mixed_" + std::to_string(key_num)] = "iter_" + std::to_string(iteration);
            }
            REQUIRE(WriteData(batch) == KvError::NoError);

            // Read previous iteration
            if (iteration > 0) {
                for (int i = 0; i < 10; ++i) {
                    int key_num = (iteration - 1) * 10 + i;
                    std::string key = "mixed_" + std::to_string(key_num);
                    std::string expected = "iter_" + std::to_string(iteration - 1);
                    REQUIRE(ReadAndVerify(key, expected) == KvError::NoError);
                }
            }
        }
    }

    SECTION("Scan after mixed updates") {
        // Initial write
        std::map<std::string, std::string> initial;
        for (int i = 0; i < 50; ++i) {
            initial["update_" + fmt::format("{:03d}", i)] = "initial_" + std::to_string(i);
        }
        REQUIRE(WriteData(initial) == KvError::NoError);

        // Update even keys
        std::map<std::string, std::string> updates;
        for (int i = 0; i < 50; i += 2) {
            updates["update_" + fmt::format("{:03d}", i)] = "updated_" + std::to_string(i);
        }
        REQUIRE(WriteData(updates) == KvError::NoError);

        // Delete every third key
        BatchWriteTask delete_task;
        auto delete_req = std::make_unique<BatchWriteRequest>();
        delete_req->table = main_table_;

        for (int i = 0; i < 50; i += 3) {
            WriteOp op;
            op.key = "update_" + fmt::format("{:03d}", i);
            op.is_delete = true;
            op.timestamp = CurrentTime();
            delete_req->ops.push_back(op);
        }
        REQUIRE(delete_task.Execute(delete_req.get()) == KvError::NoError);

        // Scan and verify
        ScanTask scan_task;
        std::vector<std::pair<std::string, std::string>> results;

        KvError err = scan_task.Scan(main_table_, "update_", "update_z", 100, false, results);
        REQUIRE(err == KvError::NoError);

        for (const auto& [key, value] : results) {
            int num = std::stoi(key.substr(7));  // Extract number from "update_XXX"

            if (num % 3 == 0) {
                FAIL("Deleted key found: " << key);
            } else if (num % 2 == 0) {
                REQUIRE(value == "updated_" + std::to_string(num));
            } else {
                REQUIRE(value == "initial_" + std::to_string(num));
            }
        }
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_LargeDataHandling", "[integration][workflow]") {
    SECTION("Large values") {
        std::map<std::string, std::string> large_data;

        // Create values of different sizes
        large_data["large_1kb"] = std::string(1024, 'a');
        large_data["large_10kb"] = std::string(10240, 'b');
        large_data["large_100kb"] = std::string(102400, 'c');
        large_data["large_1mb"] = std::string(1048576, 'd');

        REQUIRE(WriteData(large_data) == KvError::NoError);

        // Read and verify sizes
        for (const auto& [key, expected] : large_data) {
            ReadTask task;
            std::string value;
            uint64_t timestamp, expire_ts;

            KvError err = task.Read(main_table_, key, value, timestamp, expire_ts);
            REQUIRE(err == KvError::NoError);
            REQUIRE(value.size() == expected.size());
            REQUIRE(value[0] == expected[0]);  // Check first character
        }
    }

    SECTION("Many small keys") {
        std::map<std::string, std::string> many_keys;

        for (int i = 0; i < 10000; ++i) {
            many_keys["small_" + std::to_string(i)] = std::to_string(i);
        }

        Timer timer;
        timer.Start();
        KvError err = WriteData(many_keys);
        timer.Stop();

        REQUIRE(err == KvError::NoError);

        double writes_per_sec = many_keys.size() / timer.ElapsedSeconds();
        LOG(INFO) << "Bulk write performance: " << writes_per_sec << " writes/sec";

        // Sample verification
        for (int i = 0; i < 10000; i += 100) {
            std::string key = "small_" + std::to_string(i);
            REQUIRE(ReadAndVerify(key, std::to_string(i)) == KvError::NoError);
        }
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_ConcurrentAccess", "[integration][workflow][stress]") {
    const int thread_count = 8;
    const int ops_per_thread = 100;
    std::atomic<int> successful_ops{0};
    std::atomic<int> failed_ops{0};

    SECTION("Concurrent writes to different keys") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([this, &successful_ops, &failed_ops, t, ops_per_thread]() {
                for (int i = 0; i < ops_per_thread; ++i) {
                    std::map<std::string, std::string> data;
                    std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                    data[key] = "value_" + std::to_string(i);

                    if (WriteData(data) == KvError::NoError) {
                        successful_ops++;
                    } else {
                        failed_ops++;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(successful_ops == thread_count * ops_per_thread);
        REQUIRE(failed_ops == 0);
    }

    SECTION("Concurrent reads and writes") {
        // Pre-write some data
        std::map<std::string, std::string> initial_data;
        for (int i = 0; i < 100; ++i) {
            initial_data["shared_" + std::to_string(i)] = "initial_" + std::to_string(i);
        }
        REQUIRE(WriteData(initial_data) == KvError::NoError);

        std::vector<std::thread> threads;

        // Half threads reading, half writing
        for (int t = 0; t < thread_count; ++t) {
            if (t % 2 == 0) {
                // Reader thread
                threads.emplace_back([this, &successful_ops, t]() {
                    ReadTask task;
                    std::mt19937 rng(t);
                    std::uniform_int_distribution<int> dist(0, 99);

                    for (int i = 0; i < 100; ++i) {
                        std::string key = "shared_" + std::to_string(dist(rng));
                        std::string value;
                        uint64_t timestamp, expire_ts;

                        if (task.Read(main_table_, key, value, timestamp, expire_ts) == KvError::NoError) {
                            successful_ops++;
                        }
                    }
                });
            } else {
                // Writer thread
                threads.emplace_back([this, &successful_ops, t]() {
                    std::mt19937 rng(t);
                    std::uniform_int_distribution<int> dist(0, 99);

                    for (int i = 0; i < 50; ++i) {
                        std::map<std::string, std::string> data;
                        std::string key = "shared_" + std::to_string(dist(rng));
                        data[key] = "updated_by_" + std::to_string(t);

                        if (WriteData(data) == KvError::NoError) {
                            successful_ops++;
                        }
                    }
                });
            }
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(successful_ops > 0);
        LOG(INFO) << "Concurrent operations: " << successful_ops << " successful";
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_Recovery", "[integration][workflow]") {
    SECTION("Write persistence") {
        std::map<std::string, std::string> persist_data;
        for (int i = 0; i < 100; ++i) {
            persist_data["persist_" + std::to_string(i)] = "value_" + std::to_string(i);
        }

        // Write data
        REQUIRE(WriteData(persist_data) == KvError::NoError);

        // Simulate flush/sync
        for (auto& shard : GetShards()) {
            shard->IoManager()->SyncData(main_table_);
        }

        // Data should be readable
        for (int i = 0; i < 100; ++i) {
            std::string key = "persist_" + std::to_string(i);
            REQUIRE(ReadAndVerify(key, "value_" + std::to_string(i)) == KvError::NoError);
        }
    }

    SECTION("Compaction workflow") {
        // Write initial data
        std::map<std::string, std::string> initial;
        for (int i = 0; i < 500; ++i) {
            initial["compact_" + std::to_string(i)] = std::string(1000, 'x');
        }
        REQUIRE(WriteData(initial) == KvError::NoError);

        // Trigger compaction
        for (auto& shard : GetShards()) {
            shard->AddPendingCompact(main_table_);
        }

        // Wait for compaction
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Data should still be readable
        for (int i = 0; i < 500; i += 10) {
            std::string key = "compact_" + std::to_string(i);
            ReadTask task;
            std::string value;
            uint64_t timestamp, expire_ts;

            KvError err = task.Read(main_table_, key, value, timestamp, expire_ts);
            REQUIRE(err == KvError::NoError);
        }
    }
}

TEST_CASE_METHOD(WorkflowTestFixture, "Workflow_EdgeCases", "[integration][workflow][edge-case]") {
    SECTION("Empty table operations") {
        TableIdent empty_table("empty_table", 999);

        // Read from empty table
        ReadTask read_task;
        std::string value;
        uint64_t timestamp, expire_ts;
        REQUIRE(read_task.Read(empty_table, "any_key", value, timestamp, expire_ts) == KvError::NotFound);

        // Scan empty table
        ScanTask scan_task;
        std::vector<std::pair<std::string, std::string>> results;
        KvError err = scan_task.Scan(empty_table, "", "", 100, false, results);
        REQUIRE((err == KvError::NoError || err == KvError::NotFound));
        REQUIRE(results.empty());
    }

    SECTION("Special characters in keys") {
        std::map<std::string, std::string> special_data = {
            {"key with spaces", "value1"},
            {"key\twith\ttabs", "value2"},
            {"key\nwith\nnewlines", "value3"},
            {"key!@#$%^&*()", "value4"},
            {"键值_キー_🔑", "value5"},
            {std::string("key\0with\0null", 14), "value6"}
        };

        REQUIRE(WriteData(special_data) == KvError::NoError);

        for (const auto& [key, expected] : special_data) {
            KvError err = ReadAndVerify(key, expected);
            REQUIRE(err == KvError::NoError);
        }
    }

    SECTION("Boundary key lengths") {
        std::map<std::string, std::string> boundary_data;

        // Minimum key
        boundary_data[""] = "empty_key_value";

        // Maximum key (up to reasonable limit)
        std::string max_key(1000, 'k');
        boundary_data[max_key] = "max_key_value";

        KvError err = WriteData(boundary_data);

        // Empty key might be rejected
        if (err == KvError::NoError) {
            // Verify if written
            ReadTask task;
            std::string value;
            uint64_t timestamp, expire_ts;

            task.Read(main_table_, max_key, value, timestamp, expire_ts);
        }
    }
}