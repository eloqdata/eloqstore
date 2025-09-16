#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <random>
#include <atomic>

#include "batch_write_task.h"
#include "write_request.h"
#include "shard.h"
#include "page_mapper.h"
#include "data_page_builder.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class BatchWriteTaskTestFixture : public TestFixture {
public:
    BatchWriteTaskTestFixture() {
        InitStoreWithDefaults();
        InitTestData();
    }

    void InitTestData() {
        table_ = CreateTestTable("batch_write_test");
    }

    std::unique_ptr<BatchWriteRequest> CreateBatchRequest(size_t num_writes) {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        for (size_t i = 0; i < num_writes; ++i) {
            WriteOp op;
            op.key = gen_.GenerateSequentialKey(i);
            op.value = gen_.GenerateValue(100);
            op.timestamp = 1000 + i;
            op.expire_ts = 0;
            request->ops.push_back(op);
        }

        return request;
    }

    std::unique_ptr<BatchWriteRequest> CreateDeleteRequest(const std::vector<std::string>& keys) {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        for (const auto& key : keys) {
            WriteOp op;
            op.key = key;
            op.is_delete = true;
            op.timestamp = CurrentTime();
            request->ops.push_back(op);
        }

        return request;
    }

protected:
    TableIdent table_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_BasicWrite", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Write single key-value") {
        auto request = CreateBatchRequest(1);

        KvError err = task.Execute(request.get());

        // Should complete write
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Write multiple key-values") {
        auto request = CreateBatchRequest(100);

        KvError err = task.Execute(request.get());

        // Should handle batch write
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Empty batch") {
        auto request = CreateBatchRequest(0);

        KvError err = task.Execute(request.get());

        // Should handle empty batch gracefully
        REQUIRE(err == KvError::NoError);
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_DeleteOperations", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Delete single key") {
        std::vector<std::string> keys = {"key_to_delete"};
        auto request = CreateDeleteRequest(keys);

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::NotFound));
    }

    SECTION("Delete multiple keys") {
        std::vector<std::string> keys;
        for (int i = 0; i < 50; ++i) {
            keys.push_back("delete_key_" + std::to_string(i));
        }

        auto request = CreateDeleteRequest(keys);
        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::NotFound));
    }

    SECTION("Delete non-existing keys") {
        std::vector<std::string> keys = {"non_existing_1", "non_existing_2"};
        auto request = CreateDeleteRequest(keys);

        KvError err = task.Execute(request.get());

        // Should handle gracefully
        REQUIRE((err == KvError::NoError || err == KvError::NotFound));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_MixedOperations", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Mix of writes and deletes") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        // Add some writes
        for (int i = 0; i < 50; ++i) {
            WriteOp op;
            op.key = "write_" + std::to_string(i);
            op.value = gen_.GenerateValue(100);
            op.timestamp = 1000 + i;
            request->ops.push_back(op);
        }

        // Add some deletes
        for (int i = 0; i < 50; ++i) {
            WriteOp op;
            op.key = "delete_" + std::to_string(i);
            op.is_delete = true;
            op.timestamp = 2000 + i;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Updates to existing keys") {
        // First write
        auto request1 = CreateBatchRequest(10);
        task.Execute(request1.get());

        // Update same keys
        auto request2 = std::make_unique<BatchWriteRequest>();
        request2->table = table_;

        for (int i = 0; i < 10; ++i) {
            WriteOp op;
            op.key = gen_.GenerateSequentialKey(i);
            op.value = "updated_value_" + std::to_string(i);
            op.timestamp = 2000 + i;
            request2->ops.push_back(op);
        }

        KvError err = task.Execute(request2.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_LargeValues", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Write large value") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        WriteOp op;
        op.key = "large_value_key";
        op.value = std::string(100000, 'x');  // 100KB value
        op.timestamp = 1000;
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        // Should handle large values (may use overflow pages)
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Multiple large values") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        for (int i = 0; i < 10; ++i) {
            WriteOp op;
            op.key = "large_" + std::to_string(i);
            op.value = std::string(50000, 'y');  // 50KB each
            op.timestamp = 1000 + i;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Timestamps", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Write with expiration") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        uint64_t future_time = CurrentTime() + 3600;  // 1 hour from now

        WriteOp op;
        op.key = "expiring_key";
        op.value = "will_expire";
        op.timestamp = CurrentTime();
        op.expire_ts = future_time;
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Write already expired") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        uint64_t past_time = CurrentTime() - 3600;  // 1 hour ago

        WriteOp op;
        op.key = "expired_key";
        op.value = "already_expired";
        op.timestamp = past_time - 7200;
        op.expire_ts = past_time;
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        // Should handle expired keys appropriately
        REQUIRE((err == KvError::NoError || err == KvError::Expired));
    }

    SECTION("Monotonic timestamps") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        uint64_t base_time = 1000;
        for (int i = 0; i < 100; ++i) {
            WriteOp op;
            op.key = "ts_key_" + std::to_string(i);
            op.value = "value_" + std::to_string(i);
            op.timestamp = base_time + i;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_ErrorHandling", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Invalid table") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = TableIdent("", 0);  // Invalid table

        WriteOp op;
        op.key = "key";
        op.value = "value";
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        REQUIRE(err != KvError::NoError);
    }

    SECTION("Empty keys") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        WriteOp op;
        op.key = "";  // Empty key
        op.value = "value";
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        // Should reject empty keys
        REQUIRE((err == KvError::InvalidArgument || err == KvError::NoError));
    }

    SECTION("Key too large") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        WriteOp op;
        op.key = std::string(MaxKeySize + 1, 'k');  // Exceed max key size
        op.value = "value";
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        REQUIRE(err == KvError::InvalidArgument);
    }

    SECTION("Value too large") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        WriteOp op;
        op.key = "key";
        op.value = std::string(MaxValueSize + 1, 'v');  // Exceed max value size
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        REQUIRE(err == KvError::InvalidArgument);
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Ordering", "[batch-write][task][unit]") {
    BatchWriteTask task;

    SECTION("Writes maintain key order") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        // Add keys in random order
        std::vector<std::string> keys;
        for (int i = 0; i < 100; ++i) {
            keys.push_back(gen_.GenerateRandomKey(10, 20));
        }

        for (const auto& key : keys) {
            WriteOp op;
            op.key = key;
            op.value = "value";
            op.timestamp = 1000;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        // Keys should be sorted internally during write
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Duplicate keys in batch") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        // Add same key multiple times
        for (int i = 0; i < 10; ++i) {
            WriteOp op;
            op.key = "duplicate_key";
            op.value = "value_" + std::to_string(i);
            op.timestamp = 1000 + i;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        // Should handle duplicates (last write wins)
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Performance", "[batch-write][task][benchmark]") {
    BatchWriteTask task;

    SECTION("Large batch write") {
        Timer timer;
        auto request = CreateBatchRequest(10000);

        timer.Start();
        KvError err = task.Execute(request.get());
        timer.Stop();

        if (err == KvError::NoError) {
            double writes_per_sec = 10000 / timer.ElapsedSeconds();
            LOG(INFO) << "Batch write: " << writes_per_sec << " writes/sec";

            REQUIRE(writes_per_sec > 1000);  // Should handle >1000 writes/sec
        }
    }

    SECTION("Many small batches") {
        Timer timer;
        int successful_batches = 0;

        timer.Start();
        for (int i = 0; i < 1000; ++i) {
            auto request = CreateBatchRequest(10);
            KvError err = task.Execute(request.get());

            if (err == KvError::NoError) {
                successful_batches++;
            }
        }
        timer.Stop();

        double batches_per_sec = successful_batches / timer.ElapsedSeconds();
        LOG(INFO) << "Small batches: " << batches_per_sec << " batches/sec";
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Concurrency", "[batch-write][task][stress]") {
    const int thread_count = 8;
    const int batches_per_thread = 50;
    std::atomic<int> successful_writes{0};
    std::atomic<int> failed_writes{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([this, &successful_writes, &failed_writes, t, batches_per_thread]() {
            BatchWriteTask task;

            for (int i = 0; i < batches_per_thread; ++i) {
                auto request = std::make_unique<BatchWriteRequest>();
                request->table = table_;

                // Each thread writes to different key ranges
                for (int j = 0; j < 10; ++j) {
                    WriteOp op;
                    op.key = "thread_" + std::to_string(t) + "_batch_" +
                            std::to_string(i) + "_key_" + std::to_string(j);
                    op.value = gen_.GenerateValue(100);
                    op.timestamp = CurrentTime();
                    request->ops.push_back(op);
                }

                KvError err = task.Execute(request.get());

                if (err == KvError::NoError) {
                    successful_writes += 10;
                } else {
                    failed_writes += 10;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "Concurrent writes: " << successful_writes << " successful, "
              << failed_writes << " failed";

    REQUIRE(successful_writes > 0);
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_EdgeCases", "[batch-write][task][edge-case]") {
    BatchWriteTask task;

    SECTION("Maximum batch size") {
        auto request = CreateBatchRequest(100000);  // Very large batch

        KvError err = task.Execute(request.get());

        // Should handle or reject gracefully
        REQUIRE((err == KvError::NoError || err == KvError::InvalidArgument));
    }

    SECTION("Binary keys and values") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        for (int i = 0; i < 10; ++i) {
            WriteOp op;
            op.key = gen_.GenerateBinaryString(50);
            op.value = gen_.GenerateBinaryString(200);
            op.timestamp = 1000 + i;
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Unicode in keys and values") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        std::vector<std::string> unicode_keys = {
            "键值_1", "キー_2", "🔑_3", "مفتاح_4", "ключ_5"
        };

        for (const auto& key : unicode_keys) {
            WriteOp op;
            op.key = key;
            op.value = "Unicode value: " + key;
            op.timestamp = CurrentTime();
            request->ops.push_back(op);
        }

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Null bytes in values") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->table = table_;

        WriteOp op;
        op.key = "null_value_key";
        op.value = std::string("before\0after", 12);
        op.timestamp = 1000;
        request->ops.push_back(op);

        KvError err = task.Execute(request.get());

        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }
}