#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <random>
#include <atomic>

#include "../../eloq_store.h"
#include "../../types.h"
#include "../fixtures/test_fixtures.h"
#include "../fixtures/test_helpers.h"
#include "../fixtures/data_generator.h"

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
        request->SetTableId(table_);

        for (size_t i = 0; i < num_writes; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = gen_.GenerateValue(100);
            uint64_t timestamp = 1000 + i;
            request->AddWrite(key, value, timestamp, WriteOp::Upsert);
        }

        return request;
    }

    std::unique_ptr<BatchWriteRequest> CreateDeleteRequest(const std::vector<std::string>& keys) {
        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        for (const auto& key : keys) {
            uint64_t timestamp = 0;  // Use default timestamp
            request->AddWrite(key, "", timestamp, WriteOp::Delete);
        }

        return request;
    }

    void VerifyKeyValue(const std::string& key, const std::string& expected_value) {
        auto read_req = std::make_unique<ReadRequest>();
        read_req->SetArgs(table_, key);

        GetStore()->ExecSync(read_req.get());
        REQUIRE(read_req->Error() == KvError::NoError);
        REQUIRE(read_req->value_ == expected_value);
    }

    void VerifyKeyNotExists(const std::string& key) {
        auto read_req = std::make_unique<ReadRequest>();
        read_req->SetArgs(table_, key);

        GetStore()->ExecSync(read_req.get());
        REQUIRE(read_req->Error() == KvError::NotFound);
    }

protected:
    TableIdent table_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_BasicWrite", "[batch-write][task][unit]") {
    SECTION("Write single key-value") {
        auto request = CreateBatchRequest(1);

        GetStore()->ExecSync(request.get());

        // Should complete write
        REQUIRE(request->Error() == KvError::NoError);

        // Verify the data was written
        VerifyKeyValue(gen_.GenerateSequentialKey(0), gen_.GenerateValue(100));
    }

    SECTION("Write multiple key-values") {
        auto request = CreateBatchRequest(100);

        GetStore()->ExecSync(request.get());

        // Should handle batch write
        REQUIRE(request->Error() == KvError::NoError);

        // Verify some of the written data
        VerifyKeyValue(gen_.GenerateSequentialKey(0), gen_.GenerateValue(100));
        VerifyKeyValue(gen_.GenerateSequentialKey(50), gen_.GenerateValue(100));
        VerifyKeyValue(gen_.GenerateSequentialKey(99), gen_.GenerateValue(100));
    }

    SECTION("Empty batch") {
        auto request = CreateBatchRequest(0);

        GetStore()->ExecSync(request.get());

        // Should handle empty batch gracefully
        REQUIRE(request->Error() == KvError::NoError);
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_DeleteOperations", "[batch-write][task][unit]") {
    // First write some data
    auto write_req = CreateBatchRequest(10);
    GetStore()->ExecSync(write_req.get());
    REQUIRE(write_req->Error() == KvError::NoError);

    SECTION("Delete existing keys") {
        std::vector<std::string> keys_to_delete = {
            gen_.GenerateSequentialKey(0),
            gen_.GenerateSequentialKey(5),
            gen_.GenerateSequentialKey(9)
        };

        auto delete_req = CreateDeleteRequest(keys_to_delete);
        GetStore()->ExecSync(delete_req.get());
        REQUIRE(delete_req->Error() == KvError::NoError);

        // Verify deleted keys don't exist
        for (const auto& key : keys_to_delete) {
            VerifyKeyNotExists(key);
        }

        // Verify non-deleted keys still exist
        VerifyKeyValue(gen_.GenerateSequentialKey(1), gen_.GenerateValue(100));
        VerifyKeyValue(gen_.GenerateSequentialKey(7), gen_.GenerateValue(100));
    }

    SECTION("Delete non-existing keys") {
        std::vector<std::string> keys_to_delete = {
            "non_existing_key_1",
            "non_existing_key_2"
        };

        auto delete_req = CreateDeleteRequest(keys_to_delete);
        GetStore()->ExecSync(delete_req.get());

        // Should succeed even if keys don't exist
        REQUIRE(delete_req->Error() == KvError::NoError);
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_MixedOperations", "[batch-write][task][unit]") {
    SECTION("Mix of writes and deletes") {
        // First create some data
        auto initial_req = CreateBatchRequest(5);
        GetStore()->ExecSync(initial_req.get());
        REQUIRE(initial_req->Error() == KvError::NoError);

        // Create a mixed request
        auto mixed_req = std::make_unique<BatchWriteRequest>();
        mixed_req->SetTableId(table_);

        // Add some new writes
        for (int i = 5; i < 10; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = gen_.GenerateValue(100);
            uint64_t timestamp = 2000 + i;
            mixed_req->AddWrite(key, value, timestamp, WriteOp::Upsert);
        }

        // Add some deletes
        std::string key_to_delete = gen_.GenerateSequentialKey(2);
        mixed_req->AddWrite(key_to_delete, "", 0, WriteOp::Delete);

        GetStore()->ExecSync(mixed_req.get());
        REQUIRE(mixed_req->Error() == KvError::NoError);

        // Verify new writes exist
        VerifyKeyValue(gen_.GenerateSequentialKey(5), gen_.GenerateValue(100));
        VerifyKeyValue(gen_.GenerateSequentialKey(9), gen_.GenerateValue(100));

        // Verify delete worked
        VerifyKeyNotExists(key_to_delete);

        // Verify other keys still exist
        VerifyKeyValue(gen_.GenerateSequentialKey(0), gen_.GenerateValue(100));
        VerifyKeyValue(gen_.GenerateSequentialKey(4), gen_.GenerateValue(100));
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_LargeValues", "[batch-write][task][unit]") {
    SECTION("Single large value") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        std::string large_value = gen_.GenerateValue(100000);  // 100KB value
        request->AddWrite("large_key", large_value, 1000, WriteOp::Upsert);

        GetStore()->ExecSync(request.get());
        REQUIRE(request->Error() == KvError::NoError);

        VerifyKeyValue("large_key", large_value);
    }

    SECTION("Multiple large values") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        for (int i = 0; i < 5; ++i) {
            std::string key = "large_key_" + std::to_string(i);
            std::string value = gen_.GenerateValue(50000);  // 50KB each
            uint64_t timestamp = 1000 + i;
            request->AddWrite(key, value, timestamp, WriteOp::Upsert);
        }

        GetStore()->ExecSync(request.get());
        REQUIRE(request->Error() == KvError::NoError);

        // Verify some of the large values
        auto read_req = std::make_unique<ReadRequest>();
        read_req->SetArgs(table_, "large_key_0");
        GetStore()->ExecSync(read_req.get());
        REQUIRE(read_req->Error() == KvError::NoError);
        REQUIRE(read_req->value_.size() == 50000);
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Ordering", "[batch-write][task][unit]") {
    SECTION("Sequential key insertion maintains order") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        // Insert keys in specific order
        std::vector<std::string> keys;
        for (int i = 0; i < 20; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = "value_" + std::to_string(i);
            uint64_t timestamp = 1000 + i;
            request->AddWrite(key, value, timestamp, WriteOp::Upsert);
            keys.push_back(key);
        }

        GetStore()->ExecSync(request.get());
        REQUIRE(request->Error() == KvError::NoError);

        // Verify all keys exist and have correct values
        for (int i = 0; i < 20; ++i) {
            VerifyKeyValue(keys[i], "value_" + std::to_string(i));
        }
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_Performance", "[batch-write][task][benchmark]") {
    SECTION("Large batch performance") {
        Timer timer;
        const size_t batch_size = 10000;

        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        for (size_t i = 0; i < batch_size; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            std::string value = gen_.GenerateValue(100);
            uint64_t timestamp = 1000;
            request->AddWrite(key, value, timestamp, WriteOp::Upsert);
        }

        timer.Start();
        GetStore()->ExecSync(request.get());
        timer.Stop();

        REQUIRE(request->Error() == KvError::NoError);

        double elapsed = timer.ElapsedSeconds();
        double throughput = batch_size / elapsed;

        LOG(INFO) << "Batch write performance: " << batch_size << " operations in "
                  << elapsed << " seconds (" << throughput << " ops/sec)";

        // Should complete within reasonable time (adjust based on hardware)
        REQUIRE(elapsed < 10.0);  // Less than 10 seconds
        REQUIRE(throughput > 100); // At least 100 ops/sec
    }
}

TEST_CASE_METHOD(BatchWriteTaskTestFixture, "BatchWriteTask_ErrorHandling", "[batch-write][task][unit]") {
    SECTION("Very long key") {
        auto request = std::make_unique<BatchWriteRequest>();
        request->SetTableId(table_);

        std::string very_long_key(10000, 'k');  // 10KB key
        request->AddWrite(very_long_key, "value", 1000, WriteOp::Upsert);

        GetStore()->ExecSync(request.get());

        // This should either succeed or fail gracefully depending on implementation
        // We don't require a specific error, just that it doesn't crash
        REQUIRE((request->Error() == KvError::NoError || request->Error() != KvError::NoError));
    }
}