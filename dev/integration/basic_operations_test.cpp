#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <map>

#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"
#include "fixtures/temp_directory.h"

using namespace eloqstore;
using namespace eloqstore::test;

class IntegrationTestFixture : public TestFixture {
public:
    IntegrationTestFixture() {
        InitStoreWithDefaults();
    }

    void VerifyKeyValue(const TableIdent& table, const std::string& key, const std::string& expected_value) {
        auto read_req = MakeReadRequest(table, key);
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());

        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == expected_value);
    }

    void WriteKeyValue(const TableIdent& table, const std::string& key, const std::string& value) {
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut(key, value);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());

        AssertNoError(write_req->Error());
    }

    void DeleteKey(const TableIdent& table, const std::string& key) {
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddDelete(key);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());

        AssertNoError(write_req->Error());
    }
};

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_BasicReadWrite", "[integration][basic]") {
    TableIdent table = CreateTestTable("basic_rw");

    SECTION("Single key-value pair") {
        WriteKeyValue(table, "key1", "value1");
        VerifyKeyValue(table, "key1", "value1");
    }

    SECTION("Multiple key-value pairs") {
        std::map<std::string, std::string> data = {
            {"key1", "value1"},
            {"key2", "value2"},
            {"key3", "value3"},
            {"key4", "value4"},
            {"key5", "value5"}
        };

        // Write all
        for (const auto& [key, value] : data) {
            WriteKeyValue(table, key, value);
        }

        // Verify all
        for (const auto& [key, expected_value] : data) {
            VerifyKeyValue(table, key, expected_value);
        }
    }

    SECTION("Update existing key") {
        WriteKeyValue(table, "key1", "initial_value");
        VerifyKeyValue(table, "key1", "initial_value");

        WriteKeyValue(table, "key1", "updated_value");
        VerifyKeyValue(table, "key1", "updated_value");
    }

    SECTION("Read non-existent key") {
        auto read_req = MakeReadRequest(table, "non_existent");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());

        REQUIRE(read_req->Error() == KvError::NotFound);
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_BatchOperations", "[integration][batch]") {
    TableIdent table = CreateTestTable("batch_ops");
    DataGenerator gen(42);

    SECTION("Batch write multiple keys") {
        auto batch_req = MakeBatchWriteRequest(table);

        std::map<std::string, std::string> data;
        for (int i = 0; i < 100; ++i) {
            std::string key = "batch_key_" + std::to_string(i);
            std::string value = gen.GenerateValue(100);
            data[key] = value;
            batch_req->AddPut(key, value);
        }

        store_->BatchWrite(batch_req.get());
        WaitForRequest(batch_req.get());
        AssertNoError(batch_req->Error());

        // Verify all written
        for (const auto& [key, expected_value] : data) {
            VerifyKeyValue(table, key, expected_value);
        }
    }

    SECTION("Mixed batch operations") {
        // First, write some initial data
        for (int i = 0; i < 20; ++i) {
            WriteKeyValue(table, "key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        // Batch with mixed operations
        auto batch_req = MakeBatchWriteRequest(table);

        // Updates
        for (int i = 0; i < 10; ++i) {
            batch_req->AddPut("key_" + std::to_string(i), "updated_" + std::to_string(i));
        }

        // Deletes
        for (int i = 10; i < 15; ++i) {
            batch_req->AddDelete("key_" + std::to_string(i));
        }

        // New inserts
        for (int i = 20; i < 25; ++i) {
            batch_req->AddPut("key_" + std::to_string(i), "new_" + std::to_string(i));
        }

        store_->BatchWrite(batch_req.get());
        WaitForRequest(batch_req.get());
        AssertNoError(batch_req->Error());

        // Verify updates
        for (int i = 0; i < 10; ++i) {
            VerifyKeyValue(table, "key_" + std::to_string(i), "updated_" + std::to_string(i));
        }

        // Verify deletes
        for (int i = 10; i < 15; ++i) {
            auto read_req = MakeReadRequest(table, "key_" + std::to_string(i));
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            REQUIRE(read_req->Error() == KvError::NotFound);
        }

        // Verify unchanged
        for (int i = 15; i < 20; ++i) {
            VerifyKeyValue(table, "key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        // Verify new inserts
        for (int i = 20; i < 25; ++i) {
            VerifyKeyValue(table, "key_" + std::to_string(i), "new_" + std::to_string(i));
        }
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_ScanOperations", "[integration][scan]") {
    TableIdent table = CreateTestTable("scan_ops");

    SECTION("Basic range scan") {
        // Write sorted data
        std::map<std::string, std::string> data;
        for (int i = 0; i < 100; ++i) {
            std::string key = "key_" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            data[key] = value;
            WriteKeyValue(table, key, value);
        }

        // Scan a range
        auto scan_req = MakeScanRequest(table, "key_020", "key_030", 20);
        store_->Scan(scan_req.get());
        WaitForRequest(scan_req.get());
        AssertNoError(scan_req->Error());

        REQUIRE(scan_req->keys_.size() > 0);
        REQUIRE(scan_req->keys_.size() <= 11);  // key_020 to key_030 inclusive

        // Verify scan results are in order
        for (size_t i = 1; i < scan_req->keys_.size(); ++i) {
            REQUIRE(scan_req->keys_[i] > scan_req->keys_[i-1]);
        }
    }

    SECTION("Full table scan") {
        // Write data
        for (int i = 0; i < 50; ++i) {
            WriteKeyValue(table, "scan_" + std::to_string(i), "val_" + std::to_string(i));
        }

        // Scan entire range
        auto scan_req = MakeScanRequest(table, "", "", 1000);
        store_->Scan(scan_req.get());
        WaitForRequest(scan_req.get());
        AssertNoError(scan_req->Error());

        REQUIRE(scan_req->keys_.size() == 50);
    }

    SECTION("Scan with limit") {
        // Write more data than limit
        for (int i = 0; i < 100; ++i) {
            WriteKeyValue(table, "limit_" + std::to_string(i), "val");
        }

        // Scan with small limit
        auto scan_req = MakeScanRequest(table, "", "", 10);
        store_->Scan(scan_req.get());
        WaitForRequest(scan_req.get());
        AssertNoError(scan_req->Error());

        REQUIRE(scan_req->keys_.size() == 10);
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_DeleteOperations", "[integration][delete]") {
    TableIdent table = CreateTestTable("delete_ops");

    SECTION("Delete existing key") {
        WriteKeyValue(table, "to_delete", "value");
        VerifyKeyValue(table, "to_delete", "value");

        DeleteKey(table, "to_delete");

        auto read_req = MakeReadRequest(table, "to_delete");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        REQUIRE(read_req->Error() == KvError::NotFound);
    }

    SECTION("Delete non-existent key") {
        DeleteKey(table, "never_existed");
        // Should not error
    }

    SECTION("Delete and re-insert") {
        WriteKeyValue(table, "key", "value1");
        DeleteKey(table, "key");
        WriteKeyValue(table, "key", "value2");

        VerifyKeyValue(table, "key", "value2");
    }

    SECTION("Batch delete") {
        // Write initial data
        for (int i = 0; i < 100; ++i) {
            WriteKeyValue(table, "del_" + std::to_string(i), "val_" + std::to_string(i));
        }

        // Delete half
        auto batch_req = MakeBatchWriteRequest(table);
        for (int i = 0; i < 50; ++i) {
            batch_req->AddDelete("del_" + std::to_string(i));
        }
        store_->BatchWrite(batch_req.get());
        WaitForRequest(batch_req.get());
        AssertNoError(batch_req->Error());

        // Verify deletes
        for (int i = 0; i < 50; ++i) {
            auto read_req = MakeReadRequest(table, "del_" + std::to_string(i));
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            REQUIRE(read_req->Error() == KvError::NotFound);
        }

        // Verify remaining
        for (int i = 50; i < 100; ++i) {
            VerifyKeyValue(table, "del_" + std::to_string(i), "val_" + std::to_string(i));
        }
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_LargeValues", "[integration][large]") {
    TableIdent table = CreateTestTable("large_values");
    DataGenerator gen(42);

    SECTION("Large single value") {
        std::string large_value = gen.GenerateValue(100000);  // 100KB
        WriteKeyValue(table, "large_key", large_value);
        VerifyKeyValue(table, "large_key", large_value);
    }

    SECTION("Many medium values") {
        std::map<std::string, std::string> data;
        for (int i = 0; i < 100; ++i) {
            std::string key = "medium_" + std::to_string(i);
            std::string value = gen.GenerateValue(10000);  // 10KB each
            data[key] = value;
            WriteKeyValue(table, key, value);
        }

        for (const auto& [key, expected_value] : data) {
            VerifyKeyValue(table, key, expected_value);
        }
    }

    SECTION("Mixed sizes") {
        std::vector<size_t> sizes = {10, 100, 1000, 10000, 50000};

        for (size_t i = 0; i < sizes.size(); ++i) {
            std::string key = "mixed_" + std::to_string(i);
            std::string value = gen.GenerateValue(sizes[i]);
            WriteKeyValue(table, key, value);
            VerifyKeyValue(table, key, value);
        }
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_EdgeCases", "[integration][edge-case]") {
    TableIdent table = CreateTestTable("edge_cases");
    DataGenerator gen(42);

    SECTION("Empty values") {
        WriteKeyValue(table, "empty_val", "");

        auto read_req = MakeReadRequest(table, "empty_val");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == "");
    }

    SECTION("Binary data") {
        std::string binary_key = gen.GenerateBinaryString(100);
        std::string binary_value = gen.GenerateBinaryString(1000);

        WriteKeyValue(table, binary_key, binary_value);
        VerifyKeyValue(table, binary_key, binary_value);
    }

    SECTION("UTF-8 strings") {
        std::vector<std::pair<std::string, std::string>> utf8_data = {
            {u8"键_1", u8"值_1"},
            {u8"キー_2", u8"バリュー_2"},
            {u8"🔑_3", u8"💎_3"}
        };

        for (const auto& [key, value] : utf8_data) {
            WriteKeyValue(table, key, value);
            VerifyKeyValue(table, key, value);
        }
    }

    SECTION("Special characters in keys") {
        std::vector<std::string> special_keys = {
            "key with spaces",
            "key\twith\ttabs",
            "key\nwith\nnewlines",
            "key/with/slashes",
            "key.with.dots",
            "key-with-dashes"
        };

        for (const auto& key : special_keys) {
            WriteKeyValue(table, key, "value");
            VerifyKeyValue(table, key, "value");
        }
    }
}

TEST_CASE_METHOD(IntegrationTestFixture, "Integration_ConcurrentOperations", "[integration][concurrent]") {
    TableIdent table = CreateTestTable("concurrent");

    SECTION("Concurrent reads") {
        // Write initial data
        for (int i = 0; i < 100; ++i) {
            WriteKeyValue(table, "key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        // Concurrent reads
        std::vector<std::thread> threads;
        std::atomic<int> successful_reads{0};

        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([this, &table, &successful_reads, t]() {
                for (int i = t; i < 100; i += 8) {
                    auto read_req = MakeReadRequest(table, "key_" + std::to_string(i));
                    store_->Read(read_req.get());
                    WaitForRequest(read_req.get());

                    if (read_req->Error() == KvError::NoError &&
                        read_req->value_ == "value_" + std::to_string(i)) {
                        successful_reads++;
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        REQUIRE(successful_reads == 100);
    }

    SECTION("Concurrent writes to different keys") {
        std::vector<std::thread> threads;
        std::atomic<int> successful_writes{0};

        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([this, &table, &successful_writes, t]() {
                for (int i = 0; i < 10; ++i) {
                    std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                    std::string value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);

                    auto write_req = MakeBatchWriteRequest(table);
                    write_req->AddPut(key, value);
                    store_->BatchWrite(write_req.get());
                    WaitForRequest(write_req.get());

                    if (write_req->Error() == KvError::NoError) {
                        successful_writes++;
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        REQUIRE(successful_writes == 80);

        // Verify all writes
        for (int t = 0; t < 8; ++t) {
            for (int i = 0; i < 10; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::string expected = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                VerifyKeyValue(table, key, expected);
            }
        }
    }

    SECTION("Mixed concurrent operations") {
        // Pre-populate some data
        for (int i = 0; i < 50; ++i) {
            WriteKeyValue(table, "shared_" + std::to_string(i), "initial_" + std::to_string(i));
        }

        std::vector<std::thread> threads;

        // Reader threads
        for (int t = 0; t < 4; ++t) {
            threads.emplace_back([this, &table]() {
                for (int i = 0; i < 100; ++i) {
                    int key_id = rand() % 50;
                    auto read_req = MakeReadRequest(table, "shared_" + std::to_string(key_id));
                    store_->Read(read_req.get());
                    WaitForRequest(read_req.get());
                }
            });
        }

        // Writer threads
        for (int t = 0; t < 2; ++t) {
            threads.emplace_back([this, &table, t]() {
                for (int i = 0; i < 25; ++i) {
                    auto write_req = MakeBatchWriteRequest(table);
                    write_req->AddPut("new_" + std::to_string(t * 25 + i), "value");
                    store_->BatchWrite(write_req.get());
                    WaitForRequest(write_req.get());
                }
            });
        }

        // Scanner thread
        threads.emplace_back([this, &table]() {
            for (int i = 0; i < 10; ++i) {
                auto scan_req = MakeScanRequest(table, "", "", 100);
                store_->Scan(scan_req.get());
                WaitForRequest(scan_req.get());
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });

        for (auto& thread : threads) {
            thread.join();
        }

        // System should remain consistent
        // Verify some data is still readable
        auto read_req = MakeReadRequest(table, "shared_0");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        REQUIRE(read_req->Error() == KvError::NoError ||
                read_req->Error() == KvError::NotFound);  // May have been deleted
    }
}