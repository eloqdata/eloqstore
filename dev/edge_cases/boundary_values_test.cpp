#include <catch2/catch_test_macros.hpp>
#include <random>
#include <limits>

#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class BoundaryTestFixture : public TestFixture {
public:
    BoundaryTestFixture() {
        InitStoreWithDefaults();
    }
};

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_EmptyOperations", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("empty_ops");

    SECTION("Empty database operations") {
        // Read from empty database
        auto read_req = MakeReadRequest(table, "any_key");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        REQUIRE(read_req->Error() == KvError::NotFound);

        // Scan empty database
        auto scan_req = MakeScanRequest(table, "", "", 100);
        store_->Scan(scan_req.get());
        WaitForRequest(scan_req.get());
        AssertNoError(scan_req->Error());
        REQUIRE(scan_req->keys_.empty());
        REQUIRE(scan_req->values_.empty());

        // Delete from empty database
        auto batch_req = MakeBatchWriteRequest(table);
        batch_req->AddDelete("non_existent");
        store_->BatchWrite(batch_req.get());
        WaitForRequest(batch_req.get());
        AssertNoError(batch_req->Error());
    }

    SECTION("Empty key operations") {
        // Write empty key
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("", "value_for_empty_key");
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        // Read empty key
        auto read_req = MakeReadRequest(table, "");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == "value_for_empty_key");

        // Delete empty key
        auto delete_req = MakeBatchWriteRequest(table);
        delete_req->AddDelete("");
        store_->BatchWrite(delete_req.get());
        WaitForRequest(delete_req.get());
        AssertNoError(delete_req->Error());
    }

    SECTION("Empty value operations") {
        // Write empty value
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("key_with_empty_value", "");
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        // Read empty value
        auto read_req = MakeReadRequest(table, "key_with_empty_value");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == "");
    }

    SECTION("Empty key and value") {
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("", "");
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        auto read_req = MakeReadRequest(table, "");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == "");
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_MaximumSizes", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("max_sizes");
    DataGenerator gen(42);

    SECTION("Maximum key size") {
        // Try increasingly large keys
        std::vector<size_t> key_sizes = {100, 1000, 10000, 100000};

        for (size_t size : key_sizes) {
            std::string key = gen.GenerateValue(size);
            std::string value = "value_for_" + std::to_string(size);

            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, value);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());

            if (write_req->Error() == KvError::NoError) {
                // Verify can read back
                auto read_req = MakeReadRequest(table, key);
                store_->Read(read_req.get());
                WaitForRequest(read_req.get());

                if (read_req->Error() == KvError::NoError) {
                    REQUIRE(read_req->value_ == value);
                }
            }
        }
    }

    SECTION("Maximum value size") {
        // Test increasingly large values
        std::vector<size_t> value_sizes = {1000, 10000, 100000, 1000000};

        for (size_t size : value_sizes) {
            std::string key = "large_value_" + std::to_string(size);
            std::string value = gen.GenerateValue(size);

            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, value);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());

            if (write_req->Error() == KvError::NoError) {
                auto read_req = MakeReadRequest(table, key);
                store_->Read(read_req.get());
                WaitForRequest(read_req.get());

                if (read_req->Error() == KvError::NoError) {
                    REQUIRE(read_req->value_ == value);
                }
            }
        }
    }

    SECTION("Maximum batch size") {
        // Try increasingly large batches
        std::vector<size_t> batch_sizes = {10, 100, 1000, 10000};

        for (size_t batch_size : batch_sizes) {
            auto batch_req = MakeBatchWriteRequest(table);

            for (size_t i = 0; i < batch_size; ++i) {
                std::string key = "batch_" + std::to_string(batch_size) + "_" + std::to_string(i);
                std::string value = "val_" + std::to_string(i);
                batch_req->AddPut(key, value);
            }

            store_->BatchWrite(batch_req.get());
            WaitForRequest(batch_req.get());

            // Check if batch succeeded
            if (batch_req->Error() == KvError::NoError) {
                // Verify first and last entries
                auto read_first = MakeReadRequest(table, "batch_" + std::to_string(batch_size) + "_0");
                store_->Read(read_first.get());
                WaitForRequest(read_first.get());

                auto read_last = MakeReadRequest(table,
                    "batch_" + std::to_string(batch_size) + "_" + std::to_string(batch_size - 1));
                store_->Read(read_last.get());
                WaitForRequest(read_last.get());

                if (read_first->Error() == KvError::NoError) {
                    REQUIRE(read_first->value_ == "val_0");
                }
                if (read_last->Error() == KvError::NoError) {
                    REQUIRE(read_last->value_ == "val_" + std::to_string(batch_size - 1));
                }
            }
        }
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_SingleEntry", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("single_entry");

    SECTION("Database with single entry") {
        // Write single entry
        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("only_key", "only_value");
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        // Read the single entry
        auto read_req = MakeReadRequest(table, "only_key");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == "only_value");

        // Scan should return only one entry
        auto scan_req = MakeScanRequest(table, "", "", 100);
        store_->Scan(scan_req.get());
        WaitForRequest(scan_req.get());
        AssertNoError(scan_req->Error());
        REQUIRE(scan_req->keys_.size() == 1);
        REQUIRE(scan_req->keys_[0] == "only_key");
        REQUIRE(scan_req->values_[0] == "only_value");

        // Delete the single entry
        auto delete_req = MakeBatchWriteRequest(table);
        delete_req->AddDelete("only_key");
        store_->BatchWrite(delete_req.get());
        WaitForRequest(delete_req.get());
        AssertNoError(delete_req->Error());

        // Database should be empty again
        auto scan_after = MakeScanRequest(table, "", "", 100);
        store_->Scan(scan_after.get());
        WaitForRequest(scan_after.get());
        AssertNoError(scan_after->Error());
        REQUIRE(scan_after->keys_.empty());
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_NumericLimits", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("numeric_limits");

    SECTION("Integer boundary values as keys") {
        std::vector<std::pair<std::string, int64_t>> int_boundaries = {
            {"min_int8", INT8_MIN},
            {"max_int8", INT8_MAX},
            {"min_int16", INT16_MIN},
            {"max_int16", INT16_MAX},
            {"min_int32", INT32_MIN},
            {"max_int32", INT32_MAX},
            {"min_int64", INT64_MIN},
            {"max_int64", INT64_MAX},
            {"zero", 0},
            {"negative_one", -1},
            {"positive_one", 1}
        };

        for (const auto& [name, value] : int_boundaries) {
            std::string key = std::to_string(value);
            std::string val = name;

            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, val);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());
            AssertNoError(write_req->Error());

            auto read_req = MakeReadRequest(table, key);
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            AssertNoError(read_req->Error());
            REQUIRE(read_req->value_ == val);
        }
    }

    SECTION("Floating point special values") {
        std::vector<std::pair<std::string, double>> float_specials = {
            {"positive_inf", std::numeric_limits<double>::infinity()},
            {"negative_inf", -std::numeric_limits<double>::infinity()},
            {"nan", std::numeric_limits<double>::quiet_NaN()},
            {"min_positive", std::numeric_limits<double>::min()},
            {"max_finite", std::numeric_limits<double>::max()},
            {"epsilon", std::numeric_limits<double>::epsilon()}
        };

        for (const auto& [name, value] : float_specials) {
            std::ostringstream oss;
            oss << value;
            std::string key = name;
            std::string val = oss.str();

            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, val);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());
            AssertNoError(write_req->Error());

            auto read_req = MakeReadRequest(table, key);
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            AssertNoError(read_req->Error());
            REQUIRE(read_req->value_ == val);
        }
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_SpecialCharacters", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("special_chars");
    DataGenerator gen(42);

    SECTION("Null bytes in data") {
        std::string key_with_null = "key\x00with\x00null";
        std::string value_with_null = "value\x00with\x00null\x00bytes";

        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut(key_with_null, value_with_null);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        auto read_req = MakeReadRequest(table, key_with_null);
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == value_with_null);
    }

    SECTION("All byte values") {
        // Test all possible byte values
        std::string all_bytes;
        for (int i = 0; i < 256; ++i) {
            all_bytes += static_cast<char>(i);
        }

        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("all_bytes_key", all_bytes);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        auto read_req = MakeReadRequest(table, "all_bytes_key");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == all_bytes);
    }

    SECTION("Control characters") {
        std::vector<std::pair<std::string, std::string>> control_chars = {
            {"tab_key\t", "tab_value\t"},
            {"newline_key\n", "newline_value\n"},
            {"carriage_return\r", "carriage_value\r"},
            {"null_char\0", std::string("null_value\0", 11)},
            {"backspace\b", "backspace_value\b"},
            {"form_feed\f", "form_feed_value\f"},
            {"vertical_tab\v", "vertical_tab_value\v"}
        };

        for (const auto& [key, value] : control_chars) {
            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, value);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());
            AssertNoError(write_req->Error());

            auto read_req = MakeReadRequest(table, key);
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            AssertNoError(read_req->Error());
            REQUIRE(read_req->value_ == value);
        }
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_Patterns", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("patterns");

    SECTION("Repeated patterns") {
        // Same character repeated
        std::string repeated_a(10000, 'a');
        std::string repeated_0(10000, '\0');
        std::string repeated_ff(10000, '\xFF');

        std::vector<std::pair<std::string, std::string>> patterns = {
            {"repeated_a", repeated_a},
            {"repeated_null", repeated_0},
            {"repeated_ff", repeated_ff}
        };

        for (const auto& [key, value] : patterns) {
            auto write_req = MakeBatchWriteRequest(table);
            write_req->AddPut(key, value);
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());
            AssertNoError(write_req->Error());

            auto read_req = MakeReadRequest(table, key);
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());
            AssertNoError(read_req->Error());
            REQUIRE(read_req->value_ == value);
        }
    }

    SECTION("Alternating patterns") {
        std::string alternating;
        for (int i = 0; i < 5000; ++i) {
            alternating += (i % 2) ? '\xAA' : '\x55';
        }

        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("alternating", alternating);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        auto read_req = MakeReadRequest(table, "alternating");
        store_->Read(read_req.get());
        WaitForRequest(read_req.get());
        AssertNoError(read_req->Error());
        REQUIRE(read_req->value_ == alternating);
    }

    SECTION("Increasing/decreasing sequences") {
        std::string increasing, decreasing;

        for (int i = 0; i < 256; ++i) {
            increasing += static_cast<char>(i);
            decreasing += static_cast<char>(255 - i);
        }

        auto write_req = MakeBatchWriteRequest(table);
        write_req->AddPut("increasing", increasing);
        write_req->AddPut("decreasing", decreasing);
        store_->BatchWrite(write_req.get());
        WaitForRequest(write_req.get());
        AssertNoError(write_req->Error());

        auto read_inc = MakeReadRequest(table, "increasing");
        store_->Read(read_inc.get());
        WaitForRequest(read_inc.get());
        AssertNoError(read_inc->Error());
        REQUIRE(read_inc->value_ == increasing);

        auto read_dec = MakeReadRequest(table, "decreasing");
        store_->Read(read_dec.get());
        WaitForRequest(read_dec.get());
        AssertNoError(read_dec->Error());
        REQUIRE(read_dec->value_ == decreasing);
    }
}

TEST_CASE_METHOD(BoundaryTestFixture, "Boundary_ResourceLimits", "[edge-case][boundary]") {
    TableIdent table = CreateTestTable("resource_limits");
    DataGenerator gen(42);

    SECTION("Maximum concurrent operations") {
        const int num_operations = 1000;
        std::vector<std::unique_ptr<KvRequest>> requests;

        // Submit many operations without waiting
        for (int i = 0; i < num_operations; ++i) {
            auto req = MakeReadRequest(table, "key_" + std::to_string(i));
            store_->Read(req.get());
            requests.push_back(std::move(req));
        }

        // Wait for all
        int completed = 0;
        for (const auto& req : requests) {
            WaitForRequest(req.get());
            if (req->Error() == KvError::NoError ||
                req->Error() == KvError::NotFound) {
                completed++;
            }
        }

        // Most should complete successfully
        REQUIRE(completed > num_operations * 0.9);
    }

    SECTION("Rapid table creation") {
        // Create many tables rapidly
        std::vector<TableIdent> tables;
        for (int i = 0; i < 100; ++i) {
            tables.push_back(CreateTestTable("rapid_table_" + std::to_string(i)));

            // Write to each table
            auto write_req = MakeBatchWriteRequest(tables.back());
            write_req->AddPut("key", "value_" + std::to_string(i));
            store_->BatchWrite(write_req.get());
            WaitForRequest(write_req.get());
        }

        // Verify all tables are accessible
        for (size_t i = 0; i < tables.size(); ++i) {
            auto read_req = MakeReadRequest(tables[i], "key");
            store_->Read(read_req.get());
            WaitForRequest(read_req.get());

            if (read_req->Error() == KvError::NoError) {
                REQUIRE(read_req->value_ == "value_" + std::to_string(i));
            }
        }
    }
}