#include <catch2/catch_test_macros.hpp>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include "../../fixtures/test_fixtures.h"

using namespace eloqstore::test;
using namespace std::chrono_literals;

TEST_CASE("AsyncPattern_IssueAndWait", "[integration][async]") {
    TestFixture fixture;
    TableIdent table = fixture.GetTable();

    SECTION("Issue multiple async operations then wait") {
        const int num_writes = 50;
        std::vector<std::unique_ptr<BatchWriteRequest>> write_reqs;

        // Issue all async writes first
        for (int i = 0; i < num_writes; ++i) {
            auto req = std::make_unique<BatchWriteRequest>();
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "value_" + std::to_string(i);

            std::vector<WriteDataEntry> batch;
            batch.emplace_back(key, value, 0, WriteOp::Put);
            req->SetArgs(table, std::move(batch));

            // Execute async
            fixture.GetStore()->ExecAsyn(req.get());
            write_reqs.push_back(std::move(req));
        }

        // Now wait for all to complete
        for (auto& req : write_reqs) {
            while (!req->IsDone()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            REQUIRE(req->Error() == KvError::NoError);
        }

        // Verify all data with sync reads
        for (int i = 0; i < num_writes; ++i) {
            auto read_req = std::make_unique<ReadRequest>();
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            read_req->SetArgs(table, key);

            auto err = fixture.GetStore()->ExecSync(read_req.get());
            REQUIRE(err == KvError::NoError);

            std::string expected_value = "value_" + std::to_string(i);
            REQUIRE(read_req->GetValue() == expected_value);
        }
    }

    SECTION("Mixed async reads and writes") {
        // Write initial data synchronously
        for (int i = 0; i < 20; ++i) {
            auto write_req = std::make_unique<BatchWriteRequest>();
            std::string key = "key_" + std::to_string(i);
            std::string value = "initial_" + std::to_string(i);

            std::vector<WriteDataEntry> batch;
            batch.emplace_back(key, value, 0, WriteOp::Put);
            write_req->SetArgs(table, std::move(batch));

            REQUIRE(fixture.GetStore()->ExecSync(write_req.get()) == KvError::NoError);
        }

        // Now issue mixed async reads and writes
        std::vector<std::unique_ptr<Request>> all_reqs;

        for (int i = 0; i < 40; ++i) {
            if (i % 2 == 0) {
                // Async read
                auto req = std::make_unique<ReadRequest>();
                req->SetArgs(table, "key_" + std::to_string(i / 2));
                fixture.GetStore()->ExecAsyn(req.get());
                all_reqs.push_back(std::move(req));
            } else {
                // Async update
                auto req = std::make_unique<BatchWriteRequest>();
                std::string key = "key_" + std::to_string(i / 2);
                std::string value = "updated_" + std::to_string(i);

                std::vector<WriteDataEntry> batch;
                batch.emplace_back(key, value, 0, WriteOp::Put);
                req->SetArgs(table, std::move(batch));

                fixture.GetStore()->ExecAsyn(req.get());
                all_reqs.push_back(std::move(req));
            }
        }

        // Wait for all to complete
        for (auto& req : all_reqs) {
            while (!req->IsDone()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            // Note: Some reads might return old values depending on timing
            // This is expected behavior in async scenarios
        }
    }
}

TEST_CASE("AsyncPattern_Pipelining", "[integration][async]") {
    TestFixture fixture;
    TableIdent table = fixture.GetTable();

    SECTION("Pipeline writes with dependencies") {
        const int pipeline_depth = 10;
        const int operations_per_stage = 5;

        for (int stage = 0; stage < pipeline_depth; ++stage) {
            std::vector<std::unique_ptr<BatchWriteRequest>> stage_reqs;

            // Issue operations for this stage
            for (int op = 0; op < operations_per_stage; ++op) {
                auto req = std::make_unique<BatchWriteRequest>();
                std::string key = "stage_" + std::to_string(stage) + "_op_" + std::to_string(op);
                std::string value = "value_s" + std::to_string(stage) + "_o" + std::to_string(op);

                std::vector<WriteDataEntry> batch;
                batch.emplace_back(key, value, 0, WriteOp::Put);
                req->SetArgs(table, std::move(batch));

                fixture.GetStore()->ExecAsyn(req.get());
                stage_reqs.push_back(std::move(req));
            }

            // Wait for this stage to complete before next
            for (auto& req : stage_reqs) {
                while (!req->IsDone()) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
                REQUIRE(req->Error() == KvError::NoError);
            }
        }

        // Verify all data
        for (int stage = 0; stage < pipeline_depth; ++stage) {
            for (int op = 0; op < operations_per_stage; ++op) {
                auto read_req = std::make_unique<ReadRequest>();
                std::string key = "stage_" + std::to_string(stage) + "_op_" + std::to_string(op);
                read_req->SetArgs(table, key);

                REQUIRE(fixture.GetStore()->ExecSync(read_req.get()) == KvError::NoError);
            }
        }
    }
}