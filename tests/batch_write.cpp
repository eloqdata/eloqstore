#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <utility>
#include <vector>

#include "common.h"
#include "test_utils.h"

using test_util::MapVerifier;

TEST_CASE("batch entry with smaller timestamp", "[batch_write]")
{
    // TODO:
    // Input batch entry of write has smaller timestamp than existing kv entry.
}

TEST_CASE("mixed batch write with read", "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    constexpr uint64_t max_val = 10000;
    for (int i = 0; i < 20; i++)
    {
        verify.WriteRnd(0, max_val, 0, 10);
        for (int j = 0; j < 10; j++)
        {
            uint64_t start = std::rand() % max_val;
            verify.Scan(start, start + 100);
            verify.Read(std::rand() % max_val);
            verify.Floor(std::rand() % max_val);
        }
    }
}

TEST_CASE("truncate from the first key", "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(append_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    eloqstore::TableIdent tbl_id("t1", 1);
    {
        eloqstore::BatchWriteRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(1000000);
        for (int i = 1; i < 1000000; i++)
        {
            entries.emplace_back(
                std::to_string(i), "value", 1, eloqstore::WriteOp::Upsert);
        }
        std::sort(entries.begin(), entries.end());
        batch_write_req.SetArgs(tbl_id, std::move(entries));
        verify.ExecWrite(&batch_write_req);
    }
    {
        eloqstore::TruncateRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        batch_write_req.SetArgs(tbl_id, "0");
        verify.ExecWrite(&batch_write_req);
    }
}

TEST_CASE("truncate twice overflow values", "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(append_opts);
    MapVerifier verify(test_tbl_id, store, false);
    eloqstore::TableIdent tbl_id("t1", 1);
    std::string s(5000, 'x');
    {
        eloqstore::BatchWriteRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(100000);
        for (int i = 1; i < 100000; i++)
        {
            entries.emplace_back(
                std::to_string(i), s, 1, eloqstore::WriteOp::Upsert);
        }
        std::sort(entries.begin(), entries.end());
        batch_write_req.SetArgs(tbl_id, std::move(entries));
        verify.ExecWrite(&batch_write_req);
    }
    {
        eloqstore::TruncateRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        batch_write_req.SetArgs(tbl_id, "40000");
        verify.ExecWrite(&batch_write_req);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    {
        eloqstore::TruncateRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        batch_write_req.SetArgs(tbl_id, "1");
        verify.ExecWrite(&batch_write_req);
    }
}

TEST_CASE("batch write with big key", "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false, 200);
    constexpr uint64_t max_val = 10000;
    for (int i = 0; i < 20; i++)
    {
        verify.WriteRnd(0, max_val, 0, 10);
        for (int j = 0; j < 10; j++)
        {
            uint64_t start = std::rand() % max_val;
            verify.Scan(start, start + 100);
            verify.Read(std::rand() % max_val);
            verify.Floor(std::rand() % max_val);
        }
    }
    verify.Validate();
}

#ifndef NDEBUG
TEST_CASE("batch write arguments", "[batch_write]")
{
    // TODO: Batch write with duplicated or disordered keys
}
#endif