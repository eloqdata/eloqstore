#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

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