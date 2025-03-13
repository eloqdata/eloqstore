#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>

#include "common.h"
#include "test_utils.h"

TEST_CASE("delete scan", "[scan]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.Upsert(1, 100);
    verify.Delete(50, 70);
    verify.Scan(100, 200);
    verify.Delete(0, 1000);
    verify.Upsert(100, 200);
}

TEST_CASE("complex scan", "[scan]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.Upsert(1, 1000);
    verify.Upsert(2000, 3000);
    verify.Upsert(800, 1200);
    verify.Delete(2200, 2300);
    verify.Read(1);
    verify.Read(900);
    verify.Read(1100);
    verify.Read(5000);
    verify.Scan(1, 1000);
    verify.Scan(1000, 4000);
    verify.Scan(0, 100);
    verify.Delete(0, 200);
    verify.Delete(100, 300);
    verify.Scan(0, 100);
    verify.Scan(0, 500);
}

TEST_CASE("random write and scan", "[scan]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    constexpr uint64_t max_val = 1000;
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 0, 20);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}