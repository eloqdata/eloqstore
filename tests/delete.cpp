#include <catch2/catch_test_macros.hpp>

#include "common.h"
#include "test_utils.h"

TEST_CASE("simple delete", "[delete]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.Upsert(100, 300);
    verify.Delete(150, 200);
    verify.Upsert(200, 230);
    verify.Delete(0, 100);
    verify.Delete(100, 500);
    verify.Upsert(1000, 2000);
    verify.Delete(500, 1200);
}

TEST_CASE("clean data", "[delete]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    constexpr uint64_t max_val = 1000;
    verify.Delete(0, 100);
    verify.SetAutoValidate(false);
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 0, 20);
    }
    verify.Clean();
    verify.Validate();
}

TEST_CASE("decrease height", "[delete]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.Upsert(1, 1000);
    for (int i = 0; i < 1000; i += 50)
    {
        verify.Delete(i, i + 50);
    }
}

TEST_CASE("random upsert/delete and scan", "[delete]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.SetValueLength(100);
    constexpr uint64_t max_val = 50000;
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 20, 30);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}

TEST_CASE("easy truncate table partition", "[truncate]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.SetValueLength(1000);

    verify.Upsert(0, 10);
    verify.Truncate(5);
    verify.Truncate(0);
}

TEST_CASE("truncate table partition", "[truncate]")
{
    InitMemStore();
    MapVerifier verify(test_tbl_id, memstore.get());
    verify.SetValueLength(100);

    verify.Upsert(1, 100000);
    verify.Truncate(100000);
    verify.Truncate(50000);
    verify.Truncate(10000);
    verify.Truncate(1);
    verify.Truncate(0);

    verify.Upsert(1, 100000);
    verify.Truncate(50000);
    verify.Truncate(0);
}
