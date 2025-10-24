#include <catch2/catch_test_macros.hpp>

#include "common.h"
#include "test_utils.h"

using namespace test_util;

TEST_CASE("simple delete", "[delete]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(100, 300);
    verify.Delete(150, 200);
    verify.Upsert(200, 230);
    verify.Delete(0, 100);
    verify.Delete(100, 500);
    verify.Upsert(1000, 2000);
    verify.Delete(500, 1200);
}

TEST_CASE("delete from an empty table", "[delete]")
{
    eloqstore::EloqStore *store = InitStore(append_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Delete(150, 200);
    verify.Delete(0, 100);
    verify.Delete(100, 500);
    verify.Delete(500, 1200);
}

TEST_CASE("clean data", "[delete]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
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
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(1, 1000);
    for (int i = 0; i < 1000; i += 50)
    {
        verify.Delete(i, i + 50);
    }
}

TEST_CASE("random upsert/delete and scan", "[delete]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(100);
    constexpr uint64_t max_val = 50000;
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 20, 30);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = std::rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}

TEST_CASE("easy truncate table partition", "[truncate]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(1000);

    verify.Upsert(0, 10);
    verify.Truncate(5);
    verify.Truncate(0);
}

TEST_CASE("truncate table partition", "[truncate]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(100);

    verify.Upsert(1, 100000);
    verify.Truncate(100000);
    verify.Validate();
    verify.Truncate(50000);
    verify.Validate();
    verify.Truncate(10000);
    verify.Truncate(1);
    verify.Validate();
    verify.Truncate(0);

    verify.Upsert(1, 100000);
    verify.Truncate(50000);
    verify.Validate();
    verify.Truncate(0);
    verify.Validate();

    verify.SetValueSize(10000);
    verify.Upsert(1, 10000);
    verify.Clean();
    verify.Validate();
}

TEST_CASE("rand write with expire timestamp", "[TTL]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(10000);
    verify.SetMaxTTL(4);

    for (size_t i = 0; i < 20; i++)
    {
        verify.WriteRnd(0, 5000);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        verify.Validate();
    }
}

TEST_CASE("upsert with expire timestamp", "[TTL]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(1000);

    verify.SetMaxTTL(10);
    const uint32_t batch_size = 1000;
    for (size_t i = 0; i < 20; i++)
    {
        verify.Upsert(i * batch_size, (i + 1) * batch_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        verify.Validate();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // Need a write operation to trigger the last clean operation.
    verify.Delete(0, 1);
    verify.Validate();
    // Make sure the last clean task finished.
    verify.Delete(0, 1);

    // All keys should have been expired and removed.
    eloqstore::ScanRequest req;
    req.SetArgs(test_tbl_id, {}, {});
    store->ExecSync(&req);
    CHECK(req.Error() == eloqstore::KvError::NoError);
    CHECK(req.Entries().empty());
    CHECK(verify.DataSet().empty());
}

TEST_CASE("expire timestamp", "[TTL]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetMaxTTL(200);

    const uint32_t range_size = 10000;
    for (size_t i = 0; i < 100; i++)
    {
        const uint32_t begin = std::rand() % range_size;
        switch (i % 5)
        {
        case 0:
            // Hybrid with overflow value.
            verify.SetValueSize(10000);
            verify.WriteRnd(begin, begin + range_size);
            break;
        case 1:
            // Hybrid with truncate operation.
            verify.Truncate(begin);
            break;
        default:
            verify.SetValueSize(1000);
            verify.WriteRnd(begin, begin + range_size);
            break;
        }
    }
}
