#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "test_utils.h"

using namespace test_util;

namespace fs = std::filesystem;

TEST_CASE("scan nonexistent partition", "[scan]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::remove_all(dir);
    fs::create_directories(dir);

    eloqstore::KvOptions opts;
    opts.store_path = {dir.string()};

    eloqstore::TableIdent tbl_id{"empty-scan", 0};

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start() == eloqstore::KvError::NoError);
    eloqstore::ScanRequest scan_req;
    std::string start_key = "0";
    std::string end_key = "1";
    scan_req.SetArgs(tbl_id, start_key, end_key);

    store.ExecSync(&scan_req);
    REQUIRE(scan_req.Error() == eloqstore::KvError::NotFound);

    store.Stop();

    fs::remove_all(dir);
}

TEST_CASE("delete scan", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(400);
    verify.WriteRnd(1, 100);
    verify.WriteRnd(1, 100);
    verify.Delete(50, 70);
    verify.Scan(100, 200);
    verify.Delete(0, 1000);
    verify.Upsert(100, 200);
}

TEST_CASE("complex scan", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
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
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    constexpr uint64_t max_val = 1000;
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 0, 20);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = std::rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}

TEST_CASE("paginate the scan results", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.WriteRnd(0, 10000, 0, 90);
    // Paginate by entries amount.
    verify.Scan(0, 10000, 1000);
    // Paginate by result size.
    verify.Scan(0, 10000, SIZE_MAX, 10000);
    // Paginate by entries amount and size.
    verify.Scan(0, 10000, 100, 5000);

    // Paginate with overflow value.
    verify.SetValueSize(10000);
    verify.Upsert(1, 3);
    verify.Scan(0, 10, SIZE_MAX, 1000);
}

TEST_CASE("read floor", "[read]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(1000);
    verify.Upsert(2, 12);
    verify.WriteRnd(20, 50, 0, 30);

    // overflow value
    verify.SetValueSize(10000);
    verify.Upsert(15);

    for (int i = 51; i >= 0; i--)
    {
        verify.Floor(Key(i));
    }

    verify.WriteRnd(5, 50);
    for (int i = 51; i >= 0; i--)
    {
        verify.Floor(Key(i));
    }
}

// SeekFloor's linear-scan branch snapshots the floor entry's key/value/
// timestamp but not its large_value_/expire_ts_ flags, so the floor result
// keeps those two fields from the OVERSHOOT entry (the first key greater than
// the search key). A Floor query landing strictly between two keys within a
// restart region then reports the neighbor's expire timestamp.
TEST_CASE("floor reports its own expire_ts, not the next entry's",
          "[scan][floor]")
{
    eloqstore::KvOptions opts = default_opts;
    // Small restart interval so few keys span multiple restart regions and
    // the floor lands inside one (the buggy linear-scan branch).
    opts.data_page_restart_interval = 2;

    eloqstore::EloqStore *store = InitStore(opts);
    eloqstore::TableIdent tbl{"floor_expire", 0};

    constexpr uint64_t kFuture = 9'000'000'000'000ULL;  // far-future expire ms
    // Eight keys; every key never expires except "key03", the overshoot for a
    // "key02z" floor query. With restart interval 2 the restart points fall on
    // key00/key02/key04/key06, so the floor (key02) and the overshoot (key03,
    // a non-restart entry the linear scan parses before breaking) lie in the
    // same restart region — exactly the buggy branch. (When the overshoot is
    // itself a restart point the scan stops before parsing it, so it must be a
    // mid-region entry to leak.)
    std::vector<eloqstore::WriteDataEntry> entries;
    for (int i = 0; i < 8; ++i)
    {
        char key[8];
        std::snprintf(key, sizeof(key), "key%02d", i);
        uint64_t expire = (i == 3) ? kFuture : 0;
        entries.emplace_back(std::string(key),
                             std::string("v") + key,
                             /*ts=*/10,
                             eloqstore::WriteOp::Upsert,
                             expire);
    }
    eloqstore::BatchWriteRequest wreq;
    wreq.SetArgs(tbl, std::move(entries));
    store->ExecSync(&wreq);
    REQUIRE(wreq.Error() == eloqstore::KvError::NoError);

    // "key02z" floors to key02 (never-expires); the overshoot is key03
    // (expire=kFuture). The reported expire_ts_ must be key02's, i.e. 0.
    eloqstore::FloorRequest freq;
    freq.SetArgs(tbl, std::string("key02z"));
    store->ExecSync(&freq);
    REQUIRE(freq.Error() == eloqstore::KvError::NoError);
    REQUIRE(freq.floor_key_ == "key02");
    REQUIRE(freq.value_ == "vkey02");
    REQUIRE(freq.expire_ts_ == 0);
}
