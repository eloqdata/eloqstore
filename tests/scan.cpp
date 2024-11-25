#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string>
#include <string_view>

#include "common.h"
#include "global_variables.h"
#include "scan_task.h"

TEST_CASE("simple scan", "[scan]")
{
    InitEnv();

    std::string tbl_name{"t1"};
    uint32_t partition_id = 0;
    size_t n = 1000;
    uint64_t base_ts = 2;
    InitData(tbl_name, partition_id, n, base_ts);

    kvstore::TableIdent tbl_ident{tbl_name, partition_id};
    kvstore::ScanTask scan_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &scan_task;

    char buf[sizeof(uint64_t)];
    uint64_t begin_idx = 100;
    uint64_t end_idx = 200;
    ConvertIntKey(buf, begin_idx);
    std::string begin_key{buf, sizeof(uint64_t)};
    ConvertIntKey(buf, end_idx);
    std::string end_key{buf, sizeof(uint64_t)};

    kvstore::KvError err = scan_task.Scan(tbl_ident, begin_key, end_key);
    REQUIRE(err == kvstore::KvError::NoError);
    for (size_t idx = begin_idx; idx < end_idx; ++idx)
    {
        ConvertIntKey(buf, idx);
        REQUIRE(scan_task.Valid());
        REQUIRE(scan_task.Key() == std::string_view(buf, sizeof(uint64_t)));
        REQUIRE(scan_task.Value() == std::to_string(idx));
        REQUIRE(scan_task.Timestamp() == base_ts);
        err = scan_task.Next();
        REQUIRE(err == kvstore::KvError::NoError);
    }

    std::vector<kvstore::Tuple> tuples;
    err = scan_task.ScanVec(tbl_ident, begin_key, end_key, tuples);
    REQUIRE(err == kvstore::KvError::NoError);
    uint64_t idx = begin_idx;
    for (auto &[k, v, ts] : tuples)
    {
        ConvertIntKey(buf, idx);
        REQUIRE(k == std::string_view(buf, sizeof(uint64_t)));
        REQUIRE(v == std::to_string(idx));
        REQUIRE(ts == base_ts);
        idx++;
    }
}

TEST_CASE("complex scan", "[scan]")
{
    MapVerifier verify(kvstore::TableIdent{"t1", 1});
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
    verify.ScanAll();
}