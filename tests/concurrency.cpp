#include <glog/logging.h>

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <thread>

#include "common.h"
#include "test_utils.h"

using namespace test_util;

TEST_CASE("concurrently write to partition", "[concurrency]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    eloqstore::TableIdent tbl_id("concurrent-write", 1);
    eloqstore::BatchWriteRequest requests[128];
    const uint32_t batch = 100;
    for (int i = 0; i < std::size(requests); i++)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        for (int j = 0; j < batch; j++)
        {
            eloqstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = Key(j);
            ent.val_ = Value(i);
            ent.timestamp_ = i;
            ent.op_ = eloqstore::WriteOp::Upsert;
        }

        eloqstore::BatchWriteRequest &req = requests[i];
        req.SetArgs(tbl_id, std::move(entries));
        bool ok = store->ExecAsyn(&req, 0, [](eloqstore::KvRequest *req) {});
        REQUIRE(ok);
    }

    for (eloqstore::BatchWriteRequest &req : requests)
    {
        while (!req.IsDone())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    {
        eloqstore::ScanRequest scan_req;
        std::string begin = Key(0);
        std::string end = Key(batch);
        scan_req.SetArgs(tbl_id, begin, end);
        store->ExecSync(&scan_req);
        for (const eloqstore::KvEntry &ent : scan_req.Entries())
        {
            REQUIRE(ent.value_ == Value(std::size(requests) - 1));
            REQUIRE(ent.timestamp_ == std::size(requests) - 1);
        }
    }
}

TEST_CASE("easy concurrency test", "[persist][concurrency]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    ConcurrencyTester tester(store, "t1", 1, 32);
    tester.Init();
    tester.Run(20, 5, 32);
    tester.Clear();
}

TEST_CASE("hard concurrency test", "[persist][concurrency]")
{
    std::string root_path(test_path);
    eloqstore::KvOptions options = {
        .num_threads = 4,
        .store_path = {root_path + "/disk0",
                       root_path + "/disk1",
                       root_path + "/disk2"},
    };
    eloqstore::EloqStore *store = InitStore(options);
    ConcurrencyTester tester(store, "t1", 10, 1000);
    tester.Init();
    tester.Run(5000, 10, 600);
    tester.Clear();
}

TEST_CASE("stress append only mode", "[persist][append]")
{
    eloqstore::KvOptions options{
        .store_path = {test_path},
        .data_append_mode = true,
    };
    eloqstore::EloqStore *store = InitStore(options);

    ConcurrencyTester tester(store, "t1", 4, 1024);
    tester.Init();
    tester.Run(1000, 10, 10);
    tester.Clear();
}