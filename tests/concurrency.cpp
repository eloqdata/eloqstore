#include <glog/logging.h>

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <thread>

#include "common.h"
#include "test_utils.h"

TEST_CASE("concurrent tasks with memory store", "[concurrency]")
{
    InitMemStore();
    ConcurrencyTester tester(memstore.get(), "t1", 1, 16, 32, 20);
    tester.Init();
    tester.Run(5);
}

TEST_CASE("concurrently write to partition", "[concurrency]")
{
    InitMemStore();
    kvstore::TableIdent tbl_id("concurrent-write", 1);
    kvstore::WriteRequest requests[128];
    const uint32_t batch = 100;
    for (int i = 0; i < std::size(requests); i++)
    {
        std::vector<kvstore::WriteDataEntry> entries;
        for (int j = 0; j < batch; j++)
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = Key(j);
            ent.val_ = Value(i);
            ent.timestamp_ = i;
            ent.op_ = kvstore::WriteOp::Upsert;
        }

        kvstore::WriteRequest &req = requests[i];
        req.SetArgs(tbl_id, std::move(entries));
        bool ok = memstore->ExecAsyn(&req, 0, [](kvstore::KvRequest *req) {});
        REQUIRE(ok);
    }

    for (kvstore::WriteRequest &req : requests)
    {
        while (!req.IsDone())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    {
        kvstore::ScanRequest scan_req;
        std::string begin = Key(0);
        std::string end = Key(batch);
        scan_req.SetArgs(tbl_id, begin, end);
        memstore->ExecSync(&scan_req);
        for (auto [_, val, ts] : scan_req.entries_)
        {
            REQUIRE(val == Value(std::size(requests) - 1));
            REQUIRE(ts == std::size(requests) - 1);
        }
    }
}
