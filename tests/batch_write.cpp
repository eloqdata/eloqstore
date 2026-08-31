#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common.h"
#include "tasks/task_manager.h"
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
        std::string key = "0";
        batch_write_req.SetArgs(tbl_id, std::move(key));
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
        std::string key = "40000";
        batch_write_req.SetArgs(tbl_id, std::move(key));
        verify.ExecWrite(&batch_write_req);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    {
        eloqstore::TruncateRequest batch_write_req;
        std::vector<eloqstore::WriteDataEntry> entries;
        std::string key = "1";
        batch_write_req.SetArgs(tbl_id, std::move(key));
        verify.ExecWrite(&batch_write_req);
    }
}

TEST_CASE("batch write with big key", "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false, 200);
    verify.SetValueSize(300);
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

TEST_CASE("batch write abort releases pinned index pages",
          "[batch_write][abort]")
{
    eloqstore::KvOptions opts = append_opts;
    opts.store_path = {test_path};
    opts.num_threads = 1;
    opts.data_page_size = 4096;
    opts.buffer_pool_size = 4096;  // Allow only a single MemCachedPage.
    opts.auto_oom_retry_times = 0;

    auto build_entries =
        [](uint32_t start, uint32_t count, size_t key_len, size_t value_len)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(count);
        for (uint32_t i = 0; i < count; ++i)
        {
            std::string key = std::to_string(start + i);
            key.push_back('-');
            if (key.size() < key_len)
            {
                char fill = static_cast<char>('a' + ((start + i) % 26));
                key.append(key_len - key.size(), fill);
            }
            std::string value(value_len, 'v');
            entries.emplace_back(std::move(key),
                                 std::move(value),
                                 1,
                                 eloqstore::WriteOp::Upsert);
        }
        return entries;
    };

    eloqstore::EloqStore *store = InitStore(opts);

    const size_t key_len = 96;
    const size_t large_value_len = 3500;

    eloqstore::TableIdent tbl_id{"abort-oom", 0};
    eloqstore::BatchWriteRequest first_req;
    first_req.SetArgs(tbl_id, build_entries(0, 4, key_len, large_value_len));
    store->ExecSync(&first_req);
    REQUIRE(first_req.Error() == eloqstore::KvError::NoError);

    // The second write must allocate a fresh index page while the existing root
    // is pinned. With only one slot in the index buffer pool, this triggers
    // KvError::OutOfMem and calls BatchWriteTask::Abort to unpin/recycle pages.
    eloqstore::BatchWriteRequest oom_req;
    oom_req.SetArgs(tbl_id, build_entries(4, 2, key_len, large_value_len));
    store->ExecSync(&oom_req);
    REQUIRE(oom_req.Error() == eloqstore::KvError::OutOfMem);

    // After abort the pool should be reusable; a new partition can evict the
    // released page and finish normally.
    eloqstore::TableIdent recover_tbl{"abort-oom", 1};
    eloqstore::BatchWriteRequest recover_req;
    recover_req.SetArgs(recover_tbl, build_entries(0, 1, 32, 64));
    store->ExecSync(&recover_req);
    REQUIRE(recover_req.Error() == eloqstore::KvError::NoError);
}

TEST_CASE("batch write task pool handles many partitions concurrently",
          "[batch_write][task_pool]")
{
    eloqstore::KvOptions opts = append_opts;
    opts.store_path = {test_path};
    opts.num_threads = 1;                // single shard, many partitions
    opts.buffer_pool_size = 4096 * 400;  // enough for many pages

    auto make_entries = [](uint32_t base, uint32_t count)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(count);
        for (uint32_t i = 0; i < count; ++i)
        {
            std::string key = "k" + std::to_string(base + i);
            std::string val = "v" + std::to_string(base + i);
            entries.emplace_back(
                std::move(key), std::move(val), 1, eloqstore::WriteOp::Upsert);
        }
        return entries;
    };

    eloqstore::EloqStore *store = InitStore(opts);
    constexpr uint32_t partitions = 3000;
    std::vector<eloqstore::BatchWriteRequest> wave1(partitions);
    std::vector<eloqstore::BatchWriteRequest> wave2(partitions);
    auto is_expected_resource_err = [](eloqstore::KvError err)
    {
        return err == eloqstore::KvError::NoError ||
               err == eloqstore::KvError::OutOfMem ||
               err == eloqstore::KvError::OpenFileLimit;
    };

    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::TableIdent tbl_id{"pool", pid};
        wave1[pid].SetArgs(tbl_id, make_entries(0, 4));
        REQUIRE(store->ExecAsyn(&wave1[pid]));
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        wave1[pid].Wait();
    }
    // With many partitions sharing one shard, a few can exhaust bounded
    // resources. Drain the whole async wave before asserting so request
    // objects cannot be destroyed while shard tasks still hold their pointers.
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        REQUIRE(is_expected_resource_err(wave1[pid].Error()));
    }
    // Truncate everything to free index/data pages before the second wave.
    std::vector<eloqstore::TruncateRequest> trunc1(partitions);
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::TableIdent tbl_id{"pool", pid};
        trunc1[pid].SetTableId(tbl_id);
        REQUIRE(store->ExecAsyn(&trunc1[pid]));
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        trunc1[pid].Wait();
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        REQUIRE((trunc1[pid].Error() == eloqstore::KvError::NoError ||
                 trunc1[pid].Error() == eloqstore::KvError::NotFound ||
                 trunc1[pid].Error() == eloqstore::KvError::OpenFileLimit));
    }

    // Second wave reuses TaskPool slots and exercises reuse after completions.
    std::vector<uint32_t> succeeded;
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::TableIdent tbl_id{"pool", pid};
        wave2[pid].SetArgs(tbl_id, make_entries(100, 3));
        REQUIRE(store->ExecAsyn(&wave2[pid]));
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        wave2[pid].Wait();
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        if (wave2[pid].Error() == eloqstore::KvError::NoError)
        {
            succeeded.push_back(pid);
        }
        else
        {
            REQUIRE(is_expected_resource_err(wave2[pid].Error()));
        }
    }
    REQUIRE(succeeded.size() >= 3);

    // Spot-check a few partitions to ensure data landed after pooled reuse.
    auto pick = [&](size_t idx)
    { return succeeded[std::min(idx, succeeded.size() - 1)]; };
    for (uint32_t pid :
         {pick(0), pick(succeeded.size() / 2), pick(succeeded.size() - 1)})
    {
        eloqstore::TableIdent tbl_id{"pool", pid};
        eloqstore::ReadRequest read;
        read.SetArgs(tbl_id, "k100");
        store->ExecSync(&read);
        REQUIRE(read.Error() == eloqstore::KvError::NoError);
        REQUIRE(read.value_ == "v100");
    }

    // Final truncate across all partitions to make sure TaskPool objects and
    // mapping snapshots can be recycled repeatedly. Partitions whose wave2
    // write exhausted its OOM retries were never recreated after trunc1
    // cleaned them up, so Truncate on those surfaces NotFound rather than
    // NoError. Both outcomes are valid recycle paths -- the test only fails
    // on a different error class. (This case is rare in the regular build
    // but more frequent under ASan, where the slower scheduling causes more
    // wave2 batches to exhaust their retry budget.)
    std::vector<eloqstore::TruncateRequest> trunc2(partitions);
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::TableIdent tbl_id{"pool", pid};
        trunc2[pid].SetTableId(tbl_id);
        REQUIRE(store->ExecAsyn(&trunc2[pid]));
    }
    std::unordered_set<uint32_t> succeeded_set(succeeded.begin(),
                                               succeeded.end());
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        trunc2[pid].Wait();
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::KvError err = trunc2[pid].Error();
        if (succeeded_set.count(pid) != 0)
        {
            // Wave2 wrote data into this partition, so Truncate must clear it.
            REQUIRE((err == eloqstore::KvError::NoError ||
                     err == eloqstore::KvError::OpenFileLimit));
        }
        else
        {
            REQUIRE((err == eloqstore::KvError::NoError ||
                     err == eloqstore::KvError::NotFound ||
                     err == eloqstore::KvError::OpenFileLimit));
        }
    }
}

#ifndef NDEBUG
TEST_CASE("batch write arguments", "[batch_write]")
{
    // TODO: Batch write with duplicated or disordered keys
}
#endif

TEST_CASE("batch write task pool cleaned after abort", "[batch_write]")
{
    struct PoolSizeGuard
    {
        PoolSizeGuard()
        {
            eloqstore::TaskManager::SetPoolSizesForTest(1, 1, 1, 1, 1, 1);
        }
        ~PoolSizeGuard()
        {
            eloqstore::TaskManager::SetPoolSizesForTest(
                1024, 1024, 2048, 2048, 512, 256);
        }
    } guard;

    eloqstore::KvOptions opts = append_opts;
    opts.num_threads = 1;  // route all partitions to the same shard
    opts.buffer_pool_size = opts.data_page_size;  // only one index page buffer
    opts.auto_oom_retry_times = 0;

    eloqstore::EloqStore *store = InitStore(opts);
    const std::vector<eloqstore::TableIdent> partitions = {
        {"stress", 0}, {"stress", 1}, {"stress", 2}, {"stress", 3}};

    auto make_entries = [](int start, int count)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(count);
        for (int i = 0; i < count; ++i)
        {
            entries.emplace_back(std::to_string(start + i),
                                 "v",
                                 /*ts=*/1,
                                 eloqstore::WriteOp::Upsert);
        }
        std::sort(entries.begin(), entries.end());
        return entries;
    };

    auto submit_batch =
        [&](const eloqstore::TableIdent &tbl, int start, int count)
    {
        eloqstore::BatchWriteRequest req;
        req.SetArgs(tbl, make_entries(start, count));
        REQUIRE(store->ExecAsyn(&req));
        req.Wait();
        return req.Error();
    };

    bool saw_abort = false;
    // Alternate between two partition pairs; heavy batches are prone to OOM and
    // abort.
    for (int round = 0; round < 20; ++round)
    {
        eloqstore::BatchWriteRequest req_a;
        eloqstore::BatchWriteRequest req_b;
        req_a.SetArgs(partitions[0], make_entries(round * 1000, 800));
        req_b.SetArgs(partitions[1], make_entries(round * 2000, 800));
        REQUIRE(store->ExecAsyn(&req_a));
        REQUIRE(store->ExecAsyn(&req_b));
        req_a.Wait();
        req_b.Wait();
        saw_abort = saw_abort ||
                    req_a.Error() == eloqstore::KvError::OutOfMem ||
                    req_b.Error() == eloqstore::KvError::OutOfMem;

        // Smaller batches on the other partitions should still succeed even
        // after aborts.
        auto err_c = submit_batch(partitions[2], round * 10, 8);
        auto err_d = submit_batch(partitions[3], round * 10, 8);
        REQUIRE(err_c == eloqstore::KvError::NoError);
        REQUIRE(err_d == eloqstore::KvError::NoError);
    }

    REQUIRE(saw_abort);
}

// An empty BatchWriteRequest is a no-op. It must complete (not hang) and must
// not wedge the partition's write queue: dispatching an empty batch used to
// skip StartTask while leaving the per-table queue running_, so the request
// never completed and every later write to the same partition stalled behind
// it forever.
TEST_CASE("empty batch write completes and leaves the partition writable",
          "[batch_write]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    eloqstore::TableIdent tbl_id("empty_batch", 0);

    // The empty batch itself must return (before the fix ExecSync hung here).
    eloqstore::BatchWriteRequest empty_req;
    empty_req.SetArgs(tbl_id, std::vector<eloqstore::WriteDataEntry>{});
    store->ExecSync(&empty_req);
    REQUIRE(empty_req.Error() == eloqstore::KvError::NoError);

    // A subsequent write to the same partition must still complete (the queue
    // was not left wedged) and be readable back.
    eloqstore::BatchWriteRequest next_req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back("k", "v", 1, eloqstore::WriteOp::Upsert);
    next_req.SetArgs(tbl_id, std::move(entries));
    store->ExecSync(&next_req);
    REQUIRE(next_req.Error() == eloqstore::KvError::NoError);

    eloqstore::ReadRequest read;
    read.SetArgs(tbl_id, "k");
    store->ExecSync(&read);
    REQUIRE(read.Error() == eloqstore::KvError::NoError);
    REQUIRE(read.value_ == "v");
}
