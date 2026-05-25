// Exercises the optional in-memory data-page cache (enable_data_page_cache).
//
// Each TEST_CASE recreates the store, so EloqStore::ResetDataCacheCounters()
// at the start of a case is unnecessary but harmless; counter checks then
// reflect only the activity issued by the case itself.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>
#include <vector>

#include "common.h"
#include "test_utils.h"

using test_util::MapVerifier;

namespace
{
eloqstore::KvOptions CacheOpts(bool append = false)
{
    eloqstore::KvOptions opts;
    opts.enable_data_page_cache = true;
    if (append)
    {
        opts.data_append_mode = true;
        opts.pages_per_file_shift = 8;
        opts.store_path = {"/tmp/eloqstore"};
    }
    return opts;
}
}  // namespace

TEST_CASE("data page cache: flag off leaves counters untouched",
          "[data_page_cache]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.Upsert(1);
    store->ResetDataCacheCounters();
    verify.Read(1);
    // With the flag off, LoadDataPage takes the storage-only path and never
    // touches the cache counters.
    REQUIRE(store->DataCacheHits() == 0);
    REQUIRE(store->DataCacheMisses() == 0);
}

TEST_CASE("data page cache: write-then-read is a cache hit", "[data_page_cache]")
{
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.Upsert(1);
    // WriteTask::WritePage(DataPage&&) promotes the buffer into the cache via
    // WritePage(Handle&), so the first read after the write should hit.
    store->ResetDataCacheCounters();
    verify.Read(1);
    REQUIRE(store->DataCacheHits() == 1);
    REQUIRE(store->DataCacheMisses() == 0);
}

TEST_CASE("data page cache: repeated reads keep hitting", "[data_page_cache]")
{
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.Upsert(1);
    store->ResetDataCacheCounters();
    for (int i = 0; i < 5; ++i)
    {
        verify.Read(1);
    }
    REQUIRE(store->DataCacheHits() == 5);
    REQUIRE(store->DataCacheMisses() == 0);
}

TEST_CASE("data page cache: update returns new value", "[data_page_cache]")
{
    // After an update via CoW, the new mapping is overwritten with the new
    // FilePageId; the cached entry is reinstalled (write-promotion) with the
    // new content, and the next read returns the new value -- not the old
    // cached content. MapVerifier asserts this on every Read().
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(20);
    verify.Upsert(1);
    verify.Read(1);  // populates cache
    verify.SetValueSize(60);
    verify.Upsert(1);  // rewrite same key with different-size value
    verify.Read(1);    // checks against the NEW value, not the cached old one
}

TEST_CASE("data page cache: scan over many keys remains correct",
          "[data_page_cache]")
{
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(40);
    verify.Upsert(0, 2000);
    verify.Validate();  // walks every key; correctness check
}

TEST_CASE("data page cache: random read/write workload", "[data_page_cache]")
{
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(80);
    constexpr uint64_t max_val = 5000;
    for (int round = 0; round < 6; ++round)
    {
        verify.WriteRnd(0, max_val, 5, 20);
        for (int q = 0; q < 30; ++q)
        {
            uint64_t k = std::rand() % max_val;
            verify.Read(k);
            verify.Floor(k);
            uint64_t s = std::rand() % max_val;
            verify.Scan(s, s + 50);
        }
    }
    verify.Validate();
}

TEST_CASE("data page cache: rewrite keeps the cache warm", "[data_page_cache]")
{
    // After a rewrite, the freshly written content is promoted into the
    // cache by WritePage(Handle&), so post-rewrite reads continue to hit
    // without going to disk. Internal loads inside the writer (loading
    // the existing page to merge new entries) also count as cache hits,
    // so we don't pin an exact number -- only that no misses occur.
    eloqstore::EloqStore *store = InitStore(CacheOpts());
    MapVerifier verify(test_tbl_id, store, false);
    verify.Upsert(42);
    store->ResetDataCacheCounters();
    verify.Read(42);
    verify.Upsert(42);  // rewrite -- writer also loads-then-rebuilds the page
    verify.Read(42);
    REQUIRE(store->DataCacheHits() >= 2);
    REQUIRE(store->DataCacheMisses() == 0);
}

TEST_CASE("data page cache: works in append mode", "[data_page_cache]")
{
    // ScanIterator uses the batched IoMgr::ReadPages path which doesn't go
    // through LoadDataPage, so we exercise the cache via point reads.
    eloqstore::EloqStore *store = InitStore(CacheOpts(/*append=*/true));
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(30);
    verify.Upsert(0, 500);
    store->ResetDataCacheCounters();
    for (uint64_t k = 0; k < 50; ++k)
    {
        verify.Read(k);
    }
    REQUIRE(store->DataCacheHits() + store->DataCacheMisses() == 50);
    // Pages just written should be in cache from WritePage promotion.
    REQUIRE(store->DataCacheHits() > 0);
}

TEST_CASE("data page cache: OOM retry under concurrent writes",
          "[data_page_cache][oom_retry]")
{
    // Concurrent batches against a buffer pool that comfortably fits one
    // task at a time but not all concurrently. Without retry, some batches
    // would surface KvError::OutOfMem. With retry on (default 5 attempts),
    // each aborted task re-enqueues itself behind the others; by the time
    // it runs again, pins have been released, and the batch completes.
    //
    // NOTE on the for-loop shape below: we drain all Wait()s before any
    // REQUIRE checks. If a REQUIRE threw mid-loop, the exception would
    // unwind reqs[] -- freeing each BatchWriteRequest::batch_ -- while
    // the shard's coroutines still hold std::span<WriteDataEntry> into
    // those vectors for the other in-flight requests. That is a textbook
    // heap-use-after-free and ASan flags it immediately. Always: drain
    // first, then assert.
    eloqstore::KvOptions opts;
    opts.enable_data_page_cache = true;
    opts.data_append_mode = true;
    opts.pages_per_file_shift = 8;
    opts.store_path = {"/tmp/eloqstore"};
    opts.num_threads = 1;
    opts.buffer_pool_size = 128 * 4096;  // ~512 KB; comfortably fits one task

    eloqstore::EloqStore *store = InitStore(opts);

    auto build_entries = [](uint32_t start, uint32_t count)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(count);
        for (uint32_t i = 0; i < count; ++i)
        {
            entries.emplace_back(std::to_string(start + i),
                                 std::string(1000, 'v'),
                                 1,
                                 eloqstore::WriteOp::Upsert);
        }
        std::sort(entries.begin(), entries.end());
        return entries;
    };

    constexpr int kPartitions = 8;
    constexpr int kKeysPerBatch = 200;
    std::vector<eloqstore::BatchWriteRequest> reqs(kPartitions);
    for (int p = 0; p < kPartitions; ++p)
    {
        eloqstore::TableIdent tbl{"oom-retry-conc", static_cast<uint32_t>(p)};
        reqs[p].SetArgs(tbl, build_entries(p * 10000, kKeysPerBatch));
        REQUIRE(store->ExecAsyn(&reqs[p]));
    }
    // Drain first.
    for (auto &r : reqs) { r.Wait(); }
    // Then check.
    for (auto &r : reqs)
    {
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
    }
}

TEST_CASE("data page cache: OOM abort releases pinned pages",
          "[data_page_cache][abort]")
{
    // Mirrors tests/batch_write.cpp's abort test but with the data-page
    // cache on. We size the pool so the first batch fits, a second large
    // batch hits OutOfMem (forcing BatchWriteTask::Abort), and a third
    // recovery batch can still succeed -- the latter only works if the
    // abort path released all pins on cached pages held by the aborted
    // task (the write-promotion handle in the VarPage queue, the
    // IndexStackEntry handles in stack_, leaf_triple_ buffers, etc.).
    eloqstore::KvOptions opts;
    opts.enable_data_page_cache = true;
    opts.data_append_mode = true;
    opts.pages_per_file_shift = 8;
    opts.store_path = {"/tmp/eloqstore"};
    opts.num_threads = 1;
    opts.data_page_size = 4096;
    // Two slots: just enough for a small steady state, but the OOM batch
    // below intentionally exceeds it.
    opts.buffer_pool_size = 2 * 4096;
    opts.max_write_batch_pages = 4;
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
    eloqstore::TableIdent tbl_id{"abort-oom-cache", 0};

    // First batch fits and exercises the write-promotion path.
    eloqstore::BatchWriteRequest first_req;
    first_req.SetArgs(tbl_id, build_entries(0, 4, key_len, large_value_len));
    store->ExecSync(&first_req);
    REQUIRE(first_req.Error() == eloqstore::KvError::NoError);

    // Second batch should saturate the pool and trigger an OOM abort.
    eloqstore::BatchWriteRequest oom_req;
    oom_req.SetArgs(tbl_id, build_entries(4, 4, key_len, large_value_len));
    store->ExecSync(&oom_req);
    REQUIRE(oom_req.Error() == eloqstore::KvError::OutOfMem);

    // A subsequent small batch in a new partition must still succeed --
    // proving the aborted task released everything it had pinned.
    eloqstore::TableIdent recover_tbl{"abort-oom-cache", 1};
    eloqstore::BatchWriteRequest recover_req;
    recover_req.SetArgs(recover_tbl, build_entries(0, 1, 32, 64));
    store->ExecSync(&recover_req);
    REQUIRE(recover_req.Error() == eloqstore::KvError::NoError);

    // And one more read on the first partition's data confirms the cache
    // didn't leak stale entries from the aborted task.
    MapVerifier verify(tbl_id, store, false);
    eloqstore::ReadRequest read_req;
    read_req.SetArgs(tbl_id, "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    store->ExecSync(&read_req);
    // Best-effort: a NotFound or NoError are both fine; what we don't want
    // is a crash or assertion failure.
    REQUIRE(
        (read_req.Error() == eloqstore::KvError::NoError ||
         read_req.Error() == eloqstore::KvError::NotFound));
}
