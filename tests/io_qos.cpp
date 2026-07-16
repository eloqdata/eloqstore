/**
 * IO QoS (docs/design/io_qos.md) — M1 in-flight page-IO budget tests.
 *
 * These tests run with deliberately tiny caps so the blocking paths are hot,
 * then assert the accounting invariants: budgets drain to zero at quiesce,
 * high-watermarks respect the caps (except the documented oversized-request
 * admission), and disabled budgets stay untouched.
 */
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "async_io_manager.h"
#include "common.h"
#include "fail_point.h"
#include "test_utils.h"

using test_util::MapVerifier;

namespace
{
eloqstore::IoQosStats ShardStats(const eloqstore::EloqStore *store)
{
    return store->GetIoQosStats(0);
}
}  // namespace

TEST_CASE("io budgets: accounting invariants under tiny caps", "[io_qos]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 4;
    opts.max_inflight_write = 8;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    for (int round = 0; round < 4; round++)
    {
        verify.WriteRnd(0, 2000, 0, 25);
        for (int i = 0; i < 100; i++)
        {
            verify.Read(std::rand() % 2000);
        }
        verify.Scan(0, 300);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    // Budgets drain to zero once all requests have completed.
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
    // All page IO in this mode has cost 1, so watermarks are hard-capped.
    REQUIRE(stats.read_.high_watermark_ >= 1);
    REQUIRE(stats.read_.high_watermark_ <= 4);
    REQUIRE(stats.write_.high_watermark_ >= 1);
    REQUIRE(stats.write_.high_watermark_ <= 8);
}

TEST_CASE("io budgets: overflow read batch larger than the cap", "[io_qos]")
{
    // 600KB values span ~150 overflow pages; with overflow_pointers = 128,
    // GetOverflowValue issues 128-page ReadPages batches — far above the
    // 4-page read cap. Per-page acquisition must make progress regardless.
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 4;
    opts.max_inflight_write = 8;
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(600 * 1024);
    verify.Upsert(1);
    verify.Upsert(2);
    verify.Read(1);
    verify.Read(2);

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
    // Cost-1 reads: the cap is strict even for oversized batches.
    REQUIRE(stats.read_.high_watermark_ <= 4);
    // A 128-page batch through a 4-page budget must have waited.
    REQUIRE(stats.read_.blocked_count_ > 0);
}

TEST_CASE("io budgets: merged append writes and oversized admission",
          "[io_qos]")
{
    // Append mode aggregates page writes into ~1MB merged writes
    // (cost = 256 pages at 4KB). With a 64-page write cap, each merged
    // write exceeds the cap and is admitted alone once the budget drains:
    // in-flight is bounded by the single-request cost, not the cap.
    eloqstore::KvOptions opts = append_opts;
    opts.max_inflight_read = 4;
    opts.max_inflight_write = 64;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(4000);
    verify.WriteRnd(0, 3000, 0, 50);
    for (int i = 0; i < 50; i++)
    {
        verify.Read(std::rand() % 3000);
    }

    const uint32_t merged_cost_bound =
        opts.write_buffer_size / opts.data_page_size;
    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
    REQUIRE(stats.write_.high_watermark_ >= 1);
    REQUIRE(stats.write_.high_watermark_ <= merged_cost_bound);
}

TEST_CASE("io budgets: cap 512 admits two concurrent merged writes", "[io_qos]")
{
    // Counterpart of the oversized-admission test: with the cap at twice
    // the merged-write cost (512 vs 256 pages), merged writes from
    // concurrent write tasks may overlap in flight — the watermark must
    // exceed one merged write's cost — while never exceeding the cap.
    // (A single task's flushes do not reliably overlap: it yields per page
    // while building the next buffer, so concurrency comes from multiple
    // partitions' write tasks on one shard.)
    eloqstore::KvOptions opts = append_opts;
    opts.num_threads = 1;
    opts.max_inflight_write = 512;
    eloqstore::EloqStore *store = InitStore(opts);

    constexpr uint32_t num_parts = 8;
    constexpr uint32_t keys_per_part = 1200;  // ~1200 pages = ~5 flushes
    const uint64_t ts = utils::UnixTs<std::chrono::milliseconds>();
    std::array<eloqstore::BatchWriteRequest, num_parts> reqs;
    std::atomic<int> done{0};
    for (uint32_t p = 0; p < num_parts; p++)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(keys_per_part);
        for (uint32_t i = 0; i < keys_per_part; i++)
        {
            entries.emplace_back(test_util::Key(i, 7),
                                 std::string(3000, 'w'),
                                 ts,
                                 eloqstore::WriteOp::Upsert);
        }
        reqs[p].SetArgs(eloqstore::TableIdent("qos-dual", p),
                        std::move(entries));
        store->ExecAsyn(&reqs[p],
                        0,
                        [&done](eloqstore::KvRequest *)
                        { done.fetch_add(1, std::memory_order_relaxed); });
    }
    while (done.load(std::memory_order_relaxed) < int(num_parts))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    for (auto &req : reqs)
    {
        REQUIRE(req.Error() == eloqstore::KvError::NoError);
    }

    const uint32_t merged_cost =
        opts.write_buffer_size / opts.data_page_size;  // 256
    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.write_.inflight_ == 0);
    REQUIRE(stats.write_.high_watermark_ > merged_cost);
    REQUIRE(stats.write_.high_watermark_ <= 512);
}

TEST_CASE("io budgets: failed write drains the budget", "[io_qos]")
{
    // Mid-batch write failure: make the partition directory read-only after
    // the first data file exists, then write a batch large enough to need a
    // second file. Creating that file fails (EACCES) with merged writes to
    // the first file still in flight; the task aborts, and AbortWrite's
    // WaitIo must drain every in-flight page so the budgets return to zero
    // and the store stays usable.
    namespace fs = std::filesystem;
    eloqstore::KvOptions opts = append_opts;  // 1MB files (2^8 pages)
    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent tbl_id{"qos-fail", 0};
    MapVerifier verify(tbl_id, store, false);
    verify.SetValueSize(3000);  // one KV per page
    verify.Upsert(0, 10);       // creates the partition dir + file 0

    const fs::path part_dir = fs::path(test_path) / tbl_id.ToString();
    REQUIRE(fs::exists(part_dir));
    fs::permissions(part_dir,
                    fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);

    // ~600 pages: fills file 0 (256 pages) and needs file 1 -> EACCES.
    std::vector<eloqstore::WriteDataEntry> entries;
    const uint64_t ts = utils::UnixTs<std::chrono::milliseconds>();
    for (uint32_t i = 100; i < 700; i++)
    {
        entries.emplace_back(std::to_string(1000000 + i),
                             std::string(3000, 'x'),
                             ts,
                             eloqstore::WriteOp::Upsert);
    }
    eloqstore::BatchWriteRequest fail_req;
    fail_req.SetArgs(tbl_id, std::move(entries));
    store->ExecSync(&fail_req);
    fs::permissions(part_dir, fs::perms::owner_all, fs::perm_options::replace);
    REQUIRE(fail_req.Error() != eloqstore::KvError::NoError);

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.write_.inflight_ == 0);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);

    // The store must remain usable after the abort.
    eloqstore::TableIdent recover_tbl{"qos-fail", 1};
    MapVerifier recover(recover_tbl, store, false);
    recover.SetValueSize(200);
    recover.Upsert(0, 50);
    recover.Read(7);
}

TEST_CASE("io budgets: negative WriteReq CQE drains and recovers", "[io_qos]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_write = 8;
    eloqstore::EloqStore *store = InitStore(opts);
    const eloqstore::TableIdent tbl_id{"qos-write-cqe", 0};
    const uint64_t ts = utils::UnixTs<std::chrono::milliseconds>();

    eloqstore::BatchWriteRequest failed;
    failed.SetTableId(tbl_id);
    for (uint32_t i = 0; i < 100; ++i)
    {
        failed.AddWrite(test_util::Key(i, 7),
                        std::string(200, 'w'),
                        ts,
                        eloqstore::WriteOp::Upsert);
    }
    eloqstore::FailPoint::GetInstance().ArmOnce("WriteReqCqe");
    store->ExecSync(&failed);
    eloqstore::FailPoint::GetInstance().Disarm();

    REQUIRE(failed.Error() != eloqstore::KvError::NoError);
    REQUIRE(ShardStats(store).write_.inflight_ == 0);

    eloqstore::BatchWriteRequest recovery;
    recovery.SetTableId(tbl_id);
    recovery.AddWrite("recovery", "value", ts + 1, eloqstore::WriteOp::Upsert);
    store->ExecSync(&recovery);
    REQUIRE(recovery.Error() == eloqstore::KvError::NoError);
    REQUIRE(ShardStats(store).write_.inflight_ == 0);
}

TEST_CASE("io budgets: negative MergedWriteReq CQE drains and recovers",
          "[io_qos]")
{
    eloqstore::KvOptions opts = append_opts;
    opts.max_inflight_write = 64;
    eloqstore::EloqStore *store = InitStore(opts);
    const eloqstore::TableIdent tbl_id{"qos-merged-cqe", 0};
    const uint64_t ts = utils::UnixTs<std::chrono::milliseconds>();

    eloqstore::BatchWriteRequest failed;
    failed.SetTableId(tbl_id);
    for (uint32_t i = 0; i < 400; ++i)
    {
        failed.AddWrite(test_util::Key(i, 7),
                        std::string(3000, 'm'),
                        ts,
                        eloqstore::WriteOp::Upsert);
    }
    eloqstore::FailPoint::GetInstance().ArmOnce("MergedWriteReqCqe");
    store->ExecSync(&failed);
    eloqstore::FailPoint::GetInstance().Disarm();

    REQUIRE(failed.Error() != eloqstore::KvError::NoError);
    REQUIRE(ShardStats(store).write_.inflight_ == 0);

    eloqstore::BatchWriteRequest recovery;
    recovery.SetTableId(tbl_id);
    recovery.AddWrite("recovery", "value", ts + 1, eloqstore::WriteOp::Upsert);
    store->ExecSync(&recovery);
    REQUIRE(recovery.Error() == eloqstore::KvError::NoError);
    REQUIRE(ShardStats(store).write_.inflight_ == 0);
}

TEST_CASE("io budgets: shutdown while tasks queue behind the budget",
          "[io_qos]")
{
    // Several overflow reads (128-page batches) contend for a 1-page read
    // budget, then the store is stopped while they are still queued. Stop
    // must drain cleanly (no hang, no crash) and every request must
    // complete.
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 1;  // bg_cap clamps to 1 as well
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(600 * 1024);
    verify.Upsert(0, 4);

    std::array<eloqstore::ReadRequest, 4> reqs;
    std::array<std::string, 4> keys;
    std::atomic<int> done{0};
    for (uint32_t i = 0; i < reqs.size(); i++)
    {
        keys[i] = test_util::Key(i, 7);  // MapVerifier's key format
        reqs[i].SetArgs(test_tbl_id, keys[i]);
        store->ExecAsyn(&reqs[i],
                        0,
                        [&done](eloqstore::KvRequest *)
                        { done.fetch_add(1, std::memory_order_relaxed); });
    }
    store->Stop();  // blocks until the shard drains

    REQUIRE(done.load() == 4);
    for (auto &req : reqs)
    {
        REQUIRE(req.Error() == eloqstore::KvError::NoError);
    }
    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
}

TEST_CASE("io budgets: disabled read budget stays untouched", "[io_qos]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 0;  // disabled
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 1000, 0, 25);
    for (int i = 0; i < 50; i++)
    {
        verify.Read(std::rand() % 1000);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.read_.high_watermark_ == 0);
    REQUIRE(stats.read_.blocked_count_ == 0);
    // The write budget (default 32768) still counts, it just never blocks.
    REQUIRE(stats.write_.inflight_ == 0);
    REQUIRE(stats.write_.blocked_count_ == 0);
}

TEST_CASE("bg sub-budget: compaction batch reads are bounded", "[io_qos]")
{
    // Append mode with full-overwrite rounds drives space amplification past
    // file_amplify_factor, so the shard schedules compaction between batch
    // writes (per-table writes serialize behind the internal compact
    // request, so by the time the last sync write returns, earlier
    // compactions have completed). Compaction move batches issue up to
    // 128-page ReadPages bursts from a BackgroundWrite task — the
    // BaseReqPageRead BG path — which must stay within the BG sub-budget
    // (25% of 8 = 2 pages) while foreground keeps the full budget.
    eloqstore::KvOptions opts = append_opts;
    opts.file_amplify_factor = 2;
    opts.max_inflight_read = 8;
    opts.bg_read_ratio = 25;  // bg_cap = 2
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    // ~3000B values → one KV per 4KB data page, so key granularity equals
    // page granularity and the overwrite pattern below controls per-file
    // liveness exactly. (A fully-overwritten file is simply dropped by
    // compaction with no page moves — the strided pattern keeps every file
    // 40% live, i.e. SAF 2.5 > file_amplify_factor, forcing real moves.)
    verify.SetValueSize(3000);
    constexpr uint64_t num_keys = 1000;
    verify.Upsert(0, num_keys);
    for (int round = 0; round < 2; round++)
    {
        // Overwrite 3 of every 5 pages, uniformly across all files.
        for (uint64_t base = 0; base < num_keys; base += 5)
        {
            verify.Upsert(base, base + 3);
        }
    }
    for (int i = 0; i < 50; i++)
    {
        verify.Read(std::rand() % num_keys);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
    // Compaction ran and its reads were charged to the BG class...
    REQUIRE(stats.bg_read_.high_watermark_ >= 1);
    // ...and never exceeded the sub-budget.
    REQUIRE(stats.bg_read_.high_watermark_ <= 2);
    // A 128-page move batch through a 2-page sub-budget must have waited.
    REQUIRE(stats.bg_read_.blocked_count_ > 0);
    // Total budget still respected.
    REQUIRE(stats.read_.high_watermark_ <= 8);
}

TEST_CASE("bg sub-budget: foreground reads use the full budget", "[io_qos]")
{
    // Foreground overflow reads (128-page batches) may exceed the BG cap and
    // climb to the full read budget; only background is confined.
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 8;
    opts.bg_read_ratio = 25;  // bg_cap = 2
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(600 * 1024);
    verify.Upsert(1);  // fresh key: no tree reads, so no BG read traffic
    verify.Read(1);    // FG: 128-page overflow batches through cap 8

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    // FG climbed past the BG cap — proof the sub-budget does not bind FG.
    REQUIRE(stats.read_.high_watermark_ > 2);
    REQUIRE(stats.read_.high_watermark_ <= 8);
    // Nothing was charged to the BG class.
    REQUIRE(stats.bg_read_.high_watermark_ == 0);
    REQUIRE(stats.bg_read_.blocked_count_ == 0);
}

TEST_CASE("bg sub-budget: batch-write leaf loads are background", "[io_qos]")
{
    // Overwriting existing keys forces the BatchWrite task to load leaf data
    // pages from disk (single-page KvTaskPageRead path). BatchWrite is
    // classified background, so those loads are charged to the sub-budget.
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 8;
    opts.bg_read_ratio = 25;  // bg_cap = 2
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 2000, 0, 100);  // initial load
    verify.WriteRnd(0, 2000, 0, 100);  // overwrite: leaf loads from disk

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.high_watermark_ >= 1);
    REQUIRE(stats.bg_read_.high_watermark_ <= 2);
}

TEST_CASE("bg sub-budget: pending demand survives the wake gap", "[io_qos]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.num_threads = 1;
    opts.max_inflight_read = 2;
    opts.bg_read_ratio = 50;  // bg cap = 1
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    const eloqstore::TableIdent fg_tbl{"qos-wake-fg", 0};
    MapVerifier fg_seed(fg_tbl, store, false);
    fg_seed.SetValueSize(600 * 1024);
    fg_seed.Upsert(0);

    const eloqstore::TableIdent bg_tbl{"qos-wake-bg", 0};
    MapVerifier bg_seed(bg_tbl, store, false);
    bg_seed.SetValueSize(200);
    bg_seed.Upsert(0, 2000);

    std::atomic<bool> stop_fg{false};
    std::atomic<bool> fg_failed{false};
    std::atomic<uint64_t> fg_done{0};
    eloqstore::FailPoint::GetInstance().ArmPersistent("IoBudgetBgWake");

    std::vector<std::thread> readers;
    readers.reserve(8);
    for (int i = 0; i < 8; ++i)
    {
        readers.emplace_back(
            [&]
            {
                while (!stop_fg.load(std::memory_order_relaxed))
                {
                    eloqstore::ReadRequest req;
                    req.SetArgs(fg_tbl, test_util::Key(0, 7));
                    store->ExecSync(&req);
                    if (req.Error() != eloqstore::KvError::NoError)
                    {
                        fg_failed.store(true, std::memory_order_relaxed);
                    }
                    fg_done.fetch_add(1, std::memory_order_relaxed);
                }
            });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    eloqstore::BatchWriteRequest bg_req;
    bg_req.SetTableId(bg_tbl);
    bg_req.AddWrite(test_util::Key(1000, 7),
                    std::string(200, 'b'),
                    utils::UnixTs<std::chrono::milliseconds>() + 1,
                    eloqstore::WriteOp::Upsert);
    std::atomic<bool> bg_done{false};
    const uint64_t fg_before = fg_done.load(std::memory_order_relaxed);
    store->ExecAsyn(&bg_req,
                    0,
                    [&bg_done](eloqstore::KvRequest *)
                    { bg_done.store(true, std::memory_order_relaxed); });

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (bg_done.load(std::memory_order_relaxed) &&
            fg_done.load(std::memory_order_relaxed) > fg_before)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const bool bg_completed_while_armed =
        bg_done.load(std::memory_order_relaxed);
    const uint64_t fg_after = fg_done.load(std::memory_order_relaxed);

    stop_fg.store(true, std::memory_order_relaxed);
    eloqstore::FailPoint::GetInstance().Disarm();
    for (std::thread &reader : readers)
    {
        reader.join();
    }
    store->Stop();

    const eloqstore::IoQosStats stats = ShardStats(store);
    CAPTURE(fg_before, fg_after);
    REQUIRE(fg_after > fg_before);
    REQUIRE(bg_completed_while_armed);
    REQUIRE_FALSE(fg_failed.load(std::memory_order_relaxed));
    REQUIRE(bg_done.load(std::memory_order_relaxed));
    REQUIRE(bg_req.Error() == eloqstore::KvError::NoError);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);
    REQUIRE(stats.read_.high_watermark_ <= 2);
    REQUIRE(stats.bg_read_.high_watermark_ <= 1);
    REQUIRE(stats.bg_read_.blocked_count_ > 0);
}

TEST_CASE("bg sub-budget: repeated ungated contention", "[io_qos][stress]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.num_threads = 1;
    opts.max_inflight_read = 2;
    opts.bg_read_ratio = 50;
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    const eloqstore::TableIdent fg_tbl{"qos-stress-fg", 0};
    MapVerifier fg_seed(fg_tbl, store, false);
    fg_seed.SetValueSize(600 * 1024);
    fg_seed.Upsert(0);

    const eloqstore::TableIdent bg_tbl{"qos-stress-bg", 0};
    MapVerifier bg_seed(bg_tbl, store, false);
    bg_seed.SetValueSize(200);
    bg_seed.Upsert(0, 2000);

    std::atomic<bool> failed{false};
    for (uint64_t round = 1; round <= 10; ++round)
    {
        std::atomic<uint32_t> ready{0};
        std::atomic<bool> start{false};
        std::vector<std::thread> readers;
        readers.reserve(8);
        for (int i = 0; i < 8; ++i)
        {
            readers.emplace_back(
                [&]
                {
                    ready.fetch_add(1, std::memory_order_relaxed);
                    while (!start.load(std::memory_order_relaxed))
                    {
                        std::this_thread::yield();
                    }
                    for (int read = 0; read < 4; ++read)
                    {
                        eloqstore::ReadRequest req;
                        req.SetArgs(fg_tbl, test_util::Key(0, 7));
                        store->ExecSync(&req);
                        if (req.Error() != eloqstore::KvError::NoError)
                        {
                            failed.store(true, std::memory_order_relaxed);
                        }
                    }
                });
        }
        while (ready.load(std::memory_order_relaxed) != readers.size())
        {
            std::this_thread::yield();
        }
        start.store(true, std::memory_order_relaxed);

        eloqstore::BatchWriteRequest bg_req;
        bg_req.SetTableId(bg_tbl);
        bg_req.AddWrite(test_util::Key(1000, 7),
                        std::string(200, 'b'),
                        utils::UnixTs<std::chrono::milliseconds>() + round,
                        eloqstore::WriteOp::Upsert);
        store->ExecSync(&bg_req);
        if (bg_req.Error() != eloqstore::KvError::NoError)
        {
            failed.store(true, std::memory_order_relaxed);
        }
        for (std::thread &reader : readers)
        {
            reader.join();
        }
    }

    const eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE_FALSE(failed.load(std::memory_order_relaxed));
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);
    REQUIRE(stats.read_.high_watermark_ <= 2);
    REQUIRE(stats.bg_read_.high_watermark_ <= 1);
    REQUIRE(stats.bg_read_.blocked_count_ > 0);
}

TEST_CASE("io qos stats: concurrent sampling", "[io_qos][stats]")
{
    eloqstore::KvOptions opts = default_opts;
    opts.max_inflight_read = 2;
    opts.max_inflight_write = 8;
    eloqstore::EloqStore *store = InitStore(opts);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> samples{0};
    std::atomic<bool> workload_active{false};
    std::atomic<uint64_t> active_samples{0};
    std::thread sampler(
        [&]
        {
            while (!stop.load(std::memory_order_relaxed))
            {
                const eloqstore::IoQosStats stats = store->GetIoQosStats(0);
                (void) stats;
                samples.fetch_add(1, std::memory_order_relaxed);
                if (workload_active.load(std::memory_order_relaxed))
                {
                    active_samples.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(3000);
    workload_active.store(true, std::memory_order_relaxed);
    for (int round = 0; round < 3; ++round)
    {
        verify.WriteRnd(0, 1000, 0, 50);
        for (int i = 0; i < 100; ++i)
        {
            verify.Read(std::rand() % 1000);
        }
    }
    workload_active.store(false, std::memory_order_relaxed);

    stop.store(true, std::memory_order_relaxed);
    sampler.join();
    const eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(samples.load(std::memory_order_relaxed) > 0);
    REQUIRE(active_samples.load(std::memory_order_relaxed) > 0);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.bg_read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
}

TEST_CASE("io budgets: defaults are behavior-neutral", "[io_qos]")
{
    // At default caps (read 32, write 32768) a single-threaded unit
    // workload of small values must never block on a budget: foreground
    // point reads are sequential (in-flight 1) and batch-write leaf loads
    // are sequential background singles, well under bg_cap = 8.
    eloqstore::EloqStore *store = InitStore(default_opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 2000, 0, 25);
    for (int i = 0; i < 100; i++)
    {
        verify.Read(std::rand() % 2000);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.read_.inflight_ == 0);
    REQUIRE(stats.write_.inflight_ == 0);
    REQUIRE(stats.read_.blocked_count_ == 0);
    REQUIRE(stats.write_.blocked_count_ == 0);
}
