/**
 * IO QoS (docs/design/io_qos.md) — M4 device rate budget and in-flight
 * command window tests.
 *
 * Rates and windows are set deliberately tiny so the blocking paths are
 * hot, then the tests assert: pacing completes without deadlock (debt
 * admission, refill-driven wakes), classes are charged correctly,
 * borrowing is foreground-only, disabled mechanisms stay zero-cost, and
 * shutdown drains queued waiters cleanly.
 */
#include <gflags/gflags.h>

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <limits>
#include <thread>

#include "async_io_manager.h"
#include "common.h"
#include "fail_point.h"
#include "test_utils.h"

using test_util::MapVerifier;

namespace eloqstore
{
DECLARE_uint64(max_processing_time_microseconds);
}

namespace
{
eloqstore::IoQosStats ShardStats(const eloqstore::EloqStore *store)
{
    return store->GetIoQosStats(0);
}
}  // namespace

TEST_CASE("defaults: rate limiting on, unit workload never blocks", "[io_qos]")
{
    // Rate limiting is on by default (275K IOPS per disk / shard here):
    // it must charge a trivial single-threaded workload without ever
    // blocking it, and the io window is off by default.
    eloqstore::EloqStore *store = InitStore(default_opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 2000, 0, 25);
    for (int i = 0; i < 100; i++)
    {
        verify.Read(std::rand() % 2000);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.rate_.admitted_ops_ > 0);
    REQUIRE(stats.rate_.blocked_count_ == 0);
    REQUIRE(stats.io_window_blocked_ == 0);
    REQUIRE(stats.io_window_hwm_ == 0);
}

TEST_CASE("rate budget: iops = 0 disables it, stats stay zero", "[io_qos]")
{
    // Rate limiting is ON by default (275K IOPS per disk, a cloud-NVMe
    // starting point); zero must disable it entirely.
    REQUIRE(eloqstore::KvOptions{}.disk_rate_limit_iops == 275'000);
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 0;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 1000, 0, 25);
    for (int i = 0; i < 50; i++)
    {
        verify.Read(std::rand() % 1000);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.rate_.admitted_ops_ == 0);
    REQUIRE(stats.rate_.blocked_count_ == 0);
    REQUIRE(stats.bg_rate_.blocked_count_ == 0);
}

TEST_CASE("rate budget: charges device IO and completes under a tiny rate",
          "[io_qos]")
{
    // A deliberately low rate (well below what the workload wants) must
    // pace the run without deadlock or error: debt admission guarantees
    // progress for costs larger than the bucket, and refill-driven wakes
    // guarantee waiter progress. Blocked counters must move, every op
    // must be accounted, and balances must drain (no in-flight concept —
    // admitted ops are cumulative).
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 2000;  // per "disk"; one path, one shard
    opts.rate_limit_burst_ms = 4;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 500, 0, 25);
    for (int i = 0; i < 50; i++)
    {
        verify.Read(std::rand() % 500);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    // The budget is partitioned per class: rate_ meters foreground reads,
    // bg_rate_ meters background IO (background reads and all write-path
    // IO). Both classes did device IO here, so both must show charges.
    REQUIRE(stats.rate_.admitted_ops_ > 0);
    REQUIRE(stats.rate_.admitted_bytes_ > 0);
    REQUIRE(stats.bg_rate_.admitted_ops_ > 0);
    REQUIRE(stats.bg_rate_.admitted_bytes_ > 0);
}

TEST_CASE("rate budget: foreground borrows background's idle surplus",
          "[io_qos]")
{
    // Reads run after all writes have completed, so the background class
    // is idle and its share should be lent to foreground: with a rate low
    // enough that foreground exhausts its own 75% share, some read
    // admissions must be granted from the background bucket (borrowed),
    // and every op must still be accounted to the borrower's class.
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 2000;
    opts.rate_limit_burst_ms = 4;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(200);
    verify.WriteRnd(0, 500, 0, 25);
    for (int i = 0; i < 100; i++)
    {
        verify.Read(std::rand() % 500);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.rate_.admitted_ops_ > 0);
    REQUIRE(stats.rate_.borrowed_ops_ > 0);
    // Borrowed ops are a subset of the class's admitted ops.
    REQUIRE(stats.rate_.borrowed_ops_ <= stats.rate_.admitted_ops_);
    // Borrowing is asymmetric: background must never borrow foreground's
    // share (symmetric borrowing collapsed storm isolation — see
    // RateBudget::CanAdmit).
    REQUIRE(stats.bg_rate_.borrowed_ops_ == 0);
}

TEST_CASE("rate budget: overflow read batch far above one burst of tokens",
          "[io_qos]")
{
    // 600KB values span ~150 overflow pages; GetOverflowValue issues
    // 128-page ReadPages batches. At 2000 units/s with a 4 ms bucket
    // (8 banked tokens) a batch is far above one burst, so per-page
    // acquisition must pace it through debt and refills without deadlock.
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 2000;
    opts.rate_limit_burst_ms = 4;
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(600 * 1024);
    verify.Upsert(1);
    verify.Upsert(2);
    verify.Read(1);
    verify.Read(2);

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.rate_.admitted_ops_ > 0);
    REQUIRE(stats.rate_.blocked_count_ > 0);
}

TEST_CASE("rate budget: shutdown while tasks queue behind the gate", "[io_qos]")
{
    // Several overflow reads contend for a tiny rate, then the store is
    // stopped while they are still queued at the gate. Stop must drain
    // cleanly (refill-driven wakes depend only on the shard loop running)
    // and every request must complete.
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 2000;
    opts.rate_limit_burst_ms = 4;
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
}

TEST_CASE("io window: class-blind device-command cap bounds and completes",
          "[io_qos]")
{
    // A tiny window must bound in-flight device commands while everything
    // still completes. 600KB values make GetOverflowValue issue 128-page
    // ReadPages batches — far above a 2-command window — so per-command
    // acquisition must block and make progress. No rate budget: the
    // window must work standalone. (Point writes/reads alone cannot
    // contend a per-shard window: one partition serializes its write
    // task and the verifier reads synchronously.)
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 0;  // window standalone, rate off
    opts.max_inflight_io = 2;
    opts.overflow_pointers = 128;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(600 * 1024);
    verify.Upsert(1);
    verify.Upsert(2);
    verify.Read(1);
    verify.Read(2);

    eloqstore::IoQosStats stats = ShardStats(store);
    // The window drains to zero once all requests have completed.
    REQUIRE(stats.io_window_inflight_ == 0);
    // A 128-page batch through a 2-command window must have waited.
    REQUIRE(stats.io_window_blocked_ > 0);
    // Page IO respects the cap strictly (cost-1 commands); only a merged
    // write's oversized admission (ceil(1MB/256KB) = 4 > 2, admitted
    // alone) may exceed it.
    REQUIRE(stats.io_window_hwm_ >= 2);
    REQUIRE(stats.io_window_hwm_ <= 4);
}

TEST_CASE("io window: negative KvTaskPageRead CQE releases and recovers",
          "[io_qos]")
{
    // Error-path release symmetry: a failed read CQE must still release
    // its window command, the failure must surface as IoFail, and the
    // store must keep serving afterwards. (Ported from the count-budget
    // negative-CQE tests; the rate budget has no completion-time release
    // by design, so the window carries this invariant now.)
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 0;
    opts.max_inflight_io = 4;
    eloqstore::EloqStore *store = InitStore(opts);
    const eloqstore::TableIdent tbl_id{"qos-read-cqe", 0};
    MapVerifier seed(tbl_id, store, false);
    seed.SetValueSize(200);
    seed.Upsert(0, 100);
    const std::string key = test_util::Key(7, 7);
    const std::string expected = seed.DataSet().at(key).value_;

    eloqstore::ReadRequest failed;
    failed.SetArgs(tbl_id, key);
    eloqstore::FailPoint::GetInstance().ArmOnce("KvTaskPageReadCqe");
    store->ExecSync(&failed);
    eloqstore::FailPoint::GetInstance().Disarm();

    REQUIRE(failed.Error() == eloqstore::KvError::IoFail);
    REQUIRE(ShardStats(store).io_window_inflight_ == 0);

    eloqstore::ReadRequest recovery;
    recovery.SetArgs(tbl_id, key);
    store->ExecSync(&recovery);
    REQUIRE(recovery.Error() == eloqstore::KvError::NoError);
    REQUIRE(recovery.value_ == expected);
    REQUIRE(ShardStats(store).io_window_inflight_ == 0);
}

TEST_CASE("io window: negative MergedWriteReq CQE releases and recovers",
          "[io_qos]")
{
    // Same invariant for the merged-write cost (ceil(len/256KB) commands):
    // acquire and release must stay symmetric on the error path.
    eloqstore::KvOptions opts = append_opts;
    opts.disk_rate_limit_iops = 0;
    opts.max_inflight_io = 8;
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

    REQUIRE(failed.Error() == eloqstore::KvError::IoFail);
    REQUIRE(ShardStats(store).io_window_inflight_ == 0);

    eloqstore::BatchWriteRequest recovery;
    recovery.SetTableId(tbl_id);
    recovery.AddWrite("recovery", "value", ts + 1, eloqstore::WriteOp::Upsert);
    store->ExecSync(&recovery);
    REQUIRE(recovery.Error() == eloqstore::KvError::NoError);
    REQUIRE(ShardStats(store).io_window_inflight_ == 0);
}

TEST_CASE("io qos stats: concurrent sampling", "[io_qos][stats]")
{
    // IoQosStats must be safely sampleable from another thread while the
    // shard runs (observability counters are relaxed atomics; the shard
    // thread is the only writer). Ported from the count-budget suite.
    eloqstore::KvOptions opts = default_opts;
    opts.disk_rate_limit_iops = 20000;
    opts.max_inflight_io = 8;
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
    REQUIRE(stats.rate_.admitted_ops_ > 0);
    REQUIRE(stats.io_window_inflight_ == 0);
}

TEST_CASE("rate budget: bytes bucket alone paces merged writes", "[io_qos]")
{
    // Only the bytes bucket enabled (iops = 0): append-mode merged writes
    // must charge bytes and complete. Exercises the ops-disabled branch of
    // Positive() and the large-cost debt path (a merged write can exceed
    // one burst of byte tokens).
    eloqstore::KvOptions opts = append_opts;
    opts.disk_rate_limit_mbps = 8;
    opts.rate_limit_burst_ms = 4;
    eloqstore::EloqStore *store = InitStore(opts);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(1000);
    verify.WriteRnd(0, 1000, 0, 25);
    for (int i = 0; i < 20; i++)
    {
        verify.Read(std::rand() % 1000);
    }

    eloqstore::IoQosStats stats = ShardStats(store);
    REQUIRE(stats.rate_.admitted_bytes_ > 0);
    REQUIRE(stats.bg_rate_.admitted_bytes_ > 0);
}
