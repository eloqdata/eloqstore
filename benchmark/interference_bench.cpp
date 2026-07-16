/**
 * IO QoS interference benchmark (docs/design/io_qos.md, plan commit 3).
 *
 * Measures how much a background write/compaction storm degrades foreground
 * point-read tail latency, and how the IO QoS knobs (max_inflight_read,
 * bg_read_ratio, max_inflight_write) change that.
 *
 * Phases:
 *   1. load      — fill P partitions with K keys of ~val_size bytes each.
 *                  val_size defaults to 3000 so one KV fills one 4KB data
 *                  page: key granularity == page granularity, which lets the
 *                  storm control per-file liveness exactly.
 *   2. baseline  — closed-loop uniform-random point reads at fixed
 *                  concurrency for baseline_secs. No writes.
 *   3. mixed     — the measured workload: a write-dominated op mix (default
 *                  90% write key-ops / 10% point reads, --write_read_ratio)
 *                  where reads are paced off completed write batches so the
 *                  ratio holds regardless of relative speeds; set
 *                  write_read_ratio=0 for the original unthrottled
 *                  reads-vs-storm shape. The writes overwrite a rotating
 *                  strided subset of keys (span of every ratio, default 3
 *                  of 5, shifted by one each round), keeping every data
 *                  file ~40% live — above file_amplify_factor — so
 *                  compaction continuously relocates live pages through
 *                  128-page ReadPages bursts. (A full overwrite would leave
 *                  files 100% dead: compaction just drops them and generates
 *                  NO read traffic — see io_qos_impl_plan.md.)
 *
 * Reports per phase: read QPS and exact p50/p90/p99/p99.9/max latency
 * (computed from raw samples, not a sliding window), the storm's write MB/s,
 * and per-shard IoQosStats deltas (in-flight watermarks, budget-blocked
 * counts/time, fdatasync). Greppable one-line summaries are prefixed with
 * "RESULT" for sweep scripts.
 *
 * EloqStore options (including the QoS knobs) come from --kvoptions ini, so
 * sweeps only vary the ini / flags. See opts_interference.ini.
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "async_io_manager.h"  // IoQosStats
#include "coding.h"
#include "eloq_store.h"
#include "utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../external/concurrentqueue/blockingconcurrentqueue.h"

DEFINE_string(kvoptions, "", "Path to EloqStore options ini");
DEFINE_uint32(partitions, 4, "number of partitions");
DEFINE_uint32(keys_per_partition, 20000, "keys per partition");
DEFINE_uint32(val_size,
              3000,
              "value bytes; default ~3000 = one KV per 4KB data page");
DEFINE_uint32(read_concurrency, 32, "concurrent point reads (closed loop)");
DEFINE_uint32(baseline_secs, 15, "seconds of read-only baseline phase");
DEFINE_uint32(storm_secs, 60, "seconds of read + write-storm phase");
DEFINE_uint32(storm_ratio, 5, "key-stride width of the overwrite pattern");
DEFINE_uint32(storm_span, 3, "keys overwritten within each stride");
DEFINE_uint32(storm_batch_keys, 2048, "keys per storm batch-write request");
DEFINE_uint32(write_read_ratio,
              9,
              "write key-ops per point read in the mixed phase (9 = 90/10 "
              "write/read op mix). 0 = reads run unthrottled closed-loop "
              "alongside the writes (the original storm shape)");
DEFINE_bool(load, true, "load data first (false reuses an existing store)");

using namespace std::chrono;

namespace
{
constexpr char kTable[] = "ifb";

std::atomic<int> g_phase{0};  // 0 load, 1 baseline, 2 mixed, 3 done

// Read pacing for the mixed phase: every completed storm batch adds its key
// count; issuing one read consumes write_read_ratio credits, so reads track
// the write throughput at the configured op ratio (e.g. 9 -> 10% reads /
// 90% writes by ops).
std::atomic<int64_t> g_read_credits{0};

bool TryConsumeReadCredits()
{
    const int64_t need = FLAGS_write_read_ratio;
    int64_t cur = g_read_credits.load(std::memory_order_relaxed);
    while (cur >= need)
    {
        if (g_read_credits.compare_exchange_weak(
                cur, cur - need, std::memory_order_relaxed))
        {
            return true;
        }
    }
    return false;
}

void EncodeKey(char *dst, uint64_t key)
{
    eloqstore::EncodeFixed64(dst, eloqstore::ToBigEndian(key));
}

std::string MakeKey(uint64_t key)
{
    std::string s;
    s.resize(sizeof(uint64_t));
    EncodeKey(s.data(), key);
    return s;
}

// ---------------------------------------------------------------- readers

struct Reader
{
    explicit Reader(uint32_t id) : id_(id)
    {
    }
    const uint32_t id_;
    eloqstore::ReadRequest request_;
    char key_[sizeof(uint64_t)];
    uint64_t start_us_{0};
    int issue_phase_{0};
};

struct PhaseLatencies
{
    std::vector<uint64_t> samples;  // microseconds
    uint64_t not_found{0};
    uint64_t errors{0};
};

uint64_t Percentile(std::vector<uint64_t> &sorted, double p)
{
    if (sorted.empty())
    {
        return 0;
    }
    size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
    return sorted[idx];
}

void ReportPhase(const char *name, PhaseLatencies &lat, double secs)
{
    std::sort(lat.samples.begin(), lat.samples.end());
    const size_t n = lat.samples.size();
    const uint64_t qps = secs > 0 ? static_cast<uint64_t>(n / secs) : 0;
    LOG(INFO) << "RESULT phase=" << name << " reads=" << n << " qps=" << qps
              << " p50=" << Percentile(lat.samples, 0.50)
              << " p90=" << Percentile(lat.samples, 0.90)
              << " p99=" << Percentile(lat.samples, 0.99)
              << " p999=" << Percentile(lat.samples, 0.999)
              << " max=" << (n ? lat.samples.back() : 0)
              << " not_found=" << lat.not_found << " errors=" << lat.errors
              << " (latency us)";
}

/**
 * Read driver. Baseline phase: closed loop at FLAGS_read_concurrency.
 * Mixed phase with write_read_ratio > 0: reads are additionally gated on
 * credits produced by completed storm writes, holding the op mix at
 * 1 read : ratio writes (reads pause when writes stall, and vice versa
 * never outrun the ratio). Each completion is recorded into the phase the
 * request was ISSUED in (so a request straddling a phase flip does not
 * contaminate the other phase).
 */
void ReadLoop(eloqstore::EloqStore *store,
              PhaseLatencies *baseline,
              PhaseLatencies *mixed)
{
    moodycamel::BlockingConcurrentQueue<Reader *> finished;
    std::vector<std::unique_ptr<Reader>> readers(FLAGS_read_concurrency);
    std::vector<Reader *> idle;
    idle.reserve(FLAGS_read_concurrency);
    for (uint32_t i = 0; i < FLAGS_read_concurrency; i++)
    {
        readers[i] = std::make_unique<Reader>(i);
        idle.push_back(readers[i].get());
    }

    auto callback = [&finished](eloqstore::KvRequest *req)
    { finished.enqueue(reinterpret_cast<Reader *>(req->UserData())); };

    std::mt19937_64 rnd(12345);
    auto send_req = [&](Reader *reader)
    {
        const uint64_t key = rnd() % FLAGS_keys_per_partition;
        const uint32_t part = rnd() % FLAGS_partitions;
        EncodeKey(reader->key_, key);
        reader->request_.SetArgs(eloqstore::TableIdent(kTable, part),
                                 std::string_view(reader->key_, sizeof(key)));
        reader->issue_phase_ = g_phase.load(std::memory_order_relaxed);
        reader->start_us_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&reader->request_, uint64_t(reader), callback);
    };

    size_t inflight = 0;
    while (true)
    {
        Reader *reader;
        if (finished.wait_dequeue_timed(reader, milliseconds(1)))
        {
            const uint64_t lat =
                utils::UnixTs<microseconds>() - reader->start_us_;
            PhaseLatencies *dst = reader->issue_phase_ == 1   ? baseline
                                  : reader->issue_phase_ == 2 ? mixed
                                                              : nullptr;
            if (dst != nullptr)
            {
                dst->samples.push_back(lat);
                if (reader->request_.Error() == eloqstore::KvError::NotFound)
                {
                    dst->not_found++;
                }
                else if (reader->request_.Error() !=
                         eloqstore::KvError::NoError)
                {
                    dst->errors++;
                }
            }
            inflight--;
            idle.push_back(reader);
        }

        const int phase = g_phase.load(std::memory_order_relaxed);
        if (phase >= 3)
        {
            if (inflight == 0)
            {
                break;
            }
            continue;  // drain without re-issuing
        }
        while (!idle.empty())
        {
            if (phase == 2 && FLAGS_write_read_ratio > 0 &&
                !TryConsumeReadCredits())
            {
                break;  // wait for storm writes to earn more read credits
            }
            send_req(idle.back());
            idle.pop_back();
            inflight++;
        }
    }
}

// ------------------------------------------------------------ write storm

struct StormWriter
{
    explicit StormWriter(uint32_t part) : part_(part)
    {
    }
    const uint32_t part_;
    eloqstore::BatchWriteRequest request_;
    uint64_t next_key_{0};
    uint32_t round_{0};
    uint64_t bytes_written_{0};
    uint64_t keys_written_{0};
    uint32_t batch_keys_{0};  // keys in the currently in-flight batch
    bool done_{false};
};

/**
 * Build the next storm batch for one partition: ascending keys where
 * ((key + round) % ratio) < span. Rotating `round` shifts the surviving 40%
 * every pass over the keyspace, so files written by earlier rounds are
 * partially — never fully — invalidated, keeping compaction's move pipeline
 * busy for the whole phase.
 */
void NextStormBatch(StormWriter &w)
{
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.reserve(FLAGS_storm_batch_keys);
    const uint64_t ts = utils::UnixTs<milliseconds>();
    const std::string value(FLAGS_val_size, 'S' + (w.round_ & 7));
    while (entries.size() < FLAGS_storm_batch_keys)
    {
        if (w.next_key_ >= FLAGS_keys_per_partition)
        {
            w.next_key_ = 0;
            w.round_++;
            if (!entries.empty())
            {
                // Batch keys must stay sorted: never let a batch span the
                // keyspace wrap (it would append low keys after high ones).
                break;
            }
        }
        const uint64_t key = w.next_key_++;
        if ((key + w.round_) % FLAGS_storm_ratio >= FLAGS_storm_span)
        {
            continue;
        }
        entries.emplace_back(
            MakeKey(key), value, ts, eloqstore::WriteOp::Upsert);
        w.bytes_written_ += FLAGS_val_size + sizeof(uint64_t);
    }
    w.batch_keys_ = static_cast<uint32_t>(entries.size());
    w.keys_written_ += w.batch_keys_;
    w.request_.SetArgs(eloqstore::TableIdent(kTable, w.part_),
                       std::move(entries));
}

struct StormTotals
{
    uint64_t bytes{0};
    uint64_t keys{0};
};

/**
 * One outstanding batch write per partition (matching the engine's
 * per-partition write serialization), re-issued on completion until the
 * phase ends. Each completed batch grants its key count as read credits
 * (see TryConsumeReadCredits). Returns total bytes/keys submitted.
 */
StormTotals StormLoop(eloqstore::EloqStore *store)
{
    moodycamel::BlockingConcurrentQueue<StormWriter *> finished;
    std::vector<std::unique_ptr<StormWriter>> writers(FLAGS_partitions);
    for (uint32_t i = 0; i < FLAGS_partitions; i++)
    {
        writers[i] = std::make_unique<StormWriter>(i);
    }
    auto callback = [&finished](eloqstore::KvRequest *req)
    { finished.enqueue(reinterpret_cast<StormWriter *>(req->UserData())); };

    for (auto &w : writers)
    {
        NextStormBatch(*w);
        store->ExecAsyn(&w->request_, uint64_t(w.get()), callback);
    }
    size_t inflight = writers.size();
    while (inflight > 0)
    {
        StormWriter *w;
        finished.wait_dequeue(w);
        CHECK(w->request_.Error() == eloqstore::KvError::NoError)
            << "storm write failed: "
            << eloqstore::ErrorString(w->request_.Error());
        g_read_credits.fetch_add(w->batch_keys_, std::memory_order_relaxed);
        if (g_phase.load(std::memory_order_relaxed) >= 3)
        {
            inflight--;
            continue;
        }
        NextStormBatch(*w);
        store->ExecAsyn(&w->request_, uint64_t(w), callback);
    }
    StormTotals total;
    for (auto &w : writers)
    {
        total.bytes += w->bytes_written_;
        total.keys += w->keys_written_;
    }
    return total;
}

// ------------------------------------------------------------------- load

void Load(eloqstore::EloqStore *store)
{
    const std::string value(FLAGS_val_size, 'L');
    const uint64_t ts = utils::UnixTs<milliseconds>();
    for (uint32_t part = 0; part < FLAGS_partitions; part++)
    {
        for (uint64_t base = 0; base < FLAGS_keys_per_partition;
             base += FLAGS_storm_batch_keys)
        {
            const uint64_t end = std::min<uint64_t>(
                base + FLAGS_storm_batch_keys, FLAGS_keys_per_partition);
            std::vector<eloqstore::WriteDataEntry> entries;
            entries.reserve(end - base);
            for (uint64_t key = base; key < end; key++)
            {
                entries.emplace_back(
                    MakeKey(key), value, ts, eloqstore::WriteOp::Upsert);
            }
            eloqstore::BatchWriteRequest req;
            req.SetArgs(eloqstore::TableIdent(kTable, part),
                        std::move(entries));
            store->ExecSync(&req);
            CHECK(req.Error() == eloqstore::KvError::NoError)
                << "load failed: " << eloqstore::ErrorString(req.Error());
        }
        LOG(INFO) << "loaded partition " << part << " ("
                  << FLAGS_keys_per_partition << " keys)";
    }
}

// ------------------------------------------------------------------ stats

void ReportQosDelta(const char *name,
                    const eloqstore::IoQosStats &begin,
                    const eloqstore::IoQosStats &end,
                    size_t shard,
                    uint32_t data_page_size,
                    double secs)
{
    auto d = [](uint64_t b, uint64_t e) { return e - b; };
    // Budgeted page IO in MB/s over the phase: user data, compaction
    // relocations, and index pages. This is not total device traffic; metadata,
    // manifest, bulk file/snapshot, fdatasync, and segment IO are unbudgeted.
    auto mbps = [&](const eloqstore::IoQosStats::Budget &b,
                    const eloqstore::IoQosStats::Budget &e)
    {
        return secs > 0 ? (d(b.admitted_pages_, e.admitted_pages_) *
                           static_cast<double>(data_page_size)) /
                              (secs * (1 << 20))
                        : 0.0;
    };
    LOG(INFO) << "RESULT qos phase=" << name << " shard=" << shard
              << " read_hwm=" << end.read_.high_watermark_ << " read_blocked="
              << d(begin.read_.blocked_count_, end.read_.blocked_count_)
              << " read_blocked_us="
              << d(begin.read_.blocked_us_, end.read_.blocked_us_)
              << " read_budgeted_page_mbps=" << mbps(begin.read_, end.read_)
              << " bg_read_hwm=" << end.bg_read_.high_watermark_
              << " bg_read_blocked="
              << d(begin.bg_read_.blocked_count_, end.bg_read_.blocked_count_)
              << " bg_read_blocked_us="
              << d(begin.bg_read_.blocked_us_, end.bg_read_.blocked_us_)
              << " bg_read_budgeted_page_mbps="
              << mbps(begin.bg_read_, end.bg_read_)
              << " write_hwm=" << end.write_.high_watermark_
              << " write_blocked="
              << d(begin.write_.blocked_count_, end.write_.blocked_count_)
              << " write_budgeted_page_mbps=" << mbps(begin.write_, end.write_)
              << " fdatasync="
              << d(begin.fdatasync_count_, end.fdatasync_count_)
              << " fdatasync_us=" << d(begin.fdatasync_us_, end.fdatasync_us_);
}

}  // namespace

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    CHECK_GT(FLAGS_partitions, 0u);
    CHECK_GT(FLAGS_read_concurrency, 0u);
    CHECK_GT(FLAGS_storm_span, 0u);
    CHECK_GT(FLAGS_storm_ratio, FLAGS_storm_span)
        << "storm must be a PARTIAL overwrite (span < ratio); a full "
           "overwrite leaves files 100% dead and compaction generates no "
           "read traffic";

    eloqstore::KvOptions options;
    if (int res = options.LoadFromIni(FLAGS_kvoptions.c_str()); res != 0)
    {
        LOG(FATAL) << "Failed to parse " << FLAGS_kvoptions << " at " << res;
    }
    LOG(INFO) << "QoS knobs: max_inflight_read=" << options.max_inflight_read
              << " bg_read_ratio=" << options.bg_read_ratio
              << " max_inflight_write=" << options.max_inflight_write;

    eloqstore::EloqStore store(options);
    if (auto err = store.Start("main", 0); err != eloqstore::KvError::NoError)
    {
        LOG(FATAL) << "Failed to start store: " << eloqstore::ErrorString(err);
    }

    if (FLAGS_load)
    {
        Load(&store);
    }

    const size_t num_shards = store.Options().num_threads;
    std::vector<eloqstore::IoQosStats> qos_start(num_shards);
    std::vector<eloqstore::IoQosStats> qos_mid(num_shards);
    std::vector<eloqstore::IoQosStats> qos_end(num_shards);

    PhaseLatencies baseline, storm_lat;

    // Baseline phase: reads only.
    for (size_t s = 0; s < num_shards; s++)
    {
        qos_start[s] = store.GetIoQosStats(s);
    }
    g_phase.store(1, std::memory_order_relaxed);
    std::thread read_thd(ReadLoop, &store, &baseline, &storm_lat);
    std::this_thread::sleep_for(seconds(FLAGS_baseline_secs));

    // Storm phase: reads + rotating partial-overwrite write storm.
    for (size_t s = 0; s < num_shards; s++)
    {
        qos_mid[s] = store.GetIoQosStats(s);
    }
    g_phase.store(2, std::memory_order_relaxed);
    StormTotals storm_totals;
    std::thread storm_thd([&] { storm_totals = StormLoop(&store); });
    std::this_thread::sleep_for(seconds(FLAGS_storm_secs));

    g_phase.store(3, std::memory_order_relaxed);
    storm_thd.join();
    read_thd.join();
    for (size_t s = 0; s < num_shards; s++)
    {
        qos_end[s] = store.GetIoQosStats(s);
    }

    ReportPhase("baseline", baseline, FLAGS_baseline_secs);
    ReportPhase("mixed", storm_lat, FLAGS_storm_secs);
    const uint64_t read_ops = storm_lat.samples.size();
    const double achieved_write_pct =
        storm_totals.keys + read_ops > 0
            ? 100.0 * storm_totals.keys / (storm_totals.keys + read_ops)
            : 0.0;
    LOG(INFO) << "RESULT mixed write_mb_per_sec="
              << (storm_totals.bytes >> 20) /
                     std::max<uint32_t>(1, FLAGS_storm_secs)
              << " write_mb=" << (storm_totals.bytes >> 20)
              << " write_key_ops=" << storm_totals.keys
              << " read_ops=" << read_ops << " achieved_write_pct="
              << static_cast<int>(achieved_write_pct + 0.5);
    for (size_t s = 0; s < num_shards; s++)
    {
        ReportQosDelta("baseline",
                       qos_start[s],
                       qos_mid[s],
                       s,
                       options.data_page_size,
                       FLAGS_baseline_secs);
        ReportQosDelta("mixed",
                       qos_mid[s],
                       qos_end[s],
                       s,
                       options.data_page_size,
                       FLAGS_storm_secs);
    }

    store.Stop();
    return 0;
}
