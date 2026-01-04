#include <bvar/bvar.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <utility>
#include <vector>

#include "coding.h"
#include "eloq_store.h"
#include "utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../external/concurrentqueue/blockingconcurrentqueue.h"

DEFINE_string(kvoptions, "", "Path to config file of EloqStore options");
DEFINE_string(workload, "", "workload (write/read/scan/write-read/write-scan)");
DEFINE_uint32(kv_size, 128, "size of a pair of KV");
DEFINE_uint32(batch_size, 8192, "number of KVs per batch");
DEFINE_uint32(write_batchs, 32768, "number of batchs to write");
DEFINE_uint32(partitions, 128, "number of partitions");
DEFINE_uint32(max_key, 1000000, "max key limit");
DEFINE_uint32(write_interval, 0, "interval seconds between writes");
DEFINE_uint32(read_per_part, 1, "concurrent read/scan requests per partition");
DEFINE_uint32(test_secs, 60, "read/scan test time");
DEFINE_uint32(read_thds, 1, "number of client threads send read/scan requests");
DEFINE_bool(load,
            false,
            "load data sequentially for later tests (step=1, upsert-only)");
DEFINE_double(write_stats_interval_sec,
              1.0,
              "minimum seconds between periodic write performance logs");
DEFINE_double(read_stats_interval_sec,
              1.0,
              "minimum seconds between read latency logs");
DEFINE_double(scan_stats_interval_sec,
              1.0,
              "minimum seconds between scan latency logs");
DEFINE_uint64(scan_bytes, 0, "bytes to scan per request; 0 scans to the end");
DEFINE_bool(show_write_perf,
            false,
            "when true, print periodic and summary write throughput");

using namespace std::chrono;

constexpr char table[] = "bm";
std::atomic<bool> stop_{false};
bvar::LatencyRecorder g_write_latency("simple_bench_write");
bvar::LatencyRecorder g_read_latency("simple_bench_read");
bvar::LatencyRecorder g_scan_latency("simple_bench_scan");
std::atomic<uint64_t> g_write_max_latency{0};
std::atomic<uint64_t> g_read_max_latency{0};
std::atomic<uint64_t> g_scan_max_latency{0};

void UpdateMaxLatency(std::atomic<uint64_t> &target, uint64_t latency)
{
    uint64_t previous = target.load(std::memory_order_relaxed);
    while (previous < latency &&
           !target.compare_exchange_weak(
               previous, latency, std::memory_order_relaxed))
    {
    }
}

void EncodeKey(char *dst, uint64_t key)
{
    eloqstore::EncodeFixed64(dst, eloqstore::ToBigEndian(key));
}

uint64_t DecodeKey(const std::string &key)
{
    return eloqstore::BigEndianToNative(eloqstore::DecodeFixed64(key.data()));
}

thread_local std::mt19937 rand_gen(0);

static constexpr size_t key_interval = 4;
static constexpr size_t del_ratio = 4;
static constexpr double upsert_ratio = 1 - (1.0 / del_ratio);

class Writer
{
public:
    Writer(uint32_t id);
    void NextBatch();

    const uint32_t id_;
    eloqstore::BatchWriteRequest request_;
    size_t writing_key_{0};
    uint64_t start_ts_{0};
    uint64_t latency_{0};
};

Writer::Writer(uint32_t id) : id_(id)
{
    eloqstore::TableIdent tbl_id(table, id);
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.reserve(FLAGS_batch_size);
    uint64_t ts = utils::UnixTs<milliseconds>();
    for (uint64_t i = 0; i < FLAGS_batch_size; i++)
    {
        std::string key;
        key.resize(sizeof(uint64_t));
        EncodeKey(key.data(), writing_key_);
        writing_key_ += FLAGS_load ? 1 : ((rand_gen() % key_interval) + 1);
        std::string value;
        value.resize(FLAGS_kv_size - sizeof(uint64_t));
        if (!FLAGS_load && (rand_gen() % del_ratio == 0))
        {
            entries.emplace_back(std::move(key),
                                 std::move(value),
                                 ts,
                                 eloqstore::WriteOp::Delete);
        }
        else
        {
            entries.emplace_back(std::move(key),
                                 std::move(value),
                                 ts,
                                 eloqstore::WriteOp::Upsert);
        }
    }
    request_.SetArgs(tbl_id, std::move(entries));
    if (writing_key_ > FLAGS_max_key)
    {
        writing_key_ = 0;
    }
}

void Writer::NextBatch()
{
    uint64_t ts = utils::UnixTs<milliseconds>();
    for (auto &entry : request_.batch_)
    {
        writing_key_ += FLAGS_load ? 1 : ((rand_gen() % key_interval) + 1);
        EncodeKey(entry.key_.data(), writing_key_);
        entry.timestamp_ = ts;
        if (!FLAGS_load && (rand_gen() % del_ratio == 0))
        {
            entry.op_ = eloqstore::WriteOp::Delete;
        }
        else
        {
            entry.op_ = eloqstore::WriteOp::Upsert;
        }
    }
    if (writing_key_ > FLAGS_max_key)
    {
        writing_key_ = 0;
    }
}

void WriteLoop(eloqstore::EloqStore *store)
{
    moodycamel::BlockingConcurrentQueue<Writer *> finished;
    std::vector<std::unique_ptr<Writer>> writers(FLAGS_partitions);
    for (uint32_t i = 0; i < FLAGS_partitions; i++)
    {
        writers[i] = std::make_unique<Writer>(i);
    }

    auto callback = [&finished](eloqstore::KvRequest *req)
    {
        Writer *p = (Writer *) (req->UserData());
        p->latency_ = utils::UnixTs<microseconds>() - p->start_ts_;
        finished.enqueue(p);
    };

    bool recorded_write_latency = false;
    auto total_start = high_resolution_clock::now();
    auto window_start = total_start;
    const double min_window_ms =
        std::max(1.0, FLAGS_write_stats_interval_sec * 1000.0);
    for (auto &writer : writers)
    {
        writer->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&writer->request_, uint64_t(writer.get()), callback);
    }
    for (size_t i = 0; i < FLAGS_write_batchs;)
    {
        Writer *writer;
        finished.wait_dequeue(writer);

        assert(writer->request_.IsDone());
        assert(writer->request_.Error() == eloqstore::KvError::NoError);
        g_write_latency << static_cast<int64_t>(writer->latency_);
        UpdateMaxLatency(g_write_max_latency, writer->latency_);
        recorded_write_latency = true;
        writer->NextBatch();
        writer->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&writer->request_, uint64_t(writer), callback);

        i++;
        if (i % FLAGS_partitions == 0)
        {
            auto now = high_resolution_clock::now();
            double cost_ms =
                duration_cast<milliseconds>(now - window_start).count();
            if (cost_ms < min_window_ms)
            {
                continue;
            }
            const uint64_t num_kvs =
                uint64_t(FLAGS_batch_size) * FLAGS_partitions;
            const uint64_t kvs_per_sec = num_kvs * 1000 / cost_ms;
            const double upsert_ratio_current = FLAGS_load ? 1.0 : upsert_ratio;
            const uint64_t mb_per_sec =
                (static_cast<uint64_t>(kvs_per_sec * upsert_ratio_current) *
                 FLAGS_kv_size) >>
                20;
            if (FLAGS_show_write_perf)
            {
                LOG(INFO) << "write speed " << kvs_per_sec << " kvs/s | cost "
                          << cost_ms << " ms | " << mb_per_sec << " MiB/s";
            }

            if (FLAGS_write_interval > 0)
            {
                std::this_thread::sleep_for(
                    std::chrono::seconds(FLAGS_write_interval));
            }
            window_start = high_resolution_clock::now();
        }
    }
    auto total_end = high_resolution_clock::now();
    double total_cost_ms =
        duration_cast<milliseconds>(total_end - total_start).count();
    if (total_cost_ms <= 0.0)
    {
        total_cost_ms = 1.0;
    }
    if (recorded_write_latency)
    {
        const uint64_t num_kvs =
            static_cast<uint64_t>(FLAGS_batch_size) * FLAGS_write_batchs;
        const uint64_t kvs_per_sec = num_kvs * 1000 / total_cost_ms;
        const double upsert_ratio_current = FLAGS_load ? 1.0 : upsert_ratio;
        const uint64_t mb_per_sec =
            (static_cast<uint64_t>(kvs_per_sec * upsert_ratio_current) *
             FLAGS_kv_size) >>
            20;
        const auto average = static_cast<uint64_t>(g_write_latency.latency());
        const auto p50 =
            static_cast<uint64_t>(g_write_latency.latency_percentile(0.50));
        const auto p90 =
            static_cast<uint64_t>(g_write_latency.latency_percentile(0.90));
        const auto p99 =
            static_cast<uint64_t>(g_write_latency.latency_percentile(0.99));
        const auto p999 =
            static_cast<uint64_t>(g_write_latency.latency_percentile(0.999));
        const auto p9999 =
            static_cast<uint64_t>(g_write_latency.latency_percentile(0.9999));
        const auto max_latency =
            g_write_max_latency.load(std::memory_order_relaxed);
        if (FLAGS_show_write_perf)
        {
            LOG(INFO) << "write summary " << kvs_per_sec << " kvs/s | cost "
                      << total_cost_ms << " ms | " << mb_per_sec
                      << " MiB/s | average latency " << average
                      << " microseconds | p50 " << p50 << " microseconds | p90 "
                      << p90 << " microseconds | p99 " << p99
                      << " microseconds | p99.9 " << p999
                      << " microseconds | p99.99 " << p9999
                      << " microseconds | max latency " << max_latency
                      << " microseconds";
        }
    }
    for (uint32_t i = 0; i < FLAGS_partitions; i++)
    {
        Writer *writer;
        finished.wait_dequeue(writer);
    }
}

class Reader
{
public:
    Reader(size_t id) : id_(id)
    {
        eloqstore::TableIdent tbl_id(table, id % FLAGS_partitions);
        EncodeKey(key_, 0);
        std::string_view key(key_, sizeof(uint64_t));
        request_.SetArgs(std::move(tbl_id), key);
    };
    const size_t id_;
    uint64_t start_ts_{0};
    uint64_t latency_{0};
    char key_[sizeof(uint64_t)];
    eloqstore::ReadRequest request_;
    size_t read_cnt_{0};
};

void ReadLoop(eloqstore::EloqStore *store, uint32_t thd_id)
{
    moodycamel::BlockingConcurrentQueue<Reader *> finished;
    const size_t num_readers = FLAGS_read_per_part * FLAGS_partitions;
    std::vector<std::unique_ptr<Reader>> readers(num_readers);
    for (uint32_t i = 0; i < num_readers; i++)
    {
        readers[i] = std::make_unique<Reader>(i);
    }

    auto callback = [&finished](eloqstore::KvRequest *req)
    {
        Reader *reader = (Reader *) (req->UserData());
        reader->latency_ = utils::UnixTs<microseconds>() - reader->start_ts_;
        assert(req->Error() == eloqstore::KvError::NoError ||
               req->Error() == eloqstore::KvError::NotFound);
        reader->read_cnt_++;
        finished.enqueue(reader);
    };

    auto send_req = [&store, callback](Reader *reader)
    {
        EncodeKey(reader->key_, rand_gen() % FLAGS_max_key);
        reader->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&reader->request_, uint64_t(reader), callback);
    };

    const auto start = high_resolution_clock::now();
    auto last_log = start;
    const double read_interval_ms =
        std::max(1.0, FLAGS_read_stats_interval_sec * 1000.0);
    for (auto &reader : readers)
    {
        send_req(reader.get());
    }
    while (true)
    {
        Reader *reader;
        finished.wait_dequeue(reader);
        g_read_latency << static_cast<int64_t>(reader->latency_);
        UpdateMaxLatency(g_read_max_latency, reader->latency_);

        send_req(reader);

        auto now = high_resolution_clock::now();
        double cost_ms = duration_cast<milliseconds>(now - last_log).count();
        if (cost_ms < read_interval_ms)
        {
            continue;
        }

        const double qps = g_read_latency.qps();
        const auto average = static_cast<uint64_t>(g_read_latency.latency());
        const auto p50 =
            static_cast<uint64_t>(g_read_latency.latency_percentile(0.50));
        const auto p90 =
            static_cast<uint64_t>(g_read_latency.latency_percentile(0.90));
        const auto p99 =
            static_cast<uint64_t>(g_read_latency.latency_percentile(0.99));
        const auto p999 =
            static_cast<uint64_t>(g_read_latency.latency_percentile(0.999));
        const auto p9999 =
            static_cast<uint64_t>(g_read_latency.latency_percentile(0.9999));
        const auto max_latency =
            g_read_max_latency.load(std::memory_order_relaxed);
        LOG(INFO) << "[" << thd_id << "]read speed " << qps
                  << " QPS | average latency " << average
                  << " microseconds | p50 " << p50 << " microseconds | p90 "
                  << p90 << " microseconds | p99 " << p99
                  << " microseconds | p99.9 " << p999
                  << " microseconds | p99.99 " << p9999
                  << " microseconds | max latency " << max_latency
                  << " microseconds";

        last_log = now;
        if (stop_.load(std::memory_order_relaxed))
        {
            break;
        }
    }
    size_t total_kvs = 0;
    for (uint32_t i = 0; i < num_readers; i++)
    {
        Reader *reader;
        finished.wait_dequeue(reader);
        total_kvs += reader->read_cnt_;
    }
    auto now = high_resolution_clock::now();
    double cost_ms = duration_cast<milliseconds>(now - start).count();
    uint64_t qps = total_kvs * 1000 / cost_ms;
    LOG(INFO) << "[" << thd_id << "]read average " << qps << " QPS";
}

class Scanner
{
public:
    Scanner(size_t id) : id_(id)
    {
        eloqstore::TableIdent tbl_id(table, id % FLAGS_partitions);
        request_.SetPagination(page_size, 0);
        EncodeKey(begin_key_, 0);
        EncodeKey(end_key_, 0);
        request_.SetTableId(std::move(tbl_id));
    };
    static const size_t page_size = 256;
    const size_t id_;
    uint64_t start_ts_{0};
    uint64_t latency_{0};
    char begin_key_[sizeof(uint64_t)];
    char end_key_[sizeof(uint64_t)];
    eloqstore::ScanRequest request_;
    size_t kvs_cnt_{0};
    size_t last_entries_{0};
};

void ScanLoop(eloqstore::EloqStore *store, uint32_t thd_id)
{
    moodycamel::BlockingConcurrentQueue<Scanner *> finished;
    const size_t num_scanners = FLAGS_read_per_part * FLAGS_partitions;
    std::vector<std::unique_ptr<Scanner>> scanners(num_scanners);
    for (uint32_t i = 0; i < num_scanners; i++)
    {
        scanners[i] = std::make_unique<Scanner>(i);
    }

    auto callback = [&finished](eloqstore::KvRequest *req)
    {
        auto scan_req = static_cast<eloqstore::ScanRequest *>(req);
        Scanner *reader = (Scanner *) (req->UserData());
        reader->latency_ = utils::UnixTs<microseconds>() - reader->start_ts_;
        const size_t cnt = scan_req->Entries().size();
        reader->last_entries_ = cnt;
        reader->kvs_cnt_ += cnt;
        assert(scan_req->Error() == eloqstore::KvError::NoError ||
               scan_req->Error() == eloqstore::KvError::NotFound);
        finished.enqueue(reader);
    };

    auto send_req = [&store, callback](Scanner *scanner)
    {
        const uint64_t val_size = FLAGS_kv_size - sizeof(uint64_t);
        if (FLAGS_scan_bytes == 0)
        {
            uint64_t start = rand_gen() % FLAGS_max_key;
            EncodeKey(scanner->begin_key_, start);
            scanner->request_.SetArgs(
                scanner->request_.TableId(),
                std::string_view(scanner->begin_key_, sizeof(uint64_t)),
                std::string_view{} /* end empty scans to tail */);
        }
        else
        {
            uint64_t scan_kvs =
                std::max<uint64_t>(1, FLAGS_scan_bytes / val_size);
            uint64_t max_start =
                (FLAGS_max_key > scan_kvs) ? (FLAGS_max_key - scan_kvs) : 0;
            uint64_t start = (max_start > 0) ? (rand_gen() % max_start) : 0;
            uint64_t end = start + scan_kvs;
            EncodeKey(scanner->begin_key_, start);
            EncodeKey(scanner->end_key_, end);
            scanner->request_.SetArgs(
                scanner->request_.TableId(),
                std::string_view(scanner->begin_key_, sizeof(uint64_t)),
                std::string_view(scanner->end_key_, sizeof(uint64_t)));
        }

        scanner->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&scanner->request_, uint64_t(scanner), callback);
    };

    const auto start = high_resolution_clock::now();
    auto last_time = high_resolution_clock::now();
    uint64_t window_kvs = 0;
    const double scan_interval_ms =
        std::max(1.0, FLAGS_scan_stats_interval_sec * 1000.0);
    for (auto &scanner : scanners)
    {
        send_req(scanner.get());
    }
    while (true)
    {
        Scanner *scanner;
        finished.wait_dequeue(scanner);
        g_scan_latency << static_cast<int64_t>(scanner->latency_);
        UpdateMaxLatency(g_scan_max_latency, scanner->latency_);
        window_kvs += scanner->last_entries_;

        send_req(scanner);

        auto now = high_resolution_clock::now();
        double cost_ms = duration_cast<milliseconds>(now - last_time).count();
        if (cost_ms < scan_interval_ms)
        {
            continue;
        }
        if (cost_ms <= 0.0)
        {
            cost_ms = 1.0;
        }
        const double qps = g_scan_latency.qps();
        uint64_t kvps = window_kvs * 1000 / cost_ms;
        uint64_t mb_per_sec = (kvps * FLAGS_kv_size) >> 20;
        const auto average = static_cast<uint64_t>(g_scan_latency.latency());
        const auto p50 =
            static_cast<uint64_t>(g_scan_latency.latency_percentile(0.50));
        const auto p90 =
            static_cast<uint64_t>(g_scan_latency.latency_percentile(0.90));
        const auto p99 =
            static_cast<uint64_t>(g_scan_latency.latency_percentile(0.99));
        const auto p999 =
            static_cast<uint64_t>(g_scan_latency.latency_percentile(0.999));
        const auto p9999 =
            static_cast<uint64_t>(g_scan_latency.latency_percentile(0.9999));
        const auto max_latency =
            g_scan_max_latency.load(std::memory_order_relaxed);
        LOG(INFO) << "[" << thd_id << "]scan speed " << mb_per_sec << " MB/s | "
                  << kvps << " KVs/s | " << qps << " QPS | average latency "
                  << average << " microseconds | p50 " << p50
                  << " microseconds | p90 " << p90 << " microseconds | p99 "
                  << p99 << " microseconds | p99.9 " << p999
                  << " microseconds | p99.99 " << p9999
                  << " microseconds | max latency " << max_latency
                  << " microseconds";

        last_time = now;
        window_kvs = 0;
        if (stop_.load(std::memory_order_relaxed))
        {
            break;
        }
    }
    size_t total_kvs = 0;
    for (uint32_t i = 0; i < num_scanners; i++)
    {
        Scanner *scanner;
        finished.wait_dequeue(scanner);
        total_kvs += scanner->kvs_cnt_;
    }
    auto now = high_resolution_clock::now();
    double cost_ms = duration_cast<milliseconds>(now - start).count();
    uint64_t kvps = total_kvs * 1000 / cost_ms;
    uint64_t mb_per_sec = (kvps * FLAGS_kv_size) >> 20;
    LOG(INFO) << "[" << thd_id << "]scan throughput " << mb_per_sec << " MB/s "
              << kvps << " kvs/s";
}

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    CHECK(FLAGS_kv_size > sizeof(uint64_t));
    CHECK_GT(FLAGS_batch_size, 0);

    if (FLAGS_load)
    {
        const uint64_t batches_per_partition =
            (static_cast<uint64_t>(FLAGS_max_key) + FLAGS_batch_size - 1) /
            static_cast<uint64_t>(FLAGS_batch_size);
        uint64_t desired_batches =
            std::max<uint64_t>(batches_per_partition, 1) *
            static_cast<uint64_t>(FLAGS_partitions);
        if (desired_batches > std::numeric_limits<uint32_t>::max())
        {
            LOG(FATAL) << "load requires write_batchs=" << desired_batches
                       << ", which exceeds uint32_t::max. Reduce max_key or "
                          "batch size.";
        }
        FLAGS_write_batchs = static_cast<uint32_t>(desired_batches);
        FLAGS_show_write_perf = true;
        FLAGS_workload = "load";
        LOG(INFO) << "load=1, forcing workload=load and write_batchs set to "
                  << FLAGS_write_batchs << " (" << batches_per_partition
                  << " batches per partition to cover keys up to "
                  << FLAGS_max_key << ")";
    }

    eloqstore::KvOptions options;
    if (int res = options.LoadFromIni(FLAGS_kvoptions.c_str()); res != 0)
    {
        LOG(FATAL) << "Failed to parse " << FLAGS_kvoptions << " at " << res;
    }

    eloqstore::EloqStore store(options);
    store.Start();

    if (FLAGS_workload == "load")
    {
        std::thread load_thd(WriteLoop, &store);
        load_thd.join();
    }
    else if (FLAGS_workload == "write-read")
    {
        // Hybrid write and read.
        std::thread write_thd(WriteLoop, &store);
        std::thread read_thd(ReadLoop, &store, 0);
        // wait all threads exit
        write_thd.join();
        stop_.store(true, std::memory_order_relaxed);
        read_thd.join();
    }
    else if (FLAGS_workload == "write-scan")
    {
        // Hybrid write and scan.
        std::thread write_thd(WriteLoop, &store);
        std::thread scan_thd(ScanLoop, &store, 0);
        // wait all threads exit
        write_thd.join();
        stop_.store(true, std::memory_order_relaxed);
        scan_thd.join();
    }
    else if (FLAGS_workload == "write")
    {
        // Write only
        std::thread write_thd(WriteLoop, &store);
        write_thd.join();
    }
    else if (FLAGS_workload == "read")
    {
        // Read only
        std::vector<std::thread> read_thds;
        for (size_t i = 0; i < FLAGS_read_thds; i++)
        {
            read_thds.emplace_back(ReadLoop, &store, i);
        }
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_test_secs));
        stop_.store(true, std::memory_order_relaxed);
        for (auto &thd : read_thds)
        {
            thd.join();
        }
    }
    else if (FLAGS_workload == "scan")
    {
        // Scan only
        std::vector<std::thread> scan_thds;
        for (size_t i = 0; i < FLAGS_read_thds; i++)
        {
            scan_thds.emplace_back(ScanLoop, &store, i);
        }
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_test_secs));
        stop_.store(true, std::memory_order_relaxed);
        for (auto &thd : scan_thds)
        {
            thd.join();
        }
    }

    store.Stop();
}
