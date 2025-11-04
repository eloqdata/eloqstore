#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../coding.h"
#include "../eloq_store.h"
#include "../utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../concurrentqueue/blockingconcurrentqueue.h"

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

using namespace std::chrono;

constexpr char table[] = "bm";
std::atomic<bool> stop_{false};

struct LatencyMetrics
{
    uint64_t average{0};
    uint64_t p50{0};
    uint64_t p90{0};
    uint64_t p99{0};
    uint64_t p999{0};
    uint64_t max{0};
};

LatencyMetrics CalculateLatencyMetrics(const std::vector<uint64_t> &samples)
{
    LatencyMetrics metrics;
    if (samples.empty())
    {
        return metrics;
    }

    uint64_t sum = std::accumulate(samples.begin(), samples.end(), uint64_t{0});
    std::vector<uint64_t> sorted(samples);
    std::sort(sorted.begin(), sorted.end());
    auto quantile_index = [&sorted](double quantile)
    {
        size_t idx = static_cast<size_t>(
            quantile * static_cast<double>(sorted.size() - 1));
        if (idx >= sorted.size())
        {
            idx = sorted.size() - 1;
        }
        return idx;
    };

    metrics.average = sum / sorted.size();
    metrics.p50 = sorted[quantile_index(0.5)];
    metrics.p90 = sorted[quantile_index(0.9)];
    metrics.p99 = sorted[quantile_index(0.99)];
    metrics.p999 = sorted[quantile_index(0.999)];
    metrics.max = sorted.back();
    return metrics;
}

static constexpr size_t kReadLatencyWindow = 500000;
static constexpr size_t kScanLatencyWindow = 50000;

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
        writing_key_ += (rand_gen() % key_interval) + 1;
        std::string value;
        value.resize(FLAGS_kv_size - sizeof(uint64_t));
        if (rand_gen() % del_ratio == 0)
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
        writing_key_ += (rand_gen() % key_interval) + 1;
        EncodeKey(entry.key_.data(), writing_key_);
        entry.timestamp_ = ts;
        if (rand_gen() % del_ratio == 0)
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

    std::vector<uint64_t> latencies_total;
    latencies_total.reserve(FLAGS_write_batchs + FLAGS_partitions);
    auto total_start = high_resolution_clock::now();
    auto window_start = total_start;
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
        latencies_total.push_back(writer->latency_);
        writer->NextBatch();
        writer->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&writer->request_, uint64_t(writer), callback);

        i++;
        if (i % FLAGS_partitions == 0)
        {
            auto now = high_resolution_clock::now();
            double cost_ms =
                duration_cast<milliseconds>(now - window_start).count();
            if (cost_ms <= 0.0)
            {
                cost_ms = 1.0;
            }
            const uint64_t num_kvs =
                uint64_t(FLAGS_batch_size) * FLAGS_partitions;
            const uint64_t kvs_per_sec = num_kvs * 1000 / cost_ms;
            const uint64_t mb_per_sec =
                (static_cast<uint64_t>(kvs_per_sec * upsert_ratio) *
                 FLAGS_kv_size) >>
                20;
            LOG(INFO) << "write speed " << kvs_per_sec << " kvs/s | cost "
                      << cost_ms << " ms | " << mb_per_sec << " MiB/s";

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
    if (!latencies_total.empty())
    {
        const uint64_t num_kvs =
            static_cast<uint64_t>(FLAGS_batch_size) * FLAGS_write_batchs;
        const uint64_t kvs_per_sec = num_kvs * 1000 / total_cost_ms;
        const uint64_t mb_per_sec =
            (static_cast<uint64_t>(kvs_per_sec * upsert_ratio) *
             FLAGS_kv_size) >>
            20;
        LatencyMetrics metrics = CalculateLatencyMetrics(latencies_total);
        LOG(INFO) << "write summary " << kvs_per_sec << " kvs/s | cost "
                  << total_cost_ms << " ms | " << mb_per_sec
                  << " MiB/s | average latency " << metrics.average
                  << " microseconds | p50 " << metrics.p50
                  << " microseconds | p90 " << metrics.p90
                  << " microseconds | p99 " << metrics.p99
                  << " microseconds | p99.9 " << metrics.p999
                  << " microseconds | max latency " << metrics.max
                  << " microseconds";
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

    std::vector<uint64_t> latencies;
    latencies.reserve(kReadLatencyWindow);
    const auto start = high_resolution_clock::now();
    auto last_time = high_resolution_clock::now();
    for (auto &reader : readers)
    {
        send_req(reader.get());
    }
    while (true)
    {
        Reader *reader;
        finished.wait_dequeue(reader);
        latencies.push_back(reader->latency_);

        send_req(reader);

        if (latencies.size() == kReadLatencyWindow)
        {
            auto now = high_resolution_clock::now();
            double cost_ms =
                duration_cast<milliseconds>(now - last_time).count();
            uint64_t qps = latencies.size() * 1000 / cost_ms;
            LatencyMetrics metrics = CalculateLatencyMetrics(latencies);
            LOG(INFO) << "[" << thd_id << "]read speed " << qps
                      << " QPS | average latency " << metrics.average
                      << " microseconds | p50 " << metrics.p50
                      << " microseconds | p90 " << metrics.p90
                      << " microseconds | p99 " << metrics.p99
                      << " microseconds | p99.9 " << metrics.p999
                      << " microseconds | max latency " << metrics.max
                      << " microseconds";

            if (stop_.load(std::memory_order_relaxed))
            {
                break;
            }

            last_time = high_resolution_clock::now();
            latencies.clear();
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
        std::string_view key(begin_key_, sizeof(uint64_t));
        request_.SetArgs(std::move(tbl_id), key, {});
    };
    static const size_t page_size = 256;
    const size_t id_;
    uint64_t start_ts_{0};
    uint64_t latency_{0};
    char begin_key_[sizeof(uint64_t)];
    eloqstore::ScanRequest request_;
    size_t kvs_cnt_{0};
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
        reader->kvs_cnt_ += scan_req->Entries().size();
        assert(scan_req->Error() == eloqstore::KvError::NoError ||
               scan_req->Error() == eloqstore::KvError::NotFound);
        finished.enqueue(reader);
    };

    auto send_req = [&store, callback](Scanner *scanner)
    {
        EncodeKey(scanner->begin_key_, rand_gen() % FLAGS_max_key);
        scanner->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&scanner->request_, uint64_t(scanner), callback);
    };

    std::vector<uint64_t> latencies;
    latencies.reserve(kScanLatencyWindow);
    const auto start = high_resolution_clock::now();
    auto last_time = high_resolution_clock::now();
    for (auto &scanner : scanners)
    {
        send_req(scanner.get());
    }
    while (true)
    {
        Scanner *scanner;
        finished.wait_dequeue(scanner);
        latencies.push_back(scanner->latency_);

        send_req(scanner);

        if (latencies.size() == kScanLatencyWindow)
        {
            auto now = high_resolution_clock::now();
            double cost_ms =
                duration_cast<milliseconds>(now - last_time).count();
            uint64_t qps = latencies.size() * 1000 / cost_ms;
            LatencyMetrics metrics = CalculateLatencyMetrics(latencies);
            uint64_t mb_per_sec =
                (qps * Scanner::page_size * FLAGS_kv_size) >> 20;
            LOG(INFO) << "[" << thd_id << "]scan speed " << mb_per_sec
                      << " MB/s " << qps << " QPS | average latency "
                      << metrics.average << " microseconds | p50 "
                      << metrics.p50 << " microseconds | p90 " << metrics.p90
                      << " microseconds | p99 " << metrics.p99
                      << " microseconds | p99.9 " << metrics.p999
                      << " microseconds | max latency " << metrics.max
                      << " microseconds";

            if (stop_.load(std::memory_order_relaxed))
            {
                break;
            }

            last_time = high_resolution_clock::now();
            latencies.clear();
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
    uint64_t mb_per_sec = (kvps * Scanner::page_size * FLAGS_kv_size) >> 20;
    LOG(INFO) << "[" << thd_id << "]scan throughput " << mb_per_sec << " MB/s "
              << kvps << " kvs/s";
}

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    CHECK(FLAGS_kv_size > sizeof(uint64_t));

    eloqstore::KvOptions options;
    if (int res = options.LoadFromIni(FLAGS_kvoptions.c_str()); res != 0)
    {
        LOG(FATAL) << "Failed to parse " << FLAGS_kvoptions << " at " << res;
    }

    eloqstore::EloqStore store(options);
    store.Start();

    if (FLAGS_workload == "write-read")
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
