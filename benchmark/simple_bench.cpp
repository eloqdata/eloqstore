#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <random>

#include "../coding.h"
#include "../eloq_store.h"
#include "../utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../concurrentqueue/blockingconcurrentqueue.h"

DEFINE_string(kvoptions, "", "Path to config file of EloqStore options");
DEFINE_uint32(kv_size, 128, "size of a pair of KV");
DEFINE_uint32(batch_size, 2048, "number of KVs per batch");
DEFINE_uint32(write_batchs, 4096, "number of batchs to write");
DEFINE_uint32(partitions, 128, "number of partitions");
DEFINE_uint32(max_key, 1000000, "max key limit");
DEFINE_uint32(write_interval, 10, "interval seconds between writes");
DEFINE_uint32(read_per_part, 1, "concurrent read requests per partition");
DEFINE_uint32(read_secs, 600, "read test time");

using namespace std::chrono;

constexpr char table[] = "bm";
std::atomic<bool> stop_{false};

void EncodeKey(char *dst, uint64_t key)
{
    eloqstore::EncodeFixed64(dst, eloqstore::ToBigEndian(key));
}

uint64_t DecodeKey(const std::string &key)
{
    return eloqstore::BigEndianToNative(eloqstore::DecodeFixed64(key.data()));
}

thread_local std::mt19937 rand_gen(0);

static const size_t key_interval = 10;
static const size_t del_ratio = 4;

class Writer
{
public:
    Writer(uint32_t id);
    void NextBatch();

    const uint32_t id_;
    eloqstore::BatchWriteRequest request_;
    size_t writing_key_{0};
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
        finished.enqueue(p);
    };

    auto start = high_resolution_clock::now();
    for (auto &writer : writers)
    {
        store->ExecAsyn(&writer->request_, uint64_t(writer.get()), callback);
    }
    for (size_t i = 0; i < FLAGS_write_batchs;)
    {
        Writer *writer;
        finished.wait_dequeue(writer);

        assert(writer->request_.IsDone());
        assert(writer->request_.Error() == eloqstore::KvError::NoError);
        writer->NextBatch();
        store->ExecAsyn(&writer->request_, uint64_t(writer), callback);

        i++;
        if (i % FLAGS_partitions == 0)
        {
            auto now = high_resolution_clock::now();
            double cost_ms = duration_cast<milliseconds>(now - start).count();
            const uint64_t num_kvs =
                uint64_t(FLAGS_batch_size) * FLAGS_partitions;
            const uint64_t kvs_per_sec = num_kvs * 1000 / cost_ms;
            const uint64_t mb_per_sec = (kvs_per_sec * FLAGS_kv_size) >> 20;
            LOG(INFO) << "write speed " << kvs_per_sec << " kvs/s | cost "
                      << cost_ms << " ms | " << mb_per_sec << " MiB/s";

            if (FLAGS_write_interval > 0)
            {
                std::this_thread::sleep_for(
                    std::chrono::seconds(FLAGS_write_interval));
            }
            start = high_resolution_clock::now();
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
        std::string_view key(key_, sizeof(uint64_t));
        request_.SetArgs(std::move(tbl_id), key);
    };
    const size_t id_;
    uint64_t start_ts_{0};
    uint64_t latency_{0};
    char key_[sizeof(uint64_t)];
    eloqstore::ReadRequest request_;
};

void ReadLoop(eloqstore::EloqStore *store)
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
        finished.enqueue(reader);
    };

    auto send_req = [&store, callback](Reader *reader)
    {
        EncodeKey(reader->key_, rand_gen() % FLAGS_max_key);
        reader->start_ts_ = utils::UnixTs<microseconds>();
        store->ExecAsyn(&reader->request_, uint64_t(reader), callback);
    };

    uint64_t latency_sum = 0;
    uint64_t req_cnt = 0;
    uint64_t max_latency = 0;
    auto start = high_resolution_clock::now();
    for (auto &reader : readers)
    {
        send_req(reader.get());
    }
    while (true)
    {
        Reader *reader;
        finished.wait_dequeue(reader);
        latency_sum += reader->latency_;
        req_cnt++;
        max_latency = std::max(max_latency, reader->latency_);

        send_req(reader);

        if (req_cnt == 200000)
        {
            auto now = high_resolution_clock::now();
            double cost_ms = duration_cast<milliseconds>(now - start).count();
            uint64_t qps = req_cnt * 1000 / cost_ms;
            uint64_t average_latency = latency_sum / req_cnt;
            LOG(INFO) << "read speed " << qps << " QPS | average latency "
                      << average_latency << " microseconds | max latency "
                      << max_latency << " microseconds";

            if (stop_.load(std::memory_order_relaxed))
            {
                break;
            }

            start = high_resolution_clock::now();
            req_cnt = 0;
            latency_sum = 0;
            max_latency = 0;
        }
    }
    for (uint32_t i = 0; i < num_readers; i++)
    {
        Reader *reader;
        finished.wait_dequeue(reader);
    }
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

    if (FLAGS_write_batchs > 0 && FLAGS_read_secs > 0)
    {
        // Hybrid read and write.
        std::thread write_thd(WriteLoop, &store);
        std::thread read_thd(ReadLoop, &store);
        // wait all threads exit
        write_thd.join();
        stop_.store(true, std::memory_order_relaxed);
        read_thd.join();
    }
    else if (FLAGS_write_batchs == 0 && FLAGS_read_secs > 0)
    {
        // Read only
        std::thread read_thd(ReadLoop, &store);
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_read_secs));
        stop_.store(true, std::memory_order_relaxed);
        read_thd.join();
    }
    else if (FLAGS_read_secs == 0 && FLAGS_write_batchs > 0)
    {
        // Write only
        std::thread write_thd(WriteLoop, &store);
        write_thd.join();
    }

    store.Stop();
}