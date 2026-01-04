#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "coding.h"
#include "eloq_store.h"
#include "utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../external/concurrentqueue/blockingconcurrentqueue.h"

DEFINE_string(kvoptions, "", "Path to config file of EloqStore options");
DEFINE_uint32(kv_size, 256, "size of a pair of KV");
DEFINE_uint32(batch_size, 1 << 20, "number of KVs per batch");
DEFINE_uint32(inflight_batchs, 2, "number of inflight batchs");
DEFINE_uint32(write_batchs, 512, "number of batchs to write");
DEFINE_uint32(partitions, 4, "number of partitions");

using namespace std::chrono;

class Writer;
moodycamel::BlockingConcurrentQueue<Writer *> finished_;

constexpr char table[] = "bm";

void EncodeKey(char *dst, uint64_t key)
{
    eloqstore::EncodeFixed64(dst, eloqstore::ToBigEndian(key));
}

uint64_t DecodeKey(const std::string &key)
{
    return eloqstore::BigEndianToNative(eloqstore::DecodeFixed64(key.data()));
}

class Writer
{
public:
    Writer(eloqstore::EloqStore *store, uint32_t id);
    void Begin();
    void Continue();
    static void Callback(eloqstore::KvRequest *req);

private:
    eloqstore::EloqStore *store_;
    const uint32_t id_;
    std::vector<eloqstore::BatchWriteRequest> requests_;
    uint32_t finished_batch_cnt_{0};
};

Writer::Writer(eloqstore::EloqStore *store, uint32_t id)
    : store_(store), id_(id), requests_(FLAGS_inflight_batchs)
{
    eloqstore::TableIdent tbl_id(table, id);
    for (uint32_t batch_idx = 0; auto &req : requests_)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(FLAGS_batch_size);
        for (uint64_t i = 0; i < FLAGS_batch_size; i++)
        {
            std::string key;
            key.resize(sizeof(uint64_t));
            EncodeKey(key.data(), i + batch_idx * FLAGS_batch_size);
            std::string value;
            value.resize(FLAGS_kv_size - sizeof(uint64_t));
            entries.emplace_back(std::move(key),
                                 std::move(value),
                                 0,
                                 eloqstore::WriteOp::Upsert);
        }
        req.SetArgs(tbl_id, std::move(entries));

        batch_idx++;
    }
}

void Writer::Begin()
{
    for (auto &req : requests_)
    {
        bool ok =
            store_->ExecAsyn(&req,
                             uint64_t(this),
                             [](eloqstore::KvRequest *req) { Callback(req); });
        CHECK(ok);
    }
}

void Writer::Continue()
{
    auto &req = requests_[finished_batch_cnt_ % requests_.size()];
    finished_batch_cnt_++;
    assert(req.IsDone());
    assert(req.Error() == eloqstore::KvError::NoError);

    for (auto &entry : req.batch_)
    {
        uint64_t key = DecodeKey(entry.key_);
        key += FLAGS_batch_size * FLAGS_inflight_batchs;
        EncodeKey(entry.key_.data(), key);
        entry.timestamp_ = finished_batch_cnt_;
    }

    bool ok = store_->ExecAsyn(
        &req, uint64_t(this), [](eloqstore::KvRequest *req) { Callback(req); });
    CHECK(ok);
}

void Writer::Callback(eloqstore::KvRequest *req)
{
    Writer *p = (Writer *) (req->UserData());
    finished_.enqueue(p);
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

    for (auto &path : options.store_path)
    {
        if (std::filesystem::exists(path) && utils::DirEntryCount(path) > 0)
        {
            LOG(WARNING) << path
                         << " is not empty, you may testing update "
                            "performance, not insert performance.";
        }
    }

    eloqstore::EloqStore store(options);
    store.Start();

    std::vector<Writer> writers;
    for (uint32_t i = 0; i < FLAGS_partitions; i++)
    {
        writers.emplace_back(&store, i);
    }

    const uint64_t batch_bytes = FLAGS_kv_size * FLAGS_batch_size;
    const auto start = high_resolution_clock::now();
    auto last_time = start;
    for (auto &writer : writers)
    {
        writer.Begin();
    }
    for (size_t i = 0; i < FLAGS_write_batchs;)
    {
        Writer *writer;
        finished_.wait_dequeue(writer);
        writer->Continue();
        i++;

        if (i % FLAGS_partitions == 0)
        {
            auto now = high_resolution_clock::now();
            double cost_ms =
                duration_cast<milliseconds>(now - last_time).count();
            const uint64_t write_bytes = batch_bytes * FLAGS_partitions;
            const uint64_t speed = write_bytes * 1000 / cost_ms;
            const uint64_t mb_per_sec = speed >> 20;
            const uint64_t num_kvs =
                static_cast<uint64_t>(FLAGS_batch_size) * FLAGS_partitions;
            const uint64_t kvs_per_sec = num_kvs * 1000 / cost_ms;
            LOG(INFO) << "write speed " << speed << " bytes/s | " << mb_per_sec
                      << " MiB/s | " << kvs_per_sec << " kvs/s";
            last_time = now;

            // Output the current progress at regular intervals.
            std::cout << "Progress[" << i << '/' << FLAGS_write_batchs << "]\r"
                      << std::flush;
        }
    }
    auto cost = high_resolution_clock::now() - start;
    double cost_ms = duration_cast<milliseconds>(cost).count();
    std::cout << "\rBenchmark result:" << std::endl;
    std::cout << "Time spent " << cost_ms << " ms" << std::endl;
    uint64_t write_bytes = batch_bytes * FLAGS_write_batchs;
    uint64_t num_kvs =
        static_cast<uint64_t>(FLAGS_batch_size) * FLAGS_write_batchs;
    std::cout << "Total write " << write_bytes << " bytes, " << num_kvs
              << " kvs" << std::endl;
    const uint64_t speed = write_bytes * 1000 / cost_ms;
    const uint64_t mb_per_sec = speed >> 20;
    const uint64_t kvs_per_sec = num_kvs * 1000 / cost_ms;
    std::cout << "Average write speed " << speed << " bytes/s | " << mb_per_sec
              << " MiB/s | " << kvs_per_sec << " kvs/s" << std::endl;

    store.Stop();
}
