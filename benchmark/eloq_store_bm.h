#pragma once

#include <glog/logging.h>

#include <condition_variable>
#include <iomanip>
#include <list>

#include "eloq_store.h"
#include "obj_gen.h"

namespace EloqStoreBM
{
#define SET_CMD_IDX 0
#define GET_CMD_IDX 2

enum struct key_pattern_index : uint8_t
{
    key_pattern_set = 0,
    key_pattern_delimiter = 1,
    key_pattern_get = 2
};

// Utility function to get the object iterator type based on the config
inline int8_t obj_iter_type(const std::string &key_pattern, uint8_t index)
{
    if (key_pattern[index] == 'R')
    {
        return OBJECT_GENERATOR_KEY_RANDOM;
    }
    else if (key_pattern[index] == 'G')
    {
        return OBJECT_GENERATOR_KEY_GAUSSIAN;
    }
    else
    {
        return (index ==
                static_cast<uint8_t>(key_pattern_index::key_pattern_set))
                   ? OBJECT_GENERATOR_KEY_SET_ITER
                   : OBJECT_GENERATOR_KEY_GET_ITER;
    }
}

inline void test_random_key_value(uint32_t key_size,
                                  uint32_t val_size,
                                  const std::string &key_prefix,
                                  uint32_t key_min,
                                  uint32_t key_max,
                                  const std::string &key_pattern,
                                  size_t key_cnt)
{
    object_generator obj_gen;
    obj_gen.set_random_data(true);
    obj_gen.set_key_size(key_size);
    obj_gen.set_data_size_fixed(val_size);
    obj_gen.set_key_prefix(key_prefix.data());
    obj_gen.set_key_range(key_min, key_max);

    int8_t iter = obj_iter_type(key_pattern, SET_CMD_IDX);

    for (size_t i = 0; i < key_cnt; ++i)
    {
        uint64_t key_index = obj_gen.get_key_index(iter);
        obj_gen.generate_key(key_index);

        uint32_t value_len = 0;
        const char *buf = obj_gen.get_value(key_index, &value_len);
        std::ostringstream oss, key_oss;
        oss << std::hex << std::setfill('0');
        for (size_t j = 0; j < value_len; ++j)
        {
            oss << std::setw(2)
                << static_cast<unsigned>(static_cast<uint8_t>(buf[j]));
        }

        const char *key = obj_gen.get_key();
        std::hash<std::string> str_hasher;
        size_t hash_val = str_hasher(key);
        uint32_t key_len = obj_gen.get_key_len();
        key_oss << std::hex << std::setfill('0');
        for (size_t j = 0; j < key_len; ++j)
        {
            key_oss << std::setw(2)
                    << static_cast<unsigned>(static_cast<uint8_t>(key[j]));
        }

        LOG(INFO) << i << "->"
                  << " key index: " << key_index << ". key hex: 0x"
                  << key_oss.str() << ", key length: " << obj_gen.get_key_len()
                  << ". value: 0x" << oss.str()
                  << ", value length: " << value_len;
    }
}

class Benchmark;
/**
 * @brief Responsible for loading data into multiple partitions.
 */
struct LoadPartitionsOperation
{
    using req_uptr = std::unique_ptr<::eloqstore::BatchWriteRequest>;

    LoadPartitionsOperation(size_t id,
                            uint64_t part_data_cnt,
                            const Benchmark *bm);

    LoadPartitionsOperation(const LoadPartitionsOperation &rhs) = delete;
    LoadPartitionsOperation(LoadPartitionsOperation &&rhs)
        : self_id_(rhs.self_id_),
          part_idx_(rhs.part_idx_),
          target_data_cnt_(rhs.target_data_cnt_),
          loaded_data_cnt_(rhs.loaded_data_cnt_),
          batch_cnt_(rhs.batch_cnt_),
          req_(std::move(rhs.req_)),
          part_ids_(std::move(rhs.part_ids_))
    {
    }

    // The first partition id
    size_t self_id_{0};
    size_t part_idx_{0};
    const uint64_t target_data_cnt_{0};
    uint64_t loaded_data_cnt_{0};
    mutable uint32_t batch_cnt_{0};
    req_uptr req_;
    std::vector<uint32_t> part_ids_;
    const Benchmark *bm_{nullptr};
};

struct ReadOperation
{
    using req_uptr = std::unique_ptr<::eloqstore::ReadRequest>;

    explicit ReadOperation(const Benchmark *bm);

    ReadOperation(const ReadOperation &rhs) = delete;
    ReadOperation(ReadOperation &&rhs)
        : req_(std::move(rhs.req_)),
          key_(std::move(rhs.key_)),
          start_ts_(rhs.start_ts_)
    {
    }

    req_uptr req_;
    std::string key_;
    uint64_t start_ts_{0};
    const Benchmark *bm_{nullptr};
};

class BMResult
{
public:
    BMResult(uint32_t worker_cnt, const Benchmark *bm)
        : worker_count_(worker_cnt), bm_(bm)
    {
    }

    void Wait(uint64_t start_ts);

    void Nofity(bool succ,
                size_t finished_part = 0,
                size_t finished_req_cnt = 0,
                size_t sent_req_cnt = 0,
                std::list<uint64_t> *latency = nullptr);

    void ReportResult(uint64_t end_ts);

private:
    void ReportLatency();

public:
    const uint32_t worker_count_{0};

    std::mutex bm_mux_;
    std::condition_variable bm_cv_;
    std::atomic<bool> bm_terminated_{false};
    size_t bm_total_read_req_cnt_{0};
    size_t bm_total_finished_read_req_cnt_{0};
    size_t bm_finished_write_req_cnt_{0};
    size_t bm_finished_load_part_cnt_{0};
    bool bm_failed_{false};

    // The latency of the benchmark test.
    std::list<uint64_t> req_latency_all_;

private:
    const Benchmark *bm_{nullptr};
};

class Benchmark
{
public:
#ifndef NDEBUG
    static constexpr size_t sample_granular = 1;
#else
    static constexpr size_t sample_granular = 1000;
#endif

    using batch_records = std::vector<::eloqstore::WriteDataEntry>;

public:
    Benchmark(std::string &command,
              uint64_t batch_byte_size,
              size_t total_data_size,
              uint32_t partition_count,
              uint32_t key_byte_size,
              uint32_t value_byte_size,
              std::string &key_prefix,
              std::string &key_pattern,
              uint32_t key_minimum,
              uint32_t key_maximum,
              uint32_t concurr_req_count,
              uint32_t total_test_time_sec,
              uint32_t worker_cnt);

    Benchmark(const Benchmark &rhs) = delete;
    Benchmark(Benchmark &&rhs) = delete;

    bool OpenEloqStore(const eloqstore::KvOptions &kv_options);

    void CloseEloqStore();

    void RunBenchmark();

private:
    static void OnBatchWrite(::eloqstore::KvRequest *req);

    static void OnRead(::eloqstore::KvRequest *req);

    static void GenBatchRecord(const Benchmark &bm,
                               object_generator &obj_gen,
                               batch_records &out_buff,
                               size_t batch_cnt = 0);

    // workers to start load requests.
    static void LoadWorker(size_t id, const Benchmark &bm);

private:
    const uint64_t batch_byte_size_{0};
    std::string command_;
    size_t total_data_size_{0};
    const uint32_t partition_count_{0};
    uint32_t key_byte_size_{0};
    uint32_t value_byte_size_{0};
    std::string key_prefix_;
    uint32_t key_minimum_{0};
    uint32_t key_maximum_{0};
    uint32_t concurr_req_count_{0};
    uint32_t total_test_time_sec_{0};
    std::string key_pattern_;
    std::unique_ptr<::eloqstore::EloqStore> eloq_store_;
    std::vector<LoadPartitionsOperation> load_ops_;
    std::vector<ReadOperation> read_ops_;
    // one object generator per partition.
    mutable std::vector<object_generator> load_obj_gens_;
    uint64_t start_ts_{0};
    mutable BMResult result_;

    friend BMResult;
    friend LoadPartitionsOperation;
    friend ReadOperation;
};

}  // namespace EloqStoreBM
