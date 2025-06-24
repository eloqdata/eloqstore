#pragma once
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "db_stress/expected_value.h"
#include "eloq_store.h"
#include "random.h"

DECLARE_uint64(seed);
DECLARE_int64(max_key);
DECLARE_uint32(n_partitions);
DECLARE_uint32(num_readers_per_partition);
DECLARE_uint32(max_verify_ops_per_write);
DECLARE_uint64(ops_per_partition);
DECLARE_bool(open_wfile);
DECLARE_uint32(value_sz_mode);

namespace StressTest
{

uint64_t UnixTimestamp();
std::string Key(int64_t k);

class ThreadState
{
public:
    ThreadState() : max_key_(FLAGS_max_key), n_partitions_(FLAGS_n_partitions)
    {
        values_.resize(FLAGS_max_key * FLAGS_n_partitions);
    }

    ThreadState(std::string table_name)
        : max_key_(FLAGS_max_key),
          n_partitions_(FLAGS_n_partitions),
          table_name_(table_name)
    {
        values_.resize(FLAGS_max_key * FLAGS_n_partitions);
    }

    virtual void Init()
    {
    }
    virtual void FinishInit()
    {
    }
    virtual void TraceOneBatch(uint32_t partition_id,
                               std::vector<int64_t> &keys,
                               bool IsPut)
    {
    }

    virtual void Clear()
    {
    }

    virtual bool HasHistory()
    {
        return false;
    }

    size_t GetValueLen()
    {
        return sizeof(std::atomic<uint32_t>) * n_partitions_ * max_key_;
    }

    uint32_t &Value(uint32_t partition_id, int64_t key)
    {
        return values_[partition_id * max_key_ + key];
    }

    ExpectedValue Load(uint32_t partition_id, int64_t key)
    {
        return ExpectedValue(Value(partition_id, key));
    }

    void ResetValues();
    void Precommit(uint32_t partition_id, int64_t key, ExpectedValue &value);

    std::vector<uint32_t> values_;

    PendingExpectedValue PreparePut(uint32_t partition_id, int64_t key);
    PendingExpectedValue PrepareDelete(uint32_t partition_id, int64_t key);

    const int64_t max_key_;
    const uint32_t n_partitions_;
    const std::string table_name_;
};

class FileThreadState : public ThreadState
{
public:
    FileThreadState()
    {
        if (!std::filesystem::exists(shared_state_dir))
        {
            std::filesystem::create_directory(shared_state_dir);
        }
        CHECK(std::filesystem::exists(shared_state_dir));
        seqno_ = GetSeqno();
    }

    FileThreadState(std::string table_name)
    {
        if (!std::filesystem::exists(shared_state_dir))
        {
            std::filesystem::create_directory(shared_state_dir);
        }
        CHECK(std::filesystem::exists(shared_state_dir));
        seqno_ = GetSeqno();
    }

    void Init() override
    {
        ValuesFromFileToMem();
        Replay();
    }

    void FinishInit() override
    {
        ValuesFromMemToFile();
        SaveAtAndAfter();
        StartTracer();
    }

    std::string GetFileNameForState(uint32_t num)
    {
        return shared_state_file_path + std::to_string(num) +
               suffix_of_state_file;
    }

    std::string GetFileNameForTrace(uint32_t num)
    {
        return shared_state_file_path + std::to_string(num) +
               suffix_of_trace_file;
    }

    std::string GetFileNameForLatest()
    {
        return shared_state_file_path + latest_state_file_name +
               suffix_of_state_file;
    }

    std::string GetFileNameForSeqno()
    {
        return shared_state_file_path + seqno_file_name;
    }

    bool HasHistory() override
    {
        return std::filesystem::exists(GetFileNameForLatest());
    }

    uint32_t GetSeqno();
    bool DeleteFile(std::string filename);
    void ValuesFromFileToMem();
    void SaveAtAndAfter();
    void StartTracer();
    void TraceOneBatch(uint32_t partition_id,
                       std::vector<int64_t> &keys,
                       bool IsPut) override;
    void Replay();
    void ValuesFromMemToFile();
    void Clear() override;

private:
    const std::string shared_state_dir = "/tmp/db_stress_helper/" + table_name_;
    const std::string shared_state_file_path =
        "/tmp/db_stress_helper/" + table_name_ + "/";
    const std::string latest_state_file_name = "latest";
    const std::string suffix_of_state_file = ".state";
    const std::string suffix_of_trace_file = ".trace";
    const std::string seqno_file_name = "seqno";

    std::ofstream FileWriter;
    std::ifstream FileReader;
    std::ofstream Tracer;
    uint32_t seqno_;
};

// struct ThreadState1
// {
//     ThreadState *shared;
//     std::string table_name_;

//     ThreadState1(ThreadState *_shared, std::string table_name)
//         : shared(_shared), table_name_(table_name)
//     {
//     }
// };

}  // namespace StressTest