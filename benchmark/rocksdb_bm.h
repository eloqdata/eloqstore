#pragma once

#include <glog/logging.h>

#ifdef WITH_ROCKSDB
#include "random.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"

namespace RocksDBBM
{
static std::optional<int64_t> seed_base;

inline void test_rocksdb(bool with_write = true)
{
    std::string path(getenv("HOME"));
    path.append("/datas/rocksdb/testdb");

    // open a database
    rocksdb::DB *db = nullptr;
    rocksdb::Options options;
    // create database if it is missing.
    options.create_if_missing = true;
    // error if the database already exists.
    options.error_if_exists = true;
    // options.create_missing_column_families = true;
    options.atomic_flush = true;

    const size_t CF_COUNT = 10;
    std::string key_str;
    rocksdb::ColumnFamilyOptions cf_opts;
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handle_vec;

    rocksdb::Status status;
    if (with_write)
    {
        status = rocksdb::DB::Open(options, path, &db);
        if (!status.ok())
        {
            DLOG(ERROR) << "Can not open rocksdb: " << path
                        << " with error: " << status.ToString();
            return;
        }
        DLOG(INFO) << "The rocksdb database is opened.";

        // create column families:
        std::vector<std::string> cfs_vec;
        cfs_vec.resize(CF_COUNT, "table_");
        for (size_t i = 0; i < CF_COUNT; ++i)
        {
            cfs_vec[i].append(std::to_string(i));
        }
        status = db->CreateColumnFamilies(cf_opts, cfs_vec, &cf_handle_vec);
        if (!status.ok())
        {
            DLOG(ERROR) << "Can not create column families"
                        << " with error: " << status.ToString();
            // close database
            delete db;
            return;
        }

        // write
        rocksdb::WriteOptions write_options;  // = rocksdb::WriteOptions();
        // write_options.sync
        write_options.disableWAL = true;
        std::string val_str;
        for (size_t i = 0; i < CF_COUNT + 1; ++i)
        {
            key_str.clear();
            val_str.clear();
            key_str.append("key_").append(std::to_string(i));
            val_str.append("value_").append(std::to_string(i));
            rocksdb::Slice key(key_str);
            rocksdb::Slice value(val_str);
            rocksdb::ColumnFamilyHandle *cfh = nullptr;
            if (i == 0)
            {
                cfh = db->DefaultColumnFamily();
            }
            else
            {
                cfh = cf_handle_vec[i - 1];
            }
            status = db->Put(write_options, cfh, key, value);
            if (!status.ok())
            {
                DLOG(ERROR) << "Can not put " << key_str << "->" << val_str
                            << " to column family:" << cfh->GetName()
                            << " with error: " << status.ToString();
                // close database
                delete db;
                return;
            }
        }

        // flush
        rocksdb::FlushOptions flush_opt;
        status = db->Flush(flush_opt, cf_handle_vec);
        if (!status.ok())
        {
            LOG(ERROR) << "Flush column families failed with error: "
                       << status.ToString();
            delete db;
            return;
        }
        status = db->Flush(flush_opt);
        if (!status.ok())
        {
            LOG(ERROR) << "Flush default column family failed with error: "
                       << status.ToString();
            delete db;
            return;
        }
        DLOG(INFO) << "Write succeed.";
        return;
    }

    {
        // close database
        if (!db)
        {
            delete db;
            db = nullptr;
        }

        // reopen
        std::vector<std::string> cfs;
        rocksdb::DBOptions db_opt;
        status = rocksdb::DB::ListColumnFamilies(db_opt, path, &cfs);
        if (!status.ok())
        {
            DLOG(ERROR) << "Can not list column families from rocksdb: " << path
                        << " with error: " << status.ToString();
            return;
        }
        std::ostringstream oss;
        for (size_t i = 0; i < cfs.size(); ++i)
        {
            oss << cfs[i] << ",";
        }
        DLOG(INFO) << "To open rocksdb " << path
                   << " with all column families: " << oss.str();

        assert(cfs.size() == CF_COUNT + 1);
        cf_handle_vec.clear();

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
        cf_descs.reserve(CF_COUNT);
        for (size_t i = 0; i < CF_COUNT + 1; ++i)
        {
            cf_descs.emplace_back(cfs[i], cf_opts);
        }

        options.error_if_exists = false;
        status = rocksdb::DB::Open(db_opt, path, cf_descs, &cf_handle_vec, &db);
        if (!status.ok())
        {
            DLOG(ERROR) << "Can not open rocksdb: " << path
                        << " with error: " << status.ToString();
            return;
        }

        std::ostringstream oss1;
        for (size_t i = 0; i < cf_handle_vec.size(); ++i)
        {
            oss1 << cf_handle_vec[i]->GetName() << ",";
        }
        DLOG(INFO) << "The rocksdb database is opened with column families: "
                   << oss1.str();
    }

    // read
    const rocksdb::ReadOptions read_options;  // = rocksdb::ReadOptions();

    std::string val_ts;
    rocksdb::PinnableSlice pinnable_val;
    for (size_t i = 0; i < CF_COUNT + 1; ++i)
    {
        key_str.clear();
        val_ts.clear();
        key_str.append("key_").append(std::to_string(i));
        pinnable_val.Reset();
        rocksdb::Slice key(key_str);
        status = db->Get(
            read_options, cf_handle_vec[i], key, &pinnable_val, &val_ts);
        if (!status.ok())
        {
            DLOG(ERROR) << "Can not get " << key_str
                        << " from column family:" << cf_handle_vec[i]->GetName()
                        << " with error: " << status.ToString();
            // close database
            delete db;
            return;
        }
        DLOG(INFO) << "Get value for key: " << key_str
                   << " with value: " << *pinnable_val.GetSelf()
                   << " is pinned: " << pinnable_val.IsPinned()
                   << " with ts: " << val_ts
                   << " from column family:" << cf_handle_vec[i]->GetName();
    }

    // close a database
    DLOG(INFO) << "Close the rocksdb database.";
    delete db;
}

enum struct DistributionType : uint8_t
{
    kFixed = 0,
    kUniform,
    kNormal
};

static DistributionType value_size_distribution_type_e =
    DistributionType::kFixed;

static DistributionType StringToDistributionType(const char *ctype);

class BaseDistribution
{
public:
    BaseDistribution(uint32_t _min, uint32_t _max);

    virtual ~BaseDistribution() = default;

    uint32_t Generate();

private:
    virtual uint32_t Get() = 0;
    virtual bool NeedTruncate()
    {
        return true;
    }

    uint32_t min_value_size_;
    uint32_t max_value_size_;
};

class FixedDistribution : public BaseDistribution
{
public:
    explicit FixedDistribution(uint32_t size);

private:
    uint32_t Get() override;
    bool NeedTruncate() override;

    uint32_t size_;
};

class NormalDistribution : public BaseDistribution,
                           public std::normal_distribution<double>
{
public:
    NormalDistribution(uint32_t _min, uint32_t _max);

private:
    uint32_t Get() override;

    std::random_device rd_;
    std::mt19937 gen_;
};

class UniformDistribution : public BaseDistribution,
                            public std::uniform_int_distribution<uint32_t>
{
public:
    UniformDistribution(uint32_t _min, uint32_t _max);

private:
    uint32_t Get() override;
    bool NeedTruncate() override;

    std::random_device rd_;
    std::mt19937 gen_;
};

// Helper for quickly generating random data.
class RandomGenerator
{
public:
    explicit RandomGenerator(uint32_t val_size);

    rocksdb::Slice Generate(uint32_t len);

    rocksdb::Slice Generate();

private:
    const uint32_t max_value_size_{0};
    std::string data_;
    uint32_t pos_{0};
    std::unique_ptr<BaseDistribution> dist_{nullptr};
};

enum struct OperationType : uint8_t
{
    kRead = 0,
    kWrite,
    kOthers
};

struct DBWithColumnFamilies;

class Stats
{
#ifndef NDEBUG
    static constexpr uint64_t Granularity = 1;
#else
    static constexpr uint64_t Granularity = 1000;
#endif

public:
    Stats();

    void Start(int32_t id, std::list<int64_t> *la);

    void Merge(const Stats &other);

    void Stop();

    void SetId(int id);
    void SetExcludeFromMerge();

    uint64_t GetStart();

    void ResetLastOpTime();

    bool StartOps(int64_t num_ops);

    void FinishedOps(DBWithColumnFamilies *db_with_cfh,
                     rocksdb::DB *db,
                     int64_t num_ops,
                     bool collect = false);

    void AddBytes(int64_t n, int64_t req_cnt = 0, int64_t found = 0);

    void Report(const rocksdb::Slice &name);

private:
    int id_;
    uint64_t start_ = 0;
    uint64_t finish_;
    double seconds_;
    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t next_report_;
    uint64_t bytes_;
    uint64_t last_op_finish_;
    uint64_t last_report_finish_;
    std::string message_;
    bool exclude_from_merge_;
    uint64_t ops_;
    uint64_t ops_start_{0};
    std::list<int64_t> *latency_{nullptr};
    uint64_t req_cnt_{0};
    uint64_t found_{0};
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState
{
    void Report();

private:
    std::list<int64_t> merge_two_lists(std::list<int64_t> &a,
                                       std::list<int64_t> &b);

public:
    std::mutex mux_;
    std::condition_variable cv_;
    uint32_t total_{0};

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    uint32_t num_initialized_{0};
    uint32_t num_done_{0};
    bool start_{false};
    std::vector<std::list<int64_t>> latencys_;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState
{
    ThreadState(uint32_t index,
                uint32_t seed,
                uint64_t start_key,
                uint64_t end_key)
        : tid_(index),
          rand_(*seed_base + seed),
          start_key_(start_key),
          end_key_(end_key)
    {
    }

    // 0..n-1 when running in n threads
    uint32_t tid_;
    // Has different seeds for different threads
    Random64 rand_;
    Stats stats_;
    SharedState *shared_{nullptr};
    uint64_t start_key_{0};
    uint64_t end_key_{0};
};

struct DBWithColumnFamilies
{
    DBWithColumnFamilies();

    DBWithColumnFamilies(const DBWithColumnFamilies &other);

    void DeleteDBs();

    rocksdb::ColumnFamilyHandle *GetCfh(int64_t idx);

    rocksdb::DB *db_{nullptr};
    std::vector<rocksdb::ColumnFamilyHandle *> cfh_;
    // Need to be updated after all the new entries in cfh are set.
    std::atomic<size_t> num_created_;
};

class Duration
{
public:
    Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0);

    int64_t GetStage();

    bool Done(int64_t increment);

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

class Benchmark
{
public:
    Benchmark(std::string &db_path,
              uint32_t cf_cnt,
              std::string &command,
              uint32_t worker_cnt,
              uint32_t key_size,
              uint32_t val_size,
              uint64_t total_key_cnt,
              uint32_t batch_size,
              uint32_t key_maximum,
              uint32_t user_timestamp_size,
              std::string &key_prefix,
              size_t write_buffer_size,
              uint32_t write_buffer_cnt,
              uint64_t block_cache_size,
              std::string &val_size_distri_type,
              bool disable_wal,
              bool cache_index_filter_blocks,
              std::string &cache_type,
              uint64_t subcompactions,
              uint32_t test_time_sec);

    void DeleteDBs();

    ~Benchmark();

    rocksdb::Slice AllocateKey(std::unique_ptr<const char[]> *key_guard);

    void GenerateKeyFromInt(uint64_t v, int64_t num_keys, rocksdb::Slice *key);

    void ErrorExit();

    void Run();

private:
    struct ThreadArg
    {
        Benchmark *bm_{nullptr};
        SharedState *shared_{nullptr};
        std::unique_ptr<ThreadState> thread_{nullptr};
        void (Benchmark::*method_)(ThreadState *);
    };

    static std::shared_ptr<rocksdb::Cache> NewCache(int64_t capacity,
                                                    const std::string &type);

    static void ThreadBody(void *v);

    Stats RunBenchmark(uint32_t n,
                       rocksdb::Slice name,
                       void (Benchmark::*method)(ThreadState *));

    void InitializeOptionsFromFlags(rocksdb::Options *opts);

    void InitializeOptionsGeneral(rocksdb::Options *opts);

    void Open(rocksdb::Options *opts);

    void OpenDb(rocksdb::Options options,
                const std::string &db_name,
                DBWithColumnFamilies *db);

    enum struct WriteMode : uint8_t
    {
        RANDOM,
        SEQUENTIAL,
        UNIQUE_RANDOM
    };

    class KeyGenerator
    {
    public:
        KeyGenerator(Random64 *rand,
                     WriteMode mode,
                     uint64_t num,
                     uint64_t /*num_per_set*/ = 64 * 1024);

        uint64_t Next();

        // Only available for UNIQUE_RANDOM mode.
        uint64_t Fetch(uint64_t index);

    private:
        Random64 *rand_;
        WriteMode mode_;
        const uint64_t num_;
        uint64_t next_{0};
        std::vector<uint64_t> values_;
    };

    DBWithColumnFamilies *SelectDBWithCfh(ThreadState *thread);

    DBWithColumnFamilies *SelectDBWithCfh(uint64_t rand_int);

    void WriteSeq(ThreadState *thread);

    void DoWrite(ThreadState *thread, WriteMode write_mode);

    int64_t GetRandomKey(Random64 *rand);

    int64_t GetRandomCF(Random64 &rand);

    void ReadRandom(ThreadState *thread);

private:
    const size_t batch_bytes_max_{0};
    DBWithColumnFamilies db_;
    std::string db_path_;
    uint32_t column_families_count_{1};
    std::string command_;
    uint32_t worker_count_{0};
    uint32_t key_size_{0};
    uint32_t value_size_{0};
    uint64_t total_key_count_{0};
    uint32_t key_maximum_{0};
    size_t write_buffer_size_{32 << 20};  // 32MB
    uint32_t write_buffer_count_{2};
    uint64_t block_cache_size_{4 << 20};  // 4MB
    uint32_t total_thread_count_{0};
    uint32_t user_timestamp_size_{8};
    bool disable_wal_{false};
    bool cache_index_filter_blocks_{false};
    std::string cache_type_;
    uint64_t subcompactions_{1};
    uint32_t total_test_time_sec_{0};
    int32_t open_files_max_{-1};
    // keep options around to properly destroy db later
    rocksdb::Options open_options_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    std::shared_ptr<rocksdb::Cache> cache_{nullptr};
    // std::shared_ptr<rocksdb::Cache> compressed_cache_{nullptr};
    // std::vector<std::string> keys_;
    std::string key_prefix_;
    size_t key_prefix_size_{0};
    size_t magic_key_size_{0};
};

}  // namespace RocksDBBM
#endif
