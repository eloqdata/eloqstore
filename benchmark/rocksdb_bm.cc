#include "rocksdb_bm.h"

#include <iomanip>

#ifdef WITH_ROCKSDB
namespace RocksDBBM
{
static DistributionType StringToDistributionType(const char *ctype)
{
    assert(ctype);

    if (!strcasecmp(ctype, "fixed"))
    {
        return DistributionType::kFixed;
    }
    else if (!strcasecmp(ctype, "uniform"))
    {
        return DistributionType::kUniform;
    }
    else if (!strcasecmp(ctype, "normal"))
    {
        return DistributionType::kNormal;
    }

    LOG(ERROR) << "Cannot parse distribution type: " << ctype;
    exit(1);
}

inline bool EndsWith(const std::string &string, const std::string &pattern)
{
    size_t plen = pattern.size();
    size_t slen = string.size();
    if (plen <= slen)
    {
        return string.compare(slen - plen, plen, pattern) == 0;
    }
    else
    {
        return false;
    }
}

inline std::string ColumnFamilyName(size_t i)
{
    if (i == 0)
    {
        return rocksdb::kDefaultColumnFamilyName;
    }
    else
    {
        char name[100];
        snprintf(name, sizeof(name), "rocksdb_bm_%06zu", i);
        return std::string(name);
    }
}

inline const rocksdb::Comparator *BytewiseComparatorWithU64TsWrapper()
{
    rocksdb::ConfigOptions config_options;
    const rocksdb::Comparator *user_comparator = nullptr;
    rocksdb::Status s = rocksdb::Comparator::CreateFromString(
        config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
    s.PermitUncheckedError();
    return user_comparator;
}

inline rocksdb::Slice CompressibleString(Random *rnd,
                                         double compressed_fraction,
                                         int32_t len,
                                         std::string *dst)
{
    int32_t raw = static_cast<int32_t>(len * compressed_fraction);
    if (raw < 1)
    {
        raw = 1;
    }
    std::string raw_data = rnd->RandomBinaryString(raw);

    // Duplicate the random data until we have filled "len" bytes
    dst->clear();
    while (dst->size() < static_cast<uint32_t>(len))
    {
        dst->append(raw_data);
    }
    dst->resize(len);

    return rocksdb::Slice(*dst);
}

BaseDistribution::BaseDistribution(uint32_t _min, uint32_t _max)
    : min_value_size_(_min), max_value_size_(_max)
{
}

uint32_t BaseDistribution::Generate()
{
    auto val = Get();
    if (NeedTruncate())
    {
        val = std::max(min_value_size_, val);
        val = std::min(max_value_size_, val);
    }
    return val;
}

FixedDistribution::FixedDistribution(uint32_t size)
    : BaseDistribution(size, size), size_(size)
{
}

uint32_t FixedDistribution::Get()
{
    return size_;
}

bool FixedDistribution::NeedTruncate()
{
    return false;
}

NormalDistribution::NormalDistribution(uint32_t _min, uint32_t _max)
    : BaseDistribution(_min, _max),
      // 99.7% values within the range [min, max].
      std::normal_distribution<double>((double) (_min + _max) / 2.0 /*mean*/,
                                       (double) (_max - _min) / 6.0 /*stddev*/),
      gen_(rd_())
{
}

uint32_t NormalDistribution::Get()
{
    return static_cast<uint32_t>((*this)(gen_));
}

UniformDistribution::UniformDistribution(uint32_t _min, uint32_t _max)
    : BaseDistribution(_min, _max),
      std::uniform_int_distribution<uint32_t>(_min, _max),
      gen_(rd_())
{
}

uint32_t UniformDistribution::Get()
{
    return (*this)(gen_);
}

bool UniformDistribution::NeedTruncate()
{
    return false;
}

RandomGenerator::RandomGenerator(uint32_t val_size) : max_value_size_(val_size)
{
    switch (value_size_distribution_type_e)
    {
    case DistributionType::kUniform:
        assert(false);
        break;
    case DistributionType::kNormal:
        assert(false);
        break;
    case DistributionType::kFixed:
    default:
        dist_.reset(new FixedDistribution(val_size));
    }

    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    uint32_t limited = 1048576;  // 0x100000
    while (data_.size() < (unsigned) std::max(limited, max_value_size_))
    {
        // Add a short fragment that is as compressible as specified
        // by FLAGS_compression_ratio.
        CompressibleString(&rnd, 1, 100, &piece);
        data_.append(piece);
    }
    pos_ = 0;
}

rocksdb::Slice RandomGenerator::Generate(uint32_t len)
{
    assert(len <= data_.size());
    if (pos_ + len > data_.size())
    {
        pos_ = 0;
    }
    pos_ += len;
    return rocksdb::Slice(data_.data() + pos_ - len, len);
}

rocksdb::Slice RandomGenerator::Generate()
{
    auto len = dist_->Generate();
    return Generate(len);
}

Stats::Stats()
{
    Start(-1, nullptr);
}

void Stats::Start(int32_t id, std::list<int64_t> *la)
{
    id_ = id;
    next_report_ = 100;
    last_op_finish_ = start_;
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
    ops_ = 0;
    ops_start_ = 0;
    req_cnt_ = 0;
    found_ = 0;
    latency_ = la;
}

void Stats::Merge(const Stats &other)
{
    if (other.exclude_from_merge_)
    {
        return;
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_)
    {
        start_ = other.start_;
    }
    if (other.finish_ > finish_)
    {
        finish_ = other.finish_;
    }

    // Just keep the messages from one thread.
    if (message_.empty())
    {
        message_ = other.message_;
    }
    ops_ += other.ops_;
    req_cnt_ += other.ops_;
    found_ += other.ops_;
}

void Stats::Stop()
{
    finish_ = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count();
    seconds_ = (finish_ - start_) * 1e-6;
}

void Stats::SetId(int id)
{
    id_ = id;
}

void Stats::SetExcludeFromMerge()
{
    exclude_from_merge_ = true;
}

uint64_t Stats::GetStart()
{
    return start_;
}

void Stats::ResetLastOpTime()
{
    // Set to now to avoid latency from calls to SleepForMicroseconds.
    last_op_finish_ =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
}

bool Stats::StartOps(int64_t num_ops)
{
    ops_ += num_ops;
    if ((ops_ / Granularity) != ((ops_ - num_ops) / Granularity))
    {
        ops_start_ =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        DLOG(INFO) << " start: " << ops_start_;
        return true;
    }
    return false;
}

void Stats::FinishedOps(DBWithColumnFamilies *db_with_cfh,
                        rocksdb::DB *db,
                        int64_t num_ops,
                        bool collect)
{
    done_ += num_ops;
    if (collect)
    {
        uint64_t now =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        uint64_t d = now - ops_start_;
        DLOG(INFO) << "latency: " << d << "(us)" << " now: " << now
                   << " start: " << ops_start_;
        latency_->push_back(d);
        ops_start_ = 0;
    }
}

void Stats::AddBytes(int64_t n, int64_t req_cnt, int64_t found)
{
    bytes_ += n;
    req_cnt_ += req_cnt;
    found_ += found;
}

void Stats::Report(const rocksdb::Slice &name)
{
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1)
    {
        done_ = 1;
    }

    if (name == rocksdb::Slice("GET", 3))
    {
        //
        LOG(INFO) << "Read request: " << req_cnt_
                  << ", key not found count: " << (req_cnt_ - found_)
                  << " on worker: " << id_;
    }

    // report the latency
    latency_->sort();
    size_t metr_size = latency_->size();
    if (!metr_size)
    {
        return;
    }
    size_t p50 = metr_size * 0.5;
    size_t p90 = metr_size * 0.9;
    size_t p95 = metr_size * 0.95;
    size_t p99 = metr_size * 0.99;
    size_t p99_9 = metr_size * 0.999;
    size_t p99_99 = metr_size * 0.9999;
    int64_t l_p50 = 0, l_p90 = 0, l_p95 = 0, l_p99 = 0, l_p99_9 = 0,
            l_p99_99 = 0;
    size_t n = 0;
    for (auto l : *latency_)
    {
        ++n;
        if (n == p50)
        {
            l_p50 = l;
        }
        else if (n == p90)
        {
            l_p90 = l;
        }
        else if (n == p95)
        {
            l_p95 = l;
        }
        else if (n == p99)
        {
            l_p99 = l;
        }
        else if (n == p99_9)
        {
            l_p99_9 = l;
        }
        else if (n == p99_99)
        {
            l_p99_99 = l;
        }
    }
    uint64_t mean = 0, sum = 0;
    sum = std::accumulate(latency_->begin(), latency_->end(), 0);
    mean = sum / metr_size;
    LOG(INFO) << "Latency: Min->" << latency_->front() << ", Max->"
              << latency_->back() << ", Mean->" << mean << ", p50->" << l_p50
              << ", p90->" << l_p90 << ", p95->" << l_p95 << ", p99->" << l_p99
              << ", p99.9->" << l_p99_9 << ", p99.99->" << l_p99_99
              << " on worker: " << id_;
}

void SharedState::Report()
{
    while (latencys_.size() > 1)
    {
        std::vector<std::list<int64_t>> new_lists;
        for (size_t i = 0; i < latencys_.size(); i += 2)
        {
            if (i + 1 < latencys_.size())
            {
                // merge two list
                new_lists.push_back(
                    merge_two_lists(latencys_[i], latencys_[i + 1]));
            }
            else
            {
                // the last one
                new_lists.push_back(latencys_[i]);
            }
        }
        latencys_ = std::move(new_lists);
    }

    auto &total_res = latencys_[0];
    size_t metr_size = total_res.size();
    if (!metr_size)
    {
        return;
    }
    size_t p50 = metr_size * 0.5;
    size_t p90 = metr_size * 0.9;
    size_t p95 = metr_size * 0.95;
    size_t p99 = metr_size * 0.99;
    size_t p99_9 = metr_size * 0.999;
    size_t p99_99 = metr_size * 0.9999;
    int64_t l_p50 = 0, l_p90 = 0, l_p95 = 0, l_p99 = 0, l_p99_9 = 0,
            l_p99_99 = 0;
    size_t n = 0;
    for (auto l : total_res)
    {
        ++n;
        if (n == p50)
        {
            l_p50 = l;
        }
        else if (n == p90)
        {
            l_p90 = l;
        }
        else if (n == p95)
        {
            l_p95 = l;
        }
        else if (n == p99)
        {
            l_p99 = l;
        }
        else if (n == p99_9)
        {
            l_p99_9 = l;
        }
        else if (n == p99_99)
        {
            l_p99_99 = l;
        }
    }
    uint64_t mean = 0, sum = 0;
    sum = std::accumulate(total_res.begin(), total_res.end(), 0);
    mean = sum / metr_size;
    LOG(INFO) << "Latency: Min->" << total_res.front() << ", Max->"
              << total_res.back() << ", Mean->" << mean << ", p50->" << l_p50
              << ", p90->" << l_p90 << ", p95->" << l_p95 << ", p99->" << l_p99
              << ", p99.9->" << l_p99_9 << ", p99.99->" << l_p99_99;
}

std::list<int64_t> SharedState::merge_two_lists(std::list<int64_t> &a,
                                                std::list<int64_t> &b)
{
    // merge, b empty
    a.merge(b);
    return a;
}

DBWithColumnFamilies::DBWithColumnFamilies() : db_(nullptr), num_created_(0)
{
    cfh_.clear();
}

DBWithColumnFamilies::DBWithColumnFamilies(const DBWithColumnFamilies &other)
    : db_(other.db_), cfh_(other.cfh_), num_created_(other.num_created_.load())
{
}

void DBWithColumnFamilies::DeleteDBs()
{
    if (num_created_.load(std::memory_order_acquire) > 1)
    {
        std::for_each(cfh_.begin(),
                      cfh_.end(),
                      [](rocksdb::ColumnFamilyHandle *cfhi) { delete cfhi; });
    }
    cfh_.clear();
    delete db_;
    db_ = nullptr;
}

rocksdb::ColumnFamilyHandle *DBWithColumnFamilies::GetCfh(int64_t idx)
{
    assert(idx < num_created_.load(std::memory_order_acquire));
    return cfh_[idx];
}

Duration::Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage)
{
    max_seconds_ = max_seconds;
    max_ops_ = max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
}

int64_t Duration::GetStage()
{
    return std::min(ops_, max_ops_ - 1) / ops_per_stage_;
}

bool Duration::Done(int64_t increment)
{
    if (increment <= 0)
    {
        increment = 1;  // avoid Done(0) and infinite loops
    }
    ops_ += increment;

    if (max_seconds_)
    {
        // Recheck every appx 1000 ops (exact iff increment is factor of
        // 1000)
        auto granularity = 1000;
        if ((ops_ / granularity) != ((ops_ - increment) / granularity))
        {
            uint64_t now =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now()
                        .time_since_epoch())
                    .count();
            return ((now - start_at_) / 1000000) >= max_seconds_;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return ops_ > max_ops_;
    }
}

Benchmark::Benchmark(std::string &db_path,
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
                     uint32_t test_time_sec)
    : batch_bytes_max_(batch_size << 20),  // batch_size MB
      db_path_(db_path),
      column_families_count_(cf_cnt),
      command_(command),
      worker_count_(worker_cnt),
      key_size_(key_size),
      value_size_(val_size),
      total_key_count_(total_key_cnt),
      key_maximum_(key_maximum),
      write_buffer_size_(write_buffer_size),
      write_buffer_count_(write_buffer_cnt),
      block_cache_size_(block_cache_size),
      total_thread_count_(0),
      user_timestamp_size_(user_timestamp_size),
      disable_wal_(disable_wal),
      cache_index_filter_blocks_(cache_index_filter_blocks),
      cache_type_(cache_type),
      subcompactions_(subcompactions),
      total_test_time_sec_(test_time_sec),
      cache_(NewCache(block_cache_size, cache_type_)),
      key_prefix_(key_prefix),
      key_prefix_size_(key_prefix.size()),
      magic_key_size_(key_size_ > 8 ? (key_size_ - 8) : 0)
{
    int64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    seed_base = static_cast<int64_t>(now);

    value_size_distribution_type_e =
        StringToDistributionType(val_size_distri_type.data());
}

void Benchmark::DeleteDBs()
{
    db_.DeleteDBs();
}

Benchmark::~Benchmark()
{
    DeleteDBs();
    if (cache_.get() != nullptr)
    {
        // Clear cache reference first
        open_options_.write_buffer_manager.reset();
        // this will leak, but we're shutting down so nobody cares
        cache_->DisownData();
    }
}

rocksdb::Slice Benchmark::AllocateKey(std::unique_ptr<const char[]> *key_guard)
{
    char *data = new char[key_size_];
    const char *const_data = data;
    key_guard->reset(const_data);
    return rocksdb::Slice(key_guard->get(), key_size_);
}

void Benchmark::GenerateKeyFromInt(uint64_t v,
                                   int64_t num_keys,
                                   rocksdb::Slice *key)
{
    char *start = const_cast<char *>(key->data());
    char *pos = start;
    if (key_prefix_size_ > 0)
    {
        assert(false);
        memcpy(pos, key_prefix_.data(), key_prefix_size_);
        pos += key_prefix_size_;
    }

    if (magic_key_size_ > 0)
    {
        memset(pos, 0xFF, magic_key_size_);
        pos += magic_key_size_;
    }

    assert(std::endian::native == std::endian::little);
    const char *val_ptr = static_cast<const char *>(static_cast<void *>(&v));
    unsigned int actual_cnt = key_size_ - magic_key_size_;
    for (unsigned int i = actual_cnt; i > 0; --i)
    {
        pos[actual_cnt - i] = val_ptr[i - 1];
    }
}

void Benchmark::ErrorExit()
{
    DeleteDBs();
    exit(1);
}

void Benchmark::Run()
{
    Open(&open_options_);
    LOG(INFO) << "RocksDB: " << db_path_ << " opened.";

    write_options_.disableWAL = disable_wal_;

    void (Benchmark::*method)(ThreadState *) = nullptr;

    bool fresh_db = false;
    uint32_t num_threads = worker_count_;

    if (command_ == "LOAD")
    {
        fresh_db = true;
        num_threads = worker_count_ > column_families_count_
                          ? column_families_count_
                          : worker_count_;
        method = &Benchmark::WriteSeq;
    }
    else if (command_ == "GET")
    {
        // TODO? random operand?
        method = &Benchmark::ReadRandom;
    }
    else
    {
        LOG(ERROR) << "Unsupport command: " << command_;
        return;
    }

    if (fresh_db)
    {
        if (db_.db_ != nullptr)
        {
            db_.DeleteDBs();
            rocksdb::DestroyDB(db_path_, open_options_);
        }

        // use open_options for the last accessed
        Open(&open_options_);
    }

    if (method != nullptr)
    {
        rocksdb::Slice name(command_);
        Stats stats = RunBenchmark(num_threads, name, method);
    }
    LOG(INFO) << "Benchmarck " << command_ << " finished.";
}

std::shared_ptr<rocksdb::Cache> Benchmark::NewCache(int64_t capacity,
                                                    const std::string &type)
{
    if (capacity <= 0)
    {
        return nullptr;
    }

    std::shared_ptr<rocksdb::Cache> block_cache;
    if (type == "clock_cache")
    {
        LOG(ERROR) << "Old clock cache implementation has been removed.";
        exit(1);
    }
    else if (EndsWith(type, "hyper_clock_cache"))
    {
        assert(false);
    }
    else if (type == "lru_cache")
    {
        rocksdb::LRUCacheOptions opts(static_cast<size_t>(capacity),
                                      -1,
                                      false /*strict_capacity_limit*/,
                                      0.5);
        opts.secondary_cache = nullptr;

        block_cache = opts.MakeSharedCache();
    }
    else
    {
        LOG(ERROR) << "Cache type not supported.";
        exit(1);
    }

    if (!block_cache)
    {
        LOG(ERROR) << "Unable to allocate block cache with type: " << type;
        exit(1);
    }

    return block_cache;
}

void Benchmark::ThreadBody(void *v)
{
    ThreadArg *arg = static_cast<ThreadArg *>(v);
    SharedState *shared = arg->shared_;
    ThreadState *thread = arg->thread_.get();
    {
        std::unique_lock<std::mutex> lk(shared->mux_);
        shared->num_initialized_++;
        if (shared->num_initialized_ >= shared->total_)
        {
            shared->cv_.notify_all();
        }

        while (!shared->start_)
        {
            shared->cv_.wait(lk, [&shared]() { return shared->start_; });
        }
    }

    thread->stats_.Start(thread->tid_, &shared->latencys_[thread->tid_]);
    (arg->bm_->*(arg->method_))(thread);
    thread->stats_.Stop();

    {
        std::unique_lock<std::mutex> lk(shared->mux_);
        shared->num_done_++;
        if (shared->num_done_ >= shared->total_)
        {
            LOG(INFO) << "All worker finished, notify the coordinator.";
            shared->cv_.notify_all();
        }
        DLOG(INFO) << "Worker: " << thread->tid_ << " finished."
                   << " Total: " << shared->total_
                   << " Done: " << shared->num_done_;
    }
}

Stats Benchmark::RunBenchmark(uint32_t n,
                              rocksdb::Slice name,
                              void (Benchmark::*method)(ThreadState *))
{
    SharedState shared;
    shared.total_ = n;
    shared.num_initialized_ = 0;
    shared.num_done_ = 0;
    shared.start_ = false;
    shared.latencys_.resize(n);

    std::vector<std::thread> worker_thd;
    worker_thd.reserve(n);

    std::vector<ThreadArg> arg;
    arg.resize(n);

    uint64_t start_key = 0;
    uint64_t end_key = 0;
    for (int i = 0; i < n; i++)
    {
        arg[i].bm_ = this;
        arg[i].method_ = method;
        arg[i].shared_ = &shared;
        total_thread_count_++;
        // start_key = i * key_step;
        arg[i].thread_ = std::make_unique<ThreadState>(
            i, total_thread_count_, start_key, end_key);
        arg[i].thread_->shared_ = &shared;
        worker_thd.push_back(
            std::thread([this, thd_arg = &arg[i]]() { ThreadBody(thd_arg); }));
    }

    std::unique_lock<std::mutex> lk(shared.mux_);
    while (shared.num_initialized_ < n)
    {
        shared.cv_.wait(lk, [&]() { return shared.num_initialized_ == n; });
    }

    LOG(INFO) << "All " << total_thread_count_
              << " workers initialized. Start test.";

    shared.start_ = true;
    shared.cv_.notify_all();
    while (shared.num_done_ < n)
    {
        shared.cv_.wait(lk, [&]() { return shared.num_done_ == n; });
    }
    lk.unlock();

    LOG(INFO) << "All " << total_thread_count_ << " workers finished.";
    if (name == rocksdb::Slice("LOAD", 4))
    {
        rocksdb::FlushOptions flush_opt;
        uint64_t rand = 0;
        DBWithColumnFamilies *db_with_cfh = SelectDBWithCfh(rand);
        rocksdb::Status s =
            db_with_cfh->db_->Flush(flush_opt, db_with_cfh->cfh_);
        if (!s.ok())
        {
            LOG(ERROR) << "Flush column families failed with error: "
                       << s.ToString();
        }
        else
        {
#ifndef NDEBUG
            std::ostringstream oss1;
            for (size_t i = 0; i < db_with_cfh->cfh_.size(); ++i)
            {
                oss1 << db_with_cfh->cfh_[i]->GetName() << ",";
            }
            DLOG(INFO) << "Flushed all column families: " << oss1.str();
#endif
            LOG(INFO) << "Flush DB: " << db_path_ << " succeed.";

            LOG(INFO) << "Compacting DB: " << db_path_ << " ...";
            rocksdb::CompactRangeOptions cr_opts;
            cr_opts.bottommost_level_compaction =
                rocksdb::BottommostLevelCompaction::kForceOptimized;
            cr_opts.max_subcompactions = static_cast<uint32_t>(subcompactions_);
            size_t i = 0;
            for (i = 0; i < db_with_cfh->cfh_.size(); ++i)
            {
                s = db_with_cfh->db_->CompactRange(
                    cr_opts, db_with_cfh->cfh_[i], nullptr, nullptr);
                if (!s.ok())
                {
                    LOG(ERROR) << "Compact column family "
                               << db_with_cfh->cfh_[i]->GetName()
                               << " failed with error: " << s.ToString();
                    break;
                }
            }
            if (i == db_with_cfh->cfh_.size())
            {
                LOG(INFO) << "Compacting DB: " << db_path_ << " succeed.";
            }
        }
    }
    assert(worker_thd.size() == total_thread_count_);
    for (size_t i = 0; i < total_thread_count_; i++)
    {
        if (worker_thd[i].joinable())
        {
            worker_thd[i].join();
        }
    }

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++)
    {
        arg[i].thread_->stats_.Report(name);
        merge_stats.Merge(arg[i].thread_->stats_);
    }
    // merge_stats.Report(name);
    shared.Report();

    return merge_stats;
}

void Benchmark::InitializeOptionsFromFlags(rocksdb::Options *opts)
{
    LOG(INFO) << "Initializing RocksDB Options from command-line flags.";
    rocksdb::Options &options = *opts;
    rocksdb::ConfigOptions config_options(options);
    config_options.ignore_unsupported_options = false;

    assert(db_.db_ == nullptr);

    // compression

    // comparator
    // if (user_timestamp_size_ > 0)
    // {
    //     if (user_timestamp_size_ != 8)
    //     {
    //         LOG(ERROR) << "Only 64 bits timestamps are supported.";
    //         exit(1);
    //     }
    //     options.comparator = BytewiseComparatorWithU64TsWrapper();
    // }

    // options.max_open_files = open_files_max_;
    options.write_buffer_size = write_buffer_size_;  // 64MB
    // options.ttl = 0xfffffffffffffffe;
    options.use_fsync = false;
    options.max_write_buffer_number = write_buffer_count_;

    rocksdb::BlockBasedTableOptions block_based_options;

    // Whether to put index/filter blocks in the block cache.
    block_based_options.cache_index_and_filter_blocks =
        cache_index_filter_blocks_;

    if (cache_ == nullptr)
    {
        block_based_options.no_block_cache = true;
        block_based_options.cache_index_and_filter_blocks = false;
        LOG(INFO) << "No block cache.";
    }
    block_based_options.block_cache = cache_;

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
}

void Benchmark::InitializeOptionsGeneral(rocksdb::Options *opts)
{
    rocksdb::Options &options = *opts;

    // create database if it is missing.
    options.create_if_missing = command_ == "LOAD";
    // error if the database already exists.
    options.error_if_exists = command_ == "LOAD";

    // create missing column families automatically on DB::Open()
    options.create_missing_column_families = command_ == "LOAD";

    // flushing multiple column families and committing their results
    // atomically to MANIFEST
    options.atomic_flush = true;

    // Disable Row Cache
    options.row_cache = nullptr;

    options.use_direct_reads = true;

    auto table_options =
        options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
    if (table_options != nullptr)
    {
        if (block_cache_size_ > 0)
        {
            // This violates this function's rules on when to set options.
            // But we have to do it because the case of unconfigured block
            // cache in OPTIONS file is indistinguishable (it is sanitized
            // to 32MB by this point, not nullptr), and our regression tests
            // assume this will be the shared block cache, even with OPTIONS
            // file provided.
            table_options->block_cache = cache_;
            LOG(INFO) << "Use block cache.";
        }
    }
    OpenDb(options, db_path_, &db_);
}

void Benchmark::Open(rocksdb::Options *opts)
{
    InitializeOptionsFromFlags(opts);
    InitializeOptionsGeneral(opts);
}

void Benchmark::OpenDb(rocksdb::Options options,
                       const std::string &db_name,
                       DBWithColumnFamilies *db)
{
    rocksdb::Status s;
    // Open with column families if necessary.
    if (column_families_count_ > 0)
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        for (size_t i = 0; i < column_families_count_; i++)
        {
            column_families.emplace_back(ColumnFamilyName(i),
                                         rocksdb::ColumnFamilyOptions(options));
        }

        s = rocksdb::DB::Open(
            options, db_name, column_families, &db->cfh_, &db->db_);

        db->cfh_.resize(column_families_count_);
    }
    // TODO: open for ready only
    // else if(read_only)
    // {}
    else
    {
        s = rocksdb::DB::Open(options, db_name, &db->db_);
        db->cfh_.emplace_back(db->db_->DefaultColumnFamily());
    }
    db->num_created_ = column_families_count_;

    if (!s.ok())
    {
        LOG(ERROR) << "Open rocksDB " << db_name << " error: " << s.ToString();
        exit(1);
    }

    std::vector<std::string_view> cfs;
    std::ostringstream oss;
    auto &cf_handle_vec = db->cfh_;
    for (size_t i = 0; i < cf_handle_vec.size(); ++i)
    {
        oss << cf_handle_vec[i]->GetName() << ",";
    }
    LOG(INFO) << "RocksDB " << db_name
              << " opened with column families: " << oss.str();
}

DBWithColumnFamilies *Benchmark::SelectDBWithCfh(ThreadState *thread)
{
    return SelectDBWithCfh(thread->rand_.Next());
}

DBWithColumnFamilies *Benchmark::SelectDBWithCfh(uint64_t rand_int)
{
    assert(db_.db_ != nullptr);
    return &db_;
}

void Benchmark::WriteSeq(ThreadState *thread)
{
    DoWrite(thread, WriteMode::SEQUENTIAL);
}

void Benchmark::DoWrite(ThreadState *thread, WriteMode write_mode)
{
    assert(write_mode == WriteMode::SEQUENTIAL);
    const uint64_t num_ops = total_key_count_ / total_thread_count_;

    assert(total_thread_count_ <= column_families_count_);
    size_t cf_per_worker = column_families_count_ / total_thread_count_;
    assert(column_families_count_ % total_thread_count_ == 0);
    assert(db_.db_ != nullptr);

    std::vector<std::unique_ptr<KeyGenerator>> key_gens(cf_per_worker);

    const uint64_t num_per_key_gen = total_key_count_ / column_families_count_;

    for (size_t i = 0; i < cf_per_worker; i++)
    {
        key_gens[i] = std::make_unique<KeyGenerator>(
            &(thread->rand_), write_mode, num_per_key_gen);
    }

    RandomGenerator gen(value_size_);

    rocksdb::WriteBatch batch(/*reserved_bytes=*/0,
                              /*max_bytes=*/0,
                              /*protect_bytes*/ 0,
                              user_timestamp_size_);
    rocksdb::Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    rocksdb::Slice key = AllocateKey(&key_guard);

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0)
    {
        ts_guard.reset(new char[user_timestamp_size_]);
    }

    int64_t num_written = 0;
    int64_t batch_req_cnt = 0;

    size_t id = 0;
    DBWithColumnFamilies *db_with_cfh = SelectDBWithCfh(id);

    int64_t batch_cnt =
        batch_bytes_max_ / (key_size_ + user_timestamp_size_ + value_size_);

    while (num_written < num_ops)
    {
        batch.Clear();
        int64_t batch_bytes = 0;
        id = num_written / num_per_key_gen;

        int64_t cf_idx = id * total_thread_count_ + thread->tid_;

        batch_cnt = batch_cnt < (num_ops - num_written)
                        ? batch_cnt
                        : (num_ops - num_written);

        for (int64_t cnt = 0; cnt < batch_cnt && batch_bytes < batch_bytes_max_;
             ++cnt)
        {
            int64_t rand_num = key_gens[id]->Next();
            GenerateKeyFromInt(rand_num, total_key_count_, &key);

            rocksdb::Slice val;
            val = gen.Generate();

            // if (column_families_count_ <= 1)
            // {
            //     DLOG(INFO) << "write to default.";
            //     batch.Put(key, val);
            // }
            // else
            {
                // DLOG(INFO) << "write to cf index: " << cf_idx;
                batch.Put(db_with_cfh->GetCfh(cf_idx), key, val);
            }

            DLOG(INFO) << "Batch write to cf index: " << cf_idx
                       << " of name: " << db_with_cfh->GetCfh(cf_idx)->GetName()
                       << " on worker: " << thread->tid_;

            batch_bytes += val.size() + key_size_ + user_timestamp_size_;

            ++num_written;

#ifndef NDEBUG
            std::ostringstream k_oss;
            const char *key_buf = key.data();
            k_oss << "0x";
            k_oss << std::hex << std::setfill('0');
            size_t key_len = key.size();
            for (size_t i = 0; i < key_len; ++i)
            {
                k_oss << std::setw(2)
                      << static_cast<unsigned>(
                             static_cast<uint8_t>(key_buf[i]));
            }

            std::ostringstream val_oss;
            const char *val_buf = val.data();
            val_oss << "0x";
            val_oss << std::hex << std::setfill('0');
            size_t value_len = val.size();
            for (size_t j = 0; j < value_len; ++j)
            {
                val_oss << std::setw(2)
                        << static_cast<unsigned>(
                               static_cast<uint8_t>(val_buf[j]));
            }
            DLOG(INFO) << "Write key: " << k_oss.str()
                       << " -> val: " << val_oss.str();
#endif
        }

        bytes += batch_bytes;

        // if (user_timestamp_size_ > 0)
        // {
        //     uint64_t ts =
        //         std::chrono::duration_cast<std::chrono::microseconds>(
        //             std::chrono::high_resolution_clock::now()
        //                 .time_since_epoch())
        //             .count();
        //     const char *ts_ptr =
        //         static_cast<const char *>(static_cast<void *>(&ts));
        //     assert(std::endian::native == std::endian::little);
        //     assert(sizeof(ts) == user_timestamp_size_);
        //     memcpy(ts_guard.get(), ts_ptr, sizeof(ts));
        //     rocksdb::Slice user_ts(ts_guard.get(), user_timestamp_size_);
        //     s = batch.UpdateTimestamps(
        //         user_ts, [this](uint32_t) { return user_timestamp_size_;
        //         });
        //     if (!s.ok())
        //     {
        //         LOG(ERROR) << "Assign timestamp to write batch failed:"
        //                    << s.ToString();
        //         ErrorExit();
        //     }
        // }
        ++batch_req_cnt;

        bool collect = thread->stats_.StartOps(1);

        s = db_with_cfh->db_->Write(write_options_, &batch);

        thread->stats_.FinishedOps(
            db_with_cfh, db_with_cfh->db_, num_written, collect);
        if (!s.ok())
        {
            LOG(ERROR) << "Batch write error: " << s.ToString();
            ErrorExit();
        }
    }

    thread->stats_.AddBytes(bytes, batch_req_cnt);
}

int64_t Benchmark::GetRandomKey(Random64 *rand)
{
    uint64_t rand_int = rand->Next();
    int64_t key_rand = rand_int % key_maximum_;

    return key_rand;
}

int64_t Benchmark::GetRandomCF(Random64 &rand)
{
    uint64_t rand_int = rand.Next();
    int64_t cf_rand = rand_int % column_families_count_;
    return cf_rand;
}

void Benchmark::ReadRandom(ThreadState *thread)
{
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    int num_keys = 0;
    int64_t key_rand = 0;
    int64_t cf_rand = 0;
    rocksdb::ReadOptions options = read_options_;
    std::unique_ptr<const char[]> key_guard;
    rocksdb::Slice key = AllocateKey(&key_guard);
    rocksdb::PinnableSlice pinnable_val;
    std::unique_ptr<char[]> ts_guard;
    rocksdb::Slice ts;
    if (user_timestamp_size_ > 0)
    {
        ts_guard.reset(new char[user_timestamp_size_]);
    }

    Duration duration(total_test_time_sec_, 0);
    while (!duration.Done(1))
    {
        DBWithColumnFamilies *db_with_cfh = SelectDBWithCfh(thread);
        cf_rand = GetRandomCF(thread->rand_);

        key_rand = GetRandomKey(&thread->rand_);
        GenerateKeyFromInt(key_rand, total_key_count_, &key);

        read++;
        std::string ts_ret;
        std::string *ts_ptr = nullptr;
        // if (user_timestamp_size_ > 0)
        // {
        //     uint64_t read_ts =
        //         std::chrono::duration_cast<std::chrono::microseconds>(
        //             std::chrono::high_resolution_clock::now()
        //                 .time_since_epoch())
        //             .count();
        //     const char *read_ts_ptr =
        //         static_cast<const char *>(static_cast<void *>(&read_ts));
        //     assert(std::endian::native == std::endian::little);
        //     assert(sizeof(read_ts) == user_timestamp_size_);
        //     memcpy(ts_guard.get(), read_ts_ptr, sizeof(read_ts));
        //     ts = rocksdb::Slice(ts_guard.get(), user_timestamp_size_);

        //     options.timestamp = &ts;
        //     ts_ptr = &ts_ret;
        // }

        rocksdb::Status s;
        pinnable_val.Reset();

        rocksdb::ColumnFamilyHandle *cfh;
        if (column_families_count_ > 0)
        {
            cfh = db_with_cfh->GetCfh(cf_rand);
        }
        else
        {
            cfh = db_with_cfh->db_->DefaultColumnFamily();
        }

        // metrics
        bool collect = thread->stats_.StartOps(1);

        // s = db_with_cfh->db_->Get(options, cfh, key, &pinnable_val,
        // ts_ptr);
        s = db_with_cfh->db_->Get(options, cfh, key, &pinnable_val);

        if (s.ok())
        {
            found++;
            bytes += key.size() + pinnable_val.size() + user_timestamp_size_;

#ifndef NDEBUG
            std::ostringstream k_oss;
            const char *key_buf = key.data();
            k_oss << "0x";
            k_oss << std::hex << std::setfill('0');
            size_t key_len = key.size();
            for (size_t i = 0; i < key_len; ++i)
            {
                k_oss << std::setw(2)
                      << static_cast<unsigned>(
                             static_cast<uint8_t>(key_buf[i]));
            }

            std::ostringstream val_oss;
            const char *val_buf = pinnable_val.data();
            val_oss << "0x";
            val_oss << std::hex << std::setfill('0');
            size_t value_len = pinnable_val.size();
            for (size_t j = 0; j < value_len; ++j)
            {
                val_oss << std::setw(2)
                        << static_cast<unsigned>(
                               static_cast<uint8_t>(val_buf[j]));
            }
            uint64_t val_ts = 0;
            //     *(reinterpret_cast<uint64_t *>(ts_ptr->data()));
            DLOG(INFO) << "Read key: " << k_oss.str()
                       << " -> val: " << val_oss.str() << ", ts: " << val_ts
                       << " from column family: " << cfh->GetName()
                       << " with cf index: " << cf_rand
                       << " on worker: " << thread->tid_;
#endif
        }
        else if (!s.IsNotFound())
        {
            LOG(ERROR) << "Get returned an error: " << s.ToString();
            abort();
        }
#ifndef NDEBUG
        else
        {
            assert(s.IsNotFound());
            std::ostringstream k_oss;
            const char *key_buf = key.data();
            k_oss << "0x";
            k_oss << std::hex << std::setfill('0');
            size_t key_len = key.size();
            for (size_t i = 0; i < key_len; ++i)
            {
                k_oss << std::setw(2)
                      << static_cast<unsigned>(
                             static_cast<uint8_t>(key_buf[i]));
            }
            LOG(ERROR) << "key: " << k_oss.str()
                       << " not found from cf: " << cfh->GetName()
                       << " with cf index: " << cf_rand
                       << " on worker: " << thread->tid_;
        }
#endif

        thread->stats_.FinishedOps(db_with_cfh, db_with_cfh->db_, 1, collect);
    }

    thread->stats_.AddBytes(bytes, read, found);
}

Benchmark::KeyGenerator::KeyGenerator(Random64 *rand,
                                      WriteMode mode,
                                      uint64_t num,
                                      uint64_t /*num_per_set*/)
    : rand_(rand), mode_(mode), num_(num), next_(0)
{
    if (mode_ == WriteMode::UNIQUE_RANDOM)
    {
        // NOTE: if memory consumption of this approach becomes a
        // concern, we can either break it into pieces and only random
        // shuffle a section each time. Alternatively, use a bit map
        // implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        values_.resize(num_);
        for (uint64_t i = 0; i < num_; ++i)
        {
            values_[i] = i;
        }
        assert(false);
        // RandomShuffle(values_.begin(), values_.end(),
        //               static_cast<uint32_t>(*seed_base));
    }
}

uint64_t Benchmark::KeyGenerator::Next()
{
    switch (mode_)
    {
    case WriteMode::SEQUENTIAL:
        return next_++;
    case WriteMode::RANDOM:
        return rand_->Next() % num_;
    case WriteMode::UNIQUE_RANDOM:
        assert(next_ < num_);
        return values_[next_++];
    }
    assert(false);
    return std::numeric_limits<uint64_t>::max();
}

// Only available for UNIQUE_RANDOM mode.
uint64_t Benchmark::KeyGenerator::Fetch(uint64_t index)
{
    assert(mode_ == WriteMode::UNIQUE_RANDOM);
    assert(index < values_.size());
    return values_[index];
}

}  // namespace RocksDBBM
#endif
