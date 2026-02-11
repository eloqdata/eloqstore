#include "eloq_store.h"

#include <glog/logging.h>
#include <sys/resource.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "cloud_storage_service.h"
#include "common.h"
#include "file_gc.h"
#include "storage/shard.h"
#include "tasks/archive_crond.h"
#include "tasks/prewarm_task.h"
#include "utils.h"

#ifdef ELOQ_MODULE_ENABLED
#include "eloqstore_module.h"
#endif

#ifdef ELOQSTORE_WITH_TXSERVICE
#include "eloqstore_metrics.h"
#endif

namespace eloqstore
{

bool EloqStore::ValidateOptions(KvOptions &opts)
{
    if (opts.max_inflight_write == 0)
    {
        LOG(ERROR) << "Option max_inflight_write cannot be zero";
        return false;
    }
    if ((opts.data_page_size & (page_align - 1)) != 0)
    {
        LOG(ERROR) << "Option data_page_size is not page aligned";
        return false;
    }
    if ((opts.coroutine_stack_size & (page_align - 1)) != 0)
    {
        LOG(ERROR) << "Option coroutine_stack_size is not page aligned";
        return false;
    }
    if (opts.write_buffer_size != 0 || opts.write_buffer_ratio != 0.0)
    {
        if (opts.write_buffer_size == 0 || opts.write_buffer_ratio <= 0.0 ||
            opts.write_buffer_ratio >= 1.0)
        {
            LOG(ERROR) << "write_buffer_size must be non-zero and "
                          "write_buffer_ratio must be in (0, 1) when enabled";
            return false;
        }
        if ((opts.write_buffer_size & (page_align - 1)) != 0)
        {
            LOG(ERROR) << "write_buffer_size must be page aligned";
            return false;
        }
    }
    if (opts.non_page_io_batch_size == 0 ||
        (opts.non_page_io_batch_size & (page_align - 1)) != 0)
    {
        LOG(ERROR)
            << "non_page_io_batch_size must be non-zero and page aligned";
        return false;
    }

    if (opts.overflow_pointers == 0 ||
        opts.overflow_pointers > max_overflow_pointers)
    {
        LOG(ERROR) << "Invalid option overflow_pointers";
        return false;
    }
    if (opts.max_write_batch_pages == 0)
    {
        LOG(ERROR) << "Invalid option max_write_batch_pages";
        return false;
    }
    if (!opts.cloud_store_path.empty())
    {
        if (opts.max_cloud_concurrency == 0)
        {
            LOG(ERROR) << "max_cloud_concurrency must be greater than 0";
            return false;
        }
        if (opts.max_write_concurrency == 0)
        {
            opts.max_write_concurrency = opts.max_cloud_concurrency;
            LOG(WARNING) << "max_write_concurrency is not set in cloud mode, "
                         << "resetting to max_cloud_concurrency "
                         << opts.max_write_concurrency;
        }
        if (opts.cloud_request_threads == 0)
        {
            LOG(ERROR) << "cloud_request_threads must be greater than 0";
            return false;
        }
        if (opts.local_space_limit == 0)
        {
            opts.local_space_limit = size_t(1) * TB;
            LOG(WARNING) << "local_space_limit is not set in cloud mode, "
                         << "resetting to default " << opts.local_space_limit;
        }
        if (!opts.data_append_mode)
        {
            LOG(WARNING) << "append write mode should be enabled when cloud "
                            "storage is enabled, enabling append mode";
            opts.data_append_mode = true;
        }

        const uint64_t data_file_pages = 1ULL << opts.pages_per_file_shift;
        const uint64_t data_file_bytes =
            static_cast<uint64_t>(opts.data_page_size) * data_file_pages;
        if (data_file_bytes == 0)
        {
            LOG(ERROR) << "Invalid data file size in cloud mode";
            return false;
        }

        uint64_t max_fd_limit = opts.local_space_limit / data_file_bytes;
        if (max_fd_limit == 0)
        {
            opts.local_space_limit = data_file_bytes;
            max_fd_limit = 1;
            LOG(WARNING) << "local_space_limit is too small to hold one data "
                         << "file, bumping to " << opts.local_space_limit;
        }

        size_t count_used_fd = utils::CountUsedFD();
        if (opts.fd_limit > max_fd_limit + num_reserved_fd + count_used_fd)
        {
            LOG(WARNING) << "fd_limit * data_page_size * (1 << "
                            "pages_per_file_shift) exceeds local_space_limit, "
                         << "clamping fd_limit from " << opts.fd_limit << " to "
                         << max_fd_limit + num_reserved_fd + count_used_fd;
            opts.fd_limit = static_cast<uint32_t>(max_fd_limit) +
                            num_reserved_fd + count_used_fd;
        }
    }
    else if (opts.prewarm_cloud_cache)
    {
        LOG(WARNING)
            << "prewarm_cloud_cache requires cloud_store_path to be set, "
               "disabling prewarm";
        opts.prewarm_cloud_cache = false;
    }

    if (opts.data_append_mode)
    {
        if (!opts.cloud_store_path.empty() && opts.DataFileSize() > (8 << 20))
        {
            LOG(WARNING) << "smaller file size is recommended in append write "
                            "mode with cloud storage";
        }
    }
    else
    {
        if (opts.DataFileSize() < (512 << 20))
        {
            LOG(WARNING) << "bigger file size is recommended in non-append "
                            "write mode";
        }
    }
    return true;
}

EloqStore::EloqStore(const KvOptions &opts) : options_(opts), stopped_(true)
{
    if (!ValidateOptions(options_))
    {
        LOG(FATAL) << "Invalid KvOptions configuration";
    }
    if (!options_.cloud_store_path.empty())
    {
        cloud_service_ = std::make_unique<CloudStorageService>(this);
    }
}

EloqStore::~EloqStore()
{
    if (!IsStopped())
    {
        Stop();
    }
}

KvError EloqStore::Start(uint64_t term)
{
    LOG(INFO) << "===Start eloqstore, term: " << term;
    if (!IsStopped())
    {
        LOG(ERROR) << "EloqStore started , do not start again";
        return KvError::NoError;
    }

    eloq_store = this;
    // Initialize
    if (!options_.store_path.empty())
    {
        KvError err = InitStoreSpace();
        CHECK_KV_ERR(err);
    }

    if (options_.cloud_store_path.empty())
    {
        // local mode, set term to 0
        term = 0;
    }
    else
    {
        term_ = term;
    }

    // There are files opened at very early stage like stdin/stdout/stderr, glog
    // file, and root directories of data.
    uint32_t shard_fd_limit = 0;
    size_t used_fd = utils::CountUsedFD();
    if (used_fd + num_reserved_fd < options_.fd_limit)
    {
        shard_fd_limit = (options_.fd_limit - used_fd - num_reserved_fd) /
                         options_.num_threads;
    }

    shards_.resize(options_.num_threads);
    for (size_t i = 0; i < options_.num_threads; i++)
    {
        if (shards_[i] == nullptr)
        {
            shards_[i] = std::make_unique<Shard>(this, i, shard_fd_limit);
        }
        KvError err = shards_[i]->Init();
        CHECK_KV_ERR(err);
    }

    if (cloud_service_)
    {
        cloud_service_->Start();
    }

    // Start threads.
    stopped_.store(false, std::memory_order_relaxed);

    if (options_.data_append_mode && options_.num_retained_archives > 0 &&
        options_.archive_interval_secs > 0)
    {
        if (archive_crond_ == nullptr)
        {
            archive_crond_ = std::make_unique<ArchiveCrond>(this);
        }
        archive_crond_->Start();
    }

    for (auto &shard : shards_)
    {
        shard->Start();
    }

#ifdef ELOQ_MODULE_ENABLED
    module_ = std::make_unique<EloqStoreModule>(&shards_);
    eloq::register_module(module_.get());
#endif
    if (!options_.cloud_store_path.empty() && options_.prewarm_cloud_cache)
    {
        if (prewarm_service_ == nullptr)
        {
            prewarm_service_ = std::make_unique<PrewarmService>(this);
        }
        prewarm_service_->Start();
    }

    LOG(INFO) << "EloqStore is started.";
    return KvError::NoError;
}

KvError EloqStore::InitStoreSpace()
{
    const bool cloud_store = !options_.cloud_store_path.empty();
    for (const fs::path store_path : options_.store_path)
    {
        if (fs::exists(store_path))
        {
            if (!fs::is_directory(store_path))
            {
                LOG(ERROR) << "path " << store_path << " is not directory";
                return KvError::InvalidArgs;
            }
            if (cloud_store && !options_.allow_reuse_local_caches &&
                !std::filesystem::is_empty(store_path))
            {
                LOG(ERROR) << store_path
                           << " is not empty in cloud store mode, clear "
                              "the directory";
                return KvError::InvalidArgs;
            }
            for (auto &ent : fs::directory_iterator{store_path})
            {
                if (!ent.is_directory())
                {
                    LOG(ERROR) << ent.path() << " is not directory";
                    return KvError::InvalidArgs;
                }
            }
        }
        else
        {
            fs::create_directories(store_path);
        }
    }

    assert(root_fds_.empty());
    for (const fs::path store_path : options_.store_path)
    {
        int res = open(store_path.c_str(), IouringMgr::oflags_dir);
        if (res < 0)
        {
            for (int fd : root_fds_)
            {
                [[maybe_unused]] int r = close(fd);
                assert(r == 0);
            }
            root_fds_.clear();
            return ToKvError(res);
        }
        root_fds_.push_back(res);
    }
    return KvError::NoError;
}

bool EloqStore::ExecAsyn(KvRequest *req)
{
    req->user_data_ = 0;
    req->callback_ = nullptr;
    return SendRequest(req);
}

void EloqStore::ExecSync(KvRequest *req)
{
    req->user_data_ = 0;
    req->callback_ = nullptr;
    if (SendRequest(req))
    {
        req->Wait();
    }
    else
    {
        req->SetDone(KvError::NotRunning);
    }
}

KvError EloqStore::CollectTablePartitions(
    const std::string &table_name, std::vector<TableIdent> &partitions) const
{
    partitions.clear();
    std::error_code ec;
    if (options_.cloud_store_path.empty())
    {
#ifndef NDEBUG
        std::unordered_set<TableIdent> seen;
#endif
        for (const fs::path root : options_.store_path)
        {
            fs::directory_iterator dir_it(root, ec);
            if (ec)
            {
                return ToKvError(-ec.value());
            }
            fs::directory_iterator end;
            for (; dir_it != end; dir_it.increment(ec))
            {
                if (ec)
                {
                    return ToKvError(-ec.value());
                }
                const fs::directory_entry &entry = *dir_it;
                bool is_dir = entry.is_directory(ec);
                if (ec)
                {
                    return ToKvError(-ec.value());
                }
                if (!is_dir)
                {
                    continue;
                }
                std::string name = entry.path().filename().string();
                DLOG(INFO) << "CollectTablePartitions: " << name;
                TableIdent ident = TableIdent::FromString(name);
                if (!ident.IsValid() || ident.tbl_name_ != table_name)
                {
                    continue;
                }
#ifndef NDEBUG
                if (!seen.insert(ident).second)
                {
                    LOG(FATAL) << "Duplicated partition directory for table "
                               << table_name << ": " << ident;
                }
#endif
                partitions.push_back(std::move(ident));
            }
        }
    }
    else
    {
        std::vector<std::string> objects;
        ListObjectRequest list_object_request(&objects);

        bool has_more = false;
        do
        {
#ifdef ELOQSTORE_WITH_TXSERVICE
            {
                std::lock_guard<bthread::Mutex> lk(list_object_request.mutex_);
                list_object_request.done_ = false;
            }
#else
            list_object_request.done_.store(false, std::memory_order_relaxed);
#endif
            list_object_request.GetNextContinuationToken()->clear();
            objects.clear();

            shards_[utils::RandomInt(static_cast<int>(shards_.size()))]
                ->AddKvRequest(&list_object_request);
            list_object_request.Wait();

            KvError list_err = list_object_request.Error();
            if (list_err != KvError::NoError)
            {
                return list_err;
            }

            if (partitions.empty())
            {
                partitions.reserve(objects.size());
            }

            for (auto &object_name : objects)
            {
                TableIdent ident = TableIdent::FromString(object_name);
                if (!ident.IsValid() || ident.tbl_name_ != table_name)
                {
                    continue;
                }
                partitions.push_back(std::move(ident));
            }

            has_more = list_object_request.HasMoreResults();
            if (has_more)
            {
                std::string next_token =
                    std::move(*list_object_request.GetNextContinuationToken());
                list_object_request.SetContinuationToken(std::move(next_token));
                list_object_request.GetNextContinuationToken()->clear();
            }
        } while (has_more);
    }
    return KvError::NoError;
}

void EloqStore::HandleDropTableRequest(DropTableRequest *req)
{
    req->first_error_.store(static_cast<uint8_t>(KvError::NoError),
                            std::memory_order_relaxed);
    req->pending_.store(0, std::memory_order_relaxed);
    req->truncate_reqs_.clear();

    std::vector<TableIdent> partitions;
    KvError err = CollectTablePartitions(req->TableName(), partitions);
    if (err != KvError::NoError)
    {
        req->SetDone(err);
        return;
    }

    if (partitions.empty())
    {
        req->SetDone(KvError::NoError);
        return;
    }

    req->truncate_reqs_.reserve(partitions.size());
    req->pending_.store(static_cast<uint32_t>(partitions.size()),
                        std::memory_order_relaxed);

    auto on_truncate_done = [req](KvRequest *sub_req)
    {
        KvError sub_err = sub_req->Error();
        if (sub_err != KvError::NoError)
        {
            uint8_t expected = static_cast<uint8_t>(KvError::NoError);
            uint8_t desired = static_cast<uint8_t>(sub_err);
            req->first_error_.compare_exchange_strong(
                expected,
                desired,
                std::memory_order_relaxed,
                std::memory_order_relaxed);
        }
        if (req->pending_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            KvError final_err = static_cast<KvError>(
                req->first_error_.load(std::memory_order_relaxed));
            req->SetDone(final_err);
        }
    };

    req->truncate_reqs_.reserve(partitions.size());
    for (const TableIdent &partition : partitions)
    {
        auto trunc_req = std::make_unique<TruncateRequest>();
        trunc_req->SetArgs(partition, std::string_view{});
        TruncateRequest *ptr = trunc_req.get();
        req->truncate_reqs_.push_back(std::move(trunc_req));
        if (!ExecAsyn(ptr, 0, on_truncate_done))
        {
            LOG(ERROR)
                << "Handle droptable request, enqueue truncate request fail";
            ptr->SetDone(KvError::NotRunning);
        }
    }
}

void EloqStore::HandleGlobalArchiveRequest(GlobalArchiveRequest *req)
{
    req->first_error_.store(static_cast<uint8_t>(KvError::NoError),
                            std::memory_order_relaxed);
    req->pending_.store(0, std::memory_order_relaxed);
    req->archive_reqs_.clear();

    uint64_t snapshot_ts = req->GetSnapshotTimestamp();
    if (snapshot_ts == 0)
    {
        snapshot_ts = utils::UnixTs<chrono::microseconds>();
    }

    LOG(INFO) << "Creating global snapshot with timestamp " << snapshot_ts;

    std::vector<TableIdent> all_partitions;
    if (options_.cloud_store_path.empty())
    {
        std::error_code ec;
        for (const fs::path root : options_.store_path)
        {
            const fs::path db_path(root);
            fs::directory_iterator dir_it(db_path, ec);
            if (ec)
            {
                req->SetDone(ToKvError(-ec.value()));
                return;
            }
            fs::directory_iterator end;
            for (; dir_it != end; dir_it.increment(ec))
            {
                if (ec)
                {
                    req->SetDone(ToKvError(-ec.value()));
                    return;
                }
                const fs::directory_entry &ent = *dir_it;
                const fs::path ent_path = ent.path();
                bool is_dir = fs::is_directory(ent_path, ec);
                if (ec)
                {
                    req->SetDone(ToKvError(-ec.value()));
                    return;
                }
                if (!is_dir)
                {
                    continue;
                }

                TableIdent tbl_id = TableIdent::FromString(ent_path.filename());
                if (tbl_id.tbl_name_.empty())
                {
                    LOG(WARNING) << "unexpected partition " << ent.path();
                    continue;
                }

                if (options_.partition_filter &&
                    !options_.partition_filter(tbl_id))
                {
                    continue;
                }

                all_partitions.emplace_back(std::move(tbl_id));
            }
        }
    }
    else
    {
        std::vector<std::string> objects;
        ListObjectRequest list_request(&objects);
        list_request.SetRemotePath(std::string{});
        list_request.SetRecursive(false);
        do
        {
            objects.clear();
            ExecSync(&list_request);

            if (list_request.Error() != KvError::NoError)
            {
                LOG(ERROR) << "Failed to list cloud objects for snapshot: "
                           << static_cast<int>(list_request.Error());
                req->SetDone(list_request.Error());
                return;
            }

            if (all_partitions.empty())
            {
                all_partitions.reserve(objects.size());
            }

            for (auto &name : objects)
            {
                TableIdent tbl_id = TableIdent::FromString(name);
                if (!tbl_id.IsValid())
                {
                    continue;
                }

                if (options_.partition_filter &&
                    !options_.partition_filter(tbl_id))
                {
                    continue;
                }

                all_partitions.emplace_back(std::move(tbl_id));
            }

            if (list_request.HasMoreResults())
            {
                list_request.SetContinuationToken(
                    *list_request.GetNextContinuationToken());
            }
        } while (list_request.HasMoreResults());
    }

    if (all_partitions.empty())
    {
        LOG(INFO) << "No partitions to snapshot (all filtered out or none "
                     "exist)";
        req->SetDone(KvError::NoError);
        return;
    }

    LOG(INFO) << "Snapshotting " << all_partitions.size()
              << " partitions with timestamp " << snapshot_ts;

    req->archive_reqs_.reserve(all_partitions.size());
    for (const TableIdent &partition : all_partitions)
    {
        auto archive_req = std::make_unique<ArchiveRequest>();
        archive_req->SetTableId(partition);
        archive_req->SetSnapshotTimestamp(snapshot_ts);
        req->archive_reqs_.push_back(std::move(archive_req));
    }

    req->pending_.store(static_cast<uint32_t>(req->archive_reqs_.size()),
                        std::memory_order_relaxed);

    struct ArchiveScheduleState
        : public std::enable_shared_from_this<ArchiveScheduleState>
    {
        EloqStore *store = nullptr;
        GlobalArchiveRequest *req = nullptr;
        size_t total = 0;
        std::atomic<size_t> next_index{0};

        bool HandleArchiveResult(KvError sub_err)
        {
            if (sub_err != KvError::NoError)
            {
                uint8_t expected = static_cast<uint8_t>(KvError::NoError);
                uint8_t desired = static_cast<uint8_t>(sub_err);
                req->first_error_.compare_exchange_strong(
                    expected,
                    desired,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed);
            }
            if (req->pending_.fetch_sub(1, std::memory_order_acq_rel) == 1)
            {
                KvError final_err = static_cast<KvError>(
                    req->first_error_.load(std::memory_order_relaxed));
                req->SetDone(final_err);
                return true;
            }
            return false;
        }

        void OnArchiveDone(KvRequest *sub_req)
        {
            if (HandleArchiveResult(sub_req->Error()))
            {
                return;
            }
            ScheduleNext();
        }

        void ScheduleNext()
        {
            while (true)
            {
                size_t idx = next_index.fetch_add(1, std::memory_order_relaxed);
                if (idx >= total)
                {
                    return;
                }
                ArchiveRequest *ptr = req->archive_reqs_[idx].get();
                auto self = shared_from_this();
                auto on_archive_done = [self](KvRequest *sub_req)
                { self->OnArchiveDone(sub_req); };
                if (store->ExecAsyn(ptr, 0, on_archive_done))
                {
                    return;
                }

                LOG(ERROR) << "Handle global archive request, enqueue archive "
                              "request fail";
                // Clear callback_ first so SetDone doesn't invoke it.
                ptr->callback_ = nullptr;
                ptr->SetDone(KvError::NotRunning);
                if (HandleArchiveResult(KvError::NotRunning))
                {
                    return;
                }
            }
        }
    };

    auto state = std::make_shared<ArchiveScheduleState>();
    state->store = this;
    state->req = req;
    state->total = req->archive_reqs_.size();

    size_t max_inflight = options_.max_archive_tasks;
    if (max_inflight == 0)
    {
        max_inflight = 1;
    }
    if (max_inflight > state->total)
    {
        max_inflight = state->total;
    }

    for (size_t i = 0; i < max_inflight; ++i)
    {
        state->ScheduleNext();
    }
}

bool EloqStore::SendRequest(KvRequest *req)
{
    if (stopped_.load(std::memory_order_relaxed))
    {
        return false;
    }

    req->err_ = KvError::NoError;
#ifdef ELOQSTORE_WITH_TXSERVICE
    {
        std::lock_guard<bthread::Mutex> lk(req->mutex_);
        req->done_ = false;
    }
#else
    req->done_.store(false, std::memory_order_relaxed);
#endif

    if (req->Type() == RequestType::DropTable)
    {
        HandleDropTableRequest(static_cast<DropTableRequest *>(req));
        return true;
    }

    if (req->Type() == RequestType::GlobalArchive)
    {
        HandleGlobalArchiveRequest(static_cast<GlobalArchiveRequest *>(req));
        return true;
    }

    Shard *shard = shards_[req->TableId().ShardIndex(shards_.size())].get();
    return shard->AddKvRequest(req);
}

void EloqStore::Stop()
{
#ifdef ELOQ_MODULE_ENABLED
    eloq::unregister_module(module_.get());
#endif
    if (archive_crond_ != nullptr)
    {
        archive_crond_->Stop();
    }

    if (prewarm_service_ != nullptr)
    {
        prewarm_service_->Stop();
    }

    stopped_.store(true, std::memory_order_relaxed);
    for (auto &shard : shards_)
    {
        shard->Stop();
    }

    if (cloud_service_)
    {
        cloud_service_->Stop();
    }

    // Start clear resources after all threads stopped.

    shards_.clear();

    for (int fd : root_fds_)
    {
        [[maybe_unused]] int res = close(fd);
        assert(res == 0);
    }
    root_fds_.clear();
    LOG(INFO) << "EloqStore is stopped.";
}

#ifdef ELOQSTORE_WITH_TXSERVICE
void EloqStore::InitializeMetrics(metrics::MetricsRegistry *metrics_registry,
                                  const metrics::CommonLabels &common_labels)
{
    // Resize meters array to match number of shards
    metrics_meters_.resize(options_.num_threads);

    if (metrics_registry == nullptr)
    {
        return;
    }

    // Create and initialize meter for each shard
    for (size_t i = 0; i < options_.num_threads; ++i)
    {
        // Add shard_id to common labels for this shard
        metrics::CommonLabels shard_labels = common_labels;
        shard_labels["shard_id"] = std::to_string(i);

        // Create meter for this shard
        metrics_meters_[i] =
            std::make_unique<metrics::Meter>(metrics_registry, shard_labels);

        // Register metrics for this shard
        metrics_meters_[i]->Register(
            metrics::NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION,
            metrics::Type::Histogram);
        metrics_meters_[i]->Register(
            metrics::NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS,
            metrics::Type::Gauge);
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_REQUEST_LATENCY,
                                     metrics::Type::Histogram,
                                     {{"request_type",
                                       {"read",
                                        "floor",
                                        "scan",
                                        "list_object",
                                        "batch_write",
                                        "truncate",
                                        "drop_table",
                                        "archive",
                                        "compact",
                                        "clean_expired"}}});
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_REQUESTS_COMPLETED,
                                     metrics::Type::Counter,
                                     {{"request_type",
                                       {"read",
                                        "floor",
                                        "scan",
                                        "list_object",
                                        "batch_write",
                                        "truncate",
                                        "drop_table",
                                        "archive",
                                        "compact",
                                        "clean_expired"}}});
        metrics_meters_[i]->Register(
            metrics::NAME_ELOQSTORE_INDEX_BUFFER_POOL_USED,
            metrics::Type::Gauge);
        metrics_meters_[i]->Register(
            metrics::NAME_ELOQSTORE_INDEX_BUFFER_POOL_LIMIT,
            metrics::Type::Gauge);
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_OPEN_FILE_COUNT,
                                     metrics::Type::Gauge);
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_OPEN_FILE_LIMIT,
                                     metrics::Type::Gauge);
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_LOCAL_SPACE_USED,
                                     metrics::Type::Gauge);
        metrics_meters_[i]->Register(metrics::NAME_ELOQSTORE_LOCAL_SPACE_LIMIT,
                                     metrics::Type::Gauge);
    }

    enable_eloqstore_metrics_ = true;
}

metrics::Meter *EloqStore::GetMetricsMeter(size_t shard_id) const
{
    if (shard_id >= metrics_meters_.size())
    {
        return nullptr;
    }

    assert(shard_id < metrics_meters_.size());
    return metrics_meters_[shard_id].get();
}
#endif

const KvOptions &EloqStore::Options() const
{
    return options_;
}

bool EloqStore::IsStopped() const
{
    return stopped_.load(std::memory_order_relaxed);
}

void KvRequest::SetTableId(TableIdent tbl_id)
{
    tbl_id_ = std::move(tbl_id);
}

KvError KvRequest::Error() const
{
    return err_;
}

bool KvRequest::ReadOnly() const
{
    return Type() < RequestType::BatchWrite;
}

bool KvRequest::RetryableErr() const
{
    return IsRetryableErr(err_);
}

const char *KvRequest::ErrMessage() const
{
    return ErrorString(err_);
}

uint64_t KvRequest::UserData() const
{
    return user_data_;
}

void KvRequest::Wait() const
{
    CHECK(callback_ == nullptr);
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::unique_lock<bthread::Mutex> lk(mutex_);
    while (!done_)
    {
        cv_.wait(lk);
    }
#else
    done_.wait(false, std::memory_order_acquire);
#endif
}

void ReadRequest::SetArgs(TableIdent tbl_id, const char *key)
{
    assert(key != nullptr);
    SetArgs(std::move(tbl_id), std::string_view(key));
}

void ReadRequest::SetArgs(TableIdent tbl_id, std::string_view key)
{
    SetTableId(std::move(tbl_id));
    key_.emplace<std::string_view>(key);
}

void ReadRequest::SetArgs(TableIdent tbl_id, std::string key)
{
    SetTableId(std::move(tbl_id));
    key_.emplace<std::string>(std::move(key));
}

std::string_view ReadRequest::Key() const
{
    return key_.index() == 0 ? std::get<std::string_view>(key_)
                             : std::get<std::string>(key_);
}

void FloorRequest::SetArgs(TableIdent tbl_id, const char *key)
{
    assert(key != nullptr);
    SetArgs(std::move(tbl_id), std::string_view(key));
}

void FloorRequest::SetArgs(TableIdent tbl_id, std::string_view key)
{
    SetTableId(std::move(tbl_id));
    key_.emplace<std::string_view>(key);
}

void FloorRequest::SetArgs(TableIdent tbl_id, std::string key)
{
    SetTableId(std::move(tbl_id));
    key_.emplace<std::string>(std::move(key));
}

std::string_view FloorRequest::Key() const
{
    return key_.index() == 0 ? std::get<std::string_view>(key_)
                             : std::get<std::string>(key_);
}

void ScanRequest::SetArgs(TableIdent tbl_id,
                          std::string_view begin,
                          std::string_view end,
                          bool begin_inclusive)
{
    SetTableId(std::move(tbl_id));
    begin_key_.emplace<std::string_view>(begin);
    end_key_.emplace<std::string_view>(end);
    begin_inclusive_ = begin_inclusive;
}

void ScanRequest::SetArgs(TableIdent tbl_id,
                          std::string begin,
                          std::string end,
                          bool begin_inclusive)
{
    SetTableId(std::move(tbl_id));
    begin_key_.emplace<std::string>(std::move(begin));
    end_key_.emplace<std::string>(std::move(end));
    begin_inclusive_ = begin_inclusive;
}

void ScanRequest::SetArgs(TableIdent tbl_id,
                          const char *begin,
                          const char *end,
                          bool begin_inclusive)
{
    std::string_view begin_key = begin == nullptr ? std::string_view{} : begin;
    std::string_view end_key = begin == nullptr ? std::string_view{} : end;
    SetArgs(std::move(tbl_id), begin_key, end_key, begin_inclusive);
}

void ScanRequest::SetPagination(size_t entries, size_t size)
{
    page_entries_ = entries != 0 ? entries : SIZE_MAX;
    page_size_ = size != 0 ? size : SIZE_MAX;

    if (page_entries_ != SIZE_MAX)
    {
        entries_.reserve(page_entries_);
    }
}

void ScanRequest::SetPrefetchPageNum(size_t pages)
{
    prefetch_page_num_ = pages == 0 ? kDefaultScanPrefetchPageCount : pages;
    if (prefetch_page_num_ > max_read_pages_batch)
    {
        prefetch_page_num_ = max_read_pages_batch;
    }
}

std::string_view ScanRequest::BeginKey() const
{
    return begin_key_.index() == 0 ? std::get<std::string_view>(begin_key_)
                                   : std::get<std::string>(begin_key_);
}

std::string_view ScanRequest::EndKey() const
{
    return end_key_.index() == 0 ? std::get<std::string_view>(end_key_)
                                 : std::get<std::string>(end_key_);
}

tcb::span<KvEntry> ScanRequest::Entries()
{
    return tcb::span<KvEntry>(entries_.data(), num_entries_);
}

std::pair<size_t, size_t> ScanRequest::ResultSize() const
{
    size_t size = 0;
    for (size_t i = 0; i < num_entries_; i++)
    {
        const KvEntry &entry = entries_[i];
        size += entry.key_.size() + entry.value_.size();
        size += sizeof(entry.timestamp_) + sizeof(entry.expire_ts_);
    }
    return {num_entries_, size};
}

bool ScanRequest::HasRemaining() const
{
    return has_remaining_;
}

size_t ScanRequest::PrefetchPageNum() const
{
    return prefetch_page_num_;
}

void BatchWriteRequest::SetArgs(TableIdent tbl_id,
                                std::vector<WriteDataEntry> &&batch)
{
    SetTableId(std::move(tbl_id));
    batch_ = std::move(batch);
}

void BatchWriteRequest::AddWrite(std::string key,
                                 std::string value,
                                 uint64_t ts,
                                 WriteOp op)
{
    batch_.push_back({std::move(key), std::move(value), ts, op});
}

void BatchWriteRequest::Clear()
{
    batch_.clear();
    batch_.shrink_to_fit();
}

void TruncateRequest::SetArgs(TableIdent tbl_id, std::string_view position)
{
    SetTableId(std::move(tbl_id));
    position_storage_.clear();
    position_ = position;
}

void TruncateRequest::SetArgs(TableIdent tbl_id, std::string position)
{
    SetTableId(std::move(tbl_id));
    position_storage_ = std::move(position);
    position_ = position_storage_;
}

void DropTableRequest::SetArgs(std::string table_name)
{
    if (!table_name.empty())
    {
        SetTableId({table_name, std::numeric_limits<uint32_t>::max()});
    }
    else
    {
        SetTableId({});
    }
    table_name_ = std::move(table_name);
    truncate_reqs_.clear();
    pending_.store(0, std::memory_order_relaxed);
    first_error_.store(static_cast<uint8_t>(KvError::NoError),
                       std::memory_order_relaxed);
}

const std::string &DropTableRequest::TableName() const
{
    return table_name_;
}

const TableIdent &KvRequest::TableId() const
{
    return tbl_id_;
}

bool KvRequest::IsDone() const
{
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::lock_guard<bthread::Mutex> lk(mutex_);
    return done_;
#else
    return done_.load(std::memory_order_acquire);
#endif
}

void KvRequest::SetDone(KvError err)
{
    err_ = err;
#ifdef ELOQSTORE_WITH_TXSERVICE
    {
        std::lock_guard<bthread::Mutex> lk(mutex_);
        done_ = true;
    }
#else
    done_.store(true, std::memory_order_release);
#endif
    if (callback_)
    {
        // Asynchronous request
        callback_(this);
    }
    else
    {
        // Synchronous request
#ifdef ELOQSTORE_WITH_TXSERVICE
        cv_.notify_one();
#else
        done_.notify_one();
#endif
    }
}

}  // namespace eloqstore
