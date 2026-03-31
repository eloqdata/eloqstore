#include "eloq_store.h"

#include <glog/logging.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "cloud_storage_service.h"
#include "common.h"
#include "file_gc.h"
#include "standby_service.h"
#include "storage/shard.h"
#include "tasks/archive_crond.h"
#include "tasks/prewarm_task.h"
#include "utils.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>

#include "eloqstore_module.h"
#endif

#ifdef ELOQSTORE_WITH_TXSERVICE
#include "eloqstore_metrics.h"
#include "gflags/gflags.h"
#endif

namespace eloqstore
{
namespace
{
constexpr uint64_t kStorePathWeightGranularity = 1ULL << 20;  // 1 MiB
constexpr size_t kMaxStorePathLutEntries = kDefaultStorePathLutEntries;

KvError CollectLocalPartitions(const KvOptions &options,
                               std::vector<TableIdent> &partitions)
{
    partitions.clear();
    std::error_code ec;
#ifndef NDEBUG
    std::unordered_set<TableIdent> seen;
#endif
    for (const std::string &root_str : options.store_path)
    {
        const fs::path root(root_str);
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

            TableIdent tbl_id = TableIdent::FromString(entry.path().filename());
            if (!tbl_id.IsValid())
            {
                LOG(WARNING) << "unexpected partition " << entry.path();
                continue;
            }
            if (options.partition_filter && !options.partition_filter(tbl_id))
            {
                continue;
            }
#ifndef NDEBUG
            if (!seen.insert(tbl_id).second)
            {
                LOG(FATAL) << "Duplicated partition directory: " << tbl_id;
            }
#endif
            partitions.emplace_back(std::move(tbl_id));
        }
    }
    return KvError::NoError;
}
}  // namespace

bool EloqStore::ValidateOptions(KvOptions &opts)
{
    std::string cloud_provider_lower = opts.cloud_provider;
    std::transform(cloud_provider_lower.begin(),
                   cloud_provider_lower.end(),
                   cloud_provider_lower.begin(),
                   [](unsigned char c)
                   { return static_cast<char>(std::tolower(c)); });
    if (opts.cloud_endpoint.empty() &&
        (cloud_provider_lower == "gcs" || cloud_provider_lower == "google" ||
         cloud_provider_lower == "google-cloud"))
    {
        opts.cloud_endpoint = "https://storage.googleapis.com";
    }

    if (opts.num_threads == 0)
    {
        LOG(ERROR) << "Options num_threads cannot be zero";
        return false;
    }
    if (opts.max_inflight_write == 0)
    {
        LOG(ERROR) << "Option max_inflight_write cannot be zero";
        return false;
    }
    if (opts.max_global_request_batch == 0)
    {
        LOG(ERROR) << "Option max_global_request_batch cannot be zero";
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
        LOG(ERROR) << "cloud mode already support standby, reset "
                      "enable_local_standby to false";
        opts.enable_local_standby = false;
        opts.standby_master_addr.clear();
        opts.standby_master_store_paths.clear();
        opts.standby_master_addr.shrink_to_fit();
        opts.standby_master_store_paths.shrink_to_fit();
    }
    if (!opts.enable_local_standby && !opts.standby_master_addr.empty())
    {
        LOG(ERROR) << "standby_master_addr requires enable_local_standby";
        return false;
    }
    if (opts.enable_local_standby)
    {
        if (opts.store_path.empty())
        {
            LOG(ERROR) << "standby mode requires local store_path";
            return false;
        }
        if (!opts.store_path_weights.empty() &&
            opts.store_path_weights.size() != opts.store_path.size())
        {
            LOG(ERROR) << "store_path_weights must match store_path length";
            return false;
        }
        for (uint64_t weight : opts.store_path_weights)
        {
            if (weight == 0)
            {
                LOG(ERROR) << "store_path_weights entries must be > 0";
                return false;
            }
        }
        if (!opts.data_append_mode)
        {
            LOG(WARNING) << "append write mode should be enabled when standby "
                            "storage is enabled, enabling append mode";
            opts.data_append_mode = true;
        }
    }
    if (!opts.standby_master_addr.empty())
    {
        if (opts.store_path.empty())
        {
            LOG(ERROR) << "standby_master_addr requires local store_path";
            return false;
        }
        if (opts.standby_master_addr != "local" &&
            opts.standby_master_addr.find('@') == std::string::npos)
        {
            LOG(ERROR) << "standby_master_addr must be 'local' or "
                       << "'username@addr'";
            return false;
        }
        if (opts.standby_master_store_paths.size() != opts.store_path.size())
        {
            LOG(ERROR) << "standby_master_store_paths must match store_path "
                       << "length when standby_master_addr is set";
            return false;
        }
        if (!opts.standby_master_store_path_weights.empty() &&
            opts.standby_master_store_path_weights.size() !=
                opts.standby_master_store_paths.size())
        {
            LOG(ERROR) << "standby_master_store_path_weights must match "
                          "standby_master_store_paths length";
            return false;
        }
        for (uint64_t weight : opts.standby_master_store_path_weights)
        {
            if (weight == 0)
            {
                LOG(ERROR) << "standby_master_store_path_weights entries "
                              "must be > 0";
                return false;
            }
        }
        for (std::string &remote_path : opts.standby_master_store_paths)
        {
            if (remote_path.empty() || remote_path.front() != '/')
            {
                LOG(ERROR) << "standby_master_store_paths must be "
                           << "absolute paths, wrong path:" << remote_path;
                return false;
            }
        }
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

KvError EloqStore::UpdateStandbyMasterStorePaths(std::vector<std::string> paths,
                                                 std::vector<uint64_t> weights)
{
    if (paths.empty())
    {
        if (!weights.empty())
        {
            LOG(ERROR) << "standby_master_store_path_weights must be empty "
                          "when standby_master_store_paths is empty";
            return KvError::InvalidArgs;
        }
        options_.standby_master_store_paths.clear();
        options_.standby_master_store_path_weights.clear();
        options_.standby_master_store_path_lut.clear();
        if (standby_service_ != nullptr)
        {
            standby_service_->UpdateRemoteStorePaths({});
        }
        return KvError::NoError;
    }
    for (std::string &remote_path : paths)
    {
        if (remote_path.empty() || remote_path.front() != '/')
        {
            LOG(ERROR) << "standby_master_store_paths must be absolute paths";
            return KvError::InvalidArgs;
        }
        while (remote_path.size() > 1 && remote_path.back() == '/')
        {
            remote_path.pop_back();
        }
    }
    if (!weights.empty() && weights.size() != paths.size())
    {
        LOG(ERROR) << "standby_master_store_path_weights must match "
                      "standby_master_store_paths length";
        return KvError::InvalidArgs;
    }
    for (uint64_t weight : weights)
    {
        if (weight == 0)
        {
            LOG(ERROR) << "standby_master_store_path_weights entries must be "
                          "> 0";
            return KvError::InvalidArgs;
        }
    }

    options_.standby_master_store_paths = std::move(paths);
    options_.standby_master_store_path_weights = std::move(weights);
    if (options_.standby_master_store_path_weights.empty())
    {
        options_.standby_master_store_path_weights.resize(
            options_.standby_master_store_paths.size(), 1);
    }
    options_.standby_master_store_path_lut = std::move(ComputeStorePathLut(
        options_.standby_master_store_path_weights, kMaxStorePathLutEntries));
    if (options_.standby_master_store_path_lut.empty())
    {
        LOG(ERROR) << "Failed to compute standby master store path LUT";
        return KvError::InvalidArgs;
    }
    if (standby_service_ != nullptr)
    {
        standby_service_->UpdateRemoteStorePaths(
            options_.standby_master_store_paths);
    }
    return KvError::NoError;
}

KvError EloqStore::UpdateStandbyMasterAddr(std::string standby_master_addr)
{
    if (!standby_master_addr.empty() && standby_master_addr != "local" &&
        standby_master_addr.find('@') == std::string::npos)
    {
        LOG(ERROR) << "standby_master_addr must be 'local' or "
                   << "'username@addr'";
        return KvError::InvalidArgs;
    }
    options_.standby_master_addr = std::move(standby_master_addr);
    if (standby_service_ != nullptr)
    {
        standby_service_->UpdateRemoteAddr(options_.standby_master_addr);
    }
    return KvError::NoError;
}

EloqStore::EloqStore(const KvOptions &opts) : options_(opts)
{
    if (!ValidateOptions(options_))
    {
        LOG(FATAL) << "Invalid KvOptions configuration";
    }
}

EloqStore::~EloqStore()
{
    if (!IsStopped())
    {
        Stop();
    }
}

void EloqStore::CleanupRuntime(size_t started_shards)
{
    const size_t shard_count = std::min(started_shards, shards_.size());

    DLOG(INFO) << "EloqStore::CleanupRuntime stage=begin"
               << ", started_shards=" << shard_count;
    for (size_t i = 0; i < shard_count; ++i)
    {
        if (shards_[i] != nullptr)
        {
            shards_[i]->IoManager()->NotifyStoreStopping();
        }
    }
    if (prewarm_service_ != nullptr)
    {
        DLOG(INFO) << "EloqStore::CleanupRuntime stage=stop_prewarm";
        prewarm_service_->Stop();
    }
    if (archive_crond_ != nullptr)
    {
        DLOG(INFO) << "EloqStore::CleanupRuntime stage=stop_archive_crond";
        archive_crond_->Stop();
    }
#ifdef ELOQ_MODULE_ENABLED
    if (module_ != nullptr)
    {
        DLOG(INFO)
            << "EloqStore::CleanupRuntime stage=signal_module_workers_stop";
        for (size_t i = 0; i < shard_count; ++i)
        {
            if (shards_[i] != nullptr)
            {
                shards_[i]->running_status_.store(Shard::ShardStatus::Stopping,
                                                  std::memory_order_release);
                eloq::EloqModule::NotifyWorker(
                    static_cast<int>(shards_[i]->shard_id_));
            }
        }
        DLOG(INFO)
            << "EloqStore::CleanupRuntime stage=wait_module_workers_stop";
        while (true)
        {
            bool all_stopped = true;
            for (size_t i = 0; i < shard_count; ++i)
            {
                if (shards_[i] != nullptr && shards_[i]->running_status_.load(
                                                 std::memory_order_relaxed) !=
                                                 Shard::ShardStatus::Stopped)
                {
                    all_stopped = false;
                    break;
                }
            }
            if (all_stopped)
            {
                break;
            }
            bthread_usleep(1000);
        }
        DLOG(INFO) << "EloqStore::CleanupRuntime stage=unregister_module";
        eloq::unregister_module(module_.get());
    }
#endif

    DLOG(INFO) << "EloqStore::CleanupRuntime stage=stop_shards";
    for (size_t i = 0; i < shard_count; ++i)
    {
        if (shards_[i] != nullptr)
        {
            shards_[i]->Stop();
        }
    }

    DLOG(INFO) << "EloqStore::CleanupRuntime stage=clear_resources";
    shards_.clear();

    for (int fd : root_fds_)
    {
        [[maybe_unused]] int res = close(fd);
        assert(res == 0);
    }
    root_fds_.clear();
    // Stop external async services after shards to make sure running tasks
    // can finish.
    if (standby_service_)
    {
        DLOG(INFO) << "EloqStore::CleanupRuntime stage=stop_standby_service";
        standby_service_->Stop();
    }
    if (cloud_service_)
    {
        DLOG(INFO) << "EloqStore::CleanupRuntime stage=stop_cloud_service";
        cloud_service_->Stop();
    }
    if (eloq_store == this)
    {
        eloq_store = nullptr;
    }
}

KvError EloqStore::Start(std::string_view branch,
                         uint64_t term,
                         PartitonGroupId partition_group_id)
{
    LOG(INFO) << "===Start eloqstore, branch: " << branch << ", term: " << term;
    if (!ValidateOptions(options_))
    {
        return KvError::InvalidArgs;
    }
    if (!IsStopped())
    {
        return KvError::InvalidArgs;
    }

    size_t started_shards = 0;
    auto fail_start = [this, &started_shards](KvError err)
    {
        CleanupRuntime(started_shards);
        status_.store(Status::Stopped, std::memory_order_release);
        return err;
    };

    Status cur = status_.load(std::memory_order_acquire);
    while (true)
    {
        if (cur == Status::Running)
        {
            LOG(ERROR) << "EloqStore started , do not start again";
            return KvError::NoError;
        }
        if (cur == Status::Starting)
        {
            LOG(ERROR) << "EloqStore is starting, reject concurrent start";
            return KvError::Busy;
        }
        if (cur == Status::Stopping)
        {
            LOG(ERROR) << "EloqStore is stopping, reject start";
            return KvError::Busy;
        }
        if (cur == Status::Stopped &&
            status_.compare_exchange_weak(cur,
                                          Status::Starting,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        {
            break;
        }
    }

#ifdef ELOQSTORE_WITH_TXSERVICE
    const std::string store_path_list = BuildStorePathListWithWeights(
        options_.store_path, options_.store_path_weights);
    if (!store_path_list.empty())
    {
        GFLAGS_NAMESPACE::SetCommandLineOption("eloq_store_data_path_list",
                                               store_path_list.c_str());
    }
#endif

    StoreMode mode = DeriveStoreMode(options_);
    const StoreMode prev_mode =
        store_mode_.exchange(mode, std::memory_order_acq_rel);
    LOG(INFO) << "===Start eloqstore, term: " << term
              << ", partition_group_id: " << partition_group_id
              << ", mode: " << static_cast<int>(mode);
    if (prev_mode != mode)
    {
        LOG(INFO) << "EloqStore::Start update store mode, prev_mode="
                  << static_cast<int>(prev_mode)
                  << ", new_mode=" << static_cast<int>(mode);
    }
    if (mode == StoreMode::Cloud)
    {
        if (cloud_service_ == nullptr)
        {
            cloud_service_ = std::make_unique<CloudStorageService>(this);
        }
        standby_service_.reset();
    }
    else if (mode == StoreMode::StandbyMaster ||
             mode == StoreMode::StandbyReplica)
    {
        if (standby_service_ == nullptr)
        {
            standby_service_ = std::make_unique<StandbyService>(this);
        }
        cloud_service_.reset();
    }
    else
    {
        cloud_service_.reset();
        standby_service_.reset();
    }

    eloq_store = this;
    // Initialize
    if (!options_.store_path.empty())
    {
        DLOG(INFO) << "EloqStore::Start stage=init_store_space_begin";
        KvError err = InitStoreSpace();
        DLOG(INFO) << "EloqStore::Start stage=init_store_space_end err="
                   << ErrorString(err);
        if (err != KvError::NoError)
        {
            return fail_start(err);
        }
    }

    // local mode, set term to 0
    term_ = mode == StoreMode::Local ? 0 : term;
    partition_group_id_ = mode == StoreMode::Cloud ? partition_group_id : 0;
    branch_ = std::string(branch);

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
        DLOG(INFO) << "EloqStore::Start stage=shard_init_begin shard=" << i;
        if (shards_[i] == nullptr)
        {
            shards_[i] = std::make_unique<Shard>(this, i, shard_fd_limit);
        }
        KvError err = shards_[i]->Init();
        DLOG(INFO) << "EloqStore::Start stage=shard_init_end shard=" << i
                   << ", err=" << ErrorString(err);
        if (err != KvError::NoError)
        {
            return fail_start(err);
        }
    }

    if (cloud_service_)
    {
        DLOG(INFO) << "EloqStore::Start stage=sync_current_term_begin";
        DLOG(INFO) << "EloqStore::Start stage=cloud_service_start_begin";
        cloud_service_->Start();
        DLOG(INFO) << "EloqStore::Start stage=cloud_service_start_end";

        DLOG(INFO) << "EloqStore::Start stage=sync_current_term_wait";
        cloud_service_->bootstrap_state_.Wait();
        DLOG(INFO) << "EloqStore::Start stage=sync_current_term_done err="
                   << ErrorString(cloud_service_->bootstrap_state_.err_);
        if (cloud_service_->bootstrap_state_.err_ != KvError::NoError)
        {
            return fail_start(cloud_service_->bootstrap_state_.err_);
        }
    }
    else if (standby_service_)
    {
        DLOG(INFO) << "EloqStore::Start stage=standby_service_start_begin";
        standby_service_->Start();
        DLOG(INFO) << "EloqStore::Start stage=standby_service_start_end";
    }

    // Start threads.
    for (size_t i = 0; i < shards_.size(); ++i)
    {
        DLOG(INFO) << "EloqStore::Start stage=shard_start_begin shard=" << i;
        shards_[i]->Start();
        DLOG(INFO) << "EloqStore::Start stage=shard_start_end shard=" << i;
        ++started_shards;
    }

#ifdef ELOQ_MODULE_ENABLED
    DLOG(INFO) << "EloqStore::Start stage=module_register_begin";
    module_ = std::make_unique<EloqStoreModule>(&shards_);
    eloq::register_module(module_.get());
    for (auto &shard : shards_)
    {
        eloq::EloqModule::NotifyWorker(static_cast<int>(shard->shard_id_));
    }
    DLOG(INFO) << "EloqStore::Start stage=module_register_end";
#endif

    if (options_.data_append_mode && options_.num_retained_archives > 0 &&
        options_.archive_interval_secs > 0)
    {
        if (archive_crond_ == nullptr)
        {
            archive_crond_ = std::make_unique<ArchiveCrond>(this);
        }
        archive_crond_->Start();
    }

    if (options_.prewarm_cloud_cache && Mode() == StoreMode::Cloud)
    {
        if (prewarm_service_ == nullptr)
        {
            prewarm_service_ = std::make_unique<PrewarmService>(this);
        }
        prewarm_service_->Start();
    }

    status_.store(Status::Running, std::memory_order_release);
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
                    // Skip store-level CURRENT_TERM files
                    // (CURRENT_TERM_<branch>_<pg_id>)
                    std::string fname = ent.path().filename().string();
                    if (fname.compare(0,
                                      std::size(CurrentTermFileName) - 1,
                                      CurrentTermFileName) == 0)
                    {
                        continue;
                    }
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

    KvError weight_err = BuildStorePathLut();
    if (weight_err != KvError::NoError)
    {
        return weight_err;
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

KvError EloqStore::BuildStorePathLut()
{
    options_.store_path_lut.clear();
    if (options_.store_path.empty())
    {
        return KvError::NoError;
    }

    const size_t path_count = options_.store_path.size();
    std::vector<uint64_t> weights;
    if (options_.store_path_weights.size() != options_.store_path.size())
    {
        LOG(WARNING) << "store_path_weights has a different size from "
                        "store_path, reset to empty";
        options_.store_path_weights.clear();
    }
    if (options_.standby_master_store_path_weights.size() !=
        options_.standby_master_store_paths.size())
    {
        LOG(WARNING) << "standby_master_store_path_weights has a different "
                        "size from store_path, reset to empty";
        options_.standby_master_store_path_weights.clear();
    }
    if (!options_.store_path_weights.empty())
    {
        weights = options_.store_path_weights;
        for (uint64_t &weight : weights)
        {
            if (weight == 0)
            {
                weight = 1;
            }
        }
    }
    else
    {
        weights.resize(path_count, 1);

        if (!options_.enable_local_standby)
        {
            std::vector<uint64_t> device_sizes;
            std::vector<size_t> device_counts;
            std::vector<size_t> path_device_index(path_count, 0);
            std::unordered_map<uint64_t, size_t> device_lookup;

            for (size_t i = 0; i < path_count; ++i)
            {
                const fs::path &path = options_.store_path[i];
                struct stat stat_buf
                {
                };
                if (stat(path.c_str(), &stat_buf) != 0)
                {
                    int err = errno;
                    LOG(ERROR)
                        << "stat(" << path << ") failed: " << strerror(err);
                    return ToKvError(-err);
                }
                struct statvfs vfs_buf
                {
                };
                if (statvfs(path.c_str(), &vfs_buf) != 0)
                {
                    int err = errno;
                    LOG(ERROR)
                        << "statvfs(" << path << ") failed: " << strerror(err);
                    return ToKvError(-err);
                }
                uint64_t total_bytes =
                    static_cast<uint64_t>(vfs_buf.f_blocks) * vfs_buf.f_frsize;
                if (total_bytes == 0)
                {
                    total_bytes = 1;
                }
                uint64_t dev_key = static_cast<uint64_t>(stat_buf.st_dev);
                auto [it, inserted] =
                    device_lookup.emplace(dev_key, device_sizes.size());
                size_t dev_idx = it->second;
                if (inserted)
                {
                    device_sizes.push_back(total_bytes);
                    device_counts.push_back(1);
                }
                else
                {
                    device_counts[dev_idx] += 1;
                    device_sizes[dev_idx] =
                        std::min(device_sizes[dev_idx], total_bytes);
                }
                path_device_index[i] = dev_idx;
            }

            for (size_t i = 0; i < path_count; ++i)
            {
                size_t dev_idx = path_device_index[i];
                size_t dev_paths = std::max<size_t>(1, device_counts[dev_idx]);
                uint64_t per_path_bytes = device_sizes[dev_idx] / dev_paths;
                if (per_path_bytes == 0)
                {
                    per_path_bytes = 1;
                }
                uint64_t weight = per_path_bytes / kStorePathWeightGranularity;
                if (weight == 0)
                {
                    weight = 1;
                }
                weights[i] = weight;
            }
        }
    }

    for (size_t i = 0; i < weights.size(); ++i)
    {
        DLOG(INFO) << "Store path " << options_.store_path[i]
                   << " weight slots: " << weights[i];
    }
    auto lut = ComputeStorePathLut(weights, kMaxStorePathLutEntries);
    if (lut.empty())
    {
        LOG(ERROR) << "Failed to compute store path LUT";
        return KvError::InvalidArgs;
    }
    options_.store_path_lut = std::move(lut);
    if (options_.standby_master_store_path_weights.empty())
    {
        options_.standby_master_store_path_weights.resize(
            options_.standby_master_store_paths.size(), 1);
    }
    options_.standby_master_store_path_lut = std::move(ComputeStorePathLut(
        options_.standby_master_store_path_weights, kMaxStorePathLutEntries));
    if (!options_.standby_master_store_paths.empty() &&
        options_.standby_master_store_path_lut.empty())
    {
        LOG(ERROR) << "Failed to compute standby master store path LUT";
        return KvError::InvalidArgs;
    }
    DLOG(INFO) << "Constructed store_path LUT with "
               << options_.store_path_lut.size() << " entries";
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
#ifdef ELOQ_MODULE_ENABLED
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

    for (const TableIdent &partition : partitions)
    {
        auto trunc_req = std::make_unique<TruncateRequest>();
        trunc_req->SetArgs(partition, std::string_view{});
        req->truncate_reqs_.push_back(std::move(trunc_req));
    }

    struct DropTableScheduleState
        : public std::enable_shared_from_this<DropTableScheduleState>
    {
        EloqStore *store = nullptr;
        DropTableRequest *req = nullptr;
        size_t total = 0;
        std::atomic<size_t> next_index{0};

        bool HandleTruncateResult(KvError sub_err)
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

        void OnTruncateDone(KvRequest *sub_req)
        {
            if (HandleTruncateResult(sub_req->Error()))
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

                TruncateRequest *ptr = req->truncate_reqs_[idx].get();
                auto self = shared_from_this();
                auto on_truncate_done = [self](KvRequest *sub_req)
                { self->OnTruncateDone(sub_req); };
                if (store->ExecAsyn(ptr, 0, on_truncate_done))
                {
                    return;
                }

                LOG(ERROR) << "Handle droptable request, enqueue truncate "
                              "request fail";
                ptr->callback_ = nullptr;
                ptr->SetDone(KvError::NotRunning);
                if (HandleTruncateResult(KvError::NotRunning))
                {
                    return;
                }
            }
        }
    };

    auto state = std::make_shared<DropTableScheduleState>();
    state->store = this;
    state->req = req;
    state->total = req->truncate_reqs_.size();

    size_t max_inflight =
        std::max<uint32_t>(options_.max_global_request_batch, 1);
    if (max_inflight > state->total)
    {
        max_inflight = state->total;
    }

    for (size_t i = 0; i < max_inflight; ++i)
    {
        state->ScheduleNext();
    }
}

void EloqStore::HandleGlobalArchiveRequest(GlobalArchiveRequest *req)
{
    req->first_error_.store(static_cast<uint8_t>(KvError::NoError),
                            std::memory_order_relaxed);
    req->pending_.store(0, std::memory_order_relaxed);
    req->archive_reqs_.clear();

    std::string tag = req->Tag();
    const uint64_t term = req->Term();
    const GlobalArchiveRequest::Action action = req->GetAction();
    if (action == GlobalArchiveRequest::Action::Create && tag.empty())
    {
        tag = std::to_string(utils::UnixTs<chrono::microseconds>());
    }
    if (action == GlobalArchiveRequest::Action::Delete && tag.empty())
    {
        req->SetDone(KvError::InvalidArgs);
        return;
    }

    LOG(INFO) << "Handling global archive request action="
              << (action == GlobalArchiveRequest::Action::Create ? "create"
                                                                 : "delete")
              << " tag=" << tag << ", term=" << term;

    req->result_archive_ = BranchArchiveName(branch_, term_, tag);

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
#ifdef ELOQ_MODULE_ENABLED
            {
                std::lock_guard<bthread::Mutex> lk(list_request.mutex_);
                list_request.done_ = false;
            }
#else
            list_request.done_.store(false, std::memory_order_relaxed);
#endif
            list_request.err_ = KvError::NoError;
            list_request.GetNextContinuationToken()->clear();
            objects.clear();
            shards_[utils::RandomInt(static_cast<int>(shards_.size()))]
                ->AddKvRequest(&list_request);
            list_request.Wait();

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

    LOG(INFO) << "Scheduling archive action for " << all_partitions.size()
              << " partitions, tag=" << tag;

    req->archive_reqs_.reserve(all_partitions.size());
    for (const TableIdent &partition : all_partitions)
    {
        auto archive_req = std::make_unique<ArchiveRequest>();
        archive_req->SetTableId(partition);
        archive_req->SetAction(action == GlobalArchiveRequest::Action::Create
                                   ? ArchiveRequest::Action::Create
                                   : ArchiveRequest::Action::Delete);
        archive_req->SetTag(tag);
        archive_req->SetTerm(term);
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
    max_inflight = std::min(max_inflight,
                            static_cast<size_t>(std::max<uint32_t>(
                                options_.max_global_request_batch, 1)));
    if (max_inflight > state->total)
    {
        max_inflight = state->total;
    }

    for (size_t i = 0; i < max_inflight; ++i)
    {
        state->ScheduleNext();
    }
}

void EloqStore::HandleGlobalReopenRequest(GlobalReopenRequest *req)
{
    DLOG(INFO) << "HandleGlobalReopenRequest start, tag " << req->Tag()
               << ", mode " << static_cast<int>(Mode());
    req->first_error_.store(static_cast<uint8_t>(KvError::NoError),
                            std::memory_order_relaxed);
    req->pending_.store(0, std::memory_order_relaxed);
    req->reopen_reqs_.clear();

    std::vector<TableIdent> partitions;
    std::unordered_set<TableIdent> remote_partitions;
    if (Mode() == StoreMode::StandbyReplica)
    {
        std::vector<std::string> names;
        ListStandbyPartitionRequest list_request(&names);
#ifdef ELOQ_MODULE_ENABLED
        {
            std::lock_guard<bthread::Mutex> lk(list_request.mutex_);
            list_request.done_ = false;
        }
#else
        list_request.done_.store(false, std::memory_order_relaxed);
#endif
        list_request.err_ = KvError::NoError;
        shards_[utils::RandomInt(static_cast<int>(shards_.size()))]
            ->AddKvRequest(&list_request);
        list_request.Wait();
        if (list_request.Error() != KvError::NoError)
        {
            req->SetDone(list_request.Error());
            return;
        }

        partitions.reserve(names.size());
        for (const std::string &name : names)
        {
            TableIdent tbl_id = TableIdent::FromString(name);
            if (!tbl_id.IsValid())
            {
                continue;
            }
            if (options_.partition_filter && !options_.partition_filter(tbl_id))
            {
                continue;
            }
            remote_partitions.insert(tbl_id);
            partitions.emplace_back(std::move(tbl_id));
        }

        std::vector<TableIdent> local_partitions;
        KvError local_err = CollectLocalPartitions(options_, local_partitions);
        if (local_err != KvError::NoError)
        {
            req->SetDone(local_err);
            return;
        }
        for (TableIdent &tbl_id : local_partitions)
        {
            if (remote_partitions.contains(tbl_id))
            {
                continue;
            }
            partitions.emplace_back(std::move(tbl_id));
        }
    }
    else
    {
        KvError local_err = CollectLocalPartitions(options_, partitions);
        if (local_err != KvError::NoError)
        {
            req->SetDone(local_err);
            return;
        }
    }

    if (partitions.empty())
    {
        DLOG(INFO) << "HandleGlobalReopenRequest no partitions, tag "
                   << req->Tag();
        req->SetDone(KvError::NoError);
        return;
    }

    DLOG(INFO) << "HandleGlobalReopenRequest collected partitions, tag "
               << req->Tag() << ", partition_count " << partitions.size();
    req->reopen_reqs_.reserve(partitions.size());
    req->pending_.store(static_cast<uint32_t>(partitions.size()),
                        std::memory_order_relaxed);

    for (const TableIdent &partition : partitions)
    {
        auto reopen_req = std::make_unique<ReopenRequest>();
        reopen_req->SetArgs(partition);
        if (!req->Tag().empty())
        {
            reopen_req->SetTag(req->Tag());
        }
        if (Mode() == StoreMode::StandbyReplica &&
            !remote_partitions.contains(partition))
        {
            reopen_req->SetClean(true);
        }
        req->reopen_reqs_.push_back(std::move(reopen_req));
    }

    struct ReopenScheduleState
        : public std::enable_shared_from_this<ReopenScheduleState>
    {
        EloqStore *store = nullptr;
        GlobalReopenRequest *req = nullptr;
        size_t total = 0;
        std::atomic<size_t> next_index{0};

        bool HandleReopenResult(ReopenRequest *reopen_req)
        {
            KvError sub_err = reopen_req->Error();
            if (sub_err != KvError::NoError)
            {
                LOG(ERROR) << "HandleGlobalReopenRequest sub request failed, "
                           << "table " << reopen_req->TableId() << ", tag "
                           << reopen_req->Tag() << ", error "
                           << static_cast<uint32_t>(sub_err) << ", msg "
                           << reopen_req->ErrMessage();
                uint8_t expected = static_cast<uint8_t>(KvError::NoError);
                uint8_t desired = static_cast<uint8_t>(sub_err);
                req->first_error_.compare_exchange_strong(
                    expected,
                    desired,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed);
            }
            else
            {
                DLOG(INFO) << "HandleGlobalReopenRequest sub request "
                           << "succeeded, table " << reopen_req->TableId()
                           << ", tag " << reopen_req->Tag();
            }
            if (req->pending_.fetch_sub(1, std::memory_order_acq_rel) == 1)
            {
                KvError final_err = static_cast<KvError>(
                    req->first_error_.load(std::memory_order_relaxed));
                DLOG(INFO) << "HandleGlobalReopenRequest finish, tag "
                           << req->Tag() << ", final_error "
                           << static_cast<uint32_t>(final_err);
                req->SetDone(final_err);
                return true;
            }
            return false;
        }

        void OnReopenDone(KvRequest *sub_req)
        {
            auto *reopen_req = static_cast<ReopenRequest *>(sub_req);
            if (HandleReopenResult(reopen_req))
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

                ReopenRequest *ptr = req->reopen_reqs_[idx].get();
                DLOG(INFO) << "HandleGlobalReopenRequest enqueue partition "
                           << ptr->TableId() << ", tag " << req->Tag();
                auto self = shared_from_this();
                auto on_reopen_done = [self](KvRequest *sub_req)
                { self->OnReopenDone(sub_req); };
                if (store->ExecAsyn(ptr, 0, on_reopen_done))
                {
                    return;
                }

                LOG(ERROR) << "Handle global reopen request, enqueue reopen "
                              "request fail, partition "
                           << ptr->TableId() << ", tag " << req->Tag();
                ptr->callback_ = nullptr;
                ptr->SetDone(KvError::NotRunning);
                if (HandleReopenResult(ptr))
                {
                    return;
                }
            }
        }
    };

    auto state = std::make_shared<ReopenScheduleState>();
    state->store = this;
    state->req = req;
    state->total = req->reopen_reqs_.size();

    size_t max_inflight =
        std::max<uint32_t>(options_.max_global_request_batch, 1);
    if (max_inflight > state->total)
    {
        max_inflight = state->total;
    }

    for (size_t i = 0; i < max_inflight; ++i)
    {
        state->ScheduleNext();
    }
}

void EloqStore::HandleGlobalListArchiveTagsRequest(
    GlobalListArchiveTagsRequest *req)
{
    req->entries_.clear();
    std::error_code ec;
    for (const std::string &root : options_.store_path)
    {
        fs::directory_iterator part_it(fs::path(root), ec);
        if (ec)
        {
            ec.clear();
            continue;
        }

        for (; part_it != fs::directory_iterator{}; part_it.increment(ec))
        {
            if (ec)
            {
                ec.clear();
                break;
            }
            if (!part_it->is_directory(ec))
            {
                ec.clear();
                continue;
            }

            fs::directory_iterator file_it(part_it->path(), ec);
            if (ec)
            {
                ec.clear();
                continue;
            }
            for (; file_it != fs::directory_iterator{}; file_it.increment(ec))
            {
                if (ec)
                {
                    ec.clear();
                    break;
                }
                if (!file_it->is_regular_file(ec))
                {
                    ec.clear();
                    continue;
                }

                const std::string filename =
                    file_it->path().filename().string();
                auto [type, suffix] = ParseFileName(filename);
                if (type != FileNameManifest)
                {
                    continue;
                }

                uint64_t term = 0;
                std::string_view branch_name;
                std::optional<std::string> tag;
                if (!ParseManifestFileSuffix(suffix, branch_name, term, tag) ||
                    !tag.has_value())
                {
                    continue;
                }
                if (!req->prefix_.empty() && tag->rfind(req->prefix_, 0) != 0)
                {
                    continue;
                }
                req->entries_.push_back(
                    GlobalListArchiveTagsRequest::ArchiveEntry{
                        .term = term, .tag = std::move(*tag)});
            }
        }
    }

    std::sort(req->entries_.begin(),
              req->entries_.end(),
              [](const GlobalListArchiveTagsRequest::ArchiveEntry &lhs,
                 const GlobalListArchiveTagsRequest::ArchiveEntry &rhs)
              {
                  if (lhs.tag != rhs.tag)
                  {
                      return lhs.tag < rhs.tag;
                  }
                  return lhs.term < rhs.term;
              });
    req->entries_.erase(
        std::unique(req->entries_.begin(),
                    req->entries_.end(),
                    [](const GlobalListArchiveTagsRequest::ArchiveEntry &lhs,
                       const GlobalListArchiveTagsRequest::ArchiveEntry &rhs)
                    { return lhs.term == rhs.term && lhs.tag == rhs.tag; }),
        req->entries_.end());
    req->SetDone(KvError::NoError);
}

void EloqStore::HandleGlobalCreateBranchRequest(GlobalCreateBranchRequest *req)
{
    req->first_error_.store(static_cast<uint8_t>(KvError::NoError),
                            std::memory_order_relaxed);
    req->pending_.store(0, std::memory_order_relaxed);
    req->branch_reqs_.clear();

    // Early validation and salt generation.
    // The per-partition CreateBranch will normalize again, but we do it here
    // to validate up front and to build the salted internal name.
    std::string normalized = NormalizeBranchName(req->branch_name_);
    if (normalized.empty())
    {
        req->SetDone(KvError::InvalidArgs);
        return;
    }

    // Generate an 8-hex-char salt from the lower 32 bits of a timestamp.
    // If the caller supplied a salt timestamp (e.g. a backup_ts), use that so
    // the internal filename is deterministic and correlated with the backup.
    // Otherwise fall back to the live system clock.
    uint64_t salt_val =
        req->GetSaltTimestamp() != 0
            ? req->GetSaltTimestamp()
            : static_cast<uint64_t>(
                  std::chrono::system_clock::now().time_since_epoch().count());
    char salt_buf[9];
    std::snprintf(
        salt_buf, sizeof(salt_buf), "%08x", static_cast<uint32_t>(salt_val));
    std::string internal_name = normalized + "-" + salt_buf;
    req->result_branch_ = internal_name;

    LOG(INFO) << "Creating global branch " << req->GetBranchName()
              << " (internal: " << internal_name << ")";

    // Enumerate all partitions — mirrors HandleGlobalArchiveRequest.
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
                LOG(ERROR) << "Failed to list cloud objects for global branch "
                              "creation: "
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
        LOG(INFO) << "No partitions to branch (all filtered out or none exist)";
        req->SetDone(KvError::NoError);
        return;
    }

    LOG(INFO) << "Creating branch " << req->GetBranchName() << " on "
              << all_partitions.size() << " partitions";

    req->branch_reqs_.reserve(all_partitions.size());
    for (const TableIdent &partition : all_partitions)
    {
        auto branch_req = std::make_unique<CreateBranchRequest>();
        branch_req->SetTableId(partition);
        branch_req->SetArgs(internal_name);
        req->branch_reqs_.push_back(std::move(branch_req));
    }

    req->pending_.store(static_cast<uint32_t>(req->branch_reqs_.size()),
                        std::memory_order_relaxed);

    struct BranchScheduleState
        : public std::enable_shared_from_this<BranchScheduleState>
    {
        EloqStore *store = nullptr;
        GlobalCreateBranchRequest *req = nullptr;
        size_t total = 0;
        std::atomic<size_t> next_index{0};

        bool HandleBranchResult(KvError sub_err)
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

        void OnBranchDone(KvRequest *sub_req)
        {
            if (HandleBranchResult(sub_req->Error()))
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
                CreateBranchRequest *ptr = req->branch_reqs_[idx].get();
                auto self = shared_from_this();
                auto on_branch_done = [self](KvRequest *sub_req)
                { self->OnBranchDone(sub_req); };
                if (store->ExecAsyn(ptr, 0, on_branch_done))
                {
                    return;
                }

                LOG(ERROR) << "Handle global create branch request, enqueue "
                              "create branch request fail";
                // Clear callback_ first so SetDone doesn't invoke it.
                ptr->callback_ = nullptr;
                ptr->SetDone(KvError::NotRunning);
                if (HandleBranchResult(KvError::NotRunning))
                {
                    return;
                }
            }
        }
    };

    auto state = std::make_shared<BranchScheduleState>();
    state->store = this;
    state->req = req;
    state->total = req->branch_reqs_.size();

    size_t max_inflight =
        std::max<uint32_t>(options_.max_global_request_batch, 1);
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
    if (status_.load(std::memory_order_acquire) != Status::Running)
    {
        return false;
    }

    req->err_ = KvError::NoError;
#ifdef ELOQ_MODULE_ENABLED
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
    if (req->Type() == RequestType::GlobalReopen)
    {
        HandleGlobalReopenRequest(static_cast<GlobalReopenRequest *>(req));
        return true;
    }
    if (req->Type() == RequestType::GlobalListArchiveTags)
    {
        HandleGlobalListArchiveTagsRequest(
            static_cast<GlobalListArchiveTagsRequest *>(req));
        return true;
    }

    if (req->Type() == RequestType::GlobalCreateBranch)
    {
        HandleGlobalCreateBranchRequest(
            static_cast<GlobalCreateBranchRequest *>(req));
        return true;
    }

    Shard *shard = shards_[req->TableId().ShardIndex(shards_.size())].get();
    return shard->AddKvRequest(req);
}

void EloqStore::Stop()
{
    LOG(INFO) << "EloqStore stopping.";
    while (true)
    {
        Status current = status_.load(std::memory_order_acquire);
        if (current == Status::Stopped || current == Status::Stopping)
        {
            return;
        }
        if (current == Status::Starting)
        {
#ifdef ELOQ_MODULE_ENABLED
            bthread_usleep(1000);
#else
            std::this_thread::yield();
#endif
            continue;
        }
        if (current == Status::Running &&
            status_.compare_exchange_weak(current,
                                          Status::Stopping,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        {
            break;
        }
    }
    CleanupRuntime(shards_.size());
    status_.store(Status::Stopped, std::memory_order_release);
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
    return status_.load(std::memory_order_acquire) == Status::Stopped;
}

bool EloqStore::Inited() const
{
    for (const auto &shard : shards_)
    {
        if (!shard->io_mgr_and_page_pool_inited_.load(
                std::memory_order_acquire))
        {
            return false;
        }
    }
    return true;
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
#ifdef ELOQ_MODULE_ENABLED
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

void ArchiveRequest::SetSnapshotTimestamp(uint64_t ts)
{
    LOG_FIRST_N(WARNING, 1)
        << "ArchiveRequest::SetSnapshotTimestamp is deprecated. "
        << "Use SetTag(std::string) instead.";
    tag_ = std::to_string(ts);
}

void GlobalArchiveRequest::SetSnapshotTimestamp(uint64_t ts)
{
    LOG_FIRST_N(WARNING, 1)
        << "GlobalArchiveRequest::SetSnapshotTimestamp is deprecated. "
        << "Use SetTag(std::string) instead.";
    tag_ = std::to_string(ts);
}

void ReopenRequest::SetArgs(TableIdent tbl_id)
{
    SetTableId(std::move(tbl_id));
    tag_.clear();
    clean_ = false;
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
#ifdef ELOQ_MODULE_ENABLED
    std::lock_guard<bthread::Mutex> lk(mutex_);
    return done_;
#else
    return done_.load(std::memory_order_acquire);
#endif
}

void KvRequest::SetDone(KvError err)
{
    err_ = err;
#ifdef ELOQ_MODULE_ENABLED
    bool has_async_cb = false;
    {
        std::lock_guard<bthread::Mutex> lk(mutex_);
        done_ = true;
        has_async_cb = (callback_ != nullptr);
        if (!has_async_cb)
        {
            cv_.notify_one();
        }
    }
    if (has_async_cb)
    {
        callback_(this);
    }
#else
    done_.store(true, std::memory_order_release);
    if (callback_)
    {
        callback_(this);
    }
    else
    {
        done_.notify_one();
    }
#endif
}

}  // namespace eloqstore
