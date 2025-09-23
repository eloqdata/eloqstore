#include "eloq_store.h"

#include <glog/logging.h>
#include <sys/resource.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "archive_crond.h"
#include "common.h"
#include "file_gc.h"
#include "object_store.h"
#include "shard.h"
#include "utils.h"

#ifdef ELOQ_MODULE_ENABLED
#include "eloqstore_module.h"
#endif

namespace eloqstore
{

bool EloqStore::ValidateOptions(const KvOptions &opts)
{
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
        if (opts.local_space_limit == 0)
        {
            LOG(ERROR)
                << "Must set local_space_limit when cloud store is enabled ";
            return false;
        }
        if (!opts.data_append_mode)
        {
            LOG(ERROR) << "append write mode should be enabled when cloud "
                          "storage is enabled";
            return false;
        }
        if (opts.fd_limit * opts.data_page_size *
                (1 << opts.pages_per_file_shift) >
            opts.local_space_limit)
        {
            LOG(ERROR) << "fd_limit * data_page_size * (1 << "
                          "pages_per_file_shift) should be smaller than "
                       << "local_space_limit";
            return false;
        }
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
    if (!ValidateOptions(opts))
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

KvError EloqStore::Start()
{
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

    // Start threads.
    stopped_.store(false, std::memory_order_relaxed);

    if (options_.data_append_mode)
    {
        // Initialize file garbage collector for both local and cloud modes
        if (file_gc_ == nullptr)
        {
            file_gc_ = std::make_unique<FileGarbageCollector>(&options_);
        }

        // Only start thread pool in local mode
        if (options_.cloud_store_path.empty() && options_.num_gc_threads > 0)
        {
            LOG(INFO) << "local file gc thread pool started";
            file_gc_->StartLocalThreadPool(options_.num_gc_threads);
        }
        else if (!options_.cloud_store_path.empty())
        {
            LOG(INFO) << "file gc initialized for cloud mode";
            if (options_.num_gc_threads > 0)
            {
                LOG(WARNING)
                    << "num_gc_threads=" << options_.num_gc_threads
                    << " is ignored in cloud mode; GC executes via cloud path.";
            }
        }

        if (options_.num_retained_archives > 0 &&
            options_.archive_interval_secs > 0)
        {
            if (archive_crond_ == nullptr)
            {
                archive_crond_ = std::make_unique<ArchiveCrond>(this);
            }
            archive_crond_->Start();
        }
    }

    for (auto &shard : shards_)
    {
        shard->Start();
    }

#ifdef ELOQ_MODULE_ENABLED
    module_ = std::make_unique<EloqStoreModule>(&shards_);
    eloq::register_module(module_.get());
#endif

    LOG(INFO) << "EloqStore is started.";
    return KvError::NoError;
}

KvError EloqStore::InitStoreSpace()
{
    rlimit fd_limit;
    int res = getrlimit(RLIMIT_NOFILE, &fd_limit);
    if (res < 0)
    {
        LOG(ERROR) << "failed to read open file limit: " << strerror(errno);
        return ToKvError(res);
    }
    DLOG(INFO) << "rlimit open file " << fd_limit.rlim_cur << ", hard limit "
               << fd_limit.rlim_max;
    if (fd_limit.rlim_cur < options_.fd_limit)
    {
        LOG(INFO) << "increase open file limit to " << options_.fd_limit;
        fd_limit.rlim_cur = options_.fd_limit;
        if (setrlimit(RLIMIT_NOFILE, &fd_limit) != 0)
        {
            LOG(ERROR) << "failed to increase open file limit: "
                       << strerror(errno);
            return KvError::OpenFileLimit;
        }
    }

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
            if (cloud_store && !std::filesystem::is_empty(store_path))
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
        res = open(store_path.c_str(), IouringMgr::oflags_dir);
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

bool EloqStore::SendRequest(KvRequest *req)
{
    if (stopped_.load(std::memory_order_relaxed))
    {
        return false;
    }

    req->err_ = KvError::NoError;
    req->done_.store(false, std::memory_order_relaxed);

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

    stopped_.store(true, std::memory_order_relaxed);
    for (auto &shard : shards_)
    {
        shard->Stop();
    }

    if (file_gc_ != nullptr)
    {
        file_gc_->Stop();
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
    done_.wait(false, std::memory_order_acquire);
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

void TruncateRequest::SetArgs(TableIdent tbl_id, std::string_view position)
{
    SetTableId(std::move(tbl_id));
    position_ = position;
}

const TableIdent &KvRequest::TableId() const
{
    return tbl_id_;
}

bool KvRequest::IsDone() const
{
    return done_.load(std::memory_order_acquire);
}

void KvRequest::SetDone(KvError err)
{
    err_ = err;
    done_.store(true, std::memory_order_release);
    if (callback_)
    {
        // Asynchronous request
        callback_(this);
    }
    else
    {
        // Synchronous request
        done_.notify_one();
    }
}

}  // namespace eloqstore