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
#include "file_gc.h"
#include "object_store.h"
#include "shard.h"

namespace kvstore
{

EloqStore::EloqStore(const KvOptions &opts) : options_(opts), stopped_(true)
{
    if ((options_.data_page_size & (page_align - 1)) != 0)
    {
        LOG(FATAL) << "Option data_page_size is not page aligned";
    }
    if ((options_.coroutine_stack_size & (page_align - 1)) != 0)
    {
        LOG(FATAL) << "Option coroutine_stack_size is not page aligned";
    }

    if (options_.overflow_pointers == 0 ||
        options_.overflow_pointers > max_overflow_pointers)
    {
        LOG(FATAL) << "Invalid option overflow_pointers";
    }
    if (options_.max_write_batch_pages == 0 ||
        options_.max_write_batch_pages > max_write_pages_batch)
    {
        LOG(FATAL) << "Invalid option max_write_batch_pages";
    }

    if (!options_.cloud_store_path.empty())
    {
        if (options_.num_gc_threads > 0)
        {
            LOG(FATAL)
                << "num_gc_threads must be 0 when cloud store is enabled";
        }
        if (options_.local_space_limit == 0)
        {
            LOG(FATAL)
                << "Must set local_space_limit when cloud store is enabled ";
        }
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
    eloqstore = this;
    // Initialize
    if (!options_.store_path.empty())
    {
        KvError err = InitStoreSpace();
        CHECK_KV_ERR(err);
    }
    if (!options_.cloud_store_path.empty())
    {
        obj_store_ = std::make_unique<ObjectStore>(&options_);
    }
    shards_.resize(options_.num_threads);
    for (size_t i = 0; i < options_.num_threads; i++)
    {
        if (shards_[i] == nullptr)
        {
            shards_[i] = std::make_unique<Shard>(this);
        }
        KvError err = shards_[i]->Init();
        CHECK_KV_ERR(err);
    }

    // Start threads.
    stopped_.store(false, std::memory_order_relaxed);

    if (options_.data_append_mode)
    {
        if (options_.num_gc_threads > 0)
        {
            if (file_gc_ == nullptr)
            {
                file_gc_ = std::make_unique<FileGarbageCollector>(&options_);
            }
            file_gc_->Start(options_.num_gc_threads);
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

    if (!options_.cloud_store_path.empty())
    {
        obj_store_->Start();
    }

    for (auto &shard : shards_)
    {
        shard->Start();
    }

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
    uint32_t opt_limit = options_.fd_limit * options_.num_threads;
    if (fd_limit.rlim_cur < opt_limit)
    {
        LOG(INFO) << "increase open file limit from " << fd_limit.rlim_cur
                  << "(hard=" << fd_limit.rlim_max << ") to " << opt_limit;
        fd_limit.rlim_cur = opt_limit;
        if (setrlimit(RLIMIT_NOFILE, &fd_limit) != 0)
        {
            LOG(ERROR) << "failed to increase open file limit: "
                       << strerror(errno);
            return KvError::OpenFileLimit;
        }
    }

    const bool cloud_store = !options_.cloud_store_path.empty();
    for (const fs::path &store_path : options_.store_path)
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
                LOG(ERROR) << store_path << " is not empty in cloud store mode";
                return KvError::InvalidArgs;
            }
            for (auto &ent : fs::directory_iterator{store_path})
            {
                if (!ent.is_directory())
                {
                    LOG(ERROR) << ent.path() << " is not directory";
                    return KvError::InvalidArgs;
                }
                fs::path wal_path = ent.path() / FileNameManifest;
                if (!fs::exists(wal_path))
                {
                    LOG(WARNING) << "clear incomplete partition " << ent.path();
                    fs::remove_all(ent.path());
                }
            }
        }
        else
        {
            fs::create_directories(store_path);
        }
    }

    assert(root_fds_.empty());
    for (const fs::path &store_path : options_.store_path)
    {
        int res = open(store_path.c_str(), IouringMgr::oflags_dir);
        if (res < 0)
        {
            for (int fd : root_fds_)
            {
                int r = close(fd);
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

    Shard *shard = shards_[req->TableId().partition_id_ % shards_.size()].get();
    return shard->AddKvRequest(req);
}

void EloqStore::Stop()
{
    if (archive_crond_ != nullptr)
    {
        archive_crond_->Stop();
    }

    stopped_.store(true, std::memory_order_relaxed);
    for (auto &shard : shards_)
    {
        shard->Stop();
    }
    if (obj_store_ != nullptr)
    {
        obj_store_->Stop();
    }
    if (file_gc_ != nullptr)
    {
        file_gc_->Stop();
    }

    shards_.clear();

    for (int fd : root_fds_)
    {
        int res = close(fd);
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

void ReadRequest::SetArgs(TableIdent tbl_id, std::string_view key)
{
    SetTableId(std::move(tbl_id));
    key_ = key;
}

void FloorRequest::SetArgs(TableIdent tbl_id, std::string_view key)
{
    SetTableId(std::move(tbl_id));
    key_ = key;
}

void ScanRequest::SetArgs(TableIdent tbl_id,
                          std::string_view begin,
                          std::string_view end,
                          bool begin_inclusive)
{
    SetTableId(std::move(tbl_id));
    begin_key_ = begin;
    end_key_ = end;
    begin_inclusive_ = begin_inclusive;
}

void ScanRequest::SetPagination(size_t entries, size_t size)
{
    page_entries_ = entries != 0 ? entries : SIZE_MAX;
    page_size_ = size != 0 ? size : SIZE_MAX;
}

size_t ScanRequest::ResultSize() const
{
    size_t size = 0;
    for (const auto &[k, v, _] : entries_)
    {
        size += k.size() + v.size() + sizeof(uint64_t);
    }
    return size;
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

void ArchiveRequest::SetArgs(TableIdent tbl_id)
{
    SetTableId(std::move(tbl_id));
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

}  // namespace kvstore