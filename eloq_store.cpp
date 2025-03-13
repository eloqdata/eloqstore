// clang-format off
#include "concurrentqueue.h"
#include "eloq_store.h"
// clang-format on

#include <glog/logging.h>

#include <atomic>
#include <boost/context/pooled_fixedsize_stack.hpp>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "task_manager.h"

namespace fs = std::filesystem;

namespace kvstore
{
class Worker
{
public:
    Worker(const EloqStore *store);
    ~Worker();
    KvError Init(int dir_fd);
    void Start();
    void Stop();
    bool AddRequest(KvRequest *req);

private:
    void Loop();
    void HandleReq(KvRequest *req);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        thd_task = task;
        task->coro_ =
            boost::context::callcc(std::allocator_arg,
                                   stack_pool_,
                                   [task, lbd](continuation &&sink)
                                   {
                                       task->main_ = std::move(sink);
                                       KvError err = lbd();
                                       task->req_->SetDone(err);
                                       task->req_ = nullptr;
                                       task->status_ = TaskStatus::Idle;
                                       task_mgr->finished_.Enqueue(task);
                                       return std::move(task->main_);
                                   });
    }

    const EloqStore *store_;
    moodycamel::ConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagePool page_pool_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
    boost::context::pooled_fixedsize_stack stack_pool_;
};

EloqStore::EloqStore(const KvOptions &opts) : options_(opts), stopped_(true)
{
    assert(!(options_.data_page_size & (page_align - 1)));

    workers_.reserve(options_.num_threads);
    for (size_t i = 0; i < options_.num_threads; i++)
    {
        workers_.emplace_back(std::make_unique<Worker>(this));
    }
}

EloqStore::~EloqStore()
{
    if (!IsStopped())
    {
        Stop();
    }
    else if (dir_fd_ >= 0)
    {
        // This will happen when the Start() is not successful
        CloseDBDir();
    }
}

KvError EloqStore::Start()
{
    LOG(INFO) << "EloqStore is starting...";
    if (!options_.db_path.empty())
    {
        KvError err = InitDBDir();
        CHECK_KV_ERR(err);
    }

    for (auto &w : workers_)
    {
        KvError err = w->Init(dir_fd_);
        CHECK_KV_ERR(err);
    }

    stopped_.store(false, std::memory_order_relaxed);
    for (auto &w : workers_)
    {
        w->Start();
    }
    return KvError::NoError;
}

KvError EloqStore::InitDBDir()
{
    if (fs::exists(options_.db_path))
    {
        if (!fs::is_directory(options_.db_path))
        {
            LOG(ERROR) << "path " << options_.db_path << " is not directory";
            return KvError::BadDir;
        }
        for (auto &ent : fs::directory_iterator{options_.db_path})
        {
            if (!ent.is_directory())
            {
                LOG(ERROR) << "entry " << ent.path() << " is not directory";
                return KvError::BadDir;
            }
            const std::string name = ent.path().filename().string();
            TableIdent tbl_id = TableIdent::FromString(name);
            if (tbl_id.tbl_name_.empty())
            {
                LOG(ERROR) << "unexpected tablespace name " << name;
                return KvError::BadDir;
            }

            fs::path wal_path = ent.path() / IouringMgr::mani_file;
            if (!fs::exists(wal_path))
            {
                LOG(WARNING) << "remove incomplete tablespace " << name;
                fs::remove_all(ent.path());
            }
        }
    }
    else
    {
        fs::create_directories(options_.db_path);
    }
    dir_fd_ = open(options_.db_path.c_str(), IouringMgr::oflags_dir);
    if (dir_fd_ < 0)
    {
        return KvError::IoFail;
    }
    return KvError::NoError;
}

void EloqStore::CloseDBDir()
{
    if (close(dir_fd_) == 0)
    {
        dir_fd_ = -1;
    }
    else
    {
        LOG(ERROR) << "failed to close database directory " << strerror(errno);
    }
}

bool EloqStore::ExecSync(KvRequest *req)
{
    req->user_data_ = 0;
    req->callback_ = nullptr;
    if (!SendRequest(req))
    {
        return false;
    }
    req->Wait();
    return true;
}

bool EloqStore::SendRequest(KvRequest *req)
{
    req->err_ = KvError::NoError;
    req->done_.store(false, std::memory_order_relaxed);

    if (stopped_.load(std::memory_order_relaxed))
    {
        return false;
    }

    Worker *worker =
        workers_[req->TableId().partition_id_ % workers_.size()].get();
    return worker->AddRequest(req);
}

void EloqStore::Stop()
{
    stopped_.store(true, std::memory_order_relaxed);
    for (auto &w : workers_)
    {
        w->Stop();
    }

    if (dir_fd_ >= 0)
    {
        CloseDBDir();
    }
    LOG(INFO) << "EloqStore is stopped.";
}

bool EloqStore::IsStopped() const
{
    return stopped_.load(std::memory_order_relaxed);
}

KvError KvRequest::Error() const
{
    return err_;
}

const char *KvRequest::ErrMessage() const
{
    return ErrorString(err_);
}

uint64_t KvRequest::UserData() const
{
    return user_data_;
}

void KvRequest::Wait()
{
    CHECK(callback_ == nullptr);
    done_.wait(false, std::memory_order_acquire);
}

void ReadRequest::SetArgs(TableIdent tid, std::string_view key)
{
    tbl_id_ = std::move(tid);
    key_ = key;
}

void ScanRequest::SetArgs(TableIdent tid,
                          std::string_view begin,
                          std::string_view end)
{
    tbl_id_ = std::move(tid);
    begin_key_ = begin;
    end_key_ = end;
}

void WriteRequest::SetArgs(TableIdent tid, std::vector<WriteDataEntry> &&batch)
{
    tbl_id_ = std::move(tid);
    batch_ = std::move(batch);
}

void TruncateRequest::SetArgs(TableIdent tid, std::string_view position)
{
    tbl_id_ = std::move(tid);
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

Worker::Worker(const EloqStore *store)
    : store_(store),
      page_pool_(store->options_.data_page_size),
      io_mgr_(AsyncIoManager::New(&store->options_)),
      index_mgr_(io_mgr_.get()),
      stack_pool_(store->options_.coroutine_stack_size)
{
}

Worker::~Worker()
{
    if (thd_.joinable())
    {
        thd_.join();
    }
}

KvError Worker::Init(int dir_fd)
{
    return io_mgr_->Init(dir_fd);
}

void Worker::Loop()
{
    while (true)
    {
        KvRequest *reqs[128];
        size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));
        for (size_t i = 0; i < nreqs; i++)
        {
            KvRequest *req = reqs[i];
            HandleReq(req);
        }
        if (nreqs == 0 && !task_mgr_.IsActive())
        {
            if (store_->IsStopped())
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        io_mgr_->Submit();
        io_mgr_->PollComplete();

        task_mgr_.ResumeScheduled();
        task_mgr_.RecycleFinished();
    }
}

void Worker::Start()
{
    thd_ = std::thread(
        [this]
        {
            // Set thread-local variables
            page_pool = &page_pool_;
            index_mgr = &index_mgr_;
            task_mgr = &task_mgr_;
            Loop();
        });
}

void Worker::Stop()
{
    thd_.join();
}

bool Worker::AddRequest(KvRequest *req)
{
    return requests_.enqueue(req);
}

void Worker::HandleReq(KvRequest *req)
{
    switch (req->Type())
    {
    case RequestType::Read:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            auto read_req = static_cast<ReadRequest *>(req);
            KvError err = task->Read(req->TableId(),
                                     read_req->key_,
                                     read_req->value_,
                                     read_req->ts_);
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Scan:
    {
        ScanTask *task = task_mgr_.GetScanTask();
        auto lbd = [task, req]() -> KvError
        {
            auto scan_req = static_cast<ScanRequest *>(req);
            return task->Scan(req->TableId(),
                              scan_req->begin_key_,
                              scan_req->end_key_,
                              scan_req->entries_);
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Write:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (!task)
        {
            req->SetDone(KvError::WriteConflict);
            break;
        }
        auto lbd = [task, req]() -> KvError
        {
            auto write_req = static_cast<WriteRequest *>(req);
            if (!task->SetBatch(std::move(write_req->batch_)))
            {
                return KvError::InvalidArgs;
            }
            KvError err = task->Apply();
            if (err != KvError::NoError)
            {
                task->Abort();
            }
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Truncate:
    {
        TruncateTask *task = task_mgr_.GetTruncateTask(req->TableId());
        if (!task)
        {
            req->SetDone(KvError::WriteConflict);
            break;
        }
        auto lbd = [task, req]() -> KvError
        {
            auto trunc_req = static_cast<TruncateRequest *>(req);
            KvError err = task->Truncate(trunc_req->position_);
            if (err != KvError::NoError)
            {
                task->Abort();
            }
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    }
}

}  // namespace kvstore