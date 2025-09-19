#pragma once

#include <thread>
#include <utility>

#include "error.h"
#include "kv_options.h"
#include "types.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
class KvTask;
class CloudStoreMgr;

class ObjectStore
{
public:
    explicit ObjectStore(const KvOptions *options) : options_(options) {};
    ~ObjectStore();
    void Start();
    void Stop();
    bool IsRunning() const;

    class Task
    {
    public:
        virtual ~Task() = default;
        Task(CloudStoreMgr *io_mgr, KvTask *kv_task, const TableIdent *tbl_id)
            : kv_task_(kv_task), tbl_id_(tbl_id), io_mgr_(io_mgr) {};
        enum class Type : uint8_t
        {
            Download = 0,
            Upload,
            Stop
        };
        virtual Type TaskType() = 0;

        KvTask *kv_task_;
        KvError error_{KvError::NoError};

    protected:
        const TableIdent *tbl_id_;
        CloudStoreMgr *io_mgr_{nullptr};
        friend class ObjectStore;
    };

    class DownloadTask : public Task
    {
    public:
        DownloadTask(CloudStoreMgr *io_mgr,
                     KvTask *kv_task,
                     const TableIdent *tbl_id,
                     std::string_view filename)
            : Task(io_mgr, kv_task, tbl_id), filename_(filename) {};
        Type TaskType() override
        {
            return Type::Download;
        };
        std::string_view filename_;
    };

    class UploadTask : public Task
    {
    public:
        UploadTask(CloudStoreMgr *io_mgr,
                   KvTask *kv_task,
                   const TableIdent *tbl_id,
                   std::vector<std::string> filenames)
            : Task(io_mgr, kv_task, tbl_id),
              filenames_(std::move(filenames)) {};
        Type TaskType() override
        {
            return Type::Upload;
        };
        std::vector<std::string> filenames_;
    };

    moodycamel::BlockingConcurrentQueue<Task *> submit_q_;

private:
    class StopSignal : public Task
    {
    public:
        StopSignal() : Task(nullptr, nullptr, nullptr) {};
        Type TaskType() override
        {
            return Type::Stop;
        };
    };

    void WorkLoop();
    KvError ExecRclone(std::string_view cmd) const;

    const KvOptions *options_;
    std::vector<std::thread> workers_;
};
}  // namespace eloqstore