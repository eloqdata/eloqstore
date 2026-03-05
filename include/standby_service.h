#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "common.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "concurrentqueue/concurrentqueue.h"
#include "eloq_store.h"
#include "error.h"
#include "kv_options.h"
#include "types.h"

namespace eloqstore
{

class KvTask;

class StandbyService
{
public:
    explicit StandbyService(EloqStore *store);
    ~StandbyService();

    void Start();
    void Stop();

    KvError RsyncPartition(const TableIdent &tbl_id, std::string archive_tag);
    KvError CleanupLocalManifest(const TableIdent &tbl_id);
    KvError ListRemotePartitions(std::vector<std::string> *partitions);
    void ProcessReadyTasks(size_t shard_id);

private:
    struct RsyncJob
    {
        TableIdent tbl_id;
        std::string archive_tag;
    };

    struct CleanupJob
    {
        TableIdent tbl_id;
    };

    struct ListPartitionsJob
    {
        std::vector<std::string> *partitions{nullptr};
    };

    struct TaskContext
    {
        size_t shard_id{0};
        KvTask *task{nullptr};
    };

    struct Job
    {
        using Payload = std::
            variant<std::monostate, RsyncJob, CleanupJob, ListPartitionsJob>;

        Payload payload;
        TaskContext context;
    };

    struct Completion
    {
        KvTask *task{nullptr};
        KvError result{KvError::NoError};
    };

    KvError SubmitJob(Job &&job);
    void CompleteJob(const Job &job, KvError result);
    void WorkerLoop();

    KvError RunRsyncJob(const RsyncJob &job);
    KvError RunCleanupJob(const CleanupJob &job);
    KvError RunListPartitionsJob(const ListPartitionsJob &job);

    static KvError RunRsyncCommand(const std::vector<const char *> &args,
                                   const std::string &log_target);
    KvError RunSshCommand(const std::vector<const char *> &args) const;
    KvError RunCommandCapture(const std::vector<const char *> &args,
                              std::string *output) const;

    fs::path TablePath(const TableIdent &tbl_id) const;
    std::string RemotePartitionPath(const TableIdent &tbl_id) const;
    std::string RemoteArchiveManifestPath(const TableIdent &tbl_id,
                                          std::string_view archive_tag) const;
    std::string RemoteSpec(const std::string &path, bool directory) const;

    EloqStore *store_{nullptr};
    size_t shard_count_{1};

    std::thread worker_;
    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_jobs_{false};
    std::atomic<uint64_t> pending_jobs_{0};
    moodycamel::BlockingConcurrentQueue<Job> jobs_;
    std::vector<moodycamel::ConcurrentQueue<Completion>> ready_queues_;

    // replica mode remote info
    std::string remote_addr_;
    std::vector<std::string> remote_store_paths_;
};

}  // namespace eloqstore
