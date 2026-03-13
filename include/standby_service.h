#pragma once

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "common.h"
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
    void UpdateRemoteAddr(const std::string &remote_addr);
    void UpdateRemoteStorePaths(const std::vector<std::string> &store_paths);

    KvError RsyncPartition(const TableIdent &tbl_id, std::string archive_tag);
    KvError PrepareLocalManifest(const TableIdent &tbl_id,
                                 uint64_t target_term,
                                 uint64_t *source_term);
    KvError ListRemotePartitions(std::vector<std::string> *partitions);
    void ProcessReadyTasks(size_t shard_id);

private:
    struct RsyncJob
    {
        TableIdent tbl_id;
        std::string archive_tag;
    };

    struct ListPartitionsJob
    {
        std::vector<std::string> *partitions{nullptr};
    };

    struct PrepareManifestJob
    {
        TableIdent tbl_id;
        uint64_t target_term{0};
        uint64_t *source_term{nullptr};
    };

    struct TaskContext
    {
        size_t shard_id{0};
        KvTask *task{nullptr};
    };

    struct Job
    {
        using Payload = std::variant<std::monostate,
                                     RsyncJob,
                                     ListPartitionsJob,
                                     PrepareManifestJob>;

        Payload payload;
        TaskContext context;
    };

    struct Completion
    {
        KvTask *task{nullptr};
        KvError result{KvError::NoError};
    };

    enum class Step
    {
        None,
        RsyncData,
        RsyncManifest,
        ListPartitions
    };

    struct InflightJob
    {
        Job job;
        Step step{Step::None};
        std::string partition_key;
        pid_t pid{-1};
        int pidfd{-1};
        int stdout_fd{-1};
        std::string log_target;
        std::string capture_output;
        std::vector<std::string> list_store_paths;
        std::string list_remote_addr;
        size_t list_index{0};
        std::set<std::string> list_partitions;
    };

    KvError SubmitJob(Job &&job);
    void CompleteJob(const Job &job, KvError result);
    void SupervisorLoop();
    void DrainReadyQueues();
    void DrainSubmittedJobs();
    void RejectPendingJobs(KvError result);
    void LaunchPendingJobs();
    bool StartJob(Job &&job);
    static std::string PartitionKey(const Job &job);
    static void AddPartitionsFromOutput(std::string_view output,
                                        std::set<std::string> *partitions);

    KvError StartRsyncJob(std::list<InflightJob>::iterator it);
    KvError StartNextListPartitionsCommand(std::list<InflightJob>::iterator it);
    KvError RunListPartitionsJob(const ListPartitionsJob &job);
    KvError RunPrepareManifestJob(const PrepareManifestJob &job);
    void HandleExitedChildren();
    void HandleExitedChild(std::list<InflightJob>::iterator it, int status);
    void FinishInflightJob(std::list<InflightJob>::iterator it, KvError result);
    void DrainCaptureOutput(InflightJob *job);
    void ReleaseProcessHandles(InflightJob *job);
    void WakeSupervisor() const;
    static KvError SpawnChild(const std::vector<const char *> &args,
                              bool capture_stdout,
                              pid_t *pid,
                              int *pidfd,
                              int *stdout_fd);
    static KvError InterpretRsyncStatus(int status,
                                        std::string_view log_target);
    static KvError InterpretCommandStatus(int status, std::string_view command);

    fs::path TablePath(const TableIdent &tbl_id) const;
    std::string RemotePartitionPath(const TableIdent &tbl_id) const;
    std::string RemoteArchiveManifestPath(const TableIdent &tbl_id,
                                          std::string_view archive_tag) const;
    std::string RemoteSpec(const std::string &path, bool directory) const;

    EloqStore *store_{nullptr};
    size_t shard_count_{1};
    uint16_t max_concurrency_{1};

    std::thread supervisor_;
    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_jobs_{false};
    std::atomic<uint64_t> pending_jobs_{0};
    moodycamel::ConcurrentQueue<Job> jobs_;
    std::deque<Job> pending_queue_;
    std::list<InflightJob> inflight_jobs_;
    std::unordered_map<pid_t, std::list<InflightJob>::iterator>
        inflight_by_pid_;
    std::unordered_map<int, pid_t> pipe_to_pid_;
    std::unordered_set<std::string> active_partitions_;
    int epoll_fd_{-1};
    int wake_fd_{-1};
    std::vector<moodycamel::ConcurrentQueue<Completion>> ready_queues_;

    // replica mode remote info
    mutable std::mutex remote_mutex_;
    std::string remote_addr_;
    std::vector<std::string> remote_store_paths_;
};

}  // namespace eloqstore
