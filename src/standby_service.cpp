#include "standby_service.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstring>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "storage/shard.h"
#include "tasks/task.h"

extern char **environ;

namespace eloqstore
{
namespace fs = std::filesystem;
namespace
{
constexpr std::string_view kManifestTmp = "manifest.tmp";

KvError FromErrno(int err)
{
    if (err == 0)
    {
        return KvError::NoError;
    }
    return ToKvError(-err);
}

std::string QuoteForPosixShell(std::string_view value)
{
    std::string quoted;
    quoted.reserve(value.size() + 2);
    quoted.push_back('\'');
    for (char c : value)
    {
        if (c == '\'')
        {
            quoted.append("'\"'\"'");
        }
        else
        {
            quoted.push_back(c);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

}  // namespace

StandbyService::StandbyService(EloqStore *store) : store_(store)
{
    const KvOptions &options = store_->Options();
    shard_count_ = options.num_threads;
    ready_queues_.resize(shard_count_);
    if (!options.standby_master_addr.empty())
    {
        remote_addr_ = options.standby_master_addr;
        remote_store_paths_.clear();
        remote_store_paths_.reserve(options.standby_master_store_paths.size());
        for (const std::string &root : options.standby_master_store_paths)
        {
            remote_store_paths_.push_back(root);
            while (remote_store_paths_.back().size() > 1 &&
                   remote_store_paths_.back().back() == '/')
            {
                remote_store_paths_.back().pop_back();
            }
            if (remote_store_paths_.back().empty() ||
                remote_store_paths_.back().front() != '/')
            {
                remote_store_paths_.back().insert(
                    remote_store_paths_.back().begin(), '/');
            }
        }
    }
}

StandbyService::~StandbyService()
{
    Stop();
}

void StandbyService::Start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true))
    {
        return;
    }
    pending_jobs_.store(0, std::memory_order_relaxed);
    accepting_jobs_.store(true, std::memory_order_release);
    worker_ = std::thread(&StandbyService::WorkerLoop, this);
}

void StandbyService::Stop()
{
    bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (!was_running)
    {
        return;
    }
    accepting_jobs_.store(false, std::memory_order_release);
    while (pending_jobs_.load(std::memory_order_acquire) > 0)
    {
#ifdef ELOQ_MODULE_ENABLED
        bthread_usleep(1000);
#else
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
#endif
    }
    Job stop_job;
    stop_job.payload.emplace<std::monostate>();
    jobs_.enqueue(std::move(stop_job));
    if (worker_.joinable())
    {
        worker_.join();
    }
}

KvError StandbyService::RsyncPartition(const TableIdent &tbl_id,
                                       std::string archive_tag)
{
    Job job;
    auto &rsync = job.payload.emplace<RsyncJob>();
    rsync.tbl_id = tbl_id;
    rsync.archive_tag = std::move(archive_tag);
    CHECK(shard != nullptr);
    KvTask *task = ThdTask();
    CHECK(task != nullptr);
    job.context.shard_id = shard->shard_id_;
    job.context.task = task;
    return SubmitJob(std::move(job));
}

KvError StandbyService::CleanupLocalManifest(const TableIdent &tbl_id)
{
    Job job;
    auto &cleanup = job.payload.emplace<CleanupJob>();
    cleanup.tbl_id = tbl_id;
    CHECK(shard != nullptr);
    KvTask *task = ThdTask();
    CHECK(task != nullptr);
    job.context.shard_id = shard->shard_id_;
    job.context.task = task;
    return SubmitJob(std::move(job));
}

KvError StandbyService::ListRemotePartitions(
    std::vector<std::string> *partitions)
{
    if (partitions == nullptr)
    {
        return KvError::InvalidArgs;
    }
    Job job;
    auto &list = job.payload.emplace<ListPartitionsJob>();
    list.partitions = partitions;
    CHECK(shard != nullptr);
    KvTask *task = ThdTask();
    CHECK(task != nullptr);
    job.context.shard_id = shard->shard_id_;
    job.context.task = task;
    return SubmitJob(std::move(job));
}

KvError StandbyService::SubmitJob(Job &&job)
{
    if (std::holds_alternative<std::monostate>(job.payload))
    {
        return KvError::InvalidArgs;
    }
    if (job.context.task == nullptr)
    {
        return KvError::InvalidArgs;
    }
    if (!accepting_jobs_.load(std::memory_order_acquire))
    {
        return KvError::NotRunning;
    }
    pending_jobs_.fetch_add(1, std::memory_order_acq_rel);
    if (!accepting_jobs_.load(std::memory_order_acquire))
    {
        pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
        return KvError::NotRunning;
    }
    job.context.task->inflight_io_++;
    jobs_.enqueue(std::move(job));
    return KvError::NoError;
}

void StandbyService::WorkerLoop()
{
    while (true)
    {
        Job job;
        jobs_.wait_dequeue(job);

        if (std::holds_alternative<std::monostate>(job.payload))
        {
            return;
        }

        KvError result = KvError::NoError;
        if (const auto *rsync = std::get_if<RsyncJob>(&job.payload))
        {
            result = RunRsyncJob(*rsync);
        }
        else if (const auto *cleanup = std::get_if<CleanupJob>(&job.payload))
        {
            result = RunCleanupJob(*cleanup);
        }
        else if (const auto *list =
                     std::get_if<ListPartitionsJob>(&job.payload))
        {
            result = RunListPartitionsJob(*list);
        }
        CompleteJob(job, result);
    }
}

void StandbyService::CompleteJob(const Job &job, KvError result)
{
    uint64_t previous = pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
    CHECK_GT(previous, 0);
    if (job.context.task == nullptr)
    {
        return;
    }
    CHECK(!ready_queues_.empty());
    size_t idx = job.context.shard_id % ready_queues_.size();
    ready_queues_[idx].enqueue({job.context.task, result});
}

void StandbyService::ProcessReadyTasks(size_t shard_id)
{
    if (ready_queues_.empty())
    {
        return;
    }
    size_t idx = shard_id % ready_queues_.size();
    Completion completion;
    while (ready_queues_[idx].try_dequeue(completion))
    {
        if (completion.task == nullptr)
        {
            continue;
        }
        completion.task->io_res_ = static_cast<int>(completion.result);
        completion.task->FinishIo();
    }
}

fs::path StandbyService::TablePath(const TableIdent &tbl_id) const
{
    return tbl_id.StorePath(store_->Options().store_path,
                            store_->Options().store_path_lut);
}

std::string StandbyService::RemotePartitionPath(const TableIdent &tbl_id) const
{
    const KvOptions &options = store_->Options();
    size_t remote_path_idx =
        tbl_id.StorePathIndex(options.standby_master_store_paths.size(),
                              options.standby_master_store_path_lut);
    if (remote_path_idx >= remote_store_paths_.size())
    {
        return {};
    }
    std::string remote_path = remote_store_paths_[remote_path_idx];
    if (!remote_path.empty() && remote_path.back() != '/')
    {
        remote_path.push_back('/');
    }
    remote_path.append(tbl_id.ToString());
    return remote_path;
}

std::string StandbyService::RemoteArchiveManifestPath(const TableIdent &tbl_id,
                                                      std::string_view archive_tag) const
{
    std::string remote_path = RemotePartitionPath(tbl_id);
    if (remote_path.empty())
    {
        return {};
    }
    remote_path.push_back('/');
    remote_path.append(ArchiveName(store_->Term(), archive_tag));
    return remote_path;
}

std::string StandbyService::RemoteSpec(const std::string &path,
                                       bool directory) const
{
    std::string spec = path;
    if (directory && !spec.empty() && spec.back() != '/')
    {
        spec.push_back('/');
    }
    if (remote_addr_.empty() || remote_addr_ == "local")
    {
        return spec;
    }
    std::string remote = remote_addr_;
    remote.push_back(':');
    remote.append(spec);
    return remote;
}

KvError StandbyService::RunListPartitionsJob(const ListPartitionsJob &job)
{
    if (job.partitions == nullptr)
    {
        return KvError::InvalidArgs;
    }

    std::set<std::string> partitions;
    for (const std::string &store_path : remote_store_paths_)
    {
        if (store_path.empty())
        {
            continue;
        }

        std::string output;
        KvError err = KvError::NoError;
        if (remote_addr_.empty() || remote_addr_ == "local")
        {
            std::vector<const char *> argv = {
                "ls", "-1", "--", store_path.c_str(), nullptr};
            err = RunCommandCapture(argv, &output);
        }
        else
        {
            std::string cmd = "ls -1 -- " + QuoteForPosixShell(store_path);
            std::vector<const char *> argv = {
                "ssh", remote_addr_.c_str(), cmd.c_str(), nullptr};
            err = RunCommandCapture(argv, &output);
        }
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "StandbyService: failed to list partitions under "
                       << store_path << ": " << ErrorString(err);
            return err;
        }

        size_t pos = 0;
        while (pos < output.size())
        {
            size_t next = output.find('\n', pos);
            std::string name = output.substr(
                pos,
                next == std::string::npos ? std::string::npos : next - pos);
            if (!name.empty() && name.back() == '\r')
            {
                name.pop_back();
            }
            if (!name.empty())
            {
                TableIdent tbl_id = TableIdent::FromString(name);
                if (tbl_id.IsValid())
                {
                    partitions.insert(std::move(name));
                }
            }
            if (next == std::string::npos)
            {
                break;
            }
            pos = next + 1;
        }
    }

    job.partitions->assign(partitions.begin(), partitions.end());
    return KvError::NoError;
}

KvError StandbyService::RunRsyncJob(const RsyncJob &job)
{
    if (job.archive_tag.empty())
    {
        return KvError::InvalidArgs;
    }
    std::string remote_partition_path = RemotePartitionPath(job.tbl_id);
    std::string remote_manifest_path =
        RemoteArchiveManifestPath(job.tbl_id, job.archive_tag);
    if (remote_partition_path.empty() || remote_manifest_path.empty())
    {
        LOG(ERROR) << "StandbyService: remote partition path missing";
        return KvError::InvalidArgs;
    }

    fs::path table_dir = TablePath(job.tbl_id);
    std::error_code dir_ec;
    fs::create_directories(table_dir, dir_ec);
    if (dir_ec)
    {
        LOG(ERROR) << "StandbyService: failed to ensure table dir " << table_dir
                   << ": " << dir_ec.message();
        return FromErrno(dir_ec.value());
    }

    std::string dest = table_dir.string();
    if (!dest.empty() && dest.back() != '/')
    {
        dest.push_back('/');
    }
    std::string remote_partition_spec = RemoteSpec(remote_partition_path, true);
    std::vector<const char *> data_argv = {"rsync",
                                           "-a",
                                           "--inplace",
                                           "--include=data_*",
                                           "--exclude=*",
                                           remote_partition_spec.c_str(),
                                           dest.c_str(),
                                           nullptr};
    KvError rsync_err = RunRsyncCommand(data_argv, remote_partition_spec);
    if (rsync_err != KvError::NoError)
    {
        return rsync_err;
    }

    fs::path manifest_tmp = table_dir / kManifestTmp;
    std::string manifest_tmp_path = manifest_tmp.string();
    std::string remote_manifest_spec = RemoteSpec(remote_manifest_path, false);
    std::vector<const char *> manifest_argv = {"rsync",
                                               "-a",
                                               "--inplace",
                                               remote_manifest_spec.c_str(),
                                               manifest_tmp_path.c_str(),
                                               nullptr};
    rsync_err = RunRsyncCommand(manifest_argv, remote_manifest_spec);
    if (rsync_err != KvError::NoError)
    {
        return rsync_err;
    }

    if (!fs::exists(manifest_tmp))
    {
        LOG(ERROR) << "StandbyService: manifest.tmp missing after rsync: "
                   << manifest_tmp;
        return KvError::NotFound;
    }
    return KvError::NoError;
}

KvError StandbyService::RunCleanupJob(const CleanupJob &job)
{
    fs::path manifest_tmp = TablePath(job.tbl_id) / kManifestTmp;
    std::error_code ec;
    fs::remove(manifest_tmp, ec);
    if (ec)
    {
        LOG(WARNING) << "StandbyService: local cleanup failed for "
                     << manifest_tmp << ": " << ec.message();
    }
    return KvError::NoError;
}

KvError StandbyService::RunRsyncCommand(const std::vector<const char *> &args,
                                        const std::string &log_target)
{
    if (args.empty())
    {
        return KvError::InvalidArgs;
    }
    std::vector<char *> argv;
    argv.reserve(args.size() + 1);
    for (const char *arg : args)
    {
        argv.push_back(const_cast<char *>(arg));
    }
    argv.push_back(nullptr);
    pid_t pid = 0;
    int rc =
        posix_spawnp(&pid, "rsync", nullptr, nullptr, argv.data(), environ);
    if (rc != 0)
    {
        LOG(ERROR) << "StandbyService: posix_spawnp rsync failed: "
                   << strerror(rc);
        return KvError::IoFail;
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        LOG(ERROR) << "StandbyService: waitpid rsync failed: "
                   << strerror(errno);
        return KvError::IoFail;
    }
    if (WIFEXITED(status))
    {
        int code = WEXITSTATUS(status);
        if (code == 0)
        {
            return KvError::NoError;
        }
        if (code == 23 || code == 24)
        {
            LOG(WARNING) << "StandbyService: rsync source missing: exit "
                         << code << " for " << log_target;
            return KvError::NotFound;
        }
        LOG(ERROR) << "StandbyService: rsync exited with " << code;
        return KvError::IoFail;
    }
    if (WIFSIGNALED(status))
    {
        LOG(ERROR) << "StandbyService: rsync killed by signal "
                   << WTERMSIG(status);
        return KvError::IoFail;
    }
    return KvError::IoFail;
}

KvError StandbyService::RunSshCommand(
    const std::vector<const char *> &args) const
{
    if (args.empty())
    {
        return KvError::InvalidArgs;
    }
    pid_t pid = 0;
    std::vector<char *> argv;
    argv.reserve(args.size() + 1);
    for (const char *arg : args)
    {
        argv.push_back(const_cast<char *>(arg));
    }
    argv.push_back(nullptr);
    int rc = posix_spawnp(
        &pid, argv.front(), nullptr, nullptr, argv.data(), environ);
    if (rc != 0)
    {
        LOG(ERROR) << "StandbyService: posix_spawnp ssh failed: "
                   << strerror(rc);
        return KvError::IoFail;
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        LOG(ERROR) << "StandbyService: waitpid ssh failed: " << strerror(errno);
        return KvError::IoFail;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
    {
        return KvError::NoError;
    }
    LOG(WARNING) << "StandbyService: ssh command returned status " << status;
    return KvError::IoFail;
}

KvError StandbyService::RunCommandCapture(const std::vector<const char *> &args,
                                          std::string *output) const
{
    if (output == nullptr || args.empty() || args.back() != nullptr)
    {
        return KvError::InvalidArgs;
    }

    int pipe_fds[2];
    if (pipe(pipe_fds) != 0)
    {
        LOG(ERROR) << "StandbyService: pipe failed: " << strerror(errno);
        return ToKvError(-errno);
    }

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addclose(&actions, pipe_fds[0]);
    posix_spawn_file_actions_adddup2(&actions, pipe_fds[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, pipe_fds[1]);

    pid_t pid = 0;
    int rc = posix_spawnp(&pid,
                          args[0],
                          &actions,
                          nullptr,
                          const_cast<char *const *>(args.data()),
                          environ);
    posix_spawn_file_actions_destroy(&actions);
    close(pipe_fds[1]);
    if (rc != 0)
    {
        close(pipe_fds[0]);
        LOG(ERROR) << "StandbyService: posix_spawnp " << args[0]
                   << " failed: " << strerror(rc);
        return ToKvError(-rc);
    }

    output->clear();
    char buffer[4096];
    ssize_t nread = 0;
    while ((nread = read(pipe_fds[0], buffer, sizeof(buffer))) > 0)
    {
        output->append(buffer, static_cast<size_t>(nread));
    }
    int saved_errno = errno;
    close(pipe_fds[0]);
    if (nread < 0)
    {
        LOG(ERROR) << "StandbyService: read capture failed: "
                   << strerror(saved_errno);
        (void) waitpid(pid, nullptr, 0);
        return ToKvError(-saved_errno);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        LOG(ERROR) << "StandbyService: waitpid " << args[0]
                   << " failed: " << strerror(errno);
        return ToKvError(-errno);
    }
    if (WIFEXITED(status))
    {
        int code = WEXITSTATUS(status);
        if (code == 0)
        {
            return KvError::NoError;
        }
        LOG(WARNING) << "StandbyService: " << args[0] << " exited with "
                     << code;
        return KvError::IoFail;
    }
    if (WIFSIGNALED(status))
    {
        LOG(ERROR) << "StandbyService: " << args[0] << " killed by signal "
                   << WTERMSIG(status);
        return KvError::IoFail;
    }
    return KvError::IoFail;
}

}  // namespace eloqstore
