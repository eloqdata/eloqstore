#include "standby_service.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <spawn.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#endif

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
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
constexpr int kPollingFallbackMs = 50;

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

int PidfdOpen(pid_t pid)
{
#ifdef SYS_pidfd_open
    return static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
#else
    errno = ENOSYS;
    return -1;
#endif
}

void CloseFd(int *fd)
{
    if (fd != nullptr && *fd >= 0)
    {
        close(*fd);
        *fd = -1;
    }
}

}  // namespace

StandbyService::StandbyService(EloqStore *store) : store_(store)
{
    const KvOptions &options = store_->Options();
    shard_count_ = options.num_threads;
    max_concurrency_ = std::max<uint16_t>(1, options.standby_max_concurrency);
    ready_queues_.resize(shard_count_);
    UpdateRemoteStorePaths(options.standby_master_store_paths);
    if (!options.standby_master_addr.empty())
    {
        UpdateRemoteAddr(options.standby_master_addr);
    }
}

StandbyService::~StandbyService()
{
    Stop();
}

void StandbyService::Start()
{
    DrainReadyQueues();
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true))
    {
        return;
    }

    pending_jobs_.store(0, std::memory_order_relaxed);
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    CHECK_GE(epoll_fd_, 0) << "StandbyService: epoll_create1 failed: "
                           << strerror(errno);
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK_GE(wake_fd_, 0) << "StandbyService: eventfd failed: "
                          << strerror(errno);
    epoll_event wake_event{};
    wake_event.events = EPOLLIN;
    wake_event.data.fd = wake_fd_;
    const int epoll_rc =
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_event);
    CHECK_EQ(epoll_rc, 0) << "StandbyService: epoll add wake fd failed: "
                          << strerror(errno);

    accepting_jobs_.store(true, std::memory_order_release);
    supervisor_ = std::thread(&StandbyService::SupervisorLoop, this);
}

void StandbyService::Stop()
{
    bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (!was_running)
    {
        return;
    }
    accepting_jobs_.store(false, std::memory_order_release);
    WakeSupervisor();
    if (supervisor_.joinable())
    {
        supervisor_.join();
    }
    CloseFd(&wake_fd_);
    CloseFd(&epoll_fd_);
    pending_queue_.clear();
    inflight_by_pid_.clear();
    pipe_to_pid_.clear();
    active_partitions_.clear();
    inflight_jobs_.clear();
    DrainReadyQueues();
}

void StandbyService::UpdateRemoteAddr(const std::string &remote_addr)
{
    std::lock_guard<std::mutex> lk(remote_mutex_);
    remote_addr_ = remote_addr;
}

void StandbyService::UpdateRemoteStorePaths(
    const std::vector<std::string> &store_paths)
{
    std::vector<std::string> normalized;
    normalized.reserve(store_paths.size());
    for (const std::string &root : store_paths)
    {
        normalized.push_back(root);
        while (normalized.back().size() > 1 && normalized.back().back() == '/')
        {
            normalized.back().pop_back();
        }
        if (normalized.back().empty() || normalized.back().front() != '/')
        {
            normalized.back().insert(normalized.back().begin(), '/');
        }
    }

    std::lock_guard<std::mutex> lk(remote_mutex_);
    remote_store_paths_ = std::move(normalized);
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

KvError StandbyService::PrepareLocalManifest(const TableIdent &tbl_id,
                                             uint64_t target_term,
                                             uint64_t *source_term)
{
    Job job;
    auto &prepare = job.payload.emplace<PrepareManifestJob>();
    prepare.tbl_id = tbl_id;
    prepare.target_term = target_term;
    prepare.source_term = source_term;
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
    WakeSupervisor();
    return KvError::NoError;
}

void StandbyService::SupervisorLoop()
{
    while (true)
    {
        DrainSubmittedJobs();
        HandleExitedChildren();

        if (!running_.load(std::memory_order_acquire))
        {
            RejectPendingJobs(KvError::NotRunning);
        }
        else
        {
            LaunchPendingJobs();
        }

        if (!running_.load(std::memory_order_acquire) && inflight_jobs_.empty())
        {
            return;
        }

        bool needs_polling = false;
        for (const InflightJob &job : inflight_jobs_)
        {
            if (job.pidfd < 0)
            {
                needs_polling = true;
                break;
            }
        }
        const int timeout_ms = inflight_jobs_.empty()
                                   ? -1
                                   : (needs_polling ? kPollingFallbackMs : -1);

        std::array<epoll_event, 32> events{};
        int nfds = epoll_wait(epoll_fd_,
                              events.data(),
                              static_cast<int>(events.size()),
                              timeout_ms);
        if (nfds < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            LOG(ERROR) << "StandbyService: epoll_wait failed: "
                       << strerror(errno);
            continue;
        }
        for (int i = 0; i < nfds; ++i)
        {
            const int fd = events[i].data.fd;
            if (fd == wake_fd_)
            {
                uint64_t wake_count = 0;
                while (read(wake_fd_, &wake_count, sizeof(wake_count)) > 0)
                {
                }
                continue;
            }

            auto pipe_it = pipe_to_pid_.find(fd);
            if (pipe_it == pipe_to_pid_.end())
            {
                continue;
            }
            auto inflight_it = inflight_by_pid_.find(pipe_it->second);
            if (inflight_it != inflight_by_pid_.end())
            {
                DrainCaptureOutput(&*inflight_it->second);
            }
        }
    }
}

void StandbyService::CompleteJob(const Job &job, KvError result)
{
    auto dec_pending_jobs = [this]()
    {
        uint64_t previous =
            pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
        CHECK_GT(previous, 0);
    };
    if (job.context.task == nullptr)
    {
        dec_pending_jobs();
        return;
    }
    CHECK(!ready_queues_.empty());
    size_t idx = job.context.shard_id % ready_queues_.size();
    ready_queues_[idx].enqueue({job.context.task, result});
    dec_pending_jobs();
}

void StandbyService::DrainReadyQueues()
{
    Completion completion;
    for (auto &ready_queue : ready_queues_)
    {
        while (ready_queue.try_dequeue(completion))
        {
        }
    }
}

void StandbyService::DrainSubmittedJobs()
{
    Job job;
    while (jobs_.try_dequeue(job))
    {
        pending_queue_.push_back(std::move(job));
    }
}

void StandbyService::RejectPendingJobs(KvError result)
{
    while (!pending_queue_.empty())
    {
        Job job = std::move(pending_queue_.front());
        pending_queue_.pop_front();
        CompleteJob(job, result);
    }
}

void StandbyService::LaunchPendingJobs()
{
    size_t remaining = pending_queue_.size();
    while (remaining > 0 &&
           inflight_jobs_.size() < static_cast<size_t>(max_concurrency_))
    {
        Job job = std::move(pending_queue_.front());
        pending_queue_.pop_front();
        --remaining;

        const std::string partition_key = PartitionKey(job);
        if (!partition_key.empty() &&
            active_partitions_.find(partition_key) != active_partitions_.end())
        {
            pending_queue_.push_back(std::move(job));
            continue;
        }

        if (!StartJob(std::move(job)))
        {
            continue;
        }
    }
}

bool StandbyService::StartJob(Job &&job)
{
    const std::string partition_key = PartitionKey(job);
    if (const auto *prepare = std::get_if<PrepareManifestJob>(&job.payload))
    {
        if (!partition_key.empty())
        {
            active_partitions_.insert(partition_key);
        }
        KvError result = RunPrepareManifestJob(*prepare);
        if (!partition_key.empty())
        {
            active_partitions_.erase(partition_key);
        }
        CompleteJob(job, result);
        return false;
    }

    inflight_jobs_.push_back({});
    auto it = std::prev(inflight_jobs_.end());
    it->job = std::move(job);
    it->partition_key = partition_key;
    if (!partition_key.empty())
    {
        active_partitions_.insert(partition_key);
    }

    KvError result = KvError::NoError;
    if (std::holds_alternative<RsyncJob>(it->job.payload))
    {
        result = StartRsyncJob(it);
    }
    else if (const auto *list =
                 std::get_if<ListPartitionsJob>(&it->job.payload))
    {
        std::vector<std::string> remote_store_paths;
        std::string remote_addr;
        {
            std::lock_guard<std::mutex> lk(remote_mutex_);
            remote_store_paths = remote_store_paths_;
            remote_addr = remote_addr_;
        }
        it->list_store_paths = std::move(remote_store_paths);
        it->list_remote_addr = std::move(remote_addr);
        if (list->partitions == nullptr)
        {
            result = KvError::InvalidArgs;
        }
        else
        {
            result = StartNextListPartitionsCommand(it);
            if (result == KvError::NoError && it->pid < 0)
            {
                FinishInflightJob(it, KvError::NoError);
                return false;
            }
        }
    }
    else
    {
        result = KvError::InvalidArgs;
    }

    if (result != KvError::NoError)
    {
        FinishInflightJob(it, result);
    }
    return true;
}

std::string StandbyService::PartitionKey(const Job &job)
{
    if (const auto *rsync = std::get_if<RsyncJob>(&job.payload))
    {
        return rsync->tbl_id.ToString();
    }
    if (const auto *prepare = std::get_if<PrepareManifestJob>(&job.payload))
    {
        return prepare->tbl_id.ToString();
    }
    return {};
}

void StandbyService::AddPartitionsFromOutput(std::string_view output,
                                             std::set<std::string> *partitions)
{
    if (partitions == nullptr)
    {
        return;
    }

    size_t pos = 0;
    while (pos < output.size())
    {
        size_t next = output.find('\n', pos);
        std::string name = std::string(output.substr(
            pos,
            next == std::string_view::npos ? std::string_view::npos
                                           : next - pos));
        if (!name.empty() && name.back() == '\r')
        {
            name.pop_back();
        }
        if (!name.empty())
        {
            TableIdent tbl_id = TableIdent::FromString(name);
            if (tbl_id.IsValid())
            {
                partitions->insert(std::move(name));
            }
        }
        if (next == std::string_view::npos)
        {
            break;
        }
        pos = next + 1;
    }
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
        if (completion.task->Type() == TaskType::ListStandbyPartition)
        {
            DLOG(INFO) << "StandbyService::ProcessReadyTasks execute "
                          "ListStandbyPartition task, shard_id="
                       << shard_id << ", task=" << completion.task
                       << ", result=" << static_cast<int>(completion.result);
        }
        completion.task->io_res_ = static_cast<int>(completion.result);
        completion.task->FinishIo();
        if (completion.task->Type() == TaskType::ListStandbyPartition)
        {
            DLOG(INFO) << "StandbyService::ProcessReadyTasks finished "
                          "ListStandbyPartition task, shard_id="
                       << shard_id << ", task=" << completion.task
                       << ", io_res=" << completion.task->io_res_;
        }
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
    if (options.standby_master_store_paths.empty() ||
        options.standby_master_store_path_lut.empty())
    {
        LOG(ERROR) << "StandbyService::RemotePartitionPath invalid standby "
                   << "path config for " << tbl_id
                   << ", standby_master_store_paths "
                   << options.standby_master_store_paths.size()
                   << ", standby_master_store_path_lut "
                   << options.standby_master_store_path_lut.size();
        return {};
    }
    std::vector<std::string> remote_store_paths;
    {
        std::lock_guard<std::mutex> lk(remote_mutex_);
        remote_store_paths = remote_store_paths_;
    }
    size_t remote_path_idx =
        tbl_id.StorePathIndex(options.standby_master_store_paths.size(),
                              options.standby_master_store_path_lut);
    if (remote_path_idx >= remote_store_paths.size())
    {
        LOG(ERROR) << "StandbyService::RemotePartitionPath missing store path "
                   << "for " << tbl_id << ", remote_path_idx "
                   << remote_path_idx << ", remote_store_paths "
                   << remote_store_paths.size()
                   << ", standby_master_store_paths "
                   << options.standby_master_store_paths.size()
                   << ", standby_master_store_path_lut "
                   << options.standby_master_store_path_lut.size();
        return {};
    }
    std::string remote_path = remote_store_paths[remote_path_idx];
    if (!remote_path.empty() && remote_path.back() != '/')
    {
        remote_path.push_back('/');
    }
    remote_path.append(tbl_id.ToString());
    return remote_path;
}

std::string StandbyService::RemoteArchiveManifestPath(
    const TableIdent &tbl_id, std::string_view archive_tag) const
{
    std::string remote_path = RemotePartitionPath(tbl_id);
    if (remote_path.empty())
    {
        return {};
    }
    remote_path.push_back('/');
    remote_path.append(
        BranchArchiveName(MainBranchName, store_->Term(), archive_tag));
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
    std::string remote_addr;
    {
        std::lock_guard<std::mutex> lk(remote_mutex_);
        remote_addr = remote_addr_;
    }
    if (remote_addr.empty() || remote_addr == "local")
    {
        return spec;
    }
    std::string remote = std::move(remote_addr);
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
    job.partitions->clear();
    return KvError::NoError;
}

KvError StandbyService::StartRsyncJob(std::list<InflightJob>::iterator it)
{
    const auto *job = std::get_if<RsyncJob>(&it->job.payload);
    if (job == nullptr)
    {
        return KvError::InvalidArgs;
    }
    if (job->archive_tag.empty())
    {
        LOG(ERROR) << "StandbyService::StartRsyncJob empty archive tag for "
                   << job->tbl_id;
        return KvError::InvalidArgs;
    }

    std::string remote_partition_path = RemotePartitionPath(job->tbl_id);
    std::string remote_manifest_path =
        RemoteArchiveManifestPath(job->tbl_id, job->archive_tag);
    if (remote_partition_path.empty() || remote_manifest_path.empty())
    {
        LOG(ERROR) << "StandbyService: remote partition path missing for "
                   << job->tbl_id << ", tag " << job->archive_tag
                   << ", remote_partition_path " << remote_partition_path
                   << ", remote_manifest_path " << remote_manifest_path;
        return KvError::InvalidArgs;
    }

    fs::path table_dir = TablePath(job->tbl_id);
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
    std::vector<const char *> argv = {"rsync",
                                      "-a",
                                      "--inplace",
                                      "--include=data_*",
                                      "--exclude=*",
                                      remote_partition_spec.c_str(),
                                      dest.c_str(),
                                      nullptr};
    KvError err = SpawnChild(argv, false, &it->pid, &it->pidfd, &it->stdout_fd);
    if (err != KvError::NoError)
    {
        return err;
    }
    if (it->pidfd >= 0)
    {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = it->pidfd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, it->pidfd, &event) != 0)
        {
            err = ToKvError(-errno);
            ReleaseProcessHandles(&*it);
            return err;
        }
    }
    it->step = Step::RsyncData;
    it->log_target = std::move(remote_partition_spec);
    inflight_by_pid_[it->pid] = it;
    return KvError::NoError;
}

KvError StandbyService::StartNextListPartitionsCommand(
    std::list<InflightJob>::iterator it)
{
    while (it->list_index < it->list_store_paths.size() &&
           it->list_store_paths[it->list_index].empty())
    {
        ++it->list_index;
    }

    if (it->list_index >= it->list_store_paths.size())
    {
        auto *job = std::get_if<ListPartitionsJob>(&it->job.payload);
        if (job == nullptr || job->partitions == nullptr)
        {
            return KvError::InvalidArgs;
        }
        job->partitions->assign(it->list_partitions.begin(),
                                it->list_partitions.end());
        return KvError::NoError;
    }

    const std::string &store_path = it->list_store_paths[it->list_index];
    std::vector<std::string> args_storage;
    std::vector<const char *> argv;
    if (it->list_remote_addr.empty() || it->list_remote_addr == "local")
    {
        args_storage = {"ls", "-1", "--", store_path};
    }
    else
    {
        args_storage = {"ssh",
                        it->list_remote_addr,
                        "ls -1 -- " + QuoteForPosixShell(store_path)};
    }
    argv.reserve(args_storage.size() + 1);
    for (const std::string &arg : args_storage)
    {
        argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    KvError err = SpawnChild(argv, true, &it->pid, &it->pidfd, &it->stdout_fd);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "StandbyService: failed to list partitions under "
                   << store_path << ": " << ErrorString(err);
        return err;
    }
    if (it->pidfd >= 0)
    {
        epoll_event pid_event{};
        pid_event.events = EPOLLIN;
        pid_event.data.fd = it->pidfd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, it->pidfd, &pid_event) != 0)
        {
            err = ToKvError(-errno);
            ReleaseProcessHandles(&*it);
            return err;
        }
    }
    if (it->stdout_fd >= 0)
    {
        epoll_event pipe_event{};
        pipe_event.events = EPOLLIN | EPOLLHUP | EPOLLRDHUP;
        pipe_event.data.fd = it->stdout_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, it->stdout_fd, &pipe_event) !=
            0)
        {
            err = ToKvError(-errno);
            ReleaseProcessHandles(&*it);
            return err;
        }
        pipe_to_pid_[it->stdout_fd] = it->pid;
    }
    it->step = Step::ListPartitions;
    it->log_target =
        (it->list_remote_addr.empty() || it->list_remote_addr == "local")
            ? "ls"
            : "ssh";
    it->capture_output.clear();
    inflight_by_pid_[it->pid] = it;
    return KvError::NoError;
}

KvError StandbyService::RunPrepareManifestJob(const PrepareManifestJob &job)
{
    fs::path table_dir = TablePath(job.tbl_id);
    std::error_code ec;
    fs::directory_iterator it(table_dir, ec);
    if (ec)
    {
        return FromErrno(ec.value());
    }

    bool found = false;
    uint64_t selected_term = 0;
    for (const auto &entry : it)
    {
        if (!entry.is_regular_file(ec))
        {
            continue;
        }
        if (ec)
        {
            return FromErrno(ec.value());
        }
        const std::string filename = entry.path().filename().string();
        auto [type, suffix] = ParseFileName(filename);
        if (type != FileNameManifest)
        {
            continue;
        }
        uint64_t term = 0;
        std::string_view branch_name;
        std::optional<std::string> tag;
        if (!ParseManifestFileSuffix(suffix, branch_name, term, tag))
        {
            continue;
        }
        if (tag.has_value())
        {
            continue;
        }
        if (term > job.target_term)
        {
            return KvError::ExpiredTerm;
        }
        if (!found || term > selected_term)
        {
            selected_term = term;
            found = true;
        }
    }

    if (!found)
    {
        return KvError::NotFound;
    }

    if (job.source_term != nullptr)
    {
        *job.source_term = selected_term;
    }

    bool mutated = false;
    if (selected_term != job.target_term)
    {
        const fs::path src =
            table_dir / BranchManifestFileName(MainBranchName, selected_term);
        const fs::path dst =
            table_dir / BranchManifestFileName(MainBranchName, job.target_term);
        if (fs::exists(dst, ec))
        {
            if (ec)
            {
                return FromErrno(ec.value());
            }
            fs::remove(dst, ec);
            if (ec)
            {
                return FromErrno(ec.value());
            }
        }
        fs::rename(src, dst, ec);
        if (ec)
        {
            return FromErrno(ec.value());
        }
        mutated = true;
    }

    fs::directory_iterator cleanup_it(table_dir, ec);
    if (ec)
    {
        return FromErrno(ec.value());
    }
    for (const auto &entry : cleanup_it)
    {
        if (!entry.is_regular_file(ec))
        {
            continue;
        }
        if (ec)
        {
            return FromErrno(ec.value());
        }
        const std::string filename = entry.path().filename().string();
        auto [type, suffix] = ParseFileName(filename);
        if (type != FileNameManifest)
        {
            continue;
        }
        uint64_t term = 0;
        std::string_view branch_name;
        std::optional<std::string> tag;
        if (!ParseManifestFileSuffix(suffix, branch_name, term, tag))
        {
            continue;
        }
        if (tag.has_value() || term == job.target_term)
        {
            continue;
        }
        fs::remove(entry.path(), ec);
        if (ec)
        {
            return FromErrno(ec.value());
        }
        mutated = true;
    }

    if (mutated)
    {
        int dir_fd = open(table_dir.c_str(), O_RDONLY | O_DIRECTORY);
        if (dir_fd < 0)
        {
            return ToKvError(-errno);
        }
        const int sync_res = fdatasync(dir_fd);
        const int saved_errno = errno;
        close(dir_fd);
        if (sync_res < 0)
        {
            return ToKvError(-saved_errno);
        }
    }
    return KvError::NoError;
}

void StandbyService::HandleExitedChildren()
{
    while (true)
    {
        int status = 0;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid == 0)
        {
            return;
        }
        if (pid < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno != ECHILD)
            {
                LOG(ERROR) << "StandbyService: waitpid failed: "
                           << strerror(errno);
            }
            return;
        }

        auto it = inflight_by_pid_.find(pid);
        if (it == inflight_by_pid_.end())
        {
            continue;
        }
        DrainCaptureOutput(&*it->second);
        HandleExitedChild(it->second, status);
    }
}

void StandbyService::HandleExitedChild(std::list<InflightJob>::iterator it,
                                       int status)
{
    InflightJob &job = *it;
    const Step step = job.step;
    const std::string log_target = job.log_target;
    ReleaseProcessHandles(&job);

    if (step == Step::RsyncData)
    {
        KvError result = InterpretRsyncStatus(status, log_target);
        if (result != KvError::NoError)
        {
            FinishInflightJob(it, result);
            return;
        }

        const auto *rsync = std::get_if<RsyncJob>(&job.job.payload);
        CHECK(rsync != nullptr);
        fs::path table_dir = TablePath(rsync->tbl_id);
        fs::path manifest_tmp = table_dir / kManifestTmp;
        std::string manifest_tmp_path = manifest_tmp.string();
        std::string remote_manifest_spec = RemoteSpec(
            RemoteArchiveManifestPath(rsync->tbl_id, rsync->archive_tag),
            false);
        std::vector<const char *> argv = {"rsync",
                                          "-a",
                                          "--inplace",
                                          remote_manifest_spec.c_str(),
                                          manifest_tmp_path.c_str(),
                                          nullptr};
        result = SpawnChild(argv, false, &job.pid, &job.pidfd, &job.stdout_fd);
        if (result != KvError::NoError)
        {
            FinishInflightJob(it, result);
            return;
        }
        if (job.pidfd >= 0)
        {
            epoll_event event{};
            event.events = EPOLLIN;
            event.data.fd = job.pidfd;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, job.pidfd, &event) != 0)
            {
                result = ToKvError(-errno);
                ReleaseProcessHandles(&job);
                FinishInflightJob(it, result);
                return;
            }
        }
        job.step = Step::RsyncManifest;
        job.log_target = std::move(remote_manifest_spec);
        inflight_by_pid_[job.pid] = it;
        return;
    }

    if (step == Step::RsyncManifest)
    {
        KvError result = InterpretRsyncStatus(status, log_target);
        if (result != KvError::NoError)
        {
            FinishInflightJob(it, result);
            return;
        }

        const auto *rsync = std::get_if<RsyncJob>(&job.job.payload);
        CHECK(rsync != nullptr);
        fs::path manifest_tmp = TablePath(rsync->tbl_id) / kManifestTmp;
        if (!fs::exists(manifest_tmp))
        {
            LOG(ERROR) << "StandbyService: manifest.tmp missing after rsync: "
                       << manifest_tmp;
            FinishInflightJob(it, KvError::NotFound);
            return;
        }
        FinishInflightJob(it, KvError::NoError);
        return;
    }

    if (step == Step::ListPartitions)
    {
        KvError result = InterpretCommandStatus(status, log_target);
        if (result != KvError::NoError)
        {
            LOG(ERROR) << "StandbyService: failed to list partitions under "
                       << job.list_store_paths[job.list_index] << ": "
                       << ErrorString(result);
            FinishInflightJob(it, result);
            return;
        }

        AddPartitionsFromOutput(job.capture_output, &job.list_partitions);
        job.capture_output.clear();
        ++job.list_index;
        result = StartNextListPartitionsCommand(it);
        if (result == KvError::NoError && it->pid < 0)
        {
            FinishInflightJob(it, KvError::NoError);
            return;
        }
        if (result != KvError::NoError)
        {
            FinishInflightJob(it, result);
        }
    }
}

void StandbyService::FinishInflightJob(std::list<InflightJob>::iterator it,
                                       KvError result)
{
    ReleaseProcessHandles(&*it);
    if (!it->partition_key.empty())
    {
        active_partitions_.erase(it->partition_key);
    }
    CompleteJob(it->job, result);
    inflight_jobs_.erase(it);
    WakeSupervisor();
}

void StandbyService::DrainCaptureOutput(InflightJob *job)
{
    if (job == nullptr || job->stdout_fd < 0)
    {
        return;
    }
    char buffer[4096];
    while (true)
    {
        ssize_t nread = read(job->stdout_fd, buffer, sizeof(buffer));
        if (nread > 0)
        {
            job->capture_output.append(buffer, static_cast<size_t>(nread));
            continue;
        }
        if (nread == 0)
        {
            break;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            LOG(ERROR) << "StandbyService: read capture failed: "
                       << strerror(errno);
        }
        break;
    }
}

void StandbyService::ReleaseProcessHandles(InflightJob *job)
{
    if (job == nullptr)
    {
        return;
    }
    if (job->pid != -1)
    {
        inflight_by_pid_.erase(job->pid);
    }
    if (job->stdout_fd >= 0)
    {
        pipe_to_pid_.erase(job->stdout_fd);
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, job->stdout_fd, nullptr);
        CloseFd(&job->stdout_fd);
    }
    if (job->pidfd >= 0)
    {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, job->pidfd, nullptr);
        CloseFd(&job->pidfd);
    }
    job->pid = -1;
    job->step = Step::None;
    job->log_target.clear();
}

void StandbyService::WakeSupervisor() const
{
    if (wake_fd_ < 0)
    {
        return;
    }
    uint64_t one = 1;
    if (write(wake_fd_, &one, sizeof(one)) < 0 && errno != EAGAIN)
    {
        PLOG(ERROR) << "StandbyService: failed to wake supervisor";
    }
}

KvError StandbyService::SpawnChild(const std::vector<const char *> &args,
                                   bool capture_stdout,
                                   pid_t *pid,
                                   int *pidfd,
                                   int *stdout_fd)
{
    if (args.empty() || args.back() != nullptr || pid == nullptr ||
        pidfd == nullptr || stdout_fd == nullptr)
    {
        return KvError::InvalidArgs;
    }

    *pid = -1;
    *pidfd = -1;
    *stdout_fd = -1;

    int pipe_fds[2] = {-1, -1};
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_t *actions_ptr = nullptr;
    if (capture_stdout)
    {
        if (pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK) != 0)
        {
            LOG(ERROR) << "StandbyService: pipe2 failed: " << strerror(errno);
            return ToKvError(-errno);
        }
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addclose(&actions, pipe_fds[0]);
        posix_spawn_file_actions_adddup2(&actions, pipe_fds[1], STDOUT_FILENO);
        posix_spawn_file_actions_addclose(&actions, pipe_fds[1]);
        actions_ptr = &actions;
    }

    std::vector<char *> argv;
    argv.reserve(args.size());
    for (const char *arg : args)
    {
        argv.push_back(const_cast<char *>(arg));
    }

    int rc = posix_spawnp(
        pid, argv.front(), actions_ptr, nullptr, argv.data(), environ);
    if (actions_ptr != nullptr)
    {
        posix_spawn_file_actions_destroy(&actions);
        close(pipe_fds[1]);
    }
    if (rc != 0)
    {
        if (pipe_fds[0] >= 0)
        {
            close(pipe_fds[0]);
        }
        LOG(ERROR) << "StandbyService: posix_spawnp " << args.front()
                   << " failed: " << strerror(rc);
        return ToKvError(-rc);
    }

    if (capture_stdout)
    {
        *stdout_fd = pipe_fds[0];
    }

    int opened_pidfd = PidfdOpen(*pid);
    if (opened_pidfd >= 0)
    {
        *pidfd = opened_pidfd;
        return KvError::NoError;
    }
    if (errno == ENOSYS || errno == EINVAL || errno == EPERM)
    {
        return KvError::NoError;
    }

    const int saved_errno = errno;
    if (*stdout_fd >= 0)
    {
        close(*stdout_fd);
        *stdout_fd = -1;
    }
    LOG(ERROR) << "StandbyService: pidfd_open failed: "
               << strerror(saved_errno);
    return ToKvError(-saved_errno);
}

KvError StandbyService::InterpretRsyncStatus(int status,
                                             std::string_view log_target)
{
    if (WIFEXITED(status))
    {
        const int code = WEXITSTATUS(status);
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

KvError StandbyService::InterpretCommandStatus(int status,
                                               std::string_view command)
{
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
    {
        return KvError::NoError;
    }
    if (WIFSIGNALED(status))
    {
        LOG(ERROR) << "StandbyService: " << command << " killed by signal "
                   << WTERMSIG(status);
    }
    else
    {
        LOG(WARNING) << "StandbyService: " << command << " returned status "
                     << status;
    }
    return KvError::IoFail;
}

}  // namespace eloqstore
