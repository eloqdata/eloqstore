#include "tasks/prewarm_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#ifdef ELOQ_MODULE_ENABLED
#include "bthread/eloq_module.h"
#endif
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "storage/shard.h"
#include "tasks/task.h"
#include "utils.h"

namespace eloqstore
{
Prewarmer::Prewarmer(CloudStoreMgr *io_mgr) : io_mgr_(io_mgr)
{
    assert(io_mgr_ != nullptr);
}

bool Prewarmer::PopNext(PrewarmFile &file)
{
    return io_mgr_->PopPrewarmFile(file);
}

void Prewarmer::Run()
{
    bool registered_active = false;
    auto register_active = [&]()
    {
        if (!registered_active)
        {
            shard->TaskMgr()->AddExternalTask();
            io_mgr_->RegisterPrewarmActive();
            registered_active = true;
        }
    };
    auto unregister_active = [&]()
    {
        if (registered_active)
        {
            io_mgr_->UnregisterPrewarmActive();
            shard->TaskMgr()->FinishExternalTask();
            registered_active = false;
        }
    };

    while (true)
    {
        if (shutting_down_)
        {
            // Update stats
            io_mgr_->GetPrewarmStats().completion_reason =
                PrewarmStats::CompletionReason::Shutdown;

            io_mgr_->ClearPrewarmFiles();
            io_mgr_->StopAllPrewarmTasks();
            stop_ = true;
            unregister_active();
            break;
        }

        if (stop_)
        {
            // Check if listing is complete before truly stopping
            if (io_mgr_->IsPrewarmListingComplete() &&
                io_mgr_->GetPrewarmPendingCount() == 0)
            {
                // Listing complete and queue empty, log completion
                // Set end_time before calculating duration
                io_mgr_->GetPrewarmStats().end_time =
                    std::chrono::steady_clock::now();

                const auto &stats = io_mgr_->GetPrewarmStats();
                size_t files_pulled = io_mgr_->GetPrewarmFilesPulled();
                unregister_active();
                status_ = TaskStatus::Idle;
                if (io_mgr_->ActivePrewarmTasks() == 0)
                {
                    LOG(INFO) << "Shard " << shard->shard_id_ << " prewarm "
                              << stats.CompletionReasonString() << ". Pulled "
                              << files_pulled << " files in "
                              << stats.DurationSeconds() << "s";
                }
                Yield();
                continue;
            }
            else if (!io_mgr_->IsPrewarmListingComplete())
            {
                // Producer still listing, wait for more files
                unregister_active();
                status_ = TaskStatus::Idle;
                Yield();
                continue;
            }
        }
        register_active();

        PrewarmFile file;
        bool popped = PopNext(file);

        if (!popped)
        {
            // Queue empty, but might be more files coming
            if (!io_mgr_->IsPrewarmListingComplete())
            {
                // Producer still working, yield and wait
                unregister_active();
                status_ = TaskStatus::Idle;
                Yield();
                continue;
            }
            else
            {
                // Listing complete and queue empty, we're done
                io_mgr_->ClearPrewarmFiles();
                io_mgr_->StopAllPrewarmTasks();
                continue;
            }
        }

        // Check disk space BEFORE attempting download
        if (io_mgr_->LocalCacheRemained() < file.file_size)
        {
            // Set end_time before calculating duration
            io_mgr_->GetPrewarmStats().end_time =
                std::chrono::steady_clock::now();

            const auto &stats = io_mgr_->GetPrewarmStats();
            size_t files_pulled = io_mgr_->GetPrewarmFilesPulled();
            LOG(INFO) << "Shard " << shard->shard_id_
                      << " out of local disk space during prewarm. "
                      << "Pulled " << files_pulled << " files in "
                      << stats.DurationSeconds()
                      << "s before running out of space.";

            // Update completion reason
            io_mgr_->GetPrewarmStats().completion_reason =
                PrewarmStats::CompletionReason::DiskFull;

            // Signal producer to stop listing (abort)
            io_mgr_->MarkPrewarmListingComplete();

            // Clear our queue
            io_mgr_->ClearPrewarmFiles();
            io_mgr_->StopAllPrewarmTasks();

            // Exit prewarm loop
            unregister_active();
            status_ = TaskStatus::Idle;
            Yield();
            continue;
        }

        DLOG(INFO) << "Prewarm downloading: " << file.tbl_id.ToString() << "/"
                   << (file.is_manifest
                           ? "manifest"
                           : "data_" + std::to_string(file.file_id));
        auto [fd_ref, err] = io_mgr_->OpenFD(file.tbl_id, file.file_id, true);
        if (err == KvError::NoError)
        {
            fd_ref = nullptr;
        }
        else if (err == KvError::NotFound)
        {
            // Track skipped files
            io_mgr_->GetPrewarmStats().files_skipped_missing++;

            LOG(WARNING) << "Prewarm skip missing "
                         << (file.is_manifest ? "manifest" : "data file")
                         << " for " << file.tbl_id;
        }
        else
        {
            // Track failed files
            io_mgr_->GetPrewarmStats().files_failed++;

            LOG(WARNING) << "Prewarm failed for " << file.tbl_id << " file "
                         << file.file_id << ": " << ErrorString(err);

            // On persistent errors, abort prewarm
            // Set end_time before calculating duration
            io_mgr_->GetPrewarmStats().end_time =
                std::chrono::steady_clock::now();

            const auto &stats = io_mgr_->GetPrewarmStats();
            size_t files_pulled = io_mgr_->GetPrewarmFilesPulled();
            LOG(INFO) << "Shard " << shard->shard_id_
                      << " aborting prewarm due to persistent errors. "
                      << "Pulled " << files_pulled << " files, "
                      << stats.files_failed << " failed in "
                      << stats.DurationSeconds() << "s";

            io_mgr_->GetPrewarmStats().completion_reason =
                PrewarmStats::CompletionReason::DownloadError;
            io_mgr_->MarkPrewarmListingComplete();
            io_mgr_->ClearPrewarmFiles();
            io_mgr_->StopAllPrewarmTasks();
            continue;
        }

        if (shard->HasPendingRequests() ||
            shard->TaskMgr()->NumActive() > io_mgr_->ActivePrewarmTasks())
        {
            unregister_active();
            status_ = TaskStatus::BlockedIO;
            Yield();
        }
    }
}

void Prewarmer::Shutdown()
{
    if (!Options()->prewarm_cloud_cache)
        return;
    shutting_down_ = true;
    stop_ = true;
    coro_ = coro_.resume();
}

PrewarmService::PrewarmService(EloqStore *store) : store_(store)
{
    assert(store_ != nullptr);
}

PrewarmService::~PrewarmService()
{
    Stop();
}

void PrewarmService::Start()
{
    if (thread_.joinable())
    {
        return;
    }
    stop_requested_.store(false, std::memory_order_relaxed);
    thread_ = std::thread([this]() { PrewarmCloudCache(); });
}

void PrewarmService::Stop()
{
    stop_requested_.store(true, std::memory_order_release);
    AbortPrewarmWorkers();
    if (thread_.joinable())
    {
        thread_.join();
    }
}

bool PrewarmService::ListCloudObjects(
    const std::string &remote_path,
    std::vector<utils::CloudObjectInfo> &details,
    const std::string &continuation_token,
    std::string *next_continuation_token)
{
    if (store_->shards_.empty())
    {
        return false;
    }

    details.clear();

    ListObjectRequest request(nullptr);
    request.SetRemotePath(remote_path);
    request.SetDetails(&details);
    request.SetRecursive(true);
    request.SetContinuationToken(continuation_token);
    request.err_ = KvError::NoError;
    request.done_.store(false, std::memory_order_relaxed);
    request.callback_ = nullptr;

    // send request to a random shard
    size_t random_shard = std::rand() % store_->shards_.size();
    if (!store_->shards_[random_shard]->AddKvRequest(&request))
    {
        return false;
    }
    request.Wait();

    if (next_continuation_token)
    {
        *next_continuation_token = *request.GetNextContinuationToken();
    }

    return request.Error() == KvError::NoError;
}

void PrewarmService::PrewarmCloudCache()
{
    const uint16_t num_threads =
        std::max<uint16_t>(uint16_t{1}, store_->options_.num_threads);
    const size_t shard_limit =
        store_->options_.local_space_limit / static_cast<size_t>(num_threads);
    if (shard_limit == 0)
    {
        LOG(INFO) << "Skip cloud prewarm: no local cache space per shard";
        return;
    }

    // Initialize statistics and start timer
    auto prewarm_start = std::chrono::steady_clock::now();
    LOG(INFO) << "Starting cloud cache prewarm across "
              << store_->shards_.size() << " shards";

    // Initialize prewarmers for each shard
    for (size_t i = 0; i < store_->shards_.size(); ++i)
    {
        auto *cloud_mgr =
            static_cast<CloudStoreMgr *>(store_->shards_[i]->IoManager());
        if (!cloud_mgr->prewarmers_.empty())
        {
            // Reset state for new prewarm session
            cloud_mgr->prewarm_files_pulled_.store(0,
                                                   std::memory_order_relaxed);
            cloud_mgr->prewarm_listing_complete_.store(
                false, std::memory_order_relaxed);
            cloud_mgr->prewarm_queue_size_.store(0, std::memory_order_relaxed);

            // Initialize stats
            cloud_mgr->GetPrewarmStats() = PrewarmStats{};
            cloud_mgr->GetPrewarmStats().start_time = prewarm_start;
        }
    }

    std::string continuation_token;
    size_t total_files_listed = 0;
    size_t total_files_skipped = 0;
    size_t api_call_count = 0;
    PrewarmStats::CompletionReason completion_reason =
        PrewarmStats::CompletionReason::Success;
    auto should_abort = [&]()
    {
        if (stop_requested_.load(std::memory_order_acquire))
        {
            completion_reason = PrewarmStats::CompletionReason::Shutdown;
            return true;
        }
        return false;
    };

    if (should_abort())
    {
        goto listing_done;
    }

    do
    {
        if (should_abort())
        {
            goto listing_done;
        }
        // Log progress periodically
        if (api_call_count > 0 && api_call_count % 10 == 0)
        {
            LOG(INFO) << "Prewarm listing progress: " << total_files_listed
                      << " files across " << api_call_count << " API calls";
        }

        // List next batch of cloud objects
        std::vector<utils::CloudObjectInfo> batch_infos;
        std::string next_token;

        if (!ListCloudObjects("", batch_infos, continuation_token, &next_token))
        {
            LOG(WARNING) << "Failed to list cloud objects after "
                         << api_call_count << " API calls, prewarm aborted. "
                         << "Listed " << total_files_listed << " files so far.";
            completion_reason = PrewarmStats::CompletionReason::ListingError;

            // Mark listing complete so consumers can exit
            for (auto &shard : store_->shards_)
            {
                auto *cloud_mgr =
                    static_cast<CloudStoreMgr *>(shard->IoManager());
                cloud_mgr->GetPrewarmStats().completion_reason =
                    completion_reason;
                cloud_mgr->MarkPrewarmListingComplete();
            }
            goto listing_done;
        }

        api_call_count++;

        // Convert to PrewarmFile and distribute to shards
        std::vector<std::vector<PrewarmFile>> shard_files(
            store_->shards_.size());

        for (const auto &info : batch_infos)
        {
            if (info.is_dir)
            {
                total_files_skipped++;
                continue;
            }
            const std::string &path =
                !info.path.empty() ? info.path : info.name;
            if (path.empty())
            {
                total_files_skipped++;
                continue;
            }
            TableIdent tbl_id;
            std::string filename;
            if (!ExtractPartition(path, tbl_id, filename))
            {
                total_files_skipped++;
                continue;
            }
            if (store_->options_.prewarm_filter &&
                !store_->options_.prewarm_filter(tbl_id))
            {
                total_files_skipped++;
                continue;
            }
            if (filename.ends_with(TmpSuffix))
            {
                total_files_skipped++;
                continue;
            }

            PrewarmFile file;
            file.tbl_id = tbl_id;
            file.mod_time = info.mod_time;
            if (filename == FileNameManifest)
            {
                file.file_id = CloudStoreMgr::ManifestFileId();
                file.is_manifest = true;
            }
            else if (filename.rfind(FileNameData, 0) == 0)
            {
                size_t underscore = filename.find_first_of(FileNameSeparator);
                if (underscore == std::string::npos ||
                    underscore + 1 >= filename.size())
                {
                    total_files_skipped++;
                    continue;
                }
                std::string id_str = filename.substr(underscore + 1);
                try
                {
                    file.file_id = std::stoull(id_str);
                }
                catch (const std::exception &)
                {
                    total_files_skipped++;
                    continue;
                }
                file.is_manifest = false;
            }
            else
            {
                total_files_skipped++;
                continue;
            }
            file.file_size = static_cast<size_t>(info.size);

            const size_t shard_index =
                file.tbl_id.ShardIndex(shard_files.size());
            shard_files[shard_index].push_back(std::move(file));
        }

        // Sort files by priority (manifest first, then by mod_time)
        for (auto &files : shard_files)
        {
            std::sort(files.begin(),
                      files.end(),
                      [](const PrewarmFile &lhs, const PrewarmFile &rhs)
                      {
                          if (lhs.is_manifest != rhs.is_manifest)
                          {
                              return lhs.is_manifest && !rhs.is_manifest;
                          }
                          if (!lhs.is_manifest && !rhs.is_manifest &&
                              lhs.mod_time != rhs.mod_time)
                          {
                              return lhs.mod_time > rhs.mod_time;
                          }
                          return lhs.file_id > rhs.file_id;
                      });
        }

        // Append to each shard's queue (blocks if queue full)
        for (size_t i = 0; i < shard_files.size(); ++i)
        {
            if (shard_files[i].empty())
            {
                continue;
            }

            auto *cloud_mgr =
                static_cast<CloudStoreMgr *>(store_->shards_[i]->IoManager());
            if (cloud_mgr->prewarmers_.empty())
            {
                continue;
            }

            size_t batch_size = shard_files[i].size();
            total_files_listed += batch_size;
            cloud_mgr->GetPrewarmStats().total_files_listed += batch_size;

            // This will block if queue is full, or return false if aborted
            if (!cloud_mgr->AppendPrewarmFiles(shard_files[i]))
            {
                LOG(INFO) << "Prewarm listing aborted by shard " << i
                          << " consumer after " << api_call_count
                          << " API calls. " << "Listed " << total_files_listed
                          << " files total.";
                if (stop_requested_.load(std::memory_order_acquire))
                {
                    completion_reason =
                        PrewarmStats::CompletionReason::Shutdown;
                }
                else
                {
                    completion_reason =
                        PrewarmStats::CompletionReason::DiskFull;
                }

                // Mark all shards as complete
                for (auto &shard : store_->shards_)
                {
                    auto *mgr =
                        static_cast<CloudStoreMgr *>(shard->IoManager());
                    mgr->GetPrewarmStats().completion_reason =
                        completion_reason;
                    mgr->MarkPrewarmListingComplete();
                }
                goto listing_done;  // Break out of do-while loop
            }

#ifdef ELOQ_MODULE_ENABLED
            eloq::EloqModule::NotifyWorker(i);
#endif
        }

        continuation_token = std::move(next_token);

    } while (!continuation_token.empty());

listing_done:
    auto prewarm_end = std::chrono::steady_clock::now();

    // Collect and log statistics from all shards
    size_t total_pulled = 0;
    for (auto &shard : store_->shards_)
    {
        auto *cloud_mgr = static_cast<CloudStoreMgr *>(shard->IoManager());
        auto &stats = cloud_mgr->GetPrewarmStats();
        stats.end_time = prewarm_end;
        stats.total_files_pulled = cloud_mgr->GetPrewarmFilesPulled();
        stats.files_skipped_filtered =
            total_files_skipped / store_->shards_.size();

        if (completion_reason != PrewarmStats::CompletionReason::ListingError &&
            completion_reason != PrewarmStats::CompletionReason::DiskFull)
        {
            stats.completion_reason = completion_reason;
        }

        total_pulled += stats.total_files_pulled;

        cloud_mgr->MarkPrewarmListingComplete();
#ifdef ELOQ_MODULE_ENABLED
        eloq::EloqModule::NotifyWorker(
            static_cast<Shard *>(shard.get())->shard_id_);
#endif
    }

    // Log comprehensive summary
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                          prewarm_end - prewarm_start)
                          .count() /
                      1000.0;

    // Create a temp PrewarmStats object for CompletionReasonString
    PrewarmStats temp_stats;
    temp_stats.completion_reason = completion_reason;

    LOG(INFO) << "Prewarm listing " << temp_stats.CompletionReasonString()
              << ". Duration: " << duration << "s"
              << ", API calls: " << api_call_count
              << ", Files listed: " << total_files_listed
              << ", Files skipped: " << total_files_skipped
              << ", Files pulled: " << total_pulled
              << ", Shards: " << store_->shards_.size();
}

bool PrewarmService::ExtractPartition(const std::string &path,
                                      TableIdent &tbl_id,
                                      std::string &filename)
{
    size_t start = 0;
    while (start < path.size())
    {
        size_t slash = path.find('/', start);
        size_t len =
            slash == std::string::npos ? path.size() - start : slash - start;
        if (len == 0)
        {
            if (slash == std::string::npos)
            {
                break;
            }
            start = slash + 1;
            continue;
        }
        std::string component = path.substr(start, len);
        if (component.find('.') != std::string::npos)
        {
            TableIdent ident = TableIdent::FromString(component);
            if (!ident.IsValid())
            {
                return false;
            }
            tbl_id = std::move(ident);
            if (slash == std::string::npos || slash + 1 >= path.size())
            {
                return false;
            }
            filename = path.substr(slash + 1);
            return !filename.empty();
        }
        if (slash == std::string::npos)
        {
            break;
        }
        start = slash + 1;
    }
    return false;
}

void PrewarmService::AbortPrewarmWorkers()
{
    if (store_ == nullptr)
    {
        return;
    }
    for (auto &shard_ptr : store_->shards_)
    {
        if (shard_ptr == nullptr)
        {
            continue;
        }
        auto *cloud_mgr = static_cast<CloudStoreMgr *>(shard_ptr->IoManager());
        if (cloud_mgr == nullptr)
        {
            continue;
        }
        cloud_mgr->GetPrewarmStats().completion_reason =
            PrewarmStats::CompletionReason::Shutdown;
        cloud_mgr->MarkPrewarmListingComplete();
        cloud_mgr->ClearPrewarmFiles();
        cloud_mgr->StopAllPrewarmTasks();
    }
}

}  // namespace eloqstore
