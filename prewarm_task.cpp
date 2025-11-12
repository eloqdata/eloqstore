#include "prewarm_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "shard.h"
#include "utils.h"

namespace eloqstore
{
KvError PrewarmTask::Prewarm(const PrewarmRequest &request)
{
    CHECK(shard && eloq_store);
    if (eloq_store->IsPrewarmCancelled())
    {
        return KvError::NoError;
    }

    auto *cloud_mgr = static_cast<CloudStoreMgr *>(shard->IoManager());

    const TableIdent &tbl_id = request.TableId();
    auto [fd_ref, err] = cloud_mgr->OpenFD(tbl_id, request.TargetFile());
    if (err == KvError::NoError)
    {
        fd_ref = nullptr;
        return KvError::NoError;
    }

    if (err == KvError::NotFound)
    {
        LOG(WARNING) << "Prewarm skip missing file "
                     << (request.IsManifest() ? "manifest" : "data file")
                     << " for table " << tbl_id;
        return KvError::NoError;
    }

    if (err == KvError::OutOfSpace || err == KvError::OpenFileLimit)
    {
        LOG(WARNING) << "Prewarm stop for " << tbl_id << ": cannot cache "
                     << (request.IsManifest() ? "manifest" : "data file")
                     << " due to " << ErrorString(err);
        return err;
    }

    if (err == KvError::TryAgain)
    {
        LOG(WARNING) << "Prewarm retryable failure for " << tbl_id << ": "
                     << ErrorString(err);
        return err;
    }

    LOG(WARNING) << "Prewarm failed for " << tbl_id << ": " << ErrorString(err);
    return err;
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

    cancelled_.store(false, std::memory_order_release);
    thread_ = std::thread([this]() { ThreadMain(); });
}

void PrewarmService::Stop()
{
    Cancel();
    if (thread_.joinable())
    {
        thread_.join();
    }
}

void PrewarmService::Cancel()
{
    cancelled_.store(true, std::memory_order_release);
}

bool PrewarmService::IsCancelled() const
{
    return cancelled_.load(std::memory_order_relaxed);
}

void PrewarmService::ThreadMain()
{
    PrewarmCloudCache();
    cancelled_.store(true, std::memory_order_relaxed);
}

bool PrewarmService::ListCloudObjects(
    const std::string &remote_path,
    std::vector<utils::CloudObjectInfo> &details)
{
    if (IsCancelled())
    {
        return false;
    }
    if (store_->shards_.empty())
    {
        return false;
    }

    details.clear();

    ListObjectRequest request(nullptr);
    request.SetRemotePath(remote_path);
    request.SetDetailStorage(&details);
    request.SetRecursive(true);
    request.err_ = KvError::NoError;
    request.done_.store(false, std::memory_order_relaxed);
    request.callback_ = nullptr;

    if (!store_->shards_[0]->AddKvRequest(&request))
    {
        return false;
    }
    request.Wait();
    return request.Error() == KvError::NoError;
}

void PrewarmService::PrewarmCloudCache()
{
    if (IsCancelled())
    {
        return;
    }

    const uint16_t num_threads =
        std::max<uint16_t>(uint16_t{1}, store_->options_.num_threads);
    const size_t shard_limit =
        store_->options_.local_space_limit / static_cast<size_t>(num_threads);
    if (shard_limit == 0)
    {
        LOG(INFO) << "Skip cloud prewarm: no local cache space per shard";
        return;
    }

    size_t reserve_space = 0;
    if (store_->options_.reserve_space_ratio != 0)
    {
        reserve_space = static_cast<size_t>(
            static_cast<double>(shard_limit) /
            static_cast<double>(store_->options_.reserve_space_ratio));
        reserve_space = std::min(reserve_space, shard_limit);
    }
    size_t budget = shard_limit - reserve_space;
    if (budget == 0)
    {
        LOG(INFO) << "Skip cloud prewarm: reserved space consumes shard cache";
        return;
    }

    std::vector<utils::CloudObjectInfo> all_infos;
    if (!ListCloudObjects("", all_infos))
    {
        if (!IsCancelled())
        {
            LOG(WARNING) << "Skip cloud prewarm: failed to list cloud root";
        }
        return;
    }

    struct Entry
    {
        TableIdent tbl_id;
        FileId file_id;
        bool is_manifest;
        size_t cost;
        std::string mod_time;
        size_t shard_index;
    };

    std::vector<Entry> entries;
    entries.reserve(all_infos.size());
    for (const auto &info : all_infos)
    {
        if (IsCancelled())
        {
            return;
        }
        if (info.is_dir)
        {
            continue;
        }
        const std::string &path = !info.path.empty() ? info.path : info.name;
        if (path.empty())
        {
            continue;
        }
        TableIdent tbl_id;
        std::string filename;
        if (!ExtractPartition(path, tbl_id, filename))
        {
            continue;
        }
        if (store_->options_.prewarm_filter &&
            !store_->options_.prewarm_filter(tbl_id))
        {
            continue;
        }
        if (filename.ends_with(TmpSuffix))
        {
            continue;
        }
        if (filename == FileNameManifest)
        {
            entries.push_back({tbl_id,
                               CloudStoreMgr::ManifestFileId(),
                               true,
                               store_->options_.manifest_limit,
                               info.mod_time,
                               tbl_id.ShardIndex(store_->shards_.size())});
            continue;
        }
        if (filename.rfind(FileNameData, 0) == 0)
        {
            size_t underscore = filename.find_first_of(FileNameSeparator);
            if (underscore == std::string::npos ||
                underscore + 1 >= filename.size())
            {
                continue;
            }
            std::string id_str = filename.substr(underscore + 1);
            try
            {
                FileId file_id = std::stoull(id_str);
                size_t data_cost = info.size == 0
                                       ? store_->options_.DataFileSize()
                                       : static_cast<size_t>(info.size);
                if (data_cost == 0)
                {
                    data_cost = store_->options_.DataFileSize();
                }
                entries.push_back({tbl_id,
                                   file_id,
                                   false,
                                   data_cost,
                                   info.mod_time,
                                   tbl_id.ShardIndex(store_->shards_.size())});
            }
            catch (const std::exception &)
            {
                continue;
            }
        }
    }

    if (entries.empty() || IsCancelled())
    {
        return;
    }

    std::sort(entries.begin(),
              entries.end(),
              [](const Entry &lhs, const Entry &rhs)
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

    std::vector<size_t> shard_remaining(store_->shards_.size(), budget);

    const size_t kMaxPrewarmInflight =
        std::min<size_t>(20, store_->options_.num_threads * 10);
    struct InflightState
    {
        std::mutex mu;
        std::condition_variable cv;
        size_t count = 0;
    };
    auto inflight_state = std::make_shared<InflightState>();
    std::vector<std::shared_ptr<PrewarmRequest>> pending_requests;
    pending_requests.reserve(entries.size());

    auto acquire_slot = [&, inflight_state]() -> bool
    {
        std::unique_lock<std::mutex> lk(inflight_state->mu);
        inflight_state->cv.wait(lk,
                                [&]() {
                                    return IsCancelled() ||
                                           inflight_state->count <
                                               kMaxPrewarmInflight;
                                });

        if (IsCancelled())
        {
            return false;
        }

        ++inflight_state->count;
        return true;
    };

    auto release_slot = [inflight_state]()
    {
        {
            std::lock_guard<std::mutex> lk(inflight_state->mu);
            if (inflight_state->count > 0)
            {
                --inflight_state->count;
            }
        }
        inflight_state->cv.notify_one();
    };

    auto submit_entry = [&](const Entry &entry) -> bool
    {
        if (!acquire_slot())
        {
            return false;
        }

        auto req = std::make_shared<PrewarmRequest>();
        req->SetArgs(entry.tbl_id, entry.is_manifest, entry.file_id);
        PrewarmRequest *raw_req = req.get();

        bool ok = store_->ExecAsyn(
            raw_req,
            0,
            [this, entry, release_slot](KvRequest *finished_req)
            {
                KvError err = finished_req->Error();
                if (err != KvError::NoError && err != KvError::NotFound)
                {
                    const std::string file_name =
                        entry.is_manifest
                            ? std::string(FileNameManifest)
                            : std::string(FileNameData) +
                                  std::string(1, FileNameSeparator) +
                                  std::to_string(entry.file_id);
                    LOG(WARNING)
                        << "Prewarm request failed for " << entry.tbl_id
                        << " file " << file_name << ": " << ErrorString(err);
                }
                release_slot();
            });
        if (!ok)
        {
            release_slot();
            return false;
        }

        pending_requests.push_back(std::move(req));
        return true;
    };

    for (const auto &entry : entries)
    {
        if (IsCancelled())
        {
            break;
        }
        size_t &remaining = shard_remaining[entry.shard_index];
        if (entry.cost > remaining)
        {
            continue;
        }
        if (!submit_entry(entry))
        {
            break;
        }
        remaining -= entry.cost;
    }

    {
        std::unique_lock<std::mutex> lk(inflight_state->mu);
        inflight_state->cv.wait(
            lk, [&]() { return inflight_state->count == 0 || IsCancelled(); });
    }
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
};

}  // namespace eloqstore
