#include "prewarm_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "shard.h"
#include "task.h"
#include "utils.h"

namespace eloqstore
{
PrewarmTask::PrewarmTask(CloudStoreMgr *io_mgr) : io_mgr_(io_mgr)
{
    assert(io_mgr_ != nullptr);
}

bool PrewarmTask::HasPending() const
{
    return !stop_ && next_index_ < pending_.size();
}

bool PrewarmTask::PopNext(PrewarmFile &file)
{
    if (next_index_ >= pending_.size())
    {
        return false;
    }
    file = std::move(pending_[next_index_++]);
    return true;
}

void PrewarmTask::Clear()
{
    pending_.clear();
    next_index_ = 0;
}

void PrewarmTask::Run()
{
    bool registered_active = false;
    auto register_active = [&]()
    {
        if (!registered_active)
        {
            LOG(INFO) << "Shard " << shard->shard_id_
                      << " continue prewarm task";
            shard->TaskMgr()->AddExternalTask();
            registered_active = true;
        }
    };
    auto unregister_active = [&]()
    {
        if (registered_active)
        {
            LOG(INFO) << "Shard " << shard->shard_id_ << " stop prewarm task";
            shard->TaskMgr()->FinishExternalTask();
            registered_active = false;
        }
    };

    while (true)
    {
        if (shutting_down_)
        {
            Clear();
            stop_ = true;
            unregister_active();
            break;
        }

        if (stop_)
        {
            unregister_active();
            status_ = TaskStatus::Idle;
            Yield();
            continue;
        }
        register_active();

        PrewarmFile file;
        if (!PopNext(file) || io_mgr_->LocalCacheRemained() < file.file_size)
        {
            DLOG(INFO) << "Shard " << shard->shard_id_
                       << " reached local cache budget during prewarm";
            Clear();
            stop_ = true;
            continue;
        }
        DLOG(INFO) << "prewarm file id:" << file.file_id;
        auto [fd_ref, err] = io_mgr_->OpenFD(file.tbl_id, file.file_id);
        if (err == KvError::NoError)
        {
            fd_ref = nullptr;
        }
        else if (err == KvError::NotFound)
        {
            LOG(WARNING) << "Prewarm skip missing "
                         << (file.is_manifest ? "manifest" : "data file")
                         << " for " << file.tbl_id;
        }
        else
        {
            LOG(WARNING) << "Prewarm failed for " << file.tbl_id << " file "
                         << file.file_id << ": " << ErrorString(err);
            Clear();
            stop_ = true;
            continue;
        }
        if (shard->HasPendingRequests() || shard->TaskMgr()->NumActive() > 1)
        {
            unregister_active();
            status_ = TaskStatus::BlockedIO;
            Yield();
        }
    }
}

void PrewarmTask::Shutdown()
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
    thread_ = std::thread([this]() { PrewarmCloudCache(); });
}

void PrewarmService::Stop()
{
    if (thread_.joinable())
    {
        thread_.join();
    }
}

bool PrewarmService::ListCloudObjects(
    const std::string &remote_path,
    std::vector<utils::CloudObjectInfo> &details)
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
    const uint16_t num_threads =
        std::max<uint16_t>(uint16_t{1}, store_->options_.num_threads);
    const size_t shard_limit =
        store_->options_.local_space_limit / static_cast<size_t>(num_threads);
    if (shard_limit == 0)
    {
        LOG(INFO) << "Skip cloud prewarm: no local cache space per shard";
        return;
    }

    std::vector<utils::CloudObjectInfo> all_infos;
    if (!ListCloudObjects("", all_infos))
    {
        LOG(WARNING) << "Skip cloud prewarm: failed to list cloud root";
        return;
    }

    std::vector<PrewarmFile> all_files;
    all_files.reserve(all_infos.size());
    for (const auto &info : all_infos)
    {
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
                continue;
            }
            std::string id_str = filename.substr(underscore + 1);
            try
            {
                file.file_id = std::stoull(id_str);
            }
            catch (const std::exception &)
            {
                continue;
            }

            file.is_manifest = false;
        }
        else
        {
            continue;
        }
        file.file_size = static_cast<size_t>(info.size);
        all_files.push_back(std::move(file));
    }

    std::sort(all_files.begin(),
              all_files.end(),
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

    std::vector<std::vector<PrewarmFile>> shard_files(store_->shards_.size());
    for (auto &file : all_files)
    {
        const size_t shard_index = file.tbl_id.ShardIndex(shard_files.size());
        shard_files[shard_index].push_back(std::move(file));
    }

    for (size_t i = 0; i < shard_files.size(); ++i)
    {
        if (shard_files[i].empty())
        {
            continue;
        }
        auto *cloud_mgr =
            static_cast<CloudStoreMgr *>(store_->shards_[i]->IoManager());
        auto &files = shard_files[i];
        DLOG(INFO) << "files size :" << files.size();
        cloud_mgr->prewarm_task_.pending_ = std::move(files);
        cloud_mgr->prewarm_task_.stop_ = false;
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
}

}  // namespace eloqstore
