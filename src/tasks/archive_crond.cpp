#include "tasks/archive_crond.h"

#include <glog/logging.h>

#include <cassert>
#include <filesystem>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "eloq_store.h"
#include "utils.h"

namespace eloqstore
{

ArchiveCrond::ArchiveCrond(EloqStore *store) : store_(store)
{
}

void ArchiveCrond::Start()
{
    assert(!thd_.joinable());
    stopped_ = false;
    thd_ = std::thread(&ArchiveCrond::Crond, this);
    LOG(INFO) << "Archive crond started";
}

void ArchiveCrond::Stop()
{
    mu_.lock();
    stopped_ = true;
    mu_.unlock();
    if (thd_.joinable())
    {
        cond_var_.notify_one();
        thd_.join();
        LOG(INFO) << "Archive crond stopped";
    }
}

bool ArchiveCrond::IsStopped()
{
    std::scoped_lock lk(mu_);
    return stopped_;
}

void ArchiveCrond::Crond()
{
    const uint64_t interval_secs = store_->Options().archive_interval_secs;
    last_archive_ts_ = utils::UnixTs<chrono::seconds>();
    while (!IsStopped())
    {
        // Loop required to prevent spurious wakeups
        auto elapsed = utils::UnixTs<chrono::seconds>() - last_archive_ts_;
        while (elapsed < interval_secs)
        {
            auto wait_period = chrono::seconds(interval_secs - elapsed);
            std::unique_lock lk(mu_);
            cond_var_.wait_for(lk, wait_period, [this] { return stopped_; });
            if (stopped_)
            {
                // Stopped during wait.
                return;
            }
            elapsed = utils::UnixTs<chrono::seconds>() - last_archive_ts_;
        }

        StartArchiving();
        last_archive_ts_ = utils::UnixTs<chrono::seconds>();
    }
}

void ArchiveCrond::StartArchiving()
{
    LOG(INFO) << "Start archiving all partitions";
    const auto &opts = store_->Options();
    const uint32_t archive_batch = opts.max_archive_tasks;
    std::vector<ArchiveRequest> requests(archive_batch);
    size_t fail_cnt = 0;
    auto do_archiving = [&](std::span<TableIdent> tbl_ids)
    {
        const size_t batch_size = tbl_ids.size();
        for (size_t i = 0; i < batch_size; i++)
        {
            requests[i].SetTableId(std::move(tbl_ids[i]));
            bool ok = store_->ExecAsyn(&requests[i]);
            LOG_IF(FATAL, !ok) << "Failed to send archive request";
        }
        for (size_t i = 0; i < batch_size; i++)
        {
            const ArchiveRequest &req = requests[i];
            req.Wait();
            if (req.Error() != KvError::NoError)
            {
                fail_cnt++;
            }
        }
    };
    auto dispatch_archives = [&](std::vector<TableIdent> &ids)
    {
        for (size_t i = 0; i < ids.size(); i += archive_batch)
        {
            auto it_begin = ids.begin() + i;
            const size_t size = std::min(size_t(archive_batch), ids.size() - i);
            do_archiving({it_begin, size});
        }
    };

    std::vector<TableIdent> table_ids;
    table_ids.reserve(archive_batch);
    size_t total_partitions = 0;
    if (opts.cloud_store_path.empty())
    {
        for (const auto &db_path_entry : opts.store_path)
        {
            const fs::path db_path(db_path_entry);
            table_ids.clear();
            for (auto &ent : fs::directory_iterator{db_path})
            {
                if (!ent.is_directory())
                {
                    continue;
                }

                TableIdent tbl_id =
                    TableIdent::FromString(ent.path().filename());
                if (tbl_id.tbl_name_.empty())
                {
                    LOG(WARNING) << "unexpected partition " << ent.path();
                    continue;
                }
                table_ids.emplace_back(std::move(tbl_id));
            }
            total_partitions += table_ids.size();
            dispatch_archives(table_ids);
        }
    }
    else
    {
        std::vector<std::string> objects;
        ListObjectRequest list_request(&objects);
        list_request.SetRemotePath(std::string{});
        list_request.SetRecursive(false);
        store_->ExecSync(&list_request);
        if (list_request.Error() != KvError::NoError)
        {
            LOG(WARNING) << "Skip archiving: list cloud root failed, error "
                         << static_cast<int>(list_request.Error());
            return;
        }

        table_ids.clear();
        table_ids.reserve(objects.size());
        for (auto &name : objects)
        {
            TableIdent tbl_id = TableIdent::FromString(name);
            if (!tbl_id.IsValid())
            {
                continue;
            }
            table_ids.emplace_back(std::move(tbl_id));
        }
        total_partitions = table_ids.size();
        dispatch_archives(table_ids);
    }

    LOG(INFO) << "Finished archiving " << total_partitions << " partitions, "
              << fail_cnt << " failed";
}
}  // namespace eloqstore
