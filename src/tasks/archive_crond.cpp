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
    LOG(INFO) << "Start scheduled archiving";

    GlobalArchiveRequest global_req;
    store_->ExecSync(&global_req);
    if (global_req.Error() != KvError::NoError)
    {
        LOG(ERROR) << "Scheduled archiving failed: "
                   << static_cast<int>(global_req.Error());
    }
    else
    {
        LOG(INFO) << "Scheduled archiving completed successfully";
    }
}
}  // namespace eloqstore
