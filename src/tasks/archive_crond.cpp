#include "tasks/archive_crond.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#endif
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
    stop_requested_ = false;
    thd_ = std::thread(&ArchiveCrond::Crond, this);
    LOG(INFO) << "Archive crond started";
}

void ArchiveCrond::Stop()
{
    mu_.lock();
    stop_requested_ = true;
    mu_.unlock();
#ifdef ELOQ_MODULE_ENABLED
    while (!stopped_.load(std::memory_order_acquire))
    {
        cond_var_.notify_one();
        bthread_usleep(1000);
    }
    if (thd_.joinable())
    {
        thd_.join();
    }
#else
    if (thd_.joinable())
    {
        cond_var_.notify_one();
        thd_.join();
    }
    LOG(INFO) << "Archive crond stopped";
#endif
}

bool ArchiveCrond::StopRequested()
{
    std::scoped_lock lk(mu_);
    return stop_requested_;
}

void ArchiveCrond::Crond()
{
    const uint64_t interval_secs = store_->Options().archive_interval_secs;
    last_archive_ts_ = utils::UnixTs<chrono::seconds>();
    while (!StopRequested())
    {
        // Loop required to prevent spurious wakeups
        auto elapsed = utils::UnixTs<chrono::seconds>() - last_archive_ts_;
        while (elapsed < interval_secs)
        {
            std::unique_lock lk(mu_);
#ifdef ELOQ_MODULE_ENABLED
            for (uint64_t sleeped_secs = 0;
                 sleeped_secs < interval_secs - elapsed && !stop_requested_;
                 ++sleeped_secs)
            {
                bthread_usleep(1);
            }
#else
            auto wait_period = chrono::seconds(interval_secs - elapsed);
            cond_var_.wait_for(
                lk, wait_period, [this] { return stop_requested_; });
#endif
            if (stop_requested_)
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
