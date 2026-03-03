#pragma once

#include <condition_variable>
#include <thread>

#include "eloq_store.h"

namespace eloqstore
{

class ArchiveCrond
{
public:
    ArchiveCrond(EloqStore *store);
    void Start();
    void Stop();
    bool StopRequested();

private:
    void Crond();
    void StartArchiving();

    EloqStore *store_{nullptr};
    uint64_t last_archive_ts_;
    std::thread thd_;

#ifdef ELOQ_MODULE_ENABLED
    bthread::Mutex mu_;
    bthread::ConditionVariable cond_var_;
#else
    std::mutex mu_;
    std::condition_variable cond_var_;
#endif
    bool stop_requested_{true};
    std::atomic<bool> stopped_{false};
};
}  // namespace eloqstore