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
    bool IsStopped();

private:
    void Crond();
    void StartArchiving();

    EloqStore *store_{nullptr};
    uint64_t last_archive_ts_;
    std::thread thd_;

    std::mutex mu_;
    std::condition_variable cond_var_;
    bool stopped_{true};
};
}  // namespace eloqstore