#include "tasks/archive_crond.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#endif
#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include "eloq_store.h"

namespace eloqstore
{

ArchiveCrond::ArchiveCrond(EloqStore *store) : store_(store)
{
}

void ArchiveCrond::Start()
{
    assert(!thd_.joinable());
    {
        Lock lk(mu_);
        stop_requested_ = false;
        // The store stopped since the last run: it drained its worker shards,
        // so no archive of that run is in flight, and its shards are gone.
        schedules_.clear();
    }
    exited_.store(false, std::memory_order_relaxed);
    thd_ = std::thread(&ArchiveCrond::Crond, this);
    LOG(INFO) << "Archive crond started";
}

void ArchiveCrond::Stop()
{
    {
        Lock lk(mu_);
        stop_requested_ = true;
    }
    cond_var_.notify_all();
#ifdef ELOQ_MODULE_ENABLED
    // The caller may be a bthread on a brpc worker while the thread waits
    // for that worker's shard to list partitions: yield, never block.
    while (thd_.joinable() && !exited_.load(std::memory_order_acquire))
    {
        bthread_usleep(1000);
    }
#endif
    if (thd_.joinable())
    {
        thd_.join();
    }
    LOG(INFO) << "Archive crond stopped";
}

void ArchiveCrond::AddShard(DsShardId ds_shard_id)
{
    {
        Lock lk(mu_);
        ShardSchedule &schedule = schedules_[ds_shard_id];
        schedule.attached_ = true;
        ++schedule.generation_;
        schedule.next_due_ = Clock::now() + Interval();
    }
    cond_var_.notify_all();
}

void ArchiveCrond::RemoveShard(DsShardId ds_shard_id)
{
    Lock lk(mu_);
    auto it = schedules_.find(ds_shard_id);
    if (it == schedules_.end())
    {
        return;
    }
    ShardSchedule &schedule = it->second;
    schedule.attached_ = false;
    ++schedule.generation_;
    if (!schedule.in_flight_)
    {
        // No archive reads the request: drop the entry now. One in flight
        // keeps it until it completes; the thread drops it then.
        schedules_.erase(it);
    }
}

ArchiveCrond::Clock::duration ArchiveCrond::Interval() const
{
    return std::chrono::seconds(store_->Options().archive_interval_secs);
}

void ArchiveCrond::Wait(Lock &lk, std::optional<Clock::time_point> until)
{
#ifdef ELOQ_MODULE_ENABLED
    // This thread is a plain pthread: the bthread condition variable blocks
    // it, as a std one would.
    if (!until.has_value())
    {
        cond_var_.wait(lk);
        return;
    }
    const auto remaining =
        std::chrono::duration_cast<std::chrono::microseconds>(*until -
                                                              Clock::now());
    if (remaining.count() > 0)
    {
        cond_var_.wait_for(lk, remaining.count());
    }
#else
    if (!until.has_value())
    {
        cond_var_.wait(lk);
        return;
    }
    cond_var_.wait_until(lk, *until);
#endif
}

void ArchiveCrond::Crond()
{
    Lock lk(mu_);
    while (!stop_requested_)
    {
        const Clock::time_point now = Clock::now();
        std::vector<DueArchive> due;
        std::optional<Clock::time_point> earliest;
        for (auto it = schedules_.begin(); it != schedules_.end();)
        {
            ShardSchedule &schedule = it->second;
            if (schedule.in_flight_)
            {
                ++it;
                continue;
            }
            if (!schedule.attached_)
            {
                // Removed while its archive ran, which has completed now.
                it = schedules_.erase(it);
                continue;
            }
            if (schedule.next_due_ <= now)
            {
                schedule.in_flight_ = true;
                // The previous request is done: nothing reads it any more.
                schedule.req_ = std::make_unique<GlobalArchiveRequest>();
                due.push_back(
                    {it->first, schedule.generation_, schedule.req_.get()});
            }
            else if (!earliest.has_value() || schedule.next_due_ < *earliest)
            {
                earliest = schedule.next_due_;
            }
            ++it;
        }

        if (!due.empty())
        {
            // Submitting may block while an archive lists its partitions, and
            // a completion may run inline: neither holds the lock.
            lk.unlock();
            for (const DueArchive &archive : due)
            {
                Submit(archive);
            }
            lk.lock();
            continue;
        }
        Wait(lk, earliest);
    }
    lk.unlock();
    exited_.store(true, std::memory_order_release);
}

void ArchiveCrond::Submit(const DueArchive &due)
{
    const EloqStore::DsShardState state = store_->ShardState(due.ds_shard_id_);
    if (state == EloqStore::DsShardState::Detached)
    {
        // The shard went without RemoveShard: the store stopped, or an attach
        // raced its own detach. Its schedule ends here.
        Lock lk(mu_);
        auto it = schedules_.find(due.ds_shard_id_);
        if (it != schedules_.end() && it->second.generation_ == due.generation_)
        {
            it->second.attached_ = false;
        }
        if (it != schedules_.end())
        {
            it->second.in_flight_ = false;
        }
        return;
    }
    if (state != EloqStore::DsShardState::Attached)
    {
        // Expired, or being detached: try again one interval later. A detach
        // ends the schedule; a newer term comes with a new attach.
        LOG(WARNING) << "Scheduled archiving of data store shard "
                     << due.ds_shard_id_ << " put off: the shard is in state "
                     << static_cast<int>(state);
        OnArchiveDone(due.ds_shard_id_, due.generation_, KvError::NotRunning);
        return;
    }

    LOG(INFO) << "Start scheduled archiving of data store shard "
              << due.ds_shard_id_;
    due.req_->SetDsShard(due.ds_shard_id_);
    const DsShardId ds_shard_id = due.ds_shard_id_;
    const uint64_t generation = due.generation_;
    const bool sent = store_->ExecAsyn(
        due.req_,
        0,
        [this, ds_shard_id, generation](KvRequest *req)
        {
            // The request is not touched after this: the thread may free it
            // as soon as OnArchiveDone clears in_flight_.
            OnArchiveDone(ds_shard_id, generation, req->Error());
        });
    if (!sent)
    {
        // Refused before it ran (the store is stopping, or the shard left
        // Attached just now): no callback comes.
        OnArchiveDone(ds_shard_id, generation, KvError::NotRunning);
    }
}

void ArchiveCrond::OnArchiveDone(DsShardId ds_shard_id,
                                 uint64_t generation,
                                 KvError err)
{
    if (err == KvError::NoError)
    {
        LOG(INFO) << "Scheduled archiving of data store shard " << ds_shard_id
                  << " completed successfully";
    }
    else if (err != KvError::NotRunning)
    {
        LOG(ERROR) << "Scheduled archiving of data store shard " << ds_shard_id
                   << " failed: " << ErrorString(err);
    }
    {
        Lock lk(mu_);
        auto it = schedules_.find(ds_shard_id);
        if (it != schedules_.end() && it->second.in_flight_)
        {
            ShardSchedule &schedule = it->second;
            schedule.in_flight_ = false;
            if (schedule.attached_ && schedule.generation_ == generation)
            {
                // Measured from the end of this archive, as the interval
                // always was.
                schedule.next_due_ = Clock::now() + Interval();
            }
        }
    }
    cond_var_.notify_all();
}
}  // namespace eloqstore
