#pragma once

#include <cassert>
#include <cstddef>
#include <unordered_map>
#include <utility>

#include "kv_options.h"
#include "storage/root_meta.h"
#include "types.h"

namespace eloqstore
{
class IndexPageManager;

class RootMetaMgr
{
public:
    struct Entry
    {
        Entry *prev_{nullptr};
        Entry *next_{nullptr};
        TableIdent tbl_id_{};
        RootMeta meta_{};
        size_t bytes_{0};
    };

    class Handle
    {
    public:
        Handle() = default;
        Handle(RootMetaMgr *mgr, Entry *entry) : mgr_(mgr), entry_(entry)
        {
            assert(mgr != nullptr && entry != nullptr);
            mgr_->Pin(entry_);
        }
        Handle(const Handle &) = delete;
        Handle &operator=(const Handle &) = delete;
        Handle(Handle &&rhs) noexcept : mgr_(rhs.mgr_), entry_(rhs.entry_)
        {
            rhs.mgr_ = nullptr;
            rhs.entry_ = nullptr;
        }
        Handle &operator=(Handle &&rhs) noexcept
        {
            if (this != &rhs)
            {
                if (mgr_ != nullptr && entry_ != nullptr)
                {
                    mgr_->Unpin(entry_);
                }
                mgr_ = rhs.mgr_;
                entry_ = rhs.entry_;
                rhs.mgr_ = nullptr;
                rhs.entry_ = nullptr;
            }
            return *this;
        }
        ~Handle()
        {
            if (mgr_ != nullptr && entry_ != nullptr)
            {
                mgr_->Unpin(entry_);
            }
        }

        RootMeta *Get() const
        {
            return entry_ == nullptr ? nullptr : &entry_->meta_;
        }

    private:
        RootMetaMgr *mgr_{nullptr};
        Entry *entry_{nullptr};
    };

    RootMetaMgr(IndexPageManager *owner, const KvOptions *options);

    std::pair<Entry *, bool> GetOrCreate(const TableIdent &tbl_id);
    Entry *Find(const TableIdent &tbl_id);
    void Erase(const TableIdent &tbl_id);

    void Pin(Entry *entry);
    void Unpin(Entry *entry);
    void UpdateBytes(Entry *entry, size_t bytes);
    bool EvictRootForCache(Entry *entry);

    size_t UsedBytes() const
    {
        return used_bytes_;
    }

    size_t CapacityBytes() const
    {
        return capacity_bytes_;
    }

    void ReleaseMappers();

    KvError EvictIfNeeded();

private:
    IndexPageManager *owner_;
    void EnqueueFront(Entry *entry);
    void Dequeue(Entry *entry);

    const KvOptions *options_;
    size_t capacity_bytes_{0};
    size_t used_bytes_{0};
    std::unordered_map<TableIdent, Entry> entries_;
    Entry lru_head_{};
    Entry lru_tail_{};
};
}  // namespace eloqstore
