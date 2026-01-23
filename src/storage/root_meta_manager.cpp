#include "storage/root_meta_manager.h"

#include <glog/logging.h>

#include <cassert>
#include <vector>

#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"

namespace eloqstore
{
RootMetaMgr::RootMetaMgr(IndexPageManager *owner, const KvOptions *options)
    : owner_(owner), options_(options)
{
    capacity_bytes_ = options_->root_meta_cache_size;
    if (options_->num_threads > 0)
    {
        capacity_bytes_ /= options_->num_threads;
    }
    lru_head_.next_ = &lru_tail_;
    lru_tail_.prev_ = &lru_head_;
}

std::pair<RootMetaMgr::Entry *, bool> RootMetaMgr::GetOrCreate(
    const TableIdent &tbl_id)
{
    auto [it, inserted] = entries_.try_emplace(tbl_id);
    Entry *entry = &it->second;
    if (inserted)
    {
        entry->tbl_id_ = tbl_id;
        entry->prev_ = nullptr;
        entry->next_ = nullptr;
        entry->bytes_ = 0;
        EnqueueFront(entry);
    }
    return {entry, inserted};
}

RootMetaMgr::Entry *RootMetaMgr::Find(const TableIdent &tbl_id)
{
    auto it = entries_.find(tbl_id);
    if (it == entries_.end())
    {
        return nullptr;
    }
    return &it->second;
}

void RootMetaMgr::Erase(const TableIdent &tbl_id)
{
    auto it = entries_.find(tbl_id);
    if (it == entries_.end())
    {
        return;
    }

    Entry *entry = &it->second;
    CHECK(entry->meta_.ref_cnt_ == 0);
    Dequeue(entry);
    used_bytes_ -= entry->bytes_;
    CHECK(used_bytes_ >= 0);
    entries_.erase(it);
}

void RootMetaMgr::Pin(Entry *entry)
{
    CHECK(entry->meta_.ref_cnt_ != 0 || entry->prev_ != nullptr)
        << "pinning root meta that is not in LRU: " << entry->tbl_id_;
    if (entry->meta_.ref_cnt_ == 0)
    {
        Dequeue(entry);
    }
    entry->meta_.ref_cnt_++;
}

void RootMetaMgr::Unpin(Entry *entry)
{
    assert(entry->meta_.ref_cnt_ > 0);
    entry->meta_.ref_cnt_--;
    if (entry->meta_.ref_cnt_ == 0)
    {
        EnqueueFront(entry);
    }
}

void RootMetaMgr::UpdateBytes(Entry *entry, size_t bytes)
{
    if (bytes >= entry->bytes_)
    {
        used_bytes_ += (bytes - entry->bytes_);
    }
    else
    {
        used_bytes_ -= (entry->bytes_ - bytes);
    }
    entry->bytes_ = bytes;
}

bool RootMetaMgr::EvictRootForCache(Entry *entry)
{
    RootMeta &meta = entry->meta_;
    const TableIdent &tbl_id = entry->tbl_id_;
    if (meta.locked_)
    {
        return false;
    }
    CHECK(meta.ref_cnt_ == 0) << "EvictRootForCache: ref_cnt " << meta.ref_cnt_
                              << " table " << tbl_id;
    if (meta.mapper_ == nullptr)
    {
        CHECK(meta.index_pages_.empty())
            << "EvictRootForCache: mapper null but index pages exist for table "
            << tbl_id;
        LOG(INFO) << "EvictRootForCache: mapper null table " << tbl_id;
        return true;
    }
    for (MemIndexPage *page : meta.index_pages_)
    {
        CHECK(!page->IsPinned())
            << "EvictRootForCache: index page pinned table " << tbl_id;
    }
    if (meta.mapper_->MappingCount() == 0 && meta.manifest_size_ > 0)
    {
        meta.locked_ = true;
        LOG(INFO) << "Evicting manifest for table " << tbl_id << " size "
                  << meta.manifest_size_;
        owner_->IoMgr()->CleanManifest(tbl_id);
        meta.waiting_.WakeAll();
    }

    std::vector<MemIndexPage *> pages(meta.index_pages_.begin(),
                                      meta.index_pages_.end());
    for (MemIndexPage *page : pages)
    {
        owner_->RecyclePage(page);
    }
    meta.index_pages_.clear();
    return true;
}

void RootMetaMgr::ReleaseMappers()
{
    for (auto &it : entries_)
    {
        it.second.meta_.mapper_ = nullptr;
    }
}

KvError RootMetaMgr::EvictIfNeeded()
{
    Entry *cursor = lru_tail_.prev_;
    while (used_bytes_ > capacity_bytes_ && cursor != &lru_head_)
    {
        Entry *victim = cursor;
        cursor = cursor->prev_;
        CHECK(victim->prev_ != nullptr)
            << "Evict scan saw non-LRU entry for table " << victim->tbl_id_;
        if (victim->prev_ == &lru_head_ && cursor == &lru_head_)
        {
            LOG(WARNING)
                << "EvictIfNeeded: only one root meta cached, over limit "
                   "used_bytes "
                << used_bytes_ << " capacity_bytes_ " << capacity_bytes_
                << " table " << victim->tbl_id_;
            break;
        }
        if (!EvictRootForCache(victim))
        {
            continue;
        }
        LOG(INFO) << "Evicted root meta from LRU for table " << victim->tbl_id_
                  << " bytes " << victim->bytes_;
        Dequeue(victim);
        if (used_bytes_ >= victim->bytes_)
        {
            used_bytes_ -= victim->bytes_;
        }
        else
        {
            used_bytes_ = 0;
        }
        entries_.erase(victim->tbl_id_);
    }
    if (used_bytes_ > capacity_bytes_)
    {
        LOG(WARNING) << "EvictIfNeeded exit: cache still over limit used_bytes "
                     << used_bytes_ << " capacity_bytes_ " << capacity_bytes_;
        return KvError::OutOfMem;
    }
    return KvError::NoError;
}

void RootMetaMgr::EnqueueFront(Entry *entry)
{
    CHECK(entry->prev_ == nullptr && entry->next_ == nullptr)
        << "enqueue root meta already in LRU: " << entry->tbl_id_;
    entry->prev_ = &lru_head_;
    entry->next_ = lru_head_.next_;
    lru_head_.next_->prev_ = entry;
    lru_head_.next_ = entry;
}

void RootMetaMgr::Dequeue(Entry *entry)
{
    CHECK(entry->prev_ != nullptr && entry->next_ != nullptr)
        << "dequeue root meta not in LRU: " << entry->tbl_id_;
    Entry *prev = entry->prev_;
    Entry *next = entry->next_;
    prev->next_ = next;
    next->prev_ = prev;
    entry->prev_ = nullptr;
    entry->next_ = nullptr;
}

}  // namespace eloqstore
