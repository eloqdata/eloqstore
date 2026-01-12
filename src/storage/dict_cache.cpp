#include "storage/dict_cache.h"

#include <cassert>
#include <string>
#include <utility>

#include "replayer.h"

namespace eloqstore
{
DictCache::Ref::Ref(Entry *entry, DictCache *owner)
    : entry_(entry), owner_(owner)
{
    if (entry_ != nullptr)
    {
        owner_->Pin(entry_);
    }
}

DictCache::Ref::Ref(const Ref &other)
    : entry_(other.entry_), owner_(other.owner_)
{
    if (entry_ != nullptr)
    {
        owner_->Pin(entry_);
    }
}

DictCache::Ref::Ref(Ref &&other) noexcept
    : entry_(other.entry_), owner_(other.owner_)
{
    other.entry_ = nullptr;
    other.owner_ = nullptr;
}

DictCache::Ref &DictCache::Ref::operator=(const Ref &other)
{
    if (this == &other)
    {
        return *this;
    }
    Clear();
    entry_ = other.entry_;
    owner_ = other.owner_;
    if (entry_ != nullptr)
    {
        owner_->Pin(entry_);
    }
    return *this;
}

DictCache::Ref &DictCache::Ref::operator=(Ref &&other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    Clear();
    entry_ = other.entry_;
    owner_ = other.owner_;
    other.entry_ = nullptr;
    other.owner_ = nullptr;
    return *this;
}

DictCache::Ref::~Ref()
{
    Clear();
}

compression::DictCompression *DictCache::Ref::Get() const
{
    if (entry_ == nullptr)
    {
        return nullptr;
    }
    return &entry_->compression_;
}

void DictCache::Ref::Clear()
{
    if (entry_ != nullptr)
    {
        owner_->Unpin(entry_);
        entry_ = nullptr;
        owner_ = nullptr;
    }
}

DictCache::DictCache(AsyncIoManager *io_mgr, const KvOptions *options)
    : io_mgr_(io_mgr), options_(options)
{
    capacity_bytes_ = options_->dict_cache_size;
    lru_head_.next_ = &lru_tail_;
    lru_tail_.prev_ = &lru_head_;
}

std::pair<DictCache::Ref, KvError> DictCache::Acquire(
    const TableIdent &tbl_id, const DictMeta &meta)
{
    Entry *entry = GetEntry(tbl_id, meta);
    Ref ref(entry, this);
    if (meta.HasDictionary() && !entry->compression_.HasDictionary())
    {
        KvError err = LoadDictionary(entry);
        if (err != KvError::NoError)
        {
            ref.Clear();
            return {{}, err};
        }
    }
    return {std::move(ref), KvError::NoError};
}

void DictCache::UpdateDictionary(const Ref &ref, const DictMeta &meta)
{
    if (!ref)
    {
        return;
    }
    Entry *entry = ref.entry_;
    entry->meta_ = meta;
    const size_t new_bytes = entry->compression_.DictionaryBytes().size();
    if (new_bytes >= entry->bytes_)
    {
        used_bytes_ += (new_bytes - entry->bytes_);
    }
    else
    {
        used_bytes_ -= (entry->bytes_ - new_bytes);
    }
    entry->bytes_ = new_bytes;
    EvictIfNeeded();
}

DictCache::Entry *DictCache::GetEntry(const TableIdent &tbl_id,
                                      const DictMeta &meta)
{
    auto [it, inserted] = entries_.try_emplace(tbl_id);
    Entry *entry = &it->second;
    if (inserted)
    {
        entry->tbl_id_ = tbl_id;
        entry->meta_ = meta;
        entry->ref_count_ = 0;
        entry->bytes_ = 0;
        entry->in_lru_ = false;
        entry->prev_ = nullptr;
        entry->next_ = nullptr;
    }
    else
    {
        entry->meta_ = meta;
    }
    return entry;
}

void DictCache::Pin(Entry *entry)
{
    if (entry->ref_count_ == 0 && entry->in_lru_)
    {
        Dequeue(entry);
    }
    entry->ref_count_++;
}

void DictCache::Unpin(Entry *entry)
{
    assert(entry->ref_count_ > 0);
    entry->ref_count_--;
    if (entry->ref_count_ == 0)
    {
        EnqueueFront(entry);
        EvictIfNeeded();
    }
}

void DictCache::EnqueueFront(Entry *entry)
{
    if (entry->in_lru_)
    {
        return;
    }
    entry->prev_ = &lru_head_;
    entry->next_ = lru_head_.next_;
    lru_head_.next_->prev_ = entry;
    lru_head_.next_ = entry;
    entry->in_lru_ = true;
}

void DictCache::Dequeue(Entry *entry)
{
    if (!entry->in_lru_)
    {
        return;
    }
    Entry *prev = entry->prev_;
    Entry *next = entry->next_;
    prev->next_ = next;
    next->prev_ = prev;
    entry->prev_ = nullptr;
    entry->next_ = nullptr;
    entry->in_lru_ = false;
}

void DictCache::EvictIfNeeded()
{
    while (used_bytes_ > capacity_bytes_ && lru_tail_.prev_ != &lru_head_)
    {
        Entry *victim = lru_tail_.prev_;
        Dequeue(victim);
        used_bytes_ -= victim->bytes_;
        entries_.erase(victim->tbl_id_);
    }
}

KvError DictCache::LoadDictionary(Entry *entry)
{
    if (entry->meta_.dict_len == 0)
    {
        return KvError::NoError;
    }
    if (io_mgr_ == nullptr)
    {
        return KvError::NotFound;
    }
    auto [manifest, err] = io_mgr_->GetManifest(entry->tbl_id_);
    CHECK_KV_ERR(err);
    std::string dict_bytes;
    err = Replayer::ReadSnapshotDict(manifest.get(), dict_bytes);
    CHECK_KV_ERR(err);
    if (dict_bytes.size() != entry->meta_.dict_len)
    {
        return KvError::Corrupted;
    }
    entry->compression_.LoadDictionary(std::move(dict_bytes));
    UpdateDictionary(Ref(entry, this), entry->meta_);
    return KvError::NoError;
}

}  // namespace eloqstore
