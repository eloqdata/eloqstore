#include "storage/compression_manager.h"

#include <cassert>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "replayer.h"

namespace eloqstore
{
CompressionManager::Handle::Handle(CompressionManager::Entry *entry,
                                   CompressionManager *owner)
    : entry_(entry), owner_(owner)
{
    if (entry_ != nullptr)
    {
        compression_ = entry_->compression_;
        owner_->Pin(entry_);
    }
}

CompressionManager::Handle::Handle(const Handle &other)
    : entry_(other.entry_),
      owner_(other.owner_),
      compression_(other.compression_)
{
    if (entry_ != nullptr)
    {
        owner_->Pin(entry_);
    }
}

CompressionManager::Handle::Handle(Handle &&other) noexcept
    : entry_(other.entry_),
      owner_(other.owner_),
      compression_(std::move(other.compression_))
{
    other.entry_ = nullptr;
    other.owner_ = nullptr;
}

CompressionManager::Handle &CompressionManager::Handle::operator=(
    const Handle &other)
{
    if (this == &other)
    {
        return *this;
    }
    Clear();
    entry_ = other.entry_;
    owner_ = other.owner_;
    compression_ = other.compression_;
    if (entry_ != nullptr)
    {
        owner_->Pin(entry_);
    }
    return *this;
}

CompressionManager::Handle &CompressionManager::Handle::operator=(
    Handle &&other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    Clear();
    entry_ = other.entry_;
    owner_ = other.owner_;
    compression_ = std::move(other.compression_);
    other.entry_ = nullptr;
    other.owner_ = nullptr;
    return *this;
}

CompressionManager::Handle::~Handle()
{
    Clear();
}

compression::DictCompression *CompressionManager::Handle::Get() const
{
    return compression_.get();
}

std::shared_ptr<compression::DictCompression> CompressionManager::Handle::
    Shared() const
{
    return compression_;
}

void CompressionManager::Handle::Clear()
{
    if (entry_ != nullptr)
    {
        owner_->Unpin(entry_);
        entry_ = nullptr;
        owner_ = nullptr;
        compression_.reset();
    }
}

CompressionManager::CompressionManager(AsyncIoManager *io_mgr,
                                       const KvOptions *options,
                                       size_t capacity_bytes)
    : io_mgr_(io_mgr), options_(options)
{
    capacity_bytes_ = capacity_bytes;
    lru_head_.next_ = &lru_tail_;
    lru_tail_.prev_ = &lru_head_;
}

std::pair<CompressionManager::Handle, KvError>
CompressionManager::GetOrLoad(const TableIdent &tbl_id, const DictMeta &meta)
{
    return GetOrLoad(tbl_id, meta, nullptr);
}

std::pair<CompressionManager::Handle, KvError>
CompressionManager::GetOrLoad(const TableIdent &tbl_id,
                              const DictMeta &meta,
                              ManifestFile *manifest)
{
    Entry *entry = GetEntry(tbl_id, meta);
    Handle handle(entry, this);
    if (meta.HasDictionary() && !entry->compression_->HasDictionary())
    {
        LOG(INFO) << "dict cache miss, loading dictionary for "
                  << tbl_id.ToString() << " len=" << meta.dict_len;
        KvError err = LoadDictionary(entry, manifest);
        if (err != KvError::NoError)
        {
            LOG(WARNING) << "dict load failed for " << tbl_id.ToString()
                         << " err=" << static_cast<int>(err);
            handle.Clear();
            return {{}, err};
        }
    }
    return {std::move(handle), KvError::NoError};
}

void CompressionManager::UpdateDictionary(
    const std::shared_ptr<compression::DictCompression> &compression,
    const DictMeta &meta)
{
    if (!compression)
    {
        return;
    }
    for (auto it = entries_.begin(); it != entries_.end(); ++it)
    {
        Entry *entry = &it->second;
        if (entry->compression_ != compression)
        {
            continue;
        }
        entry->meta_ = meta;
        const size_t new_bytes = entry->compression_->DictionaryBytes().size();
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
        return;
    }
}

size_t CompressionManager::KeyHash::operator()(const Key &key) const
{
    return std::hash<TableIdent>()(key.tbl_id_);
}

CompressionManager::Entry *CompressionManager::GetEntry(
    const TableIdent &tbl_id, const DictMeta &meta)
{
    Key key{tbl_id};
    auto [it, inserted] = entries_.try_emplace(key);
    Entry *entry = &it->second;
    if (inserted)
    {
        entry->key_ = key;
        entry->meta_ = meta;
        entry->compression_ = std::make_shared<compression::DictCompression>();
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

void CompressionManager::Pin(Entry *entry)
{
    if (entry->ref_count_ == 0 && entry->in_lru_)
    {
        Dequeue(entry);
    }
    entry->ref_count_++;
}

void CompressionManager::Unpin(Entry *entry)
{
    assert(entry->ref_count_ > 0);
    entry->ref_count_--;
    if (entry->ref_count_ == 0)
    {
        EnqueueFront(entry);
        EvictIfNeeded();
    }
}

void CompressionManager::EnqueueFront(Entry *entry)
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

void CompressionManager::Dequeue(Entry *entry)
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

void CompressionManager::EvictIfNeeded()
{
    Entry *cursor = lru_tail_.prev_;
    while (used_bytes_ > capacity_bytes_ && cursor != &lru_head_)
    {
        Entry *victim = cursor;
        cursor = cursor->prev_;
        if (victim->ref_count_ > 0 ||
            victim->compression_.use_count() > 1 ||
            (victim->compression_ && victim->compression_->Dirty()))
        {
            continue;
        }
        Dequeue(victim);
        LOG(INFO) << "dict evicted for " << victim->key_.tbl_id_.ToString()
                  << " bytes=" << victim->bytes_;
        used_bytes_ -= victim->bytes_;
        entries_.erase(victim->key_);
    }
}

KvError CompressionManager::LoadDictionary(Entry *entry,
                                           ManifestFile *manifest)
{
    if (entry->meta_.dict_len == 0)
    {
        return KvError::NoError;
    }
    std::unique_ptr<ManifestFile> manifest_guard;
    if (manifest == nullptr)
    {
        if (io_mgr_ == nullptr)
        {
            return KvError::NotFound;
        }
        auto [manifest_ptr, err] = io_mgr_->GetManifest(entry->key_.tbl_id_);
        CHECK_KV_ERR(err);
        manifest_guard = std::move(manifest_ptr);
        manifest = manifest_guard.get();
    }
    std::string dict_bytes;
    KvError err = Replayer::ReadSnapshotDict(manifest,
                                             entry->meta_,
                                             dict_bytes);
    CHECK_KV_ERR(err);
    if (dict_bytes.size() != entry->meta_.dict_len)
    {
        return KvError::Corrupted;
    }
    entry->compression_->LoadDictionary(std::move(dict_bytes));
    LOG(INFO) << "dict loaded for " << entry->key_.tbl_id_.ToString()
              << " bytes=" << entry->meta_.dict_len;
    UpdateDictionary(entry->compression_, entry->meta_);
    return KvError::NoError;
}

}  // namespace eloqstore
