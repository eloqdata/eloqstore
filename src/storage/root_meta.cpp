#include "storage/root_meta.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <string>
#include <string_view>
#include <vector>

#include "coding.h"
#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"
#include "types.h"

namespace eloqstore
{

RootMeta::RootMeta()
    : compression_(std::make_shared<compression::DictCompression>())
{
}

ManifestBuilder::ManifestBuilder()
{
    Reset();
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    if (!resized_for_mapping_bytes_len_)
    {
        // When changing mapping with ManifestBuilder, mapping info will be
        // appended to buf_ directly, so we should pre-allocate space for
        // mapping_bytes_len and update them later.
        CHECK(buff_.size() == header_bytes);
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }

    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    if (!resized_for_mapping_bytes_len_)
    {
        CHECK(buff_.size() == header_bytes);
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }

    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::InvalidValue);
}

void ManifestBuilder::AppendFileIdTermMapping(
    std::string_view file_term_mapping)
{
    CHECK(resized_for_mapping_bytes_len_ || buff_.size() == header_bytes);
    if (!resized_for_mapping_bytes_len_)
    {
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }
    // update the mapping_len(4B)
    uint32_t mapping_len =
        static_cast<uint32_t>(buff_.size() - header_bytes - 4);
    EncodeFixed32(buff_.data() + header_bytes, mapping_len);
    // append the serialized file_term_mapping
    buff_.append(file_term_mapping);
}

std::string_view ManifestBuilder::Snapshot(PageId root_id,
                                           PageId ttl_root,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id,
                                           std::string_view dict_bytes,
                                           std::string_view file_term_mapping)
{
    // For snapshot, the structure is:
    // Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) |
    // MaxFpId(8B) | DictLen(4B) | dict_bytes(bytes) | mapping_len(4B) |
    // mapping_tbl(varint64...) | file_term_mapping_len(4B) |
    // file_term_mapping(varint64...)
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1) + 4 +
                  file_term_mapping.size());
    buff_.AppendVarint64(max_fp_id);
    buff_.AppendVarint32(dict_bytes.size());
    buff_.append(dict_bytes.data(), dict_bytes.size());
    // mapping_bytes_len(4B)
    size_t mapping_bytes_len_offset = buff_.size();
    buff_.resize(buff_.size() + 4);
    // mapping_tbl
    mapping->Serialize(buff_);
    // update the mapping_bytes_len
    uint32_t mapping_bytes_len =
        static_cast<uint32_t>(buff_.size() - mapping_bytes_len_offset - 4);
    EncodeFixed32(buff_.data() + mapping_bytes_len_offset, mapping_bytes_len);
    // file_term_mapping
    buff_.append(file_term_mapping);
    return Finalize(root_id, ttl_root);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
    resized_for_mapping_bytes_len_ = false;
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

uint32_t ManifestBuilder::CurrentSize() const
{
    return buff_.size();
}

std::string_view ManifestBuilder::Finalize(PageId new_root, PageId ttl_root)
{
    EncodeFixed32(buff_.data() + offset_root, new_root);
    EncodeFixed32(buff_.data() + offset_ttl_root, ttl_root);

    const uint32_t payload_len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, payload_len);

    const std::string_view content(buff_.data() + checksum_bytes,
                                   buff_.size() - checksum_bytes);
    EncodeFixed64(buff_.data(), CalcChecksum(content));
    buff_.AlignTo(page_align);
    return buff_.View();
}

bool ManifestBuilder::ValidateChecksum(std::string_view record)
{
    if (record.size() < header_bytes)
    {
        return false;
    }
    const uint64_t stored = DecodeFixed64(record.data());
    const std::string_view content(record.data() + checksum_bytes,
                                   record.size() - checksum_bytes);
    return stored == CalcChecksum(content);
}

uint64_t ManifestBuilder::CalcChecksum(std::string_view content)
{
    if (content.empty())
    {
        return 0;
    }

    uint64_t agg_checksum = 0;
    static constexpr size_t kCheckSumBatchSize = 1024 * 1024;
    static constexpr uint64_t kChecksumMixer = 0x9e3779b97f4a7c15ULL;
    bool can_yield = shard != nullptr;
    for (size_t off = 0; off < content.size(); off += kCheckSumBatchSize)
    {
        const size_t batch_size =
            std::min<size_t>(kCheckSumBatchSize, content.size() - off);
        const uint64_t checksum = XXH3_64bits(content.data() + off, batch_size);
        agg_checksum = std::rotl(agg_checksum, 1) ^ checksum;
        agg_checksum *= kChecksumMixer;
        if (__builtin_expect(can_yield, 1))
        {
            ThdTask()->YieldToLowPQ();
        }
    }
    return agg_checksum;
}

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
        RootMeta &meta = it.second.meta_;
        for (MappingSnapshot *snapshot : meta.mapping_snapshots_)
        {
            snapshot->idx_mgr_ = nullptr;
        }
        meta.mapping_snapshots_.clear();
        meta.mapper_ = nullptr;
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
