#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "common.h"
#include "compression.h"
#include "kv_options.h"
#include "manifest_buffer.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "tasks/task.h"

namespace eloqstore
{
// For Manifest snapshot, the structure is:
// Header :  [ Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
// Body   :  [ MaxFpId(8B) | DictLen(4B) | dict_bytes(bytes) |
//             mapping_bytes_len(4B) | mapping_tbl(varint64...) |
//             Serialized FileIdTermMapping bytes(4B|varint64...) ]

// For appended Manifest log, the structure is:
// Header  :  [ Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
// LogBody :  [ mapping_bytes_len(4B) | mapping_bytes(varint64...) |
//              | Serialized FileIdTermMapping bytes(4B|varint64...) ]
class PageMapper;
struct MappingSnapshot;
class IndexPageManager;

class ManifestBuilder
{
public:
    ManifestBuilder();
    void UpdateMapping(PageId page_id, FilePageId file_page_id);
    void DeleteMapping(PageId page_id);
    /*
     * @brief Update the mapping_bytes_len and append file_term_mapping to
     *         buff_.
     */
    void AppendFileIdTermMapping(std::string_view file_term_mapping);
    std::string_view Snapshot(PageId root_id,
                              PageId ttl_root,
                              const MappingSnapshot *mapping,
                              FilePageId max_fp_id,
                              std::string_view dict_bytes,
                              std::string_view file_term_mapping);

    std::string_view Finalize(PageId new_root, PageId ttl_root);
    static bool ValidateChecksum(std::string_view record);
    void Reset();
    bool Empty() const;
    uint32_t CurrentSize() const;

    // checksum(8B)|root(4B)|ttl_root(4B)|log_size(4B)
    static constexpr uint16_t header_bytes =
        checksum_bytes + sizeof(PageId) * 2 + sizeof(uint32_t);

    static constexpr uint16_t offset_root = checksum_bytes;
    static constexpr uint16_t offset_ttl_root = offset_root + sizeof(PageId);
    static constexpr uint16_t offset_len = offset_ttl_root + sizeof(PageId);

private:
    static uint64_t CalcChecksum(std::string_view content);
    ManifestBuffer buff_;
    bool resized_for_mapping_bytes_len_{false};
};

struct RootMeta
{
    RootMeta();
    RootMeta(const RootMeta &rhs) = delete;
    RootMeta(RootMeta &&rhs) = default;

    PageId root_id_{MaxPageId};
    PageId ttl_root_id_{MaxPageId};
    std::unique_ptr<PageMapper> mapper_{nullptr};
    absl::flat_hash_set<MappingSnapshot *> mapping_snapshots_;
    absl::flat_hash_set<MemIndexPage *> index_pages_;
    uint64_t manifest_size_{0};
    uint64_t next_expire_ts_{0};
    std::shared_ptr<compression::DictCompression> compression_{nullptr};

    uint32_t ref_cnt_{0};
    bool locked_{false};
    WaitingZone waiting_;
};

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
        Entry *EntryPtr() const
        {
            return entry_;
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

struct CowRootMeta
{
    CowRootMeta() = default;
    CowRootMeta(const CowRootMeta &) = delete;
    CowRootMeta &operator=(const CowRootMeta &) = delete;
    CowRootMeta(CowRootMeta &&rhs) = default;
    CowRootMeta &operator=(CowRootMeta &&rhs) = default;
    PageId root_id_{MaxPageId};
    PageId ttl_root_id_{MaxPageId};
    std::unique_ptr<PageMapper> mapper_{nullptr};
    uint64_t manifest_size_{};
    MappingSnapshot::Ref old_mapping_{nullptr};
    uint64_t next_expire_ts_{};
    std::shared_ptr<compression::DictCompression> compression_{nullptr};
    RootMetaMgr::Handle root_handle_{};
};

}  // namespace eloqstore
