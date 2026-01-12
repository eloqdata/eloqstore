#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

#include "manifest_buffer.h"
#include "storage/dict_meta.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "tasks/task.h"

namespace eloqstore
{
namespace compression
{
class DictCompression;
}  // namespace compression

class ManifestBuilder
{
public:
    ManifestBuilder();
    void UpdateMapping(PageId page_id, FilePageId file_page_id);
    void DeleteMapping(PageId page_id);
    std::string_view Snapshot(PageId root_id,
                              PageId ttl_root,
                              const MappingSnapshot *mapping,
                              FilePageId max_fp_id,
                              const DictMeta &dict_meta,
                              std::string_view dict_bytes);

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
};

struct CowRootMeta
{
    CowRootMeta() = default;
    CowRootMeta(CowRootMeta &&rhs) = default;
    PageId root_id_{MaxPageId};
    PageId ttl_root_id_{MaxPageId};
    std::unique_ptr<PageMapper> mapper_{nullptr};
    uint64_t manifest_size_{};
    std::shared_ptr<MappingSnapshot> old_mapping_{nullptr};
    uint64_t next_expire_ts_{};
    DictMeta dict_meta_{};
    std::shared_ptr<compression::DictCompression> compression_{};
};

struct RootMeta
{
    RootMeta() = default;
    RootMeta(const RootMeta &rhs) = delete;
    RootMeta(RootMeta &&rhs) = default;
    void Pin();
    void Unpin();

    PageId root_id_{MaxPageId};
    PageId ttl_root_id_{MaxPageId};
    std::unique_ptr<PageMapper> mapper_{nullptr};
    std::unordered_set<MappingSnapshot *> mapping_snapshots_;
    uint64_t manifest_size_{0};
    uint64_t next_expire_ts_{0};
    DictMeta dict_meta_{};
    std::weak_ptr<compression::DictCompression> compression_{};

    uint32_t ref_cnt_{0};
    bool locked_{false};
    WaitingZone waiting_;
};

}  // namespace eloqstore
