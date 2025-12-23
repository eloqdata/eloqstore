#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

#include "compression.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "task.h"

namespace eloqstore
{
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
                              std::string_view dict_bytes);

    std::string_view Finalize(PageId new_root, PageId ttl_root);
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
    std::string buff_;
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
    std::shared_ptr<compression::DictCompression> compression_{nullptr};
};

struct RootMeta
{
    RootMeta() : compression_(std::make_shared<compression::DictCompression>())
    {
    }
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
    std::shared_ptr<compression::DictCompression> compression_{nullptr};

    uint32_t ref_cnt_{0};
    bool locked_{false};
    WaitingZone waiting_;
};

}  // namespace eloqstore