#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

#include "common.h"
#include "compression.h"
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
