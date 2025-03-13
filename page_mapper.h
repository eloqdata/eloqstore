#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "table_ident.h"

#define MAPPING_BITS 2
#define MAPPING_MASK ((1 << MAPPING_BITS) - 1)
#define MAPPING_PHYSICAL 1
#define MAPPING_LOGICAL (1 << 1)

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;

struct MappingSnapshot
{
    MappingSnapshot(IndexPageManager *idx_mgr, const TableIdent *tbl);
    ~MappingSnapshot();

    uint32_t ToFilePage(uint32_t page_id) const;
    uint32_t GetNextFree(uint32_t page_id) const;

    void AddFreeFilePage(uint32_t file_page);
    void AbortFreeFilePage();

    /**
     * @brief Replaces the swizzling pointer with the file page Id.
     *
     * @param page
     */
    void Unswizzling(MemIndexPage *page);
    MemIndexPage *GetSwizzlingPointer(uint32_t page_id) const;
    void AddSwizzling(uint32_t page_id, MemIndexPage *idx_page);
    static bool IsSwizzlingPointer(uint64_t val);

    static bool IsFilePageId(uint64_t val);
    static bool IsLogicalPageId(uint64_t val);

    void Serialize(std::string &dst) const;

    IndexPageManager *idx_mgr_;
    const TableIdent *tbl_ident_;

    std::vector<uint64_t> mapping_tbl_;
    /**
     * @brief A list of file pages to be freed in this mapping snapshot.
     * To-be-freed file pages cannot be put back for re-use if someone is using
     * this snapshot.
     *
     */
    std::vector<uint32_t> to_free_file_pages_;
};

class PageMapper
{
public:
    PageMapper();
    PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident);
    PageMapper(const PageMapper &rhs);

    uint32_t GetPage();
    uint32_t GetFilePage();
    void InitPages(uint32_t page_count);
    void FreePage(uint32_t page_id);
    void FreeFilePages(std::vector<uint32_t> file_pages);
    void FreeFilePage(uint32_t file_page);

    std::shared_ptr<MappingSnapshot> GetMappingSnapshot();
    MappingSnapshot *GetMapping() const;
    void UpdateMapping(uint32_t page_id, uint32_t file_page_id);
    uint32_t UseCount();
    void FreeMappingSnapshot();

    static uint64_t EncodeFilePageId(uint32_t file_page_id);
    static uint64_t EncodeLogicalPageId(uint32_t page_id);
    static uint32_t DecodePageId(uint64_t val);

    void Serialize(std::string &dst) const;
    std::string_view Deserialize(std::string_view src);
    bool EqualTo(const PageMapper &rhs) const;

private:
    std::vector<uint64_t> &Mapping();

    uint32_t ExpandFilePage();
    bool DequeFreePage(uint32_t page_id);
    bool DelFreeFilePage(uint32_t file_page_id);

    /**
     * @brief Gets a free file page. The implementation now returns the smaller
     * file page from the free file page pool. It's more desirable to return
     * from a file who has most free pages. This would reduce replication
     * amplification, when replication granularity is on files.
     *
     * @return The free file page Id, if the free page pool is not empty. Or,
     * UINT32_MAX.
     */
    uint32_t GetFreeFilePage();

    std::shared_ptr<MappingSnapshot> mapping_;
    uint32_t free_page_head_{UINT32_MAX};

    std::set<uint32_t> free_file_pages_;
    uint32_t min_file_page_id_{UINT32_MAX};
    uint32_t max_file_page_id_{0};

    friend class Replayer;
};

}  // namespace kvstore