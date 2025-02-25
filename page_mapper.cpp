#include "page_mapper.h"

#include <cassert>
#include <cstdint>
#include <unordered_set>

#include "coding.h"
#include "index_page_manager.h"

namespace kvstore
{
// This is used for replay when recovering from WAL.
PageMapper::PageMapper()
{
    mapping_ = std::make_shared<MappingSnapshot>(nullptr, nullptr);
}

PageMapper::PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident)
{
    mapping_ = std::make_shared<MappingSnapshot>(idx_mgr, tbl_ident);
    InitPages(idx_mgr->Options()->init_page_count);
}

PageMapper::PageMapper(const PageMapper &rhs)
    : free_page_head_(rhs.free_page_head_),
      free_file_pages_(rhs.free_file_pages_),
      min_file_page_id_(rhs.min_file_page_id_),
      max_file_page_id_(rhs.max_file_page_id_)
{
    mapping_ = std::make_shared<MappingSnapshot>(rhs.mapping_->idx_mgr_,
                                                 rhs.mapping_->tbl_ident_);
    mapping_->mapping_tbl_ = std::vector<uint64_t>(rhs.mapping_->mapping_tbl_);
}

void PageMapper::InitPages(uint32_t page_count)
{
    std::vector<uint64_t> &map = mapping_->mapping_tbl_;
    assert(map.empty() && free_page_head_ == UINT32_MAX);
    map.resize(page_count);
    uint32_t page_id = map.size() - 1;
    for (auto rit = map.rbegin(); rit != map.rend(); ++rit, --page_id)
    {
        FreePage(page_id);
    }
}

void PageMapper::FreePage(uint32_t page_id)
{
    auto &map = Mapping();
    assert(page_id < map.size());
    map[page_id] = free_page_head_ == UINT32_MAX
                       ? UINT32_MAX
                       : EncodeLogicalPageId(free_page_head_);
    free_page_head_ = page_id;
}

void PageMapper::FreeFilePages(std::vector<uint32_t> file_pages)
{
    free_file_pages_.insert(file_pages.begin(), file_pages.end());
}

void PageMapper::FreeFilePage(uint32_t file_page)
{
    free_file_pages_.insert(file_page);
}

uint32_t PageMapper::GetPage()
{
    auto &map = Mapping();
    if (free_page_head_ == UINT32_MAX)
    {
        map.emplace_back(UINT32_MAX);
        return map.size() - 1;
    }
    else
    {
        uint32_t free_page = free_page_head_;
        // The free page head points to the next free page.
        free_page_head_ = mapping_->GetNextFree(free_page);
        // Sets the free page's mapped file page to null.
        map[free_page] = UINT32_MAX;
        return free_page;
    }
}

uint32_t PageMapper::GetFilePage()
{
    uint32_t fp_id = GetFreeFilePage();

    if (fp_id < UINT32_MAX)
    {
        assert(fp_id >= min_file_page_id_ && fp_id <= max_file_page_id_);
    }
    else
    {
        fp_id = ExpandFilePage();
    }

    return fp_id;
}

uint32_t PageMapper::ExpandFilePage()
{
    uint32_t fp_id;
    if (min_file_page_id_ <= max_file_page_id_)
    {
        if (min_file_page_id_ > 0)
        {
            fp_id = min_file_page_id_ - 1;
            min_file_page_id_ = fp_id;
        }
        else
        {
            fp_id = max_file_page_id_ + 1;
            max_file_page_id_ = fp_id;
        }
    }
    else
    {
        // There is no file page at all.
        min_file_page_id_ = 0;
        max_file_page_id_ = 0;
        fp_id = 0;
    }
    return fp_id;
}

std::shared_ptr<MappingSnapshot> PageMapper::GetMappingSnapshot()
{
    return mapping_;
}

MappingSnapshot *PageMapper::GetMapping()
{
    return mapping_.get();
}

uint32_t PageMapper::UseCount()
{
    return mapping_ == nullptr ? 0 : mapping_.use_count();
}

void PageMapper::FreeMappingSnapshot()
{
    mapping_ = nullptr;
}

std::vector<uint64_t> &PageMapper::Mapping()
{
    return mapping_->mapping_tbl_;
}

void PageMapper::UpdateMapping(uint32_t page_id, uint32_t file_page_id)
{
    assert(page_id < mapping_->mapping_tbl_.size());
    mapping_->mapping_tbl_[page_id] =
        PageMapper::EncodeFilePageId(file_page_id);
}

uint32_t PageMapper::GetFreeFilePage()
{
    auto &free_file_pages = free_file_pages_;
    if (free_file_pages.empty())
    {
        return UINT32_MAX;
    }
    else
    {
        auto it = free_file_pages.begin();
        uint32_t fp_id = *it;
        free_file_pages.erase(it);
        return fp_id;
    }
}

bool PageMapper::DelFreeFilePage(uint32_t file_page_id)
{
    return free_file_pages_.erase(file_page_id);
}

uint64_t PageMapper::EncodeFilePageId(uint32_t file_page_id)
{
    return (file_page_id << MAPPING_BITS) | MAPPING_PHYSICAL;
}

uint64_t PageMapper::EncodeLogicalPageId(uint32_t page_id)
{
    return (page_id << MAPPING_BITS) | MAPPING_LOGICAL;
}

uint32_t PageMapper::DecodePageId(uint64_t val)
{
    return val >> MAPPING_BITS;
}

bool PageMapper::DequeFreePage(uint32_t page_id)
{
    auto &map = Mapping();
    uint32_t prev = free_page_head_;
    if (prev == UINT32_MAX)
    {
        return false;
    }
    if (prev == page_id)
    {
        uint32_t pgid = GetPage();
        assert(pgid == page_id);
        return true;
    }
    uint32_t cur = mapping_->GetNextFree(prev);
    while (cur != UINT32_MAX && cur != page_id)
    {
        prev = cur;
        cur = mapping_->GetNextFree(cur);
    }
    if (cur == UINT32_MAX)
    {
        return false;
    }
    assert(cur == page_id);
    map[prev] = map[cur];
    map[cur] = UINT32_MAX;
    return true;
}

void PageMapper::Serialize(std::string &dst) const
{
    mapping_->Serialize(dst);
    PutVarint32(&dst, free_page_head_);

    PutVarint32(&dst, min_file_page_id_);
    PutVarint32(&dst, max_file_page_id_);
}

std::string_view PageMapper::Deserialize(std::string_view src)
{
    uint32_t sz, val;
    std::unordered_set<uint32_t> used_file_pages;

    GetVarint32(&src, &sz);
    mapping_->mapping_tbl_.resize(sz);
    for (uint32_t i = 0; i < sz; i++)
    {
        GetVarint32(&src, &val);
        mapping_->mapping_tbl_[i] = val;

        if (MappingSnapshot::IsFilePageId(val))
        {
            uint32_t fp_id = DecodePageId(val);
            used_file_pages.insert(fp_id);
        }
    }
    GetVarint32(&src, &free_page_head_);

    GetVarint32(&src, &min_file_page_id_);
    GetVarint32(&src, &max_file_page_id_);

    for (uint32_t i = min_file_page_id_; i < max_file_page_id_; i++)
    {
        if (!used_file_pages.contains(i))
        {
            free_file_pages_.insert(i);
        }
    }
    return src;
}

bool PageMapper::EqualTo(const PageMapper &rhs) const
{
    return free_page_head_ == rhs.free_page_head_ &&
           min_file_page_id_ == rhs.min_file_page_id_ &&
           max_file_page_id_ == rhs.max_file_page_id_ &&
           free_file_pages_ == rhs.free_file_pages_ &&
           mapping_->mapping_tbl_ == rhs.mapping_->mapping_tbl_ &&
           mapping_->to_free_file_pages_ == rhs.mapping_->to_free_file_pages_;
}

MappingSnapshot::MappingSnapshot(IndexPageManager *idx_mgr,
                                 const TableIdent *tbl)
    : idx_mgr_(idx_mgr), tbl_ident_(tbl)
{
}

MappingSnapshot::~MappingSnapshot()
{
    if (idx_mgr_)
    {
        idx_mgr_->FreeMappingSnapshot(this);
    }
}

uint32_t MappingSnapshot::ToFilePage(uint32_t page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    assert(IsFilePageId(val));
    return PageMapper::DecodePageId(val);
}

uint32_t MappingSnapshot::GetFilePage(uint32_t page_id) const
{
    MemIndexPage *p = GetSwizzlingPointer(page_id);
    return p ? p->FilePageId() : ToFilePage(page_id);
}

uint32_t MappingSnapshot::GetNextFree(uint32_t page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (val == UINT32_MAX)
    {
        return UINT32_MAX;
    }
    assert(IsLogicalPageId(val));
    return PageMapper::DecodePageId(val);
}

void MappingSnapshot::AddFreeFilePage(uint32_t file_page)
{
    to_free_file_pages_.emplace_back(file_page);
}

void MappingSnapshot::Unswizzling(MemIndexPage *page)
{
    uint32_t page_id = page->PageId();
    uint32_t file_page_id = page->FilePageId();

    if (page_id < mapping_tbl_.size() &&
        IsSwizzlingPointer(mapping_tbl_[page_id]) &&
        reinterpret_cast<MemIndexPage *>(mapping_tbl_[page_id]) == page)
    {
        mapping_tbl_[page_id] = PageMapper::EncodeFilePageId(file_page_id);
    }
}

MemIndexPage *MappingSnapshot::GetSwizzlingPointer(uint32_t page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (IsSwizzlingPointer(val))
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return idx_page;
    }
    else
    {
        return nullptr;
    }
}

void MappingSnapshot::AddSwizzling(uint32_t page_id, MemIndexPage *idx_page)
{
    assert(page_id < mapping_tbl_.size());

    uint64_t val = mapping_tbl_[page_id];
    if (IsSwizzlingPointer(val))
    {
        assert(reinterpret_cast<MemIndexPage *>(val) == idx_page);
    }
    else
    {
        assert(PageMapper::DecodePageId(val) == idx_page->FilePageId());
        mapping_tbl_[page_id] = reinterpret_cast<uint64_t>(idx_page);
    }
}

bool MappingSnapshot::IsSwizzlingPointer(uint64_t val)
{
    return (val & MAPPING_MASK) == 0;
}

bool MappingSnapshot::IsFilePageId(uint64_t val)
{
    return val != UINT32_MAX && (val & MAPPING_PHYSICAL);
}

bool MappingSnapshot::IsLogicalPageId(uint64_t val)
{
    return val != UINT32_MAX && (val & MAPPING_LOGICAL);
}

void MappingSnapshot::Serialize(std::string &dst) const
{
    PutVarint32(&dst, mapping_tbl_.size());
    for (uint32_t i = 0; i < mapping_tbl_.size(); i++)
    {
        uint32_t val = mapping_tbl_[i];
        MemIndexPage *p = GetSwizzlingPointer(i);
        if (p != nullptr)
        {
            val = PageMapper::EncodeFilePageId(p->FilePageId());
        }
        PutVarint32(&dst, val);
    }
}

}  // namespace kvstore