#include "page_mapper.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <utility>

#include "coding.h"
#include "index_page_manager.h"
#include "task.h"

namespace eloqstore
{
std::vector<uint64_t> MappingArena::Get()
{
    if (pool_.empty())
    {
        return {};
    }
    auto tbl = std::move(pool_.back());
    pool_.pop_back();
    tbl.clear();
    return tbl;
}

void MappingArena::Return(std::vector<uint64_t> tbl)
{
    if (tbl.capacity() == 0 || pool_.size() >= max_cached_)
    {
        return;
    }
    pool_.push_back(std::move(tbl));
}

namespace
{
void EnsureCapacity(std::vector<uint64_t> &tbl, size_t desired)
{
    if (tbl.capacity() >= desired)
    {
        return;
    }
    tbl.reserve(desired);
}
}  // namespace

PageMapper::PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident)
    : mapping_(nullptr), file_page_allocator_(nullptr)
{
    auto *arena = idx_mgr->MapperArena();
    std::vector<uint64_t> tbl =
        arena == nullptr ? std::vector<uint64_t>() : arena->Get();
    mapping_ = std::make_shared<MappingSnapshot>(
        idx_mgr, tbl_ident, std::move(tbl), arena);
    file_page_allocator_ = FilePageAllocator::Instance(Options());
    EnsureCapacity(mapping_->mapping_tbl_, Options()->init_page_count);
}

PageMapper::PageMapper(const PageMapper &rhs)
    : mapping_(std::make_shared<MappingSnapshot>(
          rhs.mapping_->idx_mgr_,
          rhs.mapping_->tbl_ident_,
          rhs.mapping_->arena_ == nullptr ? std::vector<uint64_t>()
                                          : rhs.mapping_->arena_->Get(),
          rhs.mapping_->arena_)),
      free_page_head_(rhs.free_page_head_),
      free_page_cnt_(rhs.free_page_cnt_),
      file_page_allocator_(rhs.file_page_allocator_->Clone())
{
    const auto &src_tbl = rhs.mapping_->mapping_tbl_;
    auto &dst_tbl = mapping_->mapping_tbl_;
    EnsureCapacity(dst_tbl, src_tbl.size());
    dst_tbl.clear();
    size_t copied = 0;

    while (copied < src_tbl.size())
    {
        static constexpr size_t kChunkSize = 512;
        size_t n = std::min(kChunkSize, src_tbl.size() - copied);
        dst_tbl.insert(dst_tbl.end(),
                       src_tbl.begin() + copied,
                       src_tbl.begin() + copied + n);
        copied += n;
        if (copied < src_tbl.size())
        {
            ThdTask()->YieldToNextRound();
        }
    }
    assert(file_page_allocator_->MaxFilePageId() ==
           rhs.file_page_allocator_->MaxFilePageId());
}

PageId PageMapper::GetPage()
{
    auto &map = Mapping();
    if (free_page_head_ == MaxPageId)
    {
        map.emplace_back(MappingSnapshot::InvalidValue);
        return map.size() - 1;
    }
    else
    {
        PageId free_page = free_page_head_;
        // The free page head points to the next free page.
        free_page_head_ = mapping_->GetNextFree(free_page);
        // Sets the free page's mapped file page to null.
        map[free_page] = MappingSnapshot::InvalidValue;
        free_page_cnt_--;
        return free_page;
    }
}

void PageMapper::FreePage(PageId page_id)
{
    auto &map = Mapping();
    assert(page_id < map.size());
    map[page_id] = free_page_head_ == MaxPageId
                       ? MappingSnapshot::InvalidValue
                       : MappingSnapshot::EncodePageId(free_page_head_);
    free_page_head_ = page_id;
    free_page_cnt_++;
}

FilePageAllocator *PageMapper::FilePgAllocator() const
{
    return file_page_allocator_.get();
}

uint32_t PageMapper::MappingCount() const
{
    CHECK(mapping_ != nullptr);

    return mapping_->mapping_tbl_.size() - free_page_cnt_;
}

std::shared_ptr<MappingSnapshot> PageMapper::GetMappingSnapshot() const
{
    return mapping_;
}

MappingSnapshot *PageMapper::GetMapping() const
{
    return mapping_.get();
}

uint32_t PageMapper::UseCount()
{
    return mapping_.use_count();
}

const KvOptions *PageMapper::Options() const
{
    return mapping_->idx_mgr_->Options();
}

std::vector<uint64_t> &PageMapper::Mapping()
{
    return mapping_->mapping_tbl_;
}

#ifndef NDEBUG
bool PageMapper::DebugStat() const
{
    FilePageId min_fp_id = MaxFilePageId;
    uint32_t cnt = 0;
    for (uint64_t val : mapping_->mapping_tbl_)
    {
        FilePageId fp_id = mapping_->ToFilePage(val);
        if (fp_id != MaxFilePageId)
        {
            cnt++;
            min_fp_id = std::min(min_fp_id, fp_id);
        }
    }
    assert(cnt == mapping_->mapping_tbl_.size() - free_page_cnt_);
    auto allocator = dynamic_cast<AppendAllocator *>(FilePgAllocator());
    if (allocator != nullptr && min_fp_id != MaxFilePageId)
    {
        FileId min_file_id = min_fp_id >> Options()->pages_per_file_shift;
        assert(allocator->MinFileId() <= min_file_id);
    }
    return true;
}
#endif

void PageMapper::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    assert(page_id < mapping_->mapping_tbl_.size());
    uint64_t val = MappingSnapshot::EncodeFilePageId(file_page_id);
    mapping_->mapping_tbl_[page_id] = val;
}

uint64_t MappingSnapshot::EncodeFilePageId(FilePageId file_page_id)
{
    return (file_page_id << TypeBits) | uint64_t(ValType::FilePageId);
}

uint64_t MappingSnapshot::EncodePageId(PageId page_id)
{
    return (page_id << TypeBits) | uint64_t(ValType::PageId);
}

uint64_t MappingSnapshot::DecodeId(uint64_t val)
{
    return val >> TypeBits;
}

MappingSnapshot::MappingSnapshot(IndexPageManager *idx_mgr,
                                 const TableIdent *tbl_id,
                                 MappingArena *arena)
    : idx_mgr_(idx_mgr), tbl_ident_(tbl_id), arena_(arena)
{
}

MappingSnapshot::~MappingSnapshot()
{
    idx_mgr_->FreeMappingSnapshot(this);
    if (arena_ != nullptr)
    {
        arena_->Return(std::move(mapping_tbl_));
    }
}

FilePageId MappingSnapshot::ToFilePage(PageId page_id) const
{
    if (page_id == MaxPageId)
    {
        return MaxFilePageId;
    }
    assert(page_id < mapping_tbl_.size());
    return ToFilePage(mapping_tbl_[page_id]);
}

FilePageId MappingSnapshot::ToFilePage(uint64_t val) const
{
    switch (GetValType(val))
    {
    case ValType::SwizzlingPointer:
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return idx_page->GetFilePageId();
    }
    case ValType::FilePageId:
    {
        return DecodeId(val);
    }
    case ValType::PageId:
    case ValType::Invalid:
        break;
    }
    return MaxFilePageId;
}

PageId MappingSnapshot::GetNextFree(PageId page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (val == InvalidValue)
    {
        return MaxPageId;
    }
    assert(GetValType(val) == ValType::PageId);
    return DecodeId(val);
}

void MappingSnapshot::AddFreeFilePage(FilePageId file_page)
{
    assert(file_page != MaxFilePageId);
    to_free_file_pages_.emplace_back(file_page);
}

void MappingSnapshot::ClearFreeFilePage()
{
    to_free_file_pages_.clear();
}

void MappingSnapshot::Unswizzling(MemIndexPage *page)
{
    PageId page_id = page->GetPageId();
    FilePageId file_page_id = page->GetFilePageId();

    if (page_id < mapping_tbl_.size() &&
        IsSwizzlingPointer(mapping_tbl_[page_id]) &&
        reinterpret_cast<MemIndexPage *>(mapping_tbl_[page_id]) == page)
    {
        mapping_tbl_[page_id] = EncodeFilePageId(file_page_id);
    }
}

MemIndexPage *MappingSnapshot::GetSwizzlingPointer(PageId page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (IsSwizzlingPointer(val))
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return idx_page;
    }
    return nullptr;
}

void MappingSnapshot::AddSwizzling(PageId page_id, MemIndexPage *idx_page)
{
    assert(page_id < mapping_tbl_.size());

    uint64_t val = mapping_tbl_[page_id];
    if (IsSwizzlingPointer(val))
    {
        assert(reinterpret_cast<MemIndexPage *>(val) == idx_page);
    }
    else
    {
        assert(DecodeId(val) == idx_page->GetFilePageId());
        mapping_tbl_[page_id] = reinterpret_cast<uint64_t>(idx_page);
    }
}

bool MappingSnapshot::IsSwizzlingPointer(uint64_t val)
{
    return GetValType(val) == ValType::SwizzlingPointer;
}

bool MappingSnapshot::IsFilePageId(uint64_t val)
{
    return GetValType(val) == ValType::FilePageId;
}

MappingSnapshot::ValType MappingSnapshot::GetValType(uint64_t val)
{
    return ValType(val & TypeMask);
}

void MappingSnapshot::Serialize(std::string &dst) const
{
    for (PageId i = 0; i < mapping_tbl_.size(); i++)
    {
        uint64_t val = mapping_tbl_[i];
        if (IsSwizzlingPointer(val))
        {
            MemIndexPage *p = reinterpret_cast<MemIndexPage *>(val);
            val = EncodeFilePageId(p->GetFilePageId());
        }
        PutVarint64(&dst, val);
    }
}

std::unique_ptr<FilePageAllocator> FilePageAllocator::Instance(
    const KvOptions *opts)
{
    if (opts->data_append_mode)
    {
        return std::make_unique<AppendAllocator>(opts);
    }
    else
    {
        return std::make_unique<PooledFilePages>(opts);
    }
}

FilePageAllocator::FilePageAllocator(const KvOptions *opts, FilePageId max_id)
    : pages_per_file_shift_(opts->pages_per_file_shift), max_fp_id_(max_id)
{
}

FilePageId FilePageAllocator::MaxFilePageId() const
{
    return max_fp_id_;
}

uint32_t FilePageAllocator::PagesPerFile() const
{
    return 1 << pages_per_file_shift_;
}

FileId FilePageAllocator::CurrentFileId() const
{
    return max_fp_id_ >> pages_per_file_shift_;
}

FilePageId FilePageAllocator::Allocate()
{
    return max_fp_id_++;
}

std::unique_ptr<FilePageAllocator> AppendAllocator::Clone()
{
    return std::make_unique<AppendAllocator>(*this);
}

void AppendAllocator::UpdateStat(FileId min_file_id, uint32_t hole_cnt)
{
    assert(min_file_id >= min_file_id_);
    assert((min_file_id << pages_per_file_shift_) <= max_fp_id_);
    min_file_id_ = min_file_id;
    empty_file_cnt_ = hole_cnt;
}

FileId AppendAllocator::MinFileId() const
{
    return min_file_id_;
}

size_t AppendAllocator::SpaceSize() const
{
    FilePageId min_fp_id = min_file_id_ << pages_per_file_shift_;
    assert(max_fp_id_ >= min_fp_id);
    return (max_fp_id_ - min_fp_id) -
           (empty_file_cnt_ << pages_per_file_shift_);
}

std::unique_ptr<FilePageAllocator> PooledFilePages::Clone()
{
    return std::make_unique<PooledFilePages>(*this);
}

FilePageId PooledFilePages::Allocate()
{
    if (!free_ids_.empty())
    {
        FilePageId file_page_id = free_ids_.back();
        free_ids_.pop_back();
        return file_page_id;
    }
    return FilePageAllocator::Allocate();
}

void PooledFilePages::Free(std::vector<FilePageId> fp_ids)
{
    free_ids_.insert(free_ids_.end(), fp_ids.begin(), fp_ids.end());
}

}  // namespace eloqstore
