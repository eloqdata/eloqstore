#include "storage/page_mapper.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "coding.h"
#include "manifest_buffer.h"
#include "storage/index_page_manager.h"
#include "storage/shard.h"
#include "tasks/task.h"

namespace eloqstore
{
MappingSnapshot::MappingSnapshot(IndexPageManager *idx_mgr,
                                 const TableIdent *tbl_id,
                                 MappingTbl tbl)
    : idx_mgr_(idx_mgr), tbl_ident_(tbl_id), mapping_tbl_(std::move(tbl))
{
}

MappingSnapshot::MappingTbl::MappingTbl(std::vector<uint64_t> tbl)
    : base_(std::move(tbl)), logical_size_(base_.size())
{
}

void MappingSnapshot::MappingTbl::clear()
{
    base_.clear();
    changes_.clear();
    logical_size_ = 0;
    under_copying_ = false;
}

void MappingSnapshot::MappingTbl::reserve(size_t n)
{
    if (!under_copying_)
    {
        base_.reserve(n);
    }
}

size_t MappingSnapshot::MappingTbl::size() const
{
    if (__builtin_expect(!under_copying_, 1))
        return base_.size();
    return logical_size_;
}

size_t MappingSnapshot::MappingTbl::capacity() const
{
    return base_.capacity();
}

void MappingSnapshot::MappingTbl::StartCopying()
{
    under_copying_ = true;
    size_t base_size = base_.size();
    if (base_size > logical_size_)
    {
        logical_size_ = base_size;
    }
}

void MappingSnapshot::MappingTbl::FinishCopying()
{
    ApplyChanges();
    changes_.clear();
    under_copying_ = false;
}

void MappingSnapshot::MappingTbl::ApplyChanges()
{
    if (changes_.empty())
    {
        logical_size_ = base_.size();
        return;
    }
    if (logical_size_ > base_.size())
    {
        base_.resize(logical_size_, InvalidValue);
    }
    for (const auto &entry : changes_)
    {
        base_[entry.first] = entry.second;
    }
    logical_size_ = base_.size();
}

void MappingSnapshot::MappingTbl::Set(PageId page_id, uint64_t value)
{
    size_t next_size = static_cast<size_t>(page_id) + 1;
    if (logical_size_ < next_size)
    {
        logical_size_ = next_size;
    }
    if (__builtin_expect(under_copying_, 0))
    {
        changes_[page_id] = value;
        return;
    }
    EnsureSize(page_id);
    base_[page_id] = value;
    logical_size_ = base_.size();
}

PageId MappingSnapshot::MappingTbl::PushBack(uint64_t value)
{
    PageId page_id = static_cast<PageId>(logical_size_);
    Set(page_id, value);
    return page_id;
}

uint64_t MappingSnapshot::MappingTbl::Get(PageId page_id) const
{
    if (__builtin_expect(under_copying_, 0))
    {
        auto it = changes_.find(page_id);
        if (it != changes_.end())
        {
            return it->second;
        }
    }
    CHECK(page_id < base_.size());
    return base_[page_id];
}

std::vector<uint64_t> &MappingSnapshot::MappingTbl::Base()
{
    return base_;
}

const std::vector<uint64_t> &MappingSnapshot::MappingTbl::Base() const
{
    return base_;
}

void MappingSnapshot::MappingTbl::ApplyPendingTo(MappingTbl &dst) const
{
    CHECK(logical_size_ >= dst.base_.size() &&
          logical_size_ >= dst.logical_size_);
    if (logical_size_ > dst.base_.size())
    {
        dst.base_.resize(logical_size_, MappingSnapshot::InvalidValue);
    }
    if (logical_size_ > dst.logical_size_)
    {
        dst.logical_size_ = logical_size_;
    }
    for (const auto &entry : changes_)
    {
        PageId page_id = entry.first;
        dst.base_[page_id] = entry.second;
    }
}

void MappingSnapshot::MappingTbl::EnsureSize(PageId page_id)
{
    if (page_id >= base_.size())
    {
        // TODO(chenzhao): change mapping to std::vector<std::array>
        base_.resize(page_id + 1, InvalidValue);
    }
}

PageMapper::PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident)
{
    auto *arena = idx_mgr->MapperArena();
    MappingSnapshot::MappingTbl tbl =
        arena == nullptr ? MappingSnapshot::MappingTbl() : arena->Acquire();
    mapping_ =
        std::make_shared<MappingSnapshot>(idx_mgr, tbl_ident, std::move(tbl));

    auto &mapping_tbl = mapping_->mapping_tbl_;
    mapping_tbl.reserve(idx_mgr->Options()->init_page_count);
    file_page_allocator_ = FilePageAllocator::Instance(idx_mgr->Options());
}

PageMapper::PageMapper(const PageMapper &rhs)
    : free_page_head_(rhs.free_page_head_),
      free_page_cnt_(rhs.free_page_cnt_),
      file_page_allocator_(rhs.file_page_allocator_->Clone())
{
    auto *arena = shard->IndexManager()->MapperArena();
    MappingSnapshot::MappingTbl tbl =
        arena == nullptr ? MappingSnapshot::MappingTbl() : arena->Acquire();
    mapping_ = std::make_shared<MappingSnapshot>(
        rhs.mapping_->idx_mgr_, rhs.mapping_->tbl_ident_, std::move(tbl));

    auto &src_tbl = rhs.mapping_->mapping_tbl_;
    src_tbl.StartCopying();
    const auto &src = src_tbl.Base();
    auto &dst = mapping_->mapping_tbl_.Base();
    dst.clear();
    dst.reserve(src.size());

    static constexpr size_t kCopyBatchSize = 512;
    for (size_t i = 0; i < src.size(); i += kCopyBatchSize)
    {
        size_t batch_size = std::min(kCopyBatchSize, src.size() - i);
        dst.insert(dst.end(), src.begin() + i, src.begin() + i + batch_size);
        ThdTask()->YieldToLowPQ();
    }

    src_tbl.ApplyPendingTo(mapping_->mapping_tbl_);
    src_tbl.FinishCopying();

    assert(file_page_allocator_->MaxFilePageId() ==
           rhs.file_page_allocator_->MaxFilePageId());
}

PageId PageMapper::GetPage()
{
    auto &map = Mapping();
    if (free_page_head_ == MaxPageId)
    {
        return map.PushBack(MappingSnapshot::InvalidValue);
    }
    else
    {
        PageId free_page = free_page_head_;
        // The free page head points to the next free page.
        free_page_head_ = mapping_->GetNextFree(free_page);
        // Sets the free page's mapped file page to null.
        map.Set(free_page, MappingSnapshot::InvalidValue);
        free_page_cnt_--;
        return free_page;
    }
}

void PageMapper::FreePage(PageId page_id)
{
    auto &map = Mapping();
    assert(page_id < map.size());
    uint64_t val = free_page_head_ == MaxPageId
                       ? MappingSnapshot::InvalidValue
                       : MappingSnapshot::EncodePageId(free_page_head_);
    map.Set(page_id, val);
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

uint32_t PageMapper::UseCount() const
{
    return mapping_.use_count();
}

const KvOptions *PageMapper::Options() const
{
    return mapping_->idx_mgr_->Options();
}

MappingSnapshot::MappingTbl &PageMapper::Mapping()
{
    return mapping_->mapping_tbl_;
}

#ifndef NDEBUG
bool PageMapper::DebugStat() const
{
    FilePageId min_fp_id = MaxFilePageId;
    uint32_t cnt = 0;
    const auto &tbl = mapping_->mapping_tbl_;
    for (PageId page_id = 0; page_id < tbl.size(); ++page_id)
    {
        FilePageId fp_id = mapping_->ToFilePage(tbl.Get(page_id));
        if (fp_id != MaxFilePageId)
        {
            cnt++;
            min_fp_id = std::min(min_fp_id, fp_id);
        }
    }
    assert(cnt == tbl.size() - free_page_cnt_);
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
    auto &map = Mapping();
    assert(page_id < map.size());
    uint64_t val = MappingSnapshot::EncodeFilePageId(file_page_id);
    map.Set(page_id, val);
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

MappingSnapshot::~MappingSnapshot()
{
    IndexPageManager *mgr = idx_mgr_;
    if (mgr == nullptr)
    {
        return;
    }

    mgr->FreeMappingSnapshot(this);

    mgr->MapperArena()->Release(std::move(mapping_tbl_));
}

FilePageId MappingSnapshot::ToFilePage(PageId page_id) const
{
    if (page_id == MaxPageId)
    {
        return MaxFilePageId;
    }
    assert(page_id < mapping_tbl_.size());
    return ToFilePage(mapping_tbl_.Get(page_id));
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
    uint64_t val = mapping_tbl_.Get(page_id);
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

    auto &mapping_tbl = mapping_tbl_;
    if (page_id < mapping_tbl.size())
    {
        uint64_t val = mapping_tbl.Get(page_id);
        if (IsSwizzlingPointer(val) &&
            reinterpret_cast<MemIndexPage *>(val) == page)
        {
            mapping_tbl.Set(page_id, EncodeFilePageId(file_page_id));
        }
    }
}

MemIndexPage *MappingSnapshot::GetSwizzlingPointer(PageId page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_.Get(page_id);
    if (IsSwizzlingPointer(val))
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return idx_page;
    }
    return nullptr;
}

void MappingSnapshot::AddSwizzling(PageId page_id, MemIndexPage *idx_page)
{
    auto &mapping_tbl = mapping_tbl_;
    assert(page_id < mapping_tbl.size());

    uint64_t val = mapping_tbl.Get(page_id);
    if (IsSwizzlingPointer(val))
    {
        assert(reinterpret_cast<MemIndexPage *>(val) == idx_page);
    }
    else
    {
        assert(DecodeId(val) == idx_page->GetFilePageId());
        mapping_tbl.Set(page_id, reinterpret_cast<uint64_t>(idx_page));
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

void MappingSnapshot::Serialize(ManifestBuffer &dst) const
{
    const size_t tbl_size = mapping_tbl_.size();
    const bool can_yield = shard != nullptr;
    for (PageId i = 0; i < tbl_size; i++)
    {
        uint64_t val = mapping_tbl_.Get(i);
        if (IsSwizzlingPointer(val))
        {
            MemIndexPage *p = reinterpret_cast<MemIndexPage *>(val);
            val = EncodeFilePageId(p->GetFilePageId());
        }
        dst.AppendVarint64(val);
        if (can_yield && (i & 511) == 0)
        {
            ThdTask()->YieldToLowPQ();
        }
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
