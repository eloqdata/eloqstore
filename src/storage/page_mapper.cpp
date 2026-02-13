#include "storage/page_mapper.h"

#include <butil/time.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "coding.h"
#include "manifest_buffer.h"
#include "storage/index_page_manager.h"
#include "storage/mem_index_page.h"
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

MappingSnapshot::MappingTbl::MappingTbl() = default;

MappingSnapshot::MappingTbl::MappingTbl(MappingArena *vector_arena,
                                        MappingChunkArena *chunk_arena)
    : vector_arena_(vector_arena), chunk_arena_(chunk_arena)
{
    if (vector_arena_ != nullptr)
    {
        base_ = vector_arena_->Acquire();
    }
}

MappingSnapshot::MappingTbl::~MappingTbl()
{
    if (vector_arena_ != nullptr)
    {
        CHECK(chunk_arena_ != nullptr);
        clear();
        vector_arena_->Release(std::move(base_));
        return;
    }
    CHECK(chunk_arena_ == nullptr);
}

void MappingSnapshot::MappingTbl::clear()
{
    for (auto &chunk : base_)
    {
        ReleaseChunk(std::move(chunk));
    }
    base_.clear();
    changes_.clear();
    logical_size_ = 0;
    under_copying_ = false;
}

void MappingSnapshot::MappingTbl::reserve(size_t n)
{
    if (!under_copying_)
    {
        base_.reserve(RequiredChunks(n));
    }
}

size_t MappingSnapshot::MappingTbl::size() const
{
    return logical_size_;
}

size_t MappingSnapshot::MappingTbl::capacity() const
{
    return base_.size() * kChunkSize;
}

void MappingSnapshot::MappingTbl::StartCopying()
{
    under_copying_ = true;
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
        return;
    }
    for (const auto &entry : changes_)
    {
        const PageId page_id = entry.first;
        if (logical_size_ < static_cast<size_t>(page_id) + 1)
        {
            ResizeInternal(static_cast<size_t>(page_id) + 1);
        }
        const size_t chunk_idx = static_cast<size_t>(page_id) >> kChunkShift;
        const size_t chunk_offset = static_cast<size_t>(page_id) & kChunkMask;
        (*base_[chunk_idx])[chunk_offset] = entry.second;
    }
}

void MappingSnapshot::MappingTbl::Set(PageId page_id, uint64_t value)
{
    const size_t next_size = static_cast<size_t>(page_id) + 1;
    if (under_copying_)
    {
        changes_[page_id] = value;
        if (logical_size_ < next_size)
        {
            logical_size_ = next_size;
        }
        return;
    }
    EnsureSize(page_id);
    const size_t chunk_idx = static_cast<size_t>(page_id) >> kChunkShift;
    const size_t chunk_offset = static_cast<size_t>(page_id) & kChunkMask;
    (*base_[chunk_idx])[chunk_offset] = value;
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
    CHECK(static_cast<size_t>(page_id) < logical_size_)
        << "page_id=" << page_id << ", logical_size=" << logical_size_;
    const size_t chunk_idx = static_cast<size_t>(page_id) >> kChunkShift;
    const size_t chunk_offset = static_cast<size_t>(page_id) & kChunkMask;
    return (*base_[chunk_idx])[chunk_offset];
}

void MappingSnapshot::MappingTbl::CopyFrom(const MappingTbl &src)
{
    if (this == &src)
    {
        return;
    }
    if (src.logical_size_ == 0)
    {
        clear();
        return;
    }
    ResizeInternal(src.logical_size_, /*init_new_chunks=*/false);
    ThdTask()->YieldToLowPQ();
    for (size_t chunk_idx = 0; chunk_idx < base_.size(); ++chunk_idx)
    {
        size_t offset = chunk_idx << kChunkShift;
        if (offset >= src.logical_size_)
        {
            break;
        }
        const size_t copy_elems =
            std::min(kChunkSize, src.logical_size_ - offset);
        std::memcpy(base_[chunk_idx]->data(),
                    src.base_[chunk_idx]->data(),
                    copy_elems * sizeof(uint64_t));
        ThdTask()->YieldToLowPQ();
    }
}

void MappingSnapshot::MappingTbl::ApplyPendingTo(MappingTbl &dst) const
{
    CHECK(logical_size_ >= dst.logical_size_);
    if (logical_size_ > dst.logical_size_)
    {
        dst.ResizeInternal(logical_size_);
    }
    for (const auto &entry : changes_)
    {
        dst.Set(entry.first, entry.second);
    }
}

bool MappingSnapshot::MappingTbl::operator==(const MappingTbl &rhs) const
{
    if (logical_size_ != rhs.logical_size_)
    {
        return false;
    }
    for (PageId page_id = 0; page_id < logical_size_; ++page_id)
    {
        if (Get(page_id) != rhs.Get(page_id))
        {
            return false;
        }
    }
    return true;
}

void MappingSnapshot::MappingTbl::EnsureSize(PageId page_id)
{
    if (static_cast<size_t>(page_id) < logical_size_)
    {
        return;
    }

    const size_t new_size = static_cast<size_t>(page_id) + 1;
    ResizeInternal(new_size);
}

inline size_t MappingSnapshot::MappingTbl::RequiredChunks(size_t n)
{
    return (n + kChunkSize - 1) >> kChunkShift;
}

void MappingSnapshot::MappingTbl::EnsureChunkCount(size_t count)
{
    if (base_.size() >= count)
    {
        return;
    }
    const size_t old_size = base_.size();
    base_.reserve(count);
    for (size_t i = old_size; i < count; ++i)
    {
        auto chunk = AcquireChunk();
        base_.push_back(std::move(chunk));
    }
}

void MappingSnapshot::MappingTbl::ResizeInternal(size_t new_size,
                                                 bool init_new_chunks)
{
    // must not yield within this method to avoid new_size is changed.
    if (new_size == logical_size_)
    {
        return;
    }
    const size_t required_chunks = RequiredChunks(new_size);
    const size_t current_chunks = base_.size();
    if (new_size < logical_size_)
    {
        if (required_chunks < current_chunks)
        {
            for (size_t idx = required_chunks; idx < current_chunks; ++idx)
            {
                ReleaseChunk(std::move(base_[idx]));
            }
            base_.resize(required_chunks);
        }
        logical_size_ = new_size;
        return;
    }

    EnsureChunkCount(required_chunks);
    if (init_new_chunks)
    {
        for (size_t i = current_chunks; i < required_chunks; ++i)
        {
            base_[i]->fill(InvalidValue);
        }
    }

    logical_size_ = new_size;
}

std::unique_ptr<MappingSnapshot::MappingTbl::Chunk>
MappingSnapshot::MappingTbl::AcquireChunk()
{
    if (chunk_arena_ != nullptr)
    {
        return chunk_arena_->Acquire();
    }
    auto chunk = std::make_unique<Chunk>();
    return chunk;
}

void MappingSnapshot::MappingTbl::ReleaseChunk(std::unique_ptr<Chunk> chunk)
{
    if (chunk_arena_ != nullptr)
    {
        chunk_arena_->Release(std::move(chunk));
    }
}

PageMapper::PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident)
{
    MappingArena *vector_arena = idx_mgr->MapperArena();
    MappingChunkArena *chunk_arena = idx_mgr->MapperChunkArena();
    MappingSnapshot::MappingTbl tbl(vector_arena, chunk_arena);
    mapping_ = MappingSnapshot::Ref(
        new MappingSnapshot(idx_mgr, tbl_ident, std::move(tbl)));

    auto &mapping_tbl = mapping_->mapping_tbl_;
    mapping_tbl.reserve(idx_mgr->Options()->init_page_count);
    file_page_allocator_ = FilePageAllocator::Instance(idx_mgr->Options());
}

PageMapper::PageMapper(const PageMapper &rhs)
    : free_page_head_(rhs.free_page_head_),
      free_page_cnt_(rhs.free_page_cnt_),
      file_page_allocator_(rhs.file_page_allocator_->Clone())
{
    MappingArena *vector_arena =
        shard != nullptr ? shard->IndexManager()->MapperArena() : nullptr;
    MappingChunkArena *chunk_arena =
        shard != nullptr ? shard->IndexManager()->MapperChunkArena() : nullptr;
    MappingSnapshot::MappingTbl tbl(vector_arena, chunk_arena);
    mapping_ = MappingSnapshot::Ref(new MappingSnapshot(
        rhs.mapping_->idx_mgr_, rhs.mapping_->tbl_ident_, std::move(tbl)));

    auto &src_tbl = rhs.mapping_->mapping_tbl_;
    src_tbl.StartCopying();
    mapping_->mapping_tbl_.CopyFrom(src_tbl);

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

MappingSnapshot::Ref PageMapper::GetMappingSnapshot() const
{
    return mapping_;
}

MappingSnapshot *PageMapper::GetMapping() const
{
    return mapping_.Get();
}

uint32_t PageMapper::UseCount() const
{
    CHECK(mapping_ != nullptr);
    return mapping_->RefCount();
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

MemIndexPage::Handle MappingSnapshot::GetSwizzlingHandle(PageId page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_.Get(page_id);
    if (IsSwizzlingPointer(val))
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return MemIndexPage::Handle(idx_page);
    }
    return MemIndexPage::Handle();
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
    FilePageId file_page_id = max_fp_id_++;
    return file_page_id;
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

MappingSnapshot::Ref::Ref(MappingSnapshot *mapping) : mapping_(mapping)
{
    if (mapping_ != nullptr)
    {
        mapping_->AddRef();
    }
}

MappingSnapshot::Ref::Ref(const Ref &other) : mapping_(other.mapping_)
{
    if (mapping_ != nullptr)
    {
        mapping_->AddRef();
    }
}

MappingSnapshot::Ref::Ref(Ref &&other) noexcept : mapping_(other.mapping_)
{
    other.mapping_ = nullptr;
}

MappingSnapshot::Ref &MappingSnapshot::Ref::operator=(const Ref &other)
{
    if (this != &other)
    {
        Clear();
        mapping_ = other.mapping_;
        if (mapping_ != nullptr)
        {
            mapping_->AddRef();
        }
    }
    return *this;
}

MappingSnapshot::Ref &MappingSnapshot::Ref::operator=(Ref &&other) noexcept
{
    if (this != &other)
    {
        Clear();
        mapping_ = other.mapping_;
        other.mapping_ = nullptr;
    }
    return *this;
}

MappingSnapshot::Ref::~Ref()
{
    Clear();
}

MappingSnapshot *MappingSnapshot::Ref::Get() const
{
    return mapping_;
}

MappingSnapshot *MappingSnapshot::Ref::operator->() const
{
    return mapping_;
}

void MappingSnapshot::Ref::Clear()
{
    if (mapping_ != nullptr)
    {
        mapping_->Release();
        mapping_ = nullptr;
    }
}

void MappingSnapshot::AddRef()
{
    ++ref_cnt_;
}

void MappingSnapshot::Release()
{
    CHECK(ref_cnt_ > 0);
    if (--ref_cnt_ == 0)
    {
        delete this;
    }
}

size_t MappingSnapshot::RefCount() const
{
    return ref_cnt_;
}

}  // namespace eloqstore
