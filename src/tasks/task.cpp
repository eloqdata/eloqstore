#include "tasks/task.h"

#include <glog/logging.h>

#include <cassert>
#include <string>
#include <utility>

#include "compression.h"
#include "global_registered_memory.h"
#include "io_string_buffer.h"
#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"
#include "storage/shard.h"

namespace eloqstore
{
void KvTask::Yield()
{
    // Check if we're in a valid coroutine context (running_ should be set to
    // this task) If not, we're being called from external thread context (e.g.,
    // ProcessPendingRetries) and main_ might be invalid. This is a programming
    // error.
    CHECK(shard != nullptr) << "Yield() called with shard null";
    CHECK(shard->running_ == this)
        << "Yield() called outside coroutine context. running_="
        << shard->running_ << ", KvTask=" << this
        << ". Yield() must only be called from within a coroutine.";
    shard->main_ = shard->main_.resume();
}

void KvTask::YieldToLowPQ()
{
    shard->low_priority_ready_tasks_.Enqueue(this);
    shard->main_ = shard->main_.resume();
}

void KvTask::Resume()
{
    // Resume the task only if it is blocked.
    if (status_ != TaskStatus::Ongoing)
    {
        assert(status_ == TaskStatus::Blocked ||
               status_ == TaskStatus::BlockedIO ||
               Type() == TaskType::EvictFile || Type() == TaskType::Prewarm);
        status_ = TaskStatus::Ongoing;
        shard->ready_tasks_.Enqueue(this);
    }
}

int KvTask::WaitIoResult()
{
    assert(inflight_io_ > 0);
    WaitIo();
    return io_res_;
}

void KvTask::WaitIo()
{
    while (inflight_io_ > 0)
    {
        status_ = TaskStatus::BlockedIO;
        Yield();
    }
}

void KvTask::FinishIo()
{
    assert(inflight_io_ > 0);
    inflight_io_--;
    switch (status_)
    {
    case TaskStatus::BlockedIO:
        if (inflight_io_ == 0)
        {
            Resume();
        }
        break;
    default:
        break;
    }
}

std::pair<Page, KvError> LoadPage(const TableIdent &tbl_id,
                                  FilePageId file_page_id)
{
    assert(file_page_id != MaxFilePageId);
    auto [page, err] = IoMgr()->ReadPage(tbl_id, file_page_id, Page(true));
    if (__builtin_expect(err != KvError::NoError, 0))
    {
        return {Page(false), err};
    }
    return {std::move(page), KvError::NoError};
}

std::pair<DataPage, KvError> LoadDataPage(const TableIdent &tbl_id,
                                          PageId page_id,
                                          FilePageId file_page_id)
{
    auto [page, err] = LoadPage(tbl_id, file_page_id);
    if (__builtin_expect(err != KvError::NoError, 0))
    {
        return {DataPage(), err};
    }
    return {DataPage(page_id, std::move(page)), KvError::NoError};
}

std::pair<OverflowPage, KvError> LoadOverflowPage(const TableIdent &tbl_id,
                                                  PageId page_id,
                                                  FilePageId file_page_id)
{
    auto [page, err] = LoadPage(tbl_id, file_page_id);
    if (__builtin_expect(err != KvError::NoError, 0))
    {
        return {OverflowPage(), err};
    }
    return {OverflowPage(page_id, std::move(page)), KvError::NoError};
}

KvError GetOverflowValue(const TableIdent &tbl_id,
                         const MappingSnapshot *mapping,
                         std::string_view encoded_ptrs,
                         std::string &value)
{
    std::array<FilePageId, max_overflow_pointers> ids_buf;
    // Decode and convert overflow pointers (logical) to file page ids.
    auto to_file_page_ids =
        [&](std::string_view encoded_ptrs) -> std::span<FilePageId>
    {
        uint8_t n = DecodeOverflowPointers(encoded_ptrs, ids_buf, mapping);
        return {ids_buf.data(), n};
    };

    std::span<FilePageId> page_ids = to_file_page_ids(encoded_ptrs);
    std::vector<Page> pages;
    value.clear();
    value.reserve(page_ids.size() * OverflowPage::Capacity(Options(), false));
    while (!page_ids.empty())
    {
        KvError err = IoMgr()->ReadPages(tbl_id, page_ids, pages);
        if (err != KvError::NoError)
        {
            return err;
        }
        uint8_t i = 0;
        for (Page &pg : pages)
        {
            OverflowPage page(MaxPageId, std::move(pg));
            value.append(page.GetValue());
            if (++i == pages.size())
            {
                encoded_ptrs = page.GetEncodedPointers(Options());
                page_ids = to_file_page_ids(encoded_ptrs);
            }
        }
    }

    return KvError::NoError;
}

KvError GetLargeValue(const TableIdent &tbl_id,
                      const MappingSnapshot *seg_mapping,
                      std::string_view encoded_content,
                      IoStringBuffer &large_value,
                      GlobalRegisteredMemory *global_mem,
                      uint16_t reg_mem_index_base,
                      std::function<void()> yield)
{
    assert(seg_mapping != nullptr);
    assert(global_mem != nullptr);

    auto header =
        DecodeLargeValueHeader(encoded_content, Options()->segment_size);
    if (!header || header->num_segments == 0)
    {
        return KvError::Corrupted;
    }
    const uint32_t actual_length = header->actual_length;
    const uint32_t num_segments = header->num_segments;
    std::vector<PageId> segment_ids(num_segments);
    if (!DecodeLargeValueContent(encoded_content, *header, segment_ids))
    {
        return KvError::Corrupted;
    }

    // Resolve logical segment IDs to physical file segment IDs.
    std::vector<FilePageId> physical_ids(num_segments);
    for (uint32_t i = 0; i < num_segments; ++i)
    {
        physical_ids[i] = seg_mapping->ToFilePage(segment_ids[i]);
    }

    // Allocate memory segments and build IoStringBuffer.
    for (uint32_t i = 0; i < num_segments; ++i)
    {
        auto [ptr, chunk_index] = global_mem->GetSegment(yield);
        uint16_t buf_index =
            reg_mem_index_base + static_cast<uint16_t>(chunk_index);
        large_value.Append({ptr, buf_index});
    }
    large_value.SetSize(actual_length);

    // Read segments in batches.
    const auto &fragments = large_value.Fragments();
    for (uint32_t offset = 0; offset < num_segments;
         offset += max_segments_batch)
    {
        uint32_t batch_size =
            std::min(uint32_t(max_segments_batch), num_segments - offset);
        std::array<FilePageId, max_segments_batch> batch_fp_ids;
        std::array<char *, max_segments_batch> batch_ptrs;
        std::array<uint16_t, max_segments_batch> batch_buf_indices;
        for (uint32_t i = 0; i < batch_size; ++i)
        {
            batch_fp_ids[i] = physical_ids[offset + i];
            batch_ptrs[i] = fragments[offset + i].data_;
            batch_buf_indices[i] = fragments[offset + i].buf_index_;
        }
        KvError err =
            IoMgr()->ReadSegments(tbl_id,
                                  {batch_fp_ids.data(), batch_size},
                                  {batch_ptrs.data(), batch_size},
                                  {batch_buf_indices.data(), batch_size});
        if (err != KvError::NoError)
        {
            large_value.Recycle(global_mem, reg_mem_index_base);
            return err;
        }
    }
    return KvError::NoError;
}

KvError GetLargeValueContiguous(const TableIdent &tbl_id,
                                const MappingSnapshot *seg_mapping,
                                std::string_view encoded_content,
                                char *dst,
                                size_t dst_size,
                                AsyncIoManager *io_mgr)
{
    assert(seg_mapping != nullptr);
    assert(io_mgr != nullptr);
    assert(dst != nullptr);

    const uint32_t segment_size = Options()->segment_size;
    auto header = DecodeLargeValueHeader(encoded_content, segment_size);
    if (!header || header->num_segments == 0)
    {
        return KvError::Corrupted;
    }
    const uint32_t num_segments = header->num_segments;
    std::vector<PageId> segment_ids(num_segments);
    if (!DecodeLargeValueContent(encoded_content, *header, segment_ids))
    {
        return KvError::Corrupted;
    }

    // dst_size must hold all-but-last segments fully and a 4 KiB-aligned
    // tail. The tail size can be smaller than segment_size: the final
    // ReadSegments request is issued with that smaller size, so the bytes
    // past dst_size are never touched.
    const size_t prefix_bytes =
        static_cast<size_t>(num_segments - 1) * segment_size;
    if (dst_size <= prefix_bytes ||
        dst_size > static_cast<size_t>(num_segments) * segment_size)
    {
        return KvError::InvalidArgs;
    }
    const uint32_t tail_bytes = static_cast<uint32_t>(dst_size - prefix_bytes);
    if ((tail_bytes & (4096u - 1u)) != 0)
    {
        return KvError::InvalidArgs;
    }

    // Single chunk per read is guaranteed by the caller: one
    // BufIndexForAddress lookup at the base, reused for every segment.
    const uint16_t buf_index = io_mgr->BufIndexForAddress(dst);
    if (buf_index == std::numeric_limits<uint16_t>::max())
    {
        return KvError::InvalidArgs;
    }
    assert(io_mgr->BufIndexForAddress(dst + dst_size - 1) == buf_index);

    std::vector<FilePageId> physical_ids(num_segments);
    for (uint32_t i = 0; i < num_segments; ++i)
    {
        physical_ids[i] = seg_mapping->ToFilePage(segment_ids[i]);
    }

    for (uint32_t offset = 0; offset < num_segments;
         offset += max_segments_batch)
    {
        const uint32_t batch_size = std::min(
            static_cast<uint32_t>(max_segments_batch), num_segments - offset);
        std::array<FilePageId, max_segments_batch> batch_fp_ids;
        std::array<char *, max_segments_batch> batch_ptrs;
        std::array<uint16_t, max_segments_batch> batch_buf_indices;
        for (uint32_t i = 0; i < batch_size; ++i)
        {
            batch_fp_ids[i] = physical_ids[offset + i];
            batch_ptrs[i] = dst + (offset + i) * segment_size;
            batch_buf_indices[i] = buf_index;
        }
        // Only the batch that contains the final segment can ride a partial
        // tail; pass tail_size = 0 (full reads) on every other batch.
        const bool is_last_batch = (offset + batch_size == num_segments);
        const uint32_t tail_size_arg =
            (is_last_batch && tail_bytes != segment_size) ? tail_bytes : 0;
        KvError err =
            io_mgr->ReadSegments(tbl_id,
                                 {batch_fp_ids.data(), batch_size},
                                 {batch_ptrs.data(), batch_size},
                                 {batch_buf_indices.data(), batch_size},
                                 tail_size_arg);
        if (err != KvError::NoError)
        {
            return err;
        }
    }
    return KvError::NoError;
}

KvError ResolveValueOrMetadata(const TableIdent &tbl_id,
                               MappingSnapshot *mapping,
                               DataPageIter &iter,
                               std::string &value,
                               const compression::DictCompression *compression,
                               bool extract_metadata)
{
    if (iter.IsLargeValue())
    {
        // Large entry: write the metadata trailer (or clear when absent).
        // The K segment IDs are parsed but discarded -- the caller fetches
        // the value bytes via GetLargeValue / GetLargeValueContiguous.
        if (!extract_metadata)
        {
            value.clear();
            return KvError::NoError;
        }
        auto header =
            DecodeLargeValueHeader(iter.Value(), Options()->segment_size);
        if (!header || header->num_segments == 0)
        {
            return KvError::Corrupted;
        }
        if (header->metadata_length == 0)
        {
            value.clear();
            return KvError::NoError;
        }
        if (!DecodeLargeValueContent(
                iter.Value(), *header, /*segment_ids=*/{}, &value))
        {
            return KvError::Corrupted;
        }
        return KvError::NoError;
    }

    compression::CompressionType comp_type = iter.CompressionType();
    if (comp_type == compression::CompressionType::None)
    {
        if (iter.IsOverflow())
        {
            return GetOverflowValue(tbl_id, mapping, iter.Value(), value);
        }
        value.assign(iter.Value().data(), iter.Value().size());
        return KvError::NoError;
    }

    // Compressed case: obtain raw compressed bytes, then decompress into value.
    std::string_view raw_value;
    std::string overflow_storage;
    if (iter.IsOverflow())
    {
        KvError err =
            GetOverflowValue(tbl_id, mapping, iter.Value(), overflow_storage);
        if (err != KvError::NoError)
        {
            return err;
        }
        raw_value = overflow_storage;
    }
    else
    {
        raw_value = iter.Value();
    }

    value.clear();
    bool ok = (comp_type == compression::CompressionType::Dictionary)
                  ? compression->Decompress(raw_value, value)
                  : compression::DecompressRaw(raw_value, value);
    return ok ? KvError::NoError : KvError::Corrupted;
}

uint8_t DecodeOverflowPointers(
    std::string_view encoded, std::span<PageId, max_overflow_pointers> pointers)
{
    assert(encoded.size() % sizeof(PageId) == 0);
    uint8_t n_ptrs = 0;
    while (!encoded.empty())
    {
        pointers[n_ptrs] = DecodeFixed32(encoded.data());
        encoded = encoded.substr(sizeof(PageId));
        n_ptrs++;
    }
    assert(n_ptrs <= max_overflow_pointers);
    return n_ptrs;
}

uint8_t DecodeOverflowPointers(
    std::string_view encoded,
    std::span<FilePageId, max_overflow_pointers> pointers,
    const MappingSnapshot *mapping)
{
    assert(encoded.size() % sizeof(PageId) == 0);
    uint8_t n_ptrs = 0;
    while (!encoded.empty())
    {
        PageId page_id = DecodeFixed32(encoded.data());
        pointers[n_ptrs] = mapping->ToFilePage(page_id);
        encoded = encoded.substr(sizeof(PageId));
        n_ptrs++;
    }
    assert(n_ptrs <= max_overflow_pointers);
    return n_ptrs;
}

void WaitingZone::Wait(KvTask *task)
{
    PushBack(task);
    task->status_ = TaskStatus::Blocked;
    task->Yield();
}

void WaitingZone::WakeOne()
{
    if (KvTask *task = PopFront(); task != nullptr)
    {
        assert(task->status_ == TaskStatus::Blocked);
        task->Resume();
    }
}

void WaitingZone::WakeN(size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        KvTask *task = PopFront();
        if (task == nullptr)
        {
            break;  // No more tasks to wake.
        }
        assert(task->status_ == TaskStatus::Blocked);
        task->Resume();
    }
}

void WaitingZone::WakeAll()
{
    for (KvTask *task = head_; task != nullptr; task = task->next_)
    {
        assert(task->status_ == TaskStatus::Blocked);
        task->Resume();
    }
    head_ = tail_ = nullptr;  // Clear the waiting zone.
    // Note: WakeAll did not clear the next_ pointers of the tasks.
}

bool WaitingZone::Empty() const
{
    return head_ == nullptr;
}

void WaitingZone::PushBack(KvTask *task)
{
    task->next_ = nullptr;
    if (tail_ == nullptr)
    {
        assert(head_ == nullptr);
        head_ = tail_ = task;
    }
    else
    {
        assert(head_ != nullptr);
        tail_->next_ = task;
        tail_ = task;
    }
}

KvTask *WaitingZone::PopFront()
{
    KvTask *task = head_;
    if (task != nullptr)
    {
        head_ = task->next_;
        if (head_ == nullptr)
        {
            tail_ = nullptr;
        }
        task->next_ = nullptr;  // Clear next pointer for safety.
    }
    return task;
}

void WaitingSeat::Wait(KvTask *task)
{
    assert(task != nullptr && task_ == nullptr);
    task_ = task;
    task->status_ = TaskStatus::Blocked;
    task->Yield();
}

void WaitingSeat::Wake()
{
    if (task_ != nullptr)
    {
        task_->Resume();
        task_ = nullptr;
    }
}

void Mutex::Lock()
{
    while (locked_)
    {
        waiting_.Wait(ThdTask());
    }
    locked_ = true;
}

void Mutex::Unlock()
{
    locked_ = false;
    waiting_.WakeOne();
}

KvTask *ThdTask()
{
    return shard->running_;
}

AsyncIoManager *IoMgr()
{
    return shard->IndexManager()->IoMgr();
}

const KvOptions *Options()
{
    return &eloq_store->Options();
}

const Comparator *Comp()
{
    return Options()->comparator_;
}
}  // namespace eloqstore
