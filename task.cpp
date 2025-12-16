#include "task.h"

#include <glog/logging.h>

#include <cassert>
#include <string>
#include <utility>

#include "compression.h"
#include "index_page_manager.h"
#include "shard.h"

namespace eloqstore
{
void KvTask::Yield()
{
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
    if (err != KvError::NoError)
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
    if (err != KvError::NoError)
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
    if (err != KvError::NoError)
    {
        return {OverflowPage(), err};
    }
    return {OverflowPage(page_id, std::move(page)), KvError::NoError};
}

std::pair<std::string, KvError> GetOverflowValue(const TableIdent &tbl_id,
                                                 const MappingSnapshot *mapping,
                                                 std::string_view encoded_ptrs)
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
    std::string value;
    value.reserve(page_ids.size() * OverflowPage::Capacity(Options(), false));
    while (!page_ids.empty())
    {
        KvError err = IoMgr()->ReadPages(tbl_id, page_ids, pages);
        if (err != KvError::NoError)
        {
            return {{}, err};
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

    return {std::move(value), KvError::NoError};
}

std::pair<std::string_view, KvError> ResolveValue(
    const TableIdent &tbl_id,
    MappingSnapshot *mapping,
    DataPageIter &iter,
    std::string &storage,
    const compression::DictCompression *compression)
{
    storage.clear();
    std::string_view raw_value;

    if (iter.IsOverflow())
    {
        auto ret = GetOverflowValue(tbl_id, mapping, iter.Value());
        if (ret.second != KvError::NoError)
        {
            return {{}, ret.second};
        }
        storage = std::move(ret.first);
        raw_value = storage;
    }
    else
    {
        raw_value = iter.Value();
    }

    if (iter.CompressionType() == compression::CompressionType::None)
    {
        return {raw_value, KvError::NoError};
    }
    else if (iter.CompressionType() == compression::CompressionType::Dictionary)
    {
        std::string uncompressed_value;
        if (!compression->Decompress(raw_value, uncompressed_value))
        {
            return {{}, KvError::Corrupted};
        }
        storage = std::move(uncompressed_value);
        return {storage, KvError::NoError};
    }
    else
    {
        std::string uncompressed_value;
        if (!compression::DecompressRaw(raw_value, uncompressed_value))
        {
            return {{}, KvError::Corrupted};
        }
        storage = std::move(uncompressed_value);
        return {storage, KvError::NoError};
    }
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