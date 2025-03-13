#include "index_page_manager.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <utility>

#include "async_io_manager.h"
#include "error.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "replayer.h"
#include "root_meta.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
IndexPageManager::IndexPageManager(AsyncIoManager *io_manager)
    : read_reqs_(io_manager->options_->index_page_read_queue),
      io_manager_(io_manager)
{
    active_head_.EnqueNext(&active_tail_);

    for (auto &req : read_reqs_)
    {
        RecycleReadReq(&req);
    }
}

IndexPageManager::~IndexPageManager()
{
    for (auto &[tbl, meta] : tbl_roots_)
    {
        // Destructs page mapper first, because destructing the mapping snapshot
        // needs to access the root table in the index page manager.
        if (meta.mapper_)
        {
            meta.mapper_->FreeMappingSnapshot();
        }
    }
}

const Comparator *IndexPageManager::GetComparator() const
{
    return io_manager_->options_->comparator_;
}

MemIndexPage *IndexPageManager::AllocIndexPage()
{
    MemIndexPage *next_free = free_head_.DequeNext();
    while (next_free == nullptr)
    {
        if (!IsFull())
        {
            auto &new_page = index_pages_.emplace_back(
                std::make_unique<MemIndexPage>(Options()->data_page_size));
            next_free = new_page.get();
        }
        else
        {
            bool success = Evict();
            if (!success)
            {
                // There is no page to evict because all pages are pinned.
                // Tasks trying to allocate new pages should rollback to unpin
                // pages in the task's traversal stack.
                return nullptr;
            }
            next_free = free_head_.DequeNext();
        }
    }
    assert(next_free->IsDetached());

    return next_free;
}

void IndexPageManager::FreeIndexPage(MemIndexPage *page)
{
    assert(page->IsDetached());
    assert(!page->IsPinned());
    free_head_.EnqueNext(page);
}

void IndexPageManager::EnqueuIndexPage(MemIndexPage *page)
{
    if (page->prev_ != nullptr)
    {
        assert(page->next_ != nullptr);
        page->Deque();
    }
    assert(page->prev_ == nullptr && page->next_ == nullptr);
    active_head_.EnqueNext(page);
}

bool IndexPageManager::IsFull() const
{
    return index_pages_.size() >= Options()->index_buffer_pool_size;
}

std::pair<RootMeta *, KvError> IndexPageManager::FindRoot(
    const TableIdent &tbl_ident)
{
    auto it = tbl_roots_.find(tbl_ident);
    while (it == tbl_roots_.end())
    {
        KvError err = LoadTablePartition(tbl_ident);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        it = tbl_roots_.find(tbl_ident);
    }

    RootMeta *meta = &it->second;
    if (meta->root_page_)
    {
        EnqueuIndexPage(meta->root_page_);
    }
    return {meta, KvError::NoError};
}

KvError IndexPageManager::MakeCowRoot(const TableIdent &tbl_ident,
                                      CowRootMeta &cow_meta)
{
    auto [meta, err] = FindRoot(tbl_ident);
    if (err == KvError::NoError)
    {
        // Makes a copy of the mapper.
        std::unique_ptr<PageMapper> new_mapper =
            std::make_unique<PageMapper>(*meta->mapper_);
        cow_meta.root_ = meta->root_page_;
        cow_meta.mapper_ = std::move(new_mapper);
        cow_meta.old_mapping_ = meta->mapper_->GetMappingSnapshot();
        cow_meta.manifest_size_ = meta->manifest_size_;
        return KvError::NoError;
    }
    else if (err == KvError::NotFound)
    {
        // TODO(zhanghao): This will leave a empty RootMeta entry if WriteTask
        // do not call UpdateRoot
        auto [tbl_it, _] = tbl_roots_.try_emplace(tbl_ident);
        const TableIdent *tbl_id = &tbl_it->first;
        std::unique_ptr<PageMapper> mapper =
            std::make_unique<PageMapper>(this, tbl_id);
        std::shared_ptr<MappingSnapshot> mapping = mapper->GetMappingSnapshot();
        cow_meta.root_ = nullptr;
        cow_meta.mapper_ = std::move(mapper);
        cow_meta.old_mapping_ = std::move(mapping);
        cow_meta.manifest_size_ = 0;
        return KvError::NoError;
    }
    else
    {
        return err;
    }
}

void IndexPageManager::UpdateRoot(const TableIdent &tbl_ident,
                                  MemIndexPage *new_root,
                                  std::unique_ptr<PageMapper> new_mapper,
                                  uint64_t manifest_size)
{
    auto tbl_it = tbl_roots_.find(tbl_ident);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    meta.root_page_ = new_root;
    meta.mapper_ = std::move(new_mapper);
    meta.mapping_snapshots_.insert(meta.mapper_->GetMapping());
    meta.manifest_size_ = manifest_size;
}

KvError IndexPageManager::LoadTablePartition(const TableIdent &tbl_id)
{
    auto [it_loading, success] = tbl_loading_.try_emplace(tbl_id);
    auto &loading = it_loading->second;
    if (!success)
    {
        loading.emplace_back(thd_task);
        thd_task->status_ = TaskStatus::Blocked;
        thd_task->Yield();
        return KvError::NoError;
    }

    auto load_func = [this](const TableIdent &tbl_id)
    {
        // load manifest file
        ManifestFilePtr manifest = IoMgr()->GetManifest(tbl_id);
        if (manifest == nullptr)
        {
            return KvError::NotFound;
        }

        // replay
        Replayer replayer;
        KvError err = replayer.Replay(std::move(manifest), Options());
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "load evicted table: replay failed";
            return err;
        }

        // load root page
        uint32_t root_id = replayer.root_;
        uint32_t root_fp_id;
        MemIndexPage *root_page = nullptr;
        if (root_id != UINT32_MAX)
        {
            root_page = AllocIndexPage();
            if (root_page == nullptr)
            {
                return KvError::OutOfMem;
            }
            root_fp_id = replayer.mapper_->GetMapping()->ToFilePage(root_id);
            auto [page, err] = IoMgr()->ReadPage(
                tbl_id, root_fp_id, std::move(root_page->page_));
            root_page->page_ = std::move(page);
            if (err != KvError::NoError)
            {
                FreeIndexPage(root_page);
                return err;
            }
            root_page->SetPageId(root_id);
            root_page->SetFilePageId(root_fp_id);
        }

        auto [it, b] = tbl_roots_.try_emplace(tbl_id);
        assert(b);
        RootMeta &meta = it->second;
        auto mapper = replayer.Mapper(this, &it->first);
        MappingSnapshot *mapping = mapper->GetMapping();
        meta.mapper_ = std::move(mapper);
        meta.mapping_snapshots_.insert(mapping);
        meta.manifest_size_ = replayer.file_size_;
        if (root_page)
        {
            meta.root_page_ = root_page;
            FinishIo(mapping, root_page);
        }
        return KvError::NoError;
    };

    KvError err = load_func(tbl_id);
    for (KvTask *task : loading)
    {
        task->Resume();
    }
    tbl_loading_.erase(tbl_id);
    return err;
}

std::pair<MemIndexPage *, KvError> IndexPageManager::FindPage(
    MappingSnapshot *mapping, uint32_t page_id)
{
    // First checks swizzling pointers.
    MemIndexPage *idx_page = mapping->GetSwizzlingPointer(page_id);
    while (idx_page == nullptr)
    {
        auto it = loading_zone_.find(page_id);
        if (it != loading_zone_.end())
        {
            // There is already a read request issuing an async read on the same
            // page. Waits for the request in the waiting queue.
            it->second->pending_tasks_.emplace_back(thd_task);
            thd_task->status_ = TaskStatus::Blocked;
            thd_task->Yield();
            // When resumed, the read request should have loaded the page,
            // unless the page is evicted again or corrupted.
            idx_page = mapping->GetSwizzlingPointer(page_id);
        }
        else
        {
            // This is the first request to load the page.
            ReadReq *read_req = GetFreeReadReq();
            assert(read_req != nullptr);
            auto it = loading_zone_.try_emplace(page_id, read_req);
            assert(it.second);
            ReadReq *req = it.first->second;

            MemIndexPage *new_page = AllocIndexPage();
            if (new_page == nullptr)
            {
                for (auto &task : req->pending_tasks_)
                {
                    // This task is about to rollback. Resumes other tasks
                    // waiting for it. Note: Resume() re-schedules the task to
                    // run, but does not run in-place.
                    task->Resume();
                }
                req->pending_tasks_.clear();
                RecycleReadReq(req);
                loading_zone_.erase(it.first);
                return {nullptr, KvError::OutOfMem};
            }

            // Read the page async.
            uint32_t file_page_id = mapping->ToFilePage(page_id);
            auto [page, err] = IoMgr()->ReadPage(
                *mapping->tbl_ident_, file_page_id, std::move(new_page->page_));
            new_page->page_ = std::move(page);
            if (err != KvError::NoError)
            {
                FreeIndexPage(new_page);
                return {nullptr, err};
            }
            new_page->SetPageId(page_id);
            new_page->SetFilePageId(file_page_id);
            FinishIo(mapping, new_page);

            for (auto &task : read_req->pending_tasks_)
            {
                task->Resume();
            }

            loading_zone_.erase(it.first);
            read_req->pending_tasks_.clear();
            RecycleReadReq(read_req);

            return {new_page, KvError::NoError};
        }
    }
    EnqueuIndexPage(idx_page);
    return {idx_page, KvError::NoError};
}

void IndexPageManager::FreeMappingSnapshot(MappingSnapshot *mapping)
{
    const TableIdent &tbl = *mapping->tbl_ident_;
    auto tbl_it = tbl_roots_.find(tbl);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    // Puts back file pages freed in this mapping snapshot
    assert(meta.mapper_ != nullptr);
    meta.mapper_->FreeFilePages(std::move(mapping->to_free_file_pages_));
    meta.mapping_snapshots_.erase(mapping);
}

void IndexPageManager::Unswizzling(MemIndexPage *page)
{
    auto tbl_it = tbl_roots_.find(*page->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());

    auto &mappings = tbl_it->second.mapping_snapshots_;
    for (auto &mapping : mappings)
    {
        mapping->Unswizzling(page);
    }
}

bool IndexPageManager::Evict()
{
    MemIndexPage *node = &active_tail_;

    do
    {
        while (node->prev_->IsPinned() && node->prev_ != &active_head_)
        {
            node = node->prev_;
        }

        // Has reached the head of the active list. Eviction failed.
        if (node->prev_ == &active_head_)
        {
            return false;
        }

        node = node->prev_;
        RecyclePage(node);
    } while (free_head_.next_ == nullptr);

    return true;
}

void IndexPageManager::CleanStubRoot(const TableIdent &tbl_id)
{
    auto it = tbl_roots_.find(tbl_id);
    if (it != tbl_roots_.end())
    {
        EvictRootIfEmpty(it);
    }
}

void IndexPageManager::EvictRootIfEmpty(
    std::unordered_map<TableIdent, RootMeta>::iterator root_it)
{
    RootMeta &meta = root_it->second;
    if (meta.root_page_ == nullptr && meta.mapping_snapshots_.empty() &&
        meta.ref_cnt_ == 0)
    {
        tbl_roots_.erase(root_it);
    }
}

bool IndexPageManager::RecyclePage(MemIndexPage *page)
{
    assert(!page->IsPinned());
    auto tbl_it = tbl_roots_.find(*page->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    if (meta.root_page_ == page)
    {
        if (!meta.Evict())
        {
            return false;
        }
    }
    else
    {
        assert(meta.ref_cnt_ > 0);
        --meta.ref_cnt_;
        // Unswizzling the page pointer in all mapping snapshots.
        auto &mappings = meta.mapping_snapshots_;
        for (auto &mapping : mappings)
        {
            mapping->Unswizzling(page);
        }
    }
    EvictRootIfEmpty(tbl_it);

    // Removes the page from the active list.
    page->Deque();
    assert(page->page_id_ != UINT32_MAX);
    assert(page->file_page_id_ != UINT32_MAX);
    page->page_id_ = UINT32_MAX;
    page->file_page_id_ = UINT32_MAX;
    page->tbl_ident_ = nullptr;

    FreeIndexPage(page);
    return true;
}

void IndexPageManager::FinishIo(MappingSnapshot *mapping,
                                MemIndexPage *idx_page)
{
    idx_page->tbl_ident_ = mapping->tbl_ident_;
    mapping->AddSwizzling(idx_page->PageId(), idx_page);

    auto tbl_it = tbl_roots_.find(*mapping->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());
    ++tbl_it->second.ref_cnt_;

    assert(idx_page->IsDetached());
    EnqueuIndexPage(idx_page);
}

void IndexPageManager::RecycleReadReq(ReadReq *entry)
{
    ReadReq *first = free_read_head_.next_;
    free_read_head_.next_ = entry;
    entry->next_ = first;

    if (waiting_zone_.Size() > 0)
    {
        KvTask *task = waiting_zone_.Peek();
        waiting_zone_.Dequeue();
        task->Resume();
    }
}

IndexPageManager::ReadReq *IndexPageManager::GetFreeReadReq()
{
    ReadReq *first = free_read_head_.next_;
    while (first == nullptr)
    {
        waiting_zone_.Enqueue(thd_task);
        thd_task->status_ = TaskStatus::Blocked;
        thd_task->Yield();
        first = free_read_head_.next_;
    }

    free_read_head_.next_ = first->next_;
    first->next_ = nullptr;
    return first;
}

KvError IndexPageManager::SeekIndex(MappingSnapshot *mapping,
                                    const TableIdent &tbl_ident,
                                    MemIndexPage *node,
                                    std::string_view key,
                                    uint32_t &result)
{
    IndexPageIter idx_it{node, Options()};
    idx_it.Seek(key);
    uint32_t page_id = idx_it.PageId();
    if (node->IsPointingToLeaf() || page_id == UINT32_MAX)
    {
        // Updates the cache replacement list.
        EnqueuIndexPage(node);
        result = page_id;
        return KvError::NoError;
    }
    else
    {
        auto [child, err] = FindPage(mapping, page_id);
        CHECK_KV_ERR(err);
        return SeekIndex(mapping, tbl_ident, child, key, result);
    }
}

const KvOptions *IndexPageManager::Options() const
{
    return io_manager_->options_;
}

AsyncIoManager *IndexPageManager::IoMgr() const
{
    return io_manager_;
}
}  // namespace kvstore