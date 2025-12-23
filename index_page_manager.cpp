#include "index_page_manager.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "error.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "replayer.h"
#include "root_meta.h"
#include "task.h"
#include "types.h"

namespace eloqstore
{
IndexPageManager::IndexPageManager(AsyncIoManager *io_manager)
    : io_manager_(io_manager)
{
    active_head_.EnqueNext(&active_tail_);
}

IndexPageManager::~IndexPageManager()
{
    for (auto &[tbl_id, meta] : tbl_roots_)
    {
        meta.mapper_ = nullptr;
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
            auto &new_page =
                index_pages_.emplace_back(std::make_unique<MemIndexPage>());
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
    assert(!next_free->IsPinned());
    return next_free;
}

void IndexPageManager::FreeIndexPage(MemIndexPage *page)
{
    assert(page->IsDetached());
    assert(!page->IsPinned());
    free_head_.EnqueNext(page);
}

void IndexPageManager::EnqueueIndexPage(MemIndexPage *page)
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
    // Calculate current total memory usage
    size_t current_size = index_pages_.size() * Options()->data_page_size;
    return current_size >= Options()->index_buffer_pool_size;
}

std::pair<RootMeta *, KvError> IndexPageManager::FindRoot(
    const TableIdent &tbl_id)
{
    auto load_meta = [this](const TableIdent &tbl_id, RootMeta *meta)
    {
        // load manifest file
        auto [manifest, err] = IoMgr()->GetManifest(tbl_id);
        CHECK_KV_ERR(err);

        // replay
        Replayer replayer(Options());
        err = replayer.Replay(manifest.get());
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "load evicted table: replay failed";
            return err;
        }

        meta->root_id_ = replayer.root_;
        meta->ttl_root_id_ = replayer.ttl_root_;
        auto mapper = replayer.GetMapper(this, &tbl_id);
        MappingSnapshot *mapping = mapper->GetMapping();
        meta->mapper_ = std::move(mapper);
        meta->mapping_snapshots_.insert(mapping);
        meta->Pin();
        meta->manifest_size_ = replayer.file_size_;
        meta->next_expire_ts_ = 0;
        meta->compression_->LoadDictionary(std::move(replayer.dict_bytes_));
        if (meta->ttl_root_id_ != MaxPageId)
        {
            // For simplicity, we initialize next_expire_ts_ to 1,
            // ensuring the next write operation will trigger a TTL check.
            meta->next_expire_ts_ = 1;
        }
        return KvError::NoError;
    };

    while (true)
    {
        auto [it, inserted] = tbl_roots_.try_emplace(tbl_id);
        RootMeta *meta = &it->second;

        if (inserted)
        {
            // Try to load metadata from persistent storage.
            meta->locked_ = true;
            KvError err = load_meta(it->first, meta);
            meta->waiting_.WakeAll();
            if (err != KvError::NoError)
            {
                tbl_roots_.erase(tbl_id);
                return {nullptr, err};
            }
            meta->locked_ = false;
        }
        else if (meta->locked_)
        {
            // Blocked by other loading/evicting operation.
            meta->waiting_.Wait(ThdTask());
            continue;
        }

        if (meta->mapper_ == nullptr)
        {
            // Partition not found. Possible causes:
            // 1. During the initial write to a non-existent partition,
            //    WriteTask creates a stub RootMeta (with mapper=nullptr).
            // 2. A MemIndexPage referencing this stub RootMeta is created.
            // 3. WriteTask aborts, but the stub RootMeta cannot be cleared
            //    because it is still referenced by the MemIndexPage.
            return {meta, KvError::NotFound};
        }
        return {meta, KvError::NoError};
    }
}

KvError IndexPageManager::MakeCowRoot(const TableIdent &tbl_ident,
                                      CowRootMeta &cow_meta)
{
    auto [meta, err] = FindRoot(tbl_ident);
    if (err == KvError::NoError)
    {
        // Makes a copy of the mapper.
        auto new_mapper = std::make_unique<PageMapper>(*meta->mapper_);
        cow_meta.root_id_ = meta->root_id_;
        cow_meta.ttl_root_id_ = meta->ttl_root_id_;
        cow_meta.mapper_ = std::move(new_mapper);
        cow_meta.old_mapping_ = meta->mapper_->GetMappingSnapshot();
        cow_meta.manifest_size_ = meta->manifest_size_;
        cow_meta.next_expire_ts_ = meta->next_expire_ts_;
        if (meta->compression_->Dirty())
        {
            // This only happens when the dictionary is built from values with
            // expired timestamps. If eloqstore stops before any new value
            // arrives, this dictionary can be discarded since no value has been
            // written. Otherwise, the dictionary can still be used to compress
            // subsequent values.
            assert(cow_meta.manifest_size_ == 0);
        }
        cow_meta.compression_ = meta->compression_;
    }
    else if (err == KvError::NotFound)
    {
        // It is the WriteTask's responsibility to clean up this stub RootMeta
        // if it aborted.
        auto [tbl_it, _] = tbl_roots_.try_emplace(tbl_ident);
        const TableIdent *tbl_id = &tbl_it->first;
        auto mapper = std::make_unique<PageMapper>(this, tbl_id);
        std::shared_ptr<MappingSnapshot> mapping = mapper->GetMappingSnapshot();
        cow_meta.root_id_ = MaxPageId;
        cow_meta.ttl_root_id_ = MaxPageId;
        cow_meta.mapper_ = std::move(mapper);
        cow_meta.old_mapping_ = std::move(mapping);
        cow_meta.manifest_size_ = 0;
        cow_meta.next_expire_ts_ = 0;
        cow_meta.compression_ =
            std::make_shared<compression::DictCompression>();
        meta = &tbl_it->second;
    }
    else
    {
        return err;
    }
    auto it = meta->mapping_snapshots_.insert(cow_meta.mapper_->GetMapping());
    CHECK(it.second);
    meta->Pin();  // Referenced by new MappingSnapshot.
    return KvError::NoError;
}

void IndexPageManager::UpdateRoot(const TableIdent &tbl_ident,
                                  CowRootMeta new_meta)
{
    auto tbl_it = tbl_roots_.find(tbl_ident);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    meta.root_id_ = new_meta.root_id_;
    meta.ttl_root_id_ = new_meta.ttl_root_id_;
    if (meta.mapper_ != nullptr && !Options()->data_append_mode)
    {
        assert(new_meta.mapper_ != nullptr);
        MappingSnapshot *prev_snapshot = meta.mapper_->GetMapping();
        prev_snapshot->next_snapshot_ = new_meta.mapper_->GetMappingSnapshot();
    }
    meta.mapper_ = std::move(new_meta.mapper_);
    meta.manifest_size_ = new_meta.manifest_size_;
    meta.next_expire_ts_ = new_meta.next_expire_ts_;
    meta.compression_ = std::move(new_meta.compression_);
}

std::pair<MemIndexPage *, KvError> IndexPageManager::FindPage(
    MappingSnapshot *mapping, PageId page_id)
{
    while (true)
    {
        // First checks swizzling pointers.
        MemIndexPage *idx_page = mapping->GetSwizzlingPointer(page_id);
        if (idx_page == nullptr)
        {
            // This is the first request to load the page.
            MemIndexPage *new_page = AllocIndexPage();
            if (new_page == nullptr)
            {
                return {nullptr, KvError::OutOfMem};
            }
            FilePageId file_page_id = mapping->ToFilePage(page_id);
            new_page->SetPageId(page_id);
            new_page->SetFilePageId(file_page_id);
            mapping->AddSwizzling(page_id, new_page);

            // Read the page async.
            auto [page, err] = IoMgr()->ReadPage(
                *mapping->tbl_ident_, file_page_id, std::move(new_page->page_));
            new_page->page_ = std::move(page);
            if (err != KvError::NoError)
            {
                new_page->waiting_.WakeAll();
                mapping->Unswizzling(new_page);
                FreeIndexPage(new_page);
                return {nullptr, err};
            }
            FinishIo(mapping, new_page);
            new_page->waiting_.WakeAll();
            return {new_page, KvError::NoError};
        }
        if (idx_page->IsDetached())
        {
            // This page is not loaded yet.
            idx_page->waiting_.Wait(ThdTask());
        }
        else
        {
            EnqueueIndexPage(idx_page);
            return {idx_page, KvError::NoError};
        }
    }
}

void IndexPageManager::FreeMappingSnapshot(MappingSnapshot *mapping)
{
    const TableIdent &tbl = *mapping->tbl_ident_;
    auto tbl_it = tbl_roots_.find(tbl);
    if (tbl_it == tbl_roots_.end())
    {
        return;
    }
    RootMeta &meta = tbl_it->second;
    // Puts back file pages freed in this mapping snapshot
    if (!mapping->to_free_file_pages_.empty())
    {
        assert(meta.mapper_ != nullptr);
        assert(!Options()->data_append_mode);
        auto pool =
            static_cast<PooledFilePages *>(meta.mapper_->FilePgAllocator());
        pool->Free(std::move(mapping->to_free_file_pages_));
    }
    auto n = meta.mapping_snapshots_.erase(mapping);
    CHECK(n == 1);
    meta.Unpin();
    EvictRootIfEmpty(tbl_it);
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

void IndexPageManager::EvictRootIfEmpty(const TableIdent &tbl_id)
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

    if (meta.mapper_ == nullptr)
    {
        return;
    }
    CHECK(meta.ref_cnt_ != 0);

    if (meta.ref_cnt_ == 1)
    {
        if (meta.mapper_->UseCount() == 1)
        {
            // Check if mapping table is empty (no data pages)
            if (meta.mapper_->MappingCount() == 0 && meta.manifest_size_ > 0)
            {
                // Lock the rootmeta to prevent other FindRoot operations
                meta.locked_ = true;

                const TableIdent &tbl_id = root_it->first;

                // Clean up the table from io manager first

                // Note: it will also clean manifest when data_append = false
                // although it is not remove datafile
                // Manifest record max file page id, deleting it can lead to
                // creating existing files.
                IoMgr()->CleanManifest(tbl_id);

                // Wake up any waiting threads before erasing
                meta.waiting_.WakeAll();

                // Erase from memory - this will automatically destroy
                // meta.mapper_ and trigger MappingSnapshot destructor, but
                // FreeMappingSnapshot will return early since the table is
                // already erased
                tbl_roots_.erase(root_it);

                // Note: No need to unlock since we've erased the entry
                return;
            }

            // If mapping is not empty, we can directly erase the root since
            // ref_cnt == 1 means only the mapper itself holds the reference

            DLOG(INFO) << "metadata of " << root_it->first
                       << " is evicted (ref_cnt == 1)";
            tbl_roots_.erase(root_it);
        }
        else
        {
            // This is rare.
            DLOG(INFO) << "ref_cnt == 1 but mapper use count :"
                       << meta.mapper_->UseCount();
        }
    }
}

bool IndexPageManager::RecyclePage(MemIndexPage *page)
{
    assert(!page->IsPinned());
    auto tbl_it = tbl_roots_.find(*page->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    // Unswizzling the page pointer in all mapping snapshots.
    auto &mappings = meta.mapping_snapshots_;
    for (auto &mapping : mappings)
    {
        mapping->Unswizzling(page);
    }
    meta.Unpin();

    // Removes the page from the active list.
    page->Deque();
    EvictRootIfEmpty(tbl_it);
    assert(page->page_id_ != MaxPageId);
    assert(page->file_page_id_ != MaxFilePageId);
    page->page_id_ = MaxPageId;
    page->file_page_id_ = MaxFilePageId;
    page->tbl_ident_ = nullptr;

    FreeIndexPage(page);
    return true;
}

void IndexPageManager::FinishIo(MappingSnapshot *mapping,
                                MemIndexPage *idx_page)
{
    idx_page->tbl_ident_ = mapping->tbl_ident_;
    mapping->AddSwizzling(idx_page->GetPageId(), idx_page);

    if (idx_page->IsDetached())
    {
        auto tbl_it = tbl_roots_.find(*mapping->tbl_ident_);
        assert(tbl_it != tbl_roots_.end());
        tbl_it->second.Pin();
    }
    else
    {
        // index page is moved on physical position.
    }
    EnqueueIndexPage(idx_page);
}

KvError IndexPageManager::SeekIndex(MappingSnapshot *mapping,
                                    PageId page_id,
                                    std::string_view key,
                                    std::span<PageId> results,
                                    size_t &result_size_dst)
{
    auto [node, err] = FindPage(mapping, page_id);
    CHECK_KV_ERR(err);
    IndexPageIter idx_it{node, Options()};
    idx_it.Seek(key);
    PageId child_id = idx_it.GetPageId();
    if (!node->IsPointingToLeaf())
    {
        return SeekIndex(mapping, child_id, key, results, result_size_dst);
    }

    size_t result_size = 0;
    const size_t result_limit = results.size();
    if (child_id != MaxPageId && result_size < result_limit)
    {
        results[result_size++] = child_id;
    }

    while (result_size < result_limit && idx_it.HasNext() && idx_it.Next())
    {
        child_id = idx_it.GetPageId();
        if (child_id == MaxPageId)
        {
            break;
        }
        results[result_size++] = child_id;
    }
    result_size_dst = result_size;
    return KvError::NoError;
}

KvError IndexPageManager::SeekIndex(MappingSnapshot *mapping,
                                    PageId page_id,
                                    std::string_view key,
                                    PageId &result)
{
    auto [node, err] = FindPage(mapping, page_id);
    CHECK_KV_ERR(err);
    IndexPageIter idx_it{node, Options()};
    idx_it.Seek(key);
    PageId child_id = idx_it.GetPageId();
    if (node->IsPointingToLeaf())
    {
        result = child_id;
        return KvError::NoError;
    }
    else
    {
        return SeekIndex(mapping, child_id, key, result);
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
}  // namespace eloqstore