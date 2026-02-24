#include "storage/index_page_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "error.h"
#include "kv_options.h"
#include "replayer.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
namespace
{
size_t EffectiveBufferPoolLimitBytes(const KvOptions *opts)
{
    const double ratio = std::clamp(opts->write_buffer_ratio, 0.0, 1.0);
    size_t reserved = static_cast<size_t>(
        static_cast<double>(opts->buffer_pool_size) * ratio);
    if (reserved > opts->buffer_pool_size)
    {
        reserved = opts->buffer_pool_size;
    }
    size_t limit = opts->buffer_pool_size - reserved;
    if (limit == 0)
    {
        return opts->data_page_size;
    }
    return std::max(limit, static_cast<size_t>(opts->data_page_size));
}

size_t RootMetaBytes(const RootMeta &meta)
{
    if (meta.mapper_ == nullptr)
    {
        return 0;
    }
    const auto &tbl = meta.mapper_->GetMapping()->mapping_tbl_;
    size_t bytes = tbl.capacity() * sizeof(uint64_t);
    CHECK(meta.compression_ != nullptr);
    bytes += meta.compression_->DictionaryMemoryBytes();
    return bytes;
}
}  // namespace

IndexPageManager::IndexPageManager(AsyncIoManager *io_manager)
    : io_manager_(io_manager), root_meta_mgr_(this, Options())
{
    active_head_.EnqueNext(&active_tail_);
    const size_t page_limit =
        EffectiveBufferPoolLimitBytes(Options()) / Options()->data_page_size;
    index_pages_.reserve(page_limit);
}

IndexPageManager::~IndexPageManager()
{
    root_meta_mgr_.ReleaseMappers();
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
    next_free->in_free_list_ = false;
    return next_free;
}

void IndexPageManager::FreeIndexPage(MemIndexPage *page)
{
    assert(page->IsDetached());
    assert(!page->IsPinned());
    page->in_free_list_ = true;
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
    return current_size >= EffectiveBufferPoolLimitBytes(Options());
}

size_t IndexPageManager::GetBufferPoolUsed() const
{
    return index_pages_.size() * Options()->data_page_size;
}

size_t IndexPageManager::GetBufferPoolLimit() const
{
    return EffectiveBufferPoolLimitBytes(Options());
}

std::pair<RootMetaMgr::Handle, KvError> IndexPageManager::FindRoot(
    const TableIdent &tbl_id)
{
    auto load_meta = [this](RootMetaMgr::Entry *entry)
    {
        RootMeta *meta = &entry->meta_;
        const TableIdent &entry_tbl = entry->tbl_id_;
        // load manifest file
        auto [manifest, err] = IoMgr()->GetManifest(entry_tbl);
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
        auto mapper =
            replayer.GetMapper(this, &entry_tbl, IoMgr()->ProcessTerm());
        MappingSnapshot *mapping = mapper->GetMapping();
        meta->mapper_ = std::move(mapper);
        meta->mapping_snapshots_.insert(mapping);
        root_meta_mgr_.UpdateBytes(entry, RootMetaBytes(*meta));
        err = root_meta_mgr_.EvictIfNeeded();
        CHECK_KV_ERR(err);
        meta->manifest_size_ = replayer.file_size_;
        meta->next_expire_ts_ = 0;
        if (!replayer.dict_bytes_.empty())
        {
            meta->compression_->LoadDictionary(std::move(replayer.dict_bytes_));
        }
        if (meta->ttl_root_id_ != MaxPageId)
        {
            // For simplicity, we initialize next_expire_ts_ to 1,
            // ensuring the next write operation will trigger a TTL check.
            meta->next_expire_ts_ = 1;
        }
        replayer.file_id_term_mapping_->insert_or_assign(
            IouringMgr::LruFD::kManifest, IoMgr()->ProcessTerm());
        IoMgr()->SetFileIdTermMapping(entry_tbl,
                                      replayer.file_id_term_mapping_);
        return KvError::NoError;
    };

    while (true)
    {
        auto [entry, inserted] = root_meta_mgr_.GetOrCreate(tbl_id);
        RootMeta *meta = &entry->meta_;

        if (inserted)
        {
            // Try to load metadata from persistent storage.
            meta->locked_ = true;
            KvError err = load_meta(entry);
            meta->waiting_.WakeAll();
            if (err != KvError::NoError)
            {
                if (err != KvError::NotFound)
                {
                    LOG(ERROR)
                        << "load meta failed, err: " << static_cast<int>(err);
                }
                root_meta_mgr_.Erase(tbl_id);
                return {RootMetaMgr::Handle(), err};
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
            return {RootMetaMgr::Handle(&root_meta_mgr_, entry),
                    KvError::NotFound};
        }
        return {RootMetaMgr::Handle(&root_meta_mgr_, entry), KvError::NoError};
    }
}

KvError IndexPageManager::MakeCowRoot(const TableIdent &tbl_ident,
                                      CowRootMeta &cow_meta)
{
    cow_meta.root_handle_ = RootMetaMgr::Handle();
    auto [found_handle, err] = FindRoot(tbl_ident);
    RootMeta *meta = found_handle.Get();
    if (err == KvError::NoError)
    {
        cow_meta.root_handle_ = std::move(found_handle);
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
        RootMetaMgr::Entry *entry = found_handle.EntryPtr();
        if (entry == nullptr)
        {
            auto [created_entry, _] = root_meta_mgr_.GetOrCreate(tbl_ident);
            entry = created_entry;
            cow_meta.root_handle_ = RootMetaMgr::Handle(&root_meta_mgr_, entry);
        }
        else
        {
            cow_meta.root_handle_ = std::move(found_handle);
        }
        const TableIdent *tbl_id = &entry->tbl_id_;
        auto mapper = std::make_unique<PageMapper>(this, tbl_id);
        MappingSnapshot::Ref mapping = mapper->GetMappingSnapshot();
        cow_meta.root_id_ = MaxPageId;
        cow_meta.ttl_root_id_ = MaxPageId;
        cow_meta.mapper_ = std::move(mapper);
        cow_meta.old_mapping_ = std::move(mapping);
        cow_meta.manifest_size_ = 0;
        cow_meta.next_expire_ts_ = 0;
        cow_meta.compression_ =
            std::make_shared<compression::DictCompression>();
        meta = &entry->meta_;
    }
    else
    {
        return err;
    }
    auto it = meta->mapping_snapshots_.insert(cow_meta.mapper_->GetMapping());
    CHECK(it.second);
    return KvError::NoError;
}

void IndexPageManager::UpdateRoot(const TableIdent &tbl_ident,
                                  CowRootMeta new_meta)
{
    auto *entry = root_meta_mgr_.Find(tbl_ident);
    assert(entry != nullptr);
    RootMeta &meta = entry->meta_;
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
    root_meta_mgr_.UpdateBytes(entry, RootMetaBytes(meta));
    root_meta_mgr_.EvictIfNeeded();
}

KvError IndexPageManager::InstallExternalSnapshot(const TableIdent &tbl_ident,
                                                  CowRootMeta &cow_meta)
{
    if (Options()->cloud_store_path.empty())
    {
        return KvError::InvalidArgs;
    }
    auto *cloud_mgr = static_cast<CloudStoreMgr *>(IoMgr());

    auto [root_handle, root_err] = FindRoot(tbl_ident);
    if (root_err == KvError::NoError)
    {
        RootMeta *old_meta = root_handle.Get();
        if (old_meta != nullptr && old_meta->mapper_ != nullptr)
        {
            FilePageId max_fp_id =
                old_meta->mapper_->FilePgAllocator()->MaxFilePageId();
            FileId max_file_id = max_fp_id >> Options()->pages_per_file_shift;
            if (max_file_id <= IouringMgr::LruFD::kMaxDataFile)
            {
                uint64_t term = IoMgr()
                                    ->GetFileIdTerm(tbl_ident, max_file_id)
                                    .value_or(IoMgr()->ProcessTerm());
                KvError sync_err = cloud_mgr->SyncDataFileFromRemoteIfNeeded(
                    tbl_ident, max_file_id, term);
                if (sync_err != KvError::NoError &&
                    sync_err != KvError::NotFound)
                {
                    return sync_err;
                }
            }
        }
    }

    auto [manifest, err] = cloud_mgr->RefreshManifest(tbl_ident);
    CHECK_KV_ERR(err);

    Replayer replayer(Options());
    err = replayer.Replay(manifest.get());
    CHECK_KV_ERR(err);

    auto [entry, inserted] = root_meta_mgr_.GetOrCreate(tbl_ident);
    RootMeta &meta = entry->meta_;
    auto mapper =
        replayer.GetMapper(this, &entry->tbl_id_, IoMgr()->ProcessTerm());
    MappingSnapshot *mapping = mapper->GetMapping();
    meta.mapping_snapshots_.insert(mapping);

    // Reuse swizzling pointers when file_page_id is unchanged.
    for (MemIndexPage *page : meta.index_pages_)
    {
        PageId page_id = page->GetPageId();
        if (page_id >= mapping->mapping_tbl_.size())
        {
            continue;
        }
        uint64_t val = mapping->mapping_tbl_.Get(page_id);
        if (MappingSnapshot::IsFilePageId(val) &&
            MappingSnapshot::DecodeId(val) == page->GetFilePageId())
        {
            mapping->AddSwizzling(page_id, page);
        }
    }

    cow_meta = CowRootMeta();
    cow_meta.root_id_ = replayer.root_;
    cow_meta.ttl_root_id_ = replayer.ttl_root_;
    cow_meta.mapper_ = std::move(mapper);
    cow_meta.manifest_size_ = replayer.file_size_;
    cow_meta.next_expire_ts_ = replayer.ttl_root_ != MaxPageId ? 1 : 0;
    cow_meta.compression_ = std::make_shared<compression::DictCompression>();
    if (!replayer.dict_bytes_.empty())
    {
        cow_meta.compression_->LoadDictionary(std::move(replayer.dict_bytes_));
    }

    UpdateRoot(tbl_ident, std::move(cow_meta));

    replayer.file_id_term_mapping_->insert_or_assign(
        IouringMgr::LruFD::kManifest, IoMgr()->ProcessTerm());
    IoMgr()->SetFileIdTermMapping(entry->tbl_id_,
                                  replayer.file_id_term_mapping_);

    return KvError::NoError;
}

std::pair<MemIndexPage::Handle, KvError> IndexPageManager::FindPage(
    MappingSnapshot *mapping, PageId page_id)
{
    while (true)
    {
        // First checks swizzling pointers.
        MemIndexPage::Handle handle = mapping->GetSwizzlingHandle(page_id);
        if (!handle)
        {
            // This is the first request to load the page.
            MemIndexPage *new_page = AllocIndexPage();
            if (new_page == nullptr)
            {
                return {MemIndexPage::Handle(), KvError::OutOfMem};
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
                return {MemIndexPage::Handle(), err};
            }
            FinishIo(mapping, new_page);
            new_page->waiting_.WakeAll();
            return {MemIndexPage::Handle(new_page), KvError::NoError};
        }
        if (handle->IsDetached())
        {
            // This page is not loaded yet.
            handle->waiting_.Wait(ThdTask());
        }
        else
        {
            EnqueueIndexPage(handle.Get());
            return {std::move(handle), KvError::NoError};
        }
    }
}

void IndexPageManager::FreeMappingSnapshot(MappingSnapshot *mapping)
{
    const TableIdent &tbl = *mapping->tbl_ident_;
    auto *entry = root_meta_mgr_.Find(tbl);
    if (entry == nullptr)
    {
        return;
    }
    RootMeta &meta = entry->meta_;
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

bool IndexPageManager::RecyclePage(MemIndexPage *page)
{
    assert(!page->IsPinned());
    RootMetaMgr::Entry *entry = root_meta_mgr_.Find(*page->tbl_ident_);
    if (entry != nullptr)
    {
        RootMeta &meta = entry->meta_;
        // Unswizzling the page pointer in all mapping snapshots.
        auto &mappings = meta.mapping_snapshots_;
        for (auto &mapping : mappings)
        {
            mapping->Unswizzling(page);
        }
        meta.index_pages_.erase(page);
    }

    // Removes the page from the active list.
    page->Deque();
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

    auto *entry = root_meta_mgr_.Find(*mapping->tbl_ident_);
    if (entry != nullptr)
    {
        entry->meta_.index_pages_.insert(idx_page);
    }
    EnqueueIndexPage(idx_page);
}

KvError IndexPageManager::SeekIndex(MappingSnapshot *mapping,
                                    PageId page_id,
                                    std::string_view key,
                                    PageId &result)
{
    PageId current_id = page_id;
    while (true)
    {
        auto [handle, err] = FindPage(mapping, current_id);
        CHECK_KV_ERR(err);
        IndexPageIter idx_it{handle, Options()};
        idx_it.Seek(key);
        PageId child_id = idx_it.GetPageId();

        if (handle->IsPointingToLeaf())
        {
            result = child_id;
            return KvError::NoError;
        }

        current_id = child_id;
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

MappingArena *IndexPageManager::MapperArena()
{
    return &mapping_arena_;
}

MappingChunkArena *IndexPageManager::MapperChunkArena()
{
    return &mapping_chunk_arena_;
}

RootMetaMgr *IndexPageManager::RootMetaManager()
{
    return &root_meta_mgr_;
}

}  // namespace eloqstore
