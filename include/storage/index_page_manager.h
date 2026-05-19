#pragma once

#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "comparator.h"
#include "error.h"
#include "kv_options.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "types.h"

namespace eloqstore
{
class KvTask;
class PageMapper;
class IndexPageManager;

class IndexPageManager
{
    friend class RootMetaMgr;

public:
    IndexPageManager(AsyncIoManager *io_manager);

    void Shutdown();

    const Comparator *GetComparator() const;

    /**
     * @brief Allocates a page from the buffer pool. The returned page is not
     * traced in the cache replacement list, so it cannot be evicted.
     */
    template <typename PageType>
    PageType *AllocPage();

    /**
     * @brief Returns a page to the free list.
     */
    template <typename PageType>
    void FreePage(PageType *page);

    /**
     * @brief Enqueues the page into the cache replacement list (MRU position).
     */
    template <typename PageType>
    void EnqueuePage(PageType *page);

    std::pair<RootMetaMgr::Handle, KvError> FindRoot(
        const TableIdent &tbl_ident);

    KvError MakeCowRoot(const TableIdent &tbl_ident, CowRootMeta &cow_meta);

    void UpdateRoot(const TableIdent &tbl_ident, CowRootMeta new_meta);

    std::pair<MemIndexPage::Handle, KvError> FindPage(MappingSnapshot *mapping,
                                                      PageId page_id);
    // Install an externally built snapshot (e.g. pulled from remote manifest)
    // into the RootMeta version chain without performing local COW writes.
    // A missing external manifest falls back to installing an empty snapshot.
    KvError InstallExternalSnapshot(const TableIdent &tbl_ident,
                                    CowRootMeta &cow_meta,
                                    std::string_view reopen_tag = {});
    KvError InstallEmptySnapshot(const TableIdent &tbl_ident,
                                 CowRootMeta &cow_meta);

    void FreeMappingSnapshot(MappingSnapshot *mapping);

    void FinishIo(MappingSnapshot *mapping, MemIndexPage *idx_page);

    // Given the table id, tree root and the input key, returns the logical page
    // id of the data page that might contain the key.
    KvError SeekIndex(MappingSnapshot *mapping,
                      PageId page_id,
                      std::string_view key,
                      PageId &result);

    const KvOptions *Options() const;
    AsyncIoManager *IoMgr() const;
    MappingArena *MapperArena();
    MappingChunkArena *MapperChunkArena();
    RootMetaMgr *RootMetaManager();

    /**
     * @brief Get current buffer pool used size in bytes.
     * @return Current size of allocated index pages in bytes.
     */
    size_t GetBufferPoolUsed() const;

    /**
     * @brief Get buffer pool size limit in bytes.
     * @return Total buffer pool size limit in bytes.
     */
    size_t GetBufferPoolLimit() const;
    size_t GetDataPageCacheLimit() const;

    // ---- Data page cache ----

    /**
     * @brief Find a cached data page or load it from disk.
     * Returns {Handle(), OutOfMem} when the cache is full and eviction fails.
     */
    std::pair<MemDataPage::Handle, KvError> FindDataPage(
        MappingSnapshot *mapping,
        const TableIdent &tbl_id,
        PageId page_id);

    void FinishIo(MappingSnapshot *mapping, MemDataPage *page);

    template <typename PageType>
    struct PagePool
    {
        PageType active_head_{false};
        PageType active_tail_{false};
        PageType free_head_{false};
        std::vector<std::unique_ptr<PageType>> pages_;
        size_t limit_bytes_{0};
    };

private:
    template <typename PageType>
    PagePool<PageType> &GetPool();

    template <typename PageType>
    bool IsCacheFull() const;

    template <typename PageType>
    bool EvictPage();

    bool RecyclePage(MemIndexPage *page);
    bool RecyclePage(MemDataPage *page);

    PagePool<MemIndexPage> index_cache_;
    PagePool<MemDataPage> data_cache_;

    AsyncIoManager *io_manager_;
    MappingArena mapping_arena_;
    MappingChunkArena mapping_chunk_arena_;
    RootMetaMgr root_meta_mgr_;
    bool shutdown_{false};
};

// --- inline template implementations ---

template <typename PageType>
inline auto IndexPageManager::GetPool() -> PagePool<PageType> &
{
    if constexpr (std::is_same_v<PageType, MemIndexPage>)
        return index_cache_;
    else
        return data_cache_;
}

template <typename PageType>
inline PageType *IndexPageManager::AllocPage()
{
    auto &pool = GetPool<PageType>();
    PageType *next_free = pool.free_head_.DequeNext();
    while (next_free == nullptr)
    {
        if (!IsCacheFull<PageType>())
        {
            auto &new_page =
                pool.pages_.emplace_back(std::make_unique<PageType>());
            next_free = new_page.get();
        }
        else if (!EvictPage<PageType>())
            return nullptr;
        else
            next_free = pool.free_head_.DequeNext();
    }
    next_free->in_free_list_ = false;
    return next_free;
}

template <typename PageType>
inline void IndexPageManager::FreePage(PageType *page)
{
    page->SetError(KvError::NoError);
    page->in_free_list_ = true;
    GetPool<PageType>().free_head_.EnqueNext(page);
}

template <typename PageType>
inline void IndexPageManager::EnqueuePage(PageType *page)
{
    if (!page->IsDetached()) page->Deque();
    GetPool<PageType>().active_head_.EnqueNext(page);
}

template <typename PageType>
inline bool IndexPageManager::IsCacheFull() const
{
    const auto &pool =
        const_cast<IndexPageManager *>(this)->GetPool<PageType>();
    return pool.pages_.size() * Options()->data_page_size >= pool.limit_bytes_;
}

template <typename PageType>
inline bool IndexPageManager::EvictPage()
{
    auto &pool = GetPool<PageType>();
    auto *node = &pool.active_tail_;
    do
    {
        while (node->prev_->IsPinned() && node->prev_ != &pool.active_head_)
            node = node->prev_;
        if (node->prev_ == &pool.active_head_) return false;
        node = node->prev_;
        RecyclePage(node);
    } while (pool.free_head_.next_ == nullptr);
    return true;
}

}  // namespace eloqstore
