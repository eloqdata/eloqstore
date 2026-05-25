#pragma once

#include <atomic>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "comparator.h"
#include "error.h"
#include "kv_options.h"
#include "storage/mem_cached_page.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "types.h"

namespace eloqstore
{
class KvTask;
class PageMapper;
class PageManager;

class PageManager
{
    friend class RootMetaMgr;

public:
    PageManager(AsyncIoManager *io_manager);

    void Shutdown();

    const Comparator *GetComparator() const;

    /**
     * @brief Allocates a cached page slot from the buffer pool. The returned
     * page is not traced in the cache replacement list, so it cannot be
     * evicted. Whoever getting a new page should enqueue it later for cache
     * replacement.
     *
     * @return MemCachedPage*
     */
    MemCachedPage *AllocPage();
    void FreePage(MemCachedPage *page);

    /**
     * @brief Enqueues the cached page into the cache replacement list.
     *
     * @param page
     */
    void EnqueuePage(MemCachedPage *page);

    std::pair<RootMetaMgr::Handle, KvError> FindRoot(
        const TableIdent &tbl_ident);

    KvError MakeCowRoot(const TableIdent &tbl_ident, CowRootMeta &cow_meta);

    void UpdateRoot(const TableIdent &tbl_ident, CowRootMeta new_meta);

    std::pair<MemCachedPage::Handle, KvError> FindPage(MappingSnapshot *mapping,
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

    void FinishIo(MappingSnapshot *mapping, MemCachedPage *idx_page);

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
     * @return Current size of allocated cached pages in bytes.
     */
    size_t GetBufferPoolUsed() const;

    /**
     * @brief Get buffer pool size limit in bytes.
     * @return Total buffer pool size limit in bytes.
     */
    size_t GetBufferPoolLimit() const;

    /**
     * @brief Recycle @p page if no reader holds a pin on it. No-op when the
     * page is pinned, already on the free list, or null. Writers call this
     * after rewriting a PageId via CoW so the orphaned cache slot is freed
     * eagerly instead of waiting for LRU eviction.
     */
    void TryRecycleCachedPage(MemCachedPage *page);

    // Data-page cache effectiveness counters. Only meaningful when
    // KvOptions::enable_data_page_cache is on; bumped from LoadDataPage and
    // LoadDataPageForUpdate. Index-page cache traffic is not counted here.
    //
    // Each shard's counters are written only by that shard's own thread
    // (single-writer), but are read across threads via
    // EloqStore::DataCacheHits()/DataCacheMisses() which aggregate over
    // shards. std::atomic with memory_order_relaxed is sufficient: these
    // are statistics, no causal ordering across counter updates is needed.
    size_t DataCacheHits() const
    {
        return data_cache_hits_.load(std::memory_order_relaxed);
    }
    size_t DataCacheMisses() const
    {
        return data_cache_misses_.load(std::memory_order_relaxed);
    }
    void ResetDataCacheCounters()
    {
        data_cache_hits_.store(0, std::memory_order_relaxed);
        data_cache_misses_.store(0, std::memory_order_relaxed);
    }
    void IncrDataCacheHit()
    {
        data_cache_hits_.fetch_add(1, std::memory_order_relaxed);
    }
    void IncrDataCacheMiss()
    {
        data_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    /**
     * @brief Returns if memory is full. TODO: Replaces with a reasonable
     * implementation.
     *
     * @return true
     * @return false
     */
    bool IsFull() const;

    bool Evict();

    bool RecyclePage(MemCachedPage *page);

    /**
     * @brief Reserved head and tail for the active list. The head points to the
     * most-recently accessed, and the tail points to the least-recently
     * accessed.
     *
     */
    MemCachedPage active_head_{false};
    MemCachedPage active_tail_{false};
    MemCachedPage free_head_{false};

    /**
     * @brief Pool of cached page slots (index and, when
     * KvOptions::enable_data_page_cache is on, data).
     *
     */
    std::vector<std::unique_ptr<MemCachedPage>> cached_pages_;

    AsyncIoManager *io_manager_;
    MappingArena mapping_arena_;
    MappingChunkArena mapping_chunk_arena_;
    RootMetaMgr root_meta_mgr_;
    bool shutdown_{false};
    std::atomic<size_t> data_cache_hits_{0};
    std::atomic<size_t> data_cache_misses_{0};
};
}  // namespace eloqstore
