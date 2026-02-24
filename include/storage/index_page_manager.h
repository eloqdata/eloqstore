#pragma once

#include <memory>
#include <span>
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
    ~IndexPageManager();

    const Comparator *GetComparator() const;

    /**
     * @brief Allocates an index page from buffer pool. The returned page is not
     * traced in the cache replacement list, so it cannot be evicted. Whoever
     * getting a new page should enqueue it later for cache replacement.
     *
     * @return MemIndexPage*
     */
    MemIndexPage *AllocIndexPage();
    void FreeIndexPage(MemIndexPage *page);

    /**
     * @brief Enqueues the index page into the cache replacement list.
     *
     * @param page
     */
    void EnqueueIndexPage(MemIndexPage *page);

    std::pair<RootMetaMgr::Handle, KvError> FindRoot(
        const TableIdent &tbl_ident);

    KvError MakeCowRoot(const TableIdent &tbl_ident, CowRootMeta &cow_meta);

    void UpdateRoot(const TableIdent &tbl_ident, CowRootMeta new_meta);

    std::pair<MemIndexPage::Handle, KvError> FindPage(MappingSnapshot *mapping,
                                                      PageId page_id);
    // Install an externally built snapshot (e.g. pulled from remote manifest)
    // into the RootMeta version chain without performing local COW writes.
    KvError InstallExternalSnapshot(const TableIdent &tbl_ident,
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

    bool RecyclePage(MemIndexPage *page);

    /**
     * @brief Reserved head and tail for the active list. The head points to the
     * most-recently accessed, and the tail points to the least-recently
     * accessed.
     *
     */
    MemIndexPage active_head_{false};
    MemIndexPage active_tail_{false};
    MemIndexPage free_head_{false};

    /**
     * @brief A pool of index pages.
     *
     */
    std::vector<std::unique_ptr<MemIndexPage>> index_pages_;

    AsyncIoManager *io_manager_;
    MappingArena mapping_arena_;
    MappingChunkArena mapping_chunk_arena_;
    RootMetaMgr root_meta_mgr_;
};
}  // namespace eloqstore
