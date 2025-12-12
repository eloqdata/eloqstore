#pragma once

#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include "async_io_manager.h"
#include "comparator.h"
#include "error.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "root_meta.h"
#include "types.h"

namespace eloqstore
{
class KvTask;
class PageMapper;
class MappingArena;

class IndexPageManager
{
public:
    IndexPageManager(AsyncIoManager *io_manager,
                     MappingArena *mapping_arena = nullptr);
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

    std::pair<RootMeta *, KvError> FindRoot(const TableIdent &tbl_ident);

    KvError MakeCowRoot(const TableIdent &tbl_ident, CowRootMeta &cow_meta);

    void UpdateRoot(const TableIdent &tbl_ident, CowRootMeta new_meta);

    std::pair<MemIndexPage *, KvError> FindPage(MappingSnapshot *mapping,
                                                PageId page_id);

    void FreeMappingSnapshot(MappingSnapshot *mapping);

    void Unswizzling(MemIndexPage *page);

    void FinishIo(MappingSnapshot *mapping, MemIndexPage *idx_page);

    // Given the table id, tree root and the input key, returns the logical page
    // id of the data page that might contain the key.
    KvError SeekIndex(MappingSnapshot *mapping,
                      PageId page_id,
                      std::string_view key,
                      PageId &result);

    KvError SeekIndex(MappingSnapshot *mapping,
                      PageId page_id,
                      std::string_view key,
                      std::span<PageId> results,
                      size_t &result_size);

    const KvOptions *Options() const;
    AsyncIoManager *IoMgr() const;
    MappingArena *MapperArena() const;

    void EvictRootIfEmpty(const TableIdent &tbl_id);

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

    /**
     * @brief Evicts the root entry from the root table, if (1) the current root
     * page has been evicted, (2) no mapping snapshot of the table is active,
     * and (3) all pages belonging to the tree have been evicted.
     *
     * @param root_it
     */
    void EvictRootIfEmpty(
        std::unordered_map<TableIdent, RootMeta>::iterator root_it);

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

    /**
     * @brief Cached roots, which maps a table identity to an in-memory
     * page.
     *
     */
    std::unordered_map<TableIdent, RootMeta> tbl_roots_;

    AsyncIoManager *io_manager_;
    MappingArena *mapping_arena_;
};
}  // namespace eloqstore
