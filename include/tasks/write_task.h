#pragma once

#include <vector>

#include "error.h"
#include "storage/data_page.h"
#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class WriteTask : public KvTask
{
public:
    WriteTask() = default;
    WriteTask(const WriteTask &) = delete;

    void Abort() override;
    virtual void Reset(const TableIdent &tbl_id);
    const TableIdent &TableId() const;

    /**
     * @brief The index/data page has been flushed.
     * Enqueues the page into the cache replacement list (so that the page is
     * allowed to be evicted) if it is a index page.
     */
    void WritePageCallback(VarPage page, KvError err);

    KvError WaitWrite();
    // write_err_ record the result of the last failed write
    // request.
    KvError write_err_{KvError::NoError};

protected:
    KvError FlushManifest();
    KvError UpdateMeta();

    /**
     * @brief Request shard to create a compaction task if space amplification
     * factor is too big.
     */
    void CompactIfNeeded(PageMapper *mapper) const;
    void TriggerTTL();
    void TriggerFileGC() const;

    std::pair<DataPage, KvError> LoadDataPage(PageId page_id);
    std::pair<OverflowPage, KvError> LoadOverflowPage(PageId page_id);

    std::pair<PageId, FilePageId> AllocatePage(PageId page_id);
    void FreePage(PageId page_id);

    FilePageId ToFilePage(PageId page_id);

    TableIdent tbl_ident_;

    CowRootMeta cow_meta_;
    ManifestBuilder wal_builder_;

    KvError WritePage(DataPage &&page);
    KvError WritePage(OverflowPage &&page);
    KvError WritePage(MemIndexPage *page);
    KvError WritePage(VarPage page, FilePageId file_page_id);

    KvError FlushBatchPages();
    /**
     * @brief When the append-only mode is enabled, the pages ready to be
     * written are put into this batch. The batch is then sequentially
     * written to the disk when it is full or when a data file switch is
     * required.
     */
    std::vector<VarPage> batch_pages_;
    /**
     * @brief First file page id of this batch of pages.
     */
    FilePageId batch_fp_id_{MaxFilePageId};

    // Track whether FileIdTermMapping changed in this write task.
    // If it changed, we must force a full snapshot (WAL append doesn't include
    // FileIdTermMapping).
    bool file_id_term_mapping_dirty_{false};
};

}  // namespace eloqstore
