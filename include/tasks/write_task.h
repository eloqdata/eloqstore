#pragma once

#include <optional>
#include <string>
#include <utility>

#include "direct_io_buffer.h"
#include "error.h"
#include "storage/data_page.h"
#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "tasks/task.h"
#include "tasks/write_buffer_aggregator.h"
#include "types.h"

namespace eloqstore
{
class WriteTask : public KvTask
{
public:
    struct UploadState
    {
        DirectIoBuffer buffer;
        std::string filename;
        uint64_t start_offset{0};
        uint64_t end_offset{0};
        bool initialized{false};
        bool invalid{false};

        void ResetMetadata()
        {
            filename.clear();
            start_offset = 0;
            end_offset = 0;
            initialized = false;
            invalid = false;
            buffer.clear();
        }
    };

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

    UploadState &MutableUploadState()
    {
        return upload_state_;
    }
    const UploadState &GetUploadState() const
    {
        return upload_state_;
    }

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
    KvError TriggerLocalFileGC() const;

    std::pair<DataPage, KvError> LoadDataPage(PageId page_id);
    std::pair<OverflowPage, KvError> LoadOverflowPage(PageId page_id);

    std::pair<PageId, FilePageId> AllocatePage(PageId page_id);
    std::string_view TaskTypeName() const;
    void FreePage(PageId page_id);

    FilePageId ToFilePage(PageId page_id);

    TableIdent tbl_ident_;

    CowRootMeta cow_meta_;
    ManifestBuilder wal_builder_;

    KvError WritePage(DataPage &&page);
    KvError WritePage(OverflowPage &&page);
    KvError WritePage(MemIndexPage::Handle &page);
    KvError WritePage(MemIndexPage::Handle &page, FilePageId file_page_id);
    KvError WritePage(VarPage page, FilePageId file_page_id);
    KvError AppendWritePage(VarPage page, FilePageId file_page_id);
    void FlushAppendWrites();
    std::pair<FileId, uint32_t> ConvFilePageId(FilePageId file_page_id) const;

    // Track whether FileIdTermMapping changed in this write task.
    // If it changed, we must force a full snapshot (WAL append doesn't include
    // FileIdTermMapping).
    bool file_id_term_mapping_dirty_{false};
    std::optional<FileId> last_append_file_id_;
    WriteBufferAggregator append_aggregator_{0};
    UploadState upload_state_;
};

}  // namespace eloqstore
