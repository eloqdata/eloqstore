#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "direct_io_buffer.h"
#include "error.h"
#include "storage/data_page.h"
#include "storage/index_page_manager.h"
#include "storage/object_store.h"
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
    void ResetUploadState();
    void EnsureUploadStateBuffer();
    void AddInflightUploadTask();
    void CompletePendingUploadTask(ObjectStore::UploadTask *task);
    void AddPendingUploadTask(std::unique_ptr<ObjectStore::UploadTask> task)
    {
        pending_upload_tasks_.emplace_back(std::move(task));
    }

    KvError WaitWrite();
    KvError DeleteArchive(uint64_t term, std::string_view tag);
    void ReleaseDirBusy();
    bool NeedWaitDirEviction() const
    {
        return need_wait_dir_eviction_;
    }
    void ClearNeedWaitDirEviction()
    {
        need_wait_dir_eviction_ = false;
    }
    // write_err_ record the result of the last failed write
    // request.
    KvError write_err_{KvError::NoError};

protected:
    KvError FlushManifest();
    /**
     * @brief Flush the current `cow_meta_` to manifest and publish it as the
     * new root. If @p trigger_compact is true (the default, used by
     * BatchWrite paths), also evaluate both mappers against their
     * amplification factors and set the shard pending-compact signal when
     * warranted. Background compaction itself must pass `false` to avoid
     * re-signaling compaction on the state it just produced.
     */
    KvError UpdateMeta(bool trigger_compact = true);

    /**
     * @brief Request shard to create a compaction task if space amplification
     * factor is too big.
     */
    void CompactIfNeeded(PageMapper *mapper, uint32_t amplify_factor) const;

    /**
     * @brief Pure predicate: returns true iff @p mapper warrants compaction at
     * @p amp_factor (empty mapping with non-empty space, or live-ratio above
     * the threshold). Assumes the mapper's allocator is an AppendAllocator;
     * callers are responsible for gating (e.g. opts->data_append_mode for the
     * data mapper).
     */
    static bool MapperExceedsAmplification(const PageMapper *mapper,
                                           uint32_t amp_factor);
    void TriggerTTL();
    void TriggerFileGC() const;
    KvError TriggerLocalFileGC() const;

    std::pair<DataPage, KvError> LoadDataPage(PageId page_id);
    std::pair<OverflowPage, KvError> LoadOverflowPage(PageId page_id);

    std::pair<PageId, FilePageId> AllocatePage(PageId page_id);

    /**
     * @brief Allocate a segment on cow_meta_.segment_mapper_.
     *
     * If @p page_id is MaxPageId, a fresh logical segment id is obtained via
     * PageMapper::GetPage(); otherwise the supplied logical id is remapped.
     * Stamps the term for the file that receives the new physical segment
     * (and for any file the allocator crossed into), updates the segment
     * mapping, and records the mapping delta for the manifest.
     *
     * Precondition: cow_meta_.segment_mapper_ is non-null.
     */
    std::pair<PageId, FilePageId> AllocateSegment(PageId page_id);
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
    KvError WaitPendingUploads();
    KvError ConsumePendingUploadResults();
    std::pair<FileId, uint32_t> ConvFilePageId(FilePageId file_page_id) const;
    virtual DirectIoBuffer AcquireUploadStateBuffer();
    virtual void ReleaseUploadStateBuffer(DirectIoBuffer buffer);

    // Segment mapping deltas accumulated during this write task.
    // Format: varint32(page_id) + varint64(encoded_file_page_id) pairs.
    // Serialized with a Fixed32 length prefix in FlushManifest.
    std::string seg_mapping_deltas_;
    void RecordSegmentMappingUpdate(PageId page_id, FilePageId file_page_id);
    void RecordSegmentMappingDelete(PageId page_id);

    std::optional<FileId> last_append_file_id_;
    // Mirror of last_append_file_id_ for the segment side: caches the
    // most-recently-stamped segment file id so AllocateSegment can skip
    // the branch/term GetBranchNameAndTerm lookup when consecutive
    // segments land in the same file (the common case in batched writes
    // and compaction rewrites).
    std::optional<FileId> last_seen_segment_file_id_;
    WriteBufferAggregator append_aggregator_{0};
    UploadState upload_state_;
    uint32_t inflight_upload_tasks_{0};
    WaitingSeat upload_waiting_;
    std::vector<std::unique_ptr<ObjectStore::UploadTask>> pending_upload_tasks_;
    bool dir_busy_registered_{false};
    bool need_wait_dir_eviction_{false};
};

}  // namespace eloqstore
