#include "tasks/write_task.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "async_io_manager.h"
#include "error.h"
#include "file_gc.h"
#include "storage/data_page.h"
#include "storage/index_page_manager.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "storage/shard.h"
#include "types.h"
#include "utils.h"

namespace eloqstore
{
namespace
{
KvError BuildRetainedFiles(const TableIdent &tbl_id,
                           absl::flat_hash_set<FileId> &retained_files,
                           std::vector<MappingSnapshot::Ref> &snapshot_array)
{
    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    RootMeta *meta = root_handle.Get();
    const uint8_t shift = Options()->pages_per_file_shift;
    size_t approx_file_cnt = 0;
    snapshot_array.clear();
    snapshot_array.reserve(meta->mapping_snapshots_.size());
    for (MappingSnapshot *mapping : meta->mapping_snapshots_)
    {
        const size_t page_cnt = mapping->mapping_tbl_.size();
        const size_t file_cnt = (page_cnt >> shift) + 1;
        if (file_cnt > approx_file_cnt)
        {
            approx_file_cnt = file_cnt;
        }
        snapshot_array.emplace_back(MappingSnapshot::Ref(mapping));
    }
    retained_files.clear();
    retained_files.reserve(approx_file_cnt);
    for (const MappingSnapshot::Ref &mapping : snapshot_array)
    {
        GetRetainedFiles(retained_files, mapping->mapping_tbl_, shift);
        ThdTask()->YieldToLowPQ();
    }
    return KvError::NoError;
}
}  // namespace
std::string_view WriteTask::TaskTypeName() const
{
    switch (Type())
    {
    case TaskType::Read:
        return "Read";
    case TaskType::BatchWrite:
        return "BatchWrite";
    case TaskType::BackgroundWrite:
        return "BackgroundWrite";
    case TaskType::EvictFile:
        return "EvictFile";
    case TaskType::Prewarm:
        return "Prewarm";
    case TaskType::Scan:
        return "Scan";
    case TaskType::ListObject:
        return "ListObject";
    case TaskType::Reopen:
        return "Reopen";
    default:
        return "Unknown";
    }
}

const TableIdent &WriteTask::TableId() const
{
    return tbl_ident_;
}

void WriteTask::Reset(const TableIdent &tbl_id)
{
    tbl_ident_ = tbl_id;
    write_err_ = KvError::NoError;
    wal_builder_.Reset();
    file_id_term_mapping_dirty_ = false;
    last_append_file_id_.reset();
    cow_meta_ = CowRootMeta();
    size_t buf_size = Options()->write_buffer_size;
    if (buf_size == 0)
    {
        buf_size = 1 * MB;
    }
    append_aggregator_ = WriteBufferAggregator(buf_size);
    append_aggregator_.Reset();
    upload_state_.ResetMetadata();
    if (!Options()->cloud_store_path.empty())
    {
        upload_state_.buffer.EnsureDefaultReserve();
    }
}

void WriteTask::Abort()
{
    LOG(INFO) << "WriteTask to " << tbl_ident_ << " is aborted";
    if (Options()->data_append_mode)
    {
        // Drain pending async writes before task is freed.
        (void) WaitWrite();
    }
    // Always invoke AbortWrite so CloudStoreMgr can clear per-table upload
    // segments and io manager can reset dirty state.
    IoMgr()->AbortWrite(tbl_ident_);

    if (cow_meta_.old_mapping_ != nullptr)
    {
        // Cancel all free file page operations.
        cow_meta_.old_mapping_->ClearFreeFilePage();
    }
    cow_meta_ = CowRootMeta();
    last_append_file_id_.reset();
    upload_state_.ResetMetadata();
}

KvError WriteTask::WritePage(DataPage &&page)
{
    SetChecksum({page.PagePtr(), Options()->data_page_size});
    auto [_, fp_id] = AllocatePage(page.GetPageId());
    return WritePage(std::move(page), fp_id);
}

KvError WriteTask::WritePage(OverflowPage &&page)
{
    SetChecksum({page.PagePtr(), Options()->data_page_size});
    auto [_, fp_id] = AllocatePage(page.GetPageId());
    return WritePage(std::move(page), fp_id);
}

KvError WriteTask::WritePage(MemIndexPage::Handle &page)
{
    SetChecksum({page->PagePtr(), Options()->data_page_size});
    auto [page_id, file_page_id] = AllocatePage(page->GetPageId());
    page->SetPageId(page_id);
    page->SetFilePageId(file_page_id);
    return WritePage(page, file_page_id);
}

KvError WriteTask::WritePage(MemIndexPage::Handle &page,
                             FilePageId file_page_id)
{
    SetChecksum({page->PagePtr(), Options()->data_page_size});
    // Create a temporary handle for VarPage to keep pinning during IO.
    MemIndexPage::Handle io_handle(page.Get());
    return WritePage(VarPage(std::move(io_handle)), file_page_id);
}

KvError WriteTask::WritePage(VarPage page, FilePageId file_page_id)
{
    const KvOptions *opts = Options();
    assert(ValidateChecksum({VarPagePtr(page), opts->data_page_size}));
    if (opts->data_append_mode && IoMgr()->HasWriteBufferPool())
    {
        return AppendWritePage(std::move(page), file_page_id);
    }

    KvError err = IoMgr()->WritePage(tbl_ident_, std::move(page), file_page_id);
    CHECK_KV_ERR(err);
    if (inflight_io_ >= opts->max_write_batch_pages)
    {
        // Avoid long running WriteTask block ReadTask/ScanTask
        err = WaitWrite();
        CHECK_KV_ERR(err);
    }
    else
    {
        YieldToLowPQ();
    }
    return KvError::NoError;
}

std::pair<FileId, uint32_t> WriteTask::ConvFilePageId(
    FilePageId file_page_id) const
{
    FileId file_id = file_page_id >> Options()->pages_per_file_shift;
    uint32_t offset =
        (file_page_id & (uint32_t{1} << Options()->pages_per_file_shift) - 1) *
        Options()->data_page_size;
    return {file_id, offset};
}

KvError WriteTask::AppendWritePage(VarPage page, FilePageId file_page_id)
{
    const KvOptions *opts = Options();
    const size_t page_size = opts->data_page_size;
    auto [file_id, offset] = ConvFilePageId(file_page_id);
    const bool cloud_append_mode =
        opts->data_append_mode && !opts->cloud_store_path.empty();

    char *page_ptr = VarPagePtr(page);
    if (!append_aggregator_.HasBuffer() ||
        !append_aggregator_.CanAppend(file_id, offset, page_size))
    {
        const bool file_switched = cloud_append_mode &&
                                   last_append_file_id_.has_value() &&
                                   last_append_file_id_.value() != file_id;
        const FileId sealed_file_id =
            file_switched ? last_append_file_id_.value() : file_id;
        // Flush any pending writes in the aggregator
        FlushAppendWrites();
        // In cloud append mode, trigger immediate upload of sealed file
        // This ensures sealed data files are uploaded promptly
        if (file_switched)
        {
            // Wait for flush to complete before uploading
            KvError err = WaitWrite();
            CHECK_KV_ERR(err);
            // Trigger upload of the sealed file (may use in-memory segments)
            err = IoMgr()->OnDataFileSealed(tbl_ident_, sealed_file_id);
            CHECK_KV_ERR(err);
        }
        uint16_t buf_index = 0;
        char *buf = IoMgr()->AcquireWriteBuffer(buf_index);
        if (buf == nullptr)
        {
            return KvError::OutOfMem;
        }
        bool use_fixed = IoMgr()->WriteBufferUseFixed();
        append_aggregator_.SetBuffer(
            buf, buf_index, file_id, offset, use_fixed);
    }

    char *dst = append_aggregator_.TryReserve(file_id, offset, page_size);
    if (dst == nullptr)
    {
        return KvError::OutOfMem;
    }
    std::memcpy(dst, page_ptr, page_size);

    append_aggregator_.AddPage(std::move(page), nullptr, 0);
    last_append_file_id_ = file_id;

    if (append_aggregator_.ShouldFlush(page_size))
    {
        FlushAppendWrites();
    }

    YieldToLowPQ();
    return KvError::NoError;
}

void WriteTask::FlushAppendWrites()
{
    if (!append_aggregator_.HasData())
    {
        return;
    }
    WriteBufferBatch batch = append_aggregator_.TakeBatch();
    if (batch.bytes == 0)
    {
        if (batch.buffer != nullptr)
        {
            IoMgr()->ReleaseWriteBuffer(batch.buffer, batch.buffer_index);
        }
        return;
    }

    KvError err = IoMgr()->SubmitMergedWrite(tbl_ident_,
                                             batch.file_id,
                                             batch.start_offset,
                                             batch.buffer,
                                             batch.bytes,
                                             batch.buffer_index,
                                             batch.pages,
                                             batch.release_ptrs,
                                             batch.release_indices,
                                             batch.use_fixed);
    if (err != KvError::NoError)
    {
        for (VarPage &page : batch.pages)
        {
            WritePageCallback(std::move(page), err);
        }
        IoMgr()->ReleaseWriteBuffer(batch.buffer, batch.buffer_index);
        for (size_t i = 0; i < batch.release_ptrs.size(); ++i)
        {
            if (batch.release_ptrs[i] != nullptr)
            {
                IoMgr()->ReleaseWriteBuffer(batch.release_ptrs[i],
                                            batch.release_indices[i]);
            }
        }
        write_err_ = err;
    }
}

void WriteTask::WritePageCallback(VarPage page, KvError err)
{
    if (err != KvError::NoError)
    {
        write_err_ = err;
    }

    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
    {
        MemIndexPage::Handle &handle = std::get<MemIndexPage::Handle>(page);
        MemIndexPage *idx_page = handle.Get();
        if (err == KvError::NoError)
        {
            shard->IndexManager()->FinishIo(cow_meta_.mapper_->GetMapping(),
                                            idx_page);
        }
        else
        {
            // Only free if it's still detached (i.e., not in active list).
            if (idx_page->IsDetached())
            {
                handle.Reset();
                CHECK(!idx_page->IsPinned());
                shard->IndexManager()->FreeIndexPage(idx_page);
            }
        }
        break;
    }
    case VarPageType::DataPage:
    case VarPageType::OverflowPage:
    case VarPageType::Page:
        break;
    }
}

KvError WriteTask::WaitWrite()
{
    if (Options()->data_append_mode && IoMgr()->HasWriteBufferPool())
    {
        FlushAppendWrites();
    }
    WaitIo();
    KvError err = write_err_;
    write_err_ = KvError::NoError;
    return err;
}

std::pair<PageId, FilePageId> WriteTask::AllocatePage(PageId page_id)
{
    if (!Options()->data_append_mode && page_id != MaxPageId)
    {
        FilePageId old_fp_id = ToFilePage(page_id);
        if (old_fp_id != MaxFilePageId)
        {
            // The page is mapped to a new file page. The old file page will be
            // recycled. However, the old file page shall only be recycled when
            // the old mapping snapshot is destructed, i.e., no one is using the
            // old mapping.
            cow_meta_.old_mapping_->AddFreeFilePage(old_fp_id);
        }
    }

    if (page_id == MaxPageId)
    {
        page_id = cow_meta_.mapper_->GetPage();
    }

    FileId file_id_before_allocate =
        cow_meta_.mapper_->FilePgAllocator()->CurrentFileId();
    FilePageId file_page_id = cow_meta_.mapper_->FilePgAllocator()->Allocate();
    FileId file_id_after_allocate =
        cow_meta_.mapper_->FilePgAllocator()->CurrentFileId();
    if (!IoMgr()
             ->GetFileIdTerm(tbl_ident_, file_id_before_allocate)
             .has_value())
    {
        IoMgr()->SetFileIdTerm(
            tbl_ident_, file_id_before_allocate, IoMgr()->ProcessTerm());
        file_id_term_mapping_dirty_ = true;
    }
    if (file_id_before_allocate != file_id_after_allocate)
    {
        IoMgr()->SetFileIdTerm(
            tbl_ident_, file_id_after_allocate, IoMgr()->ProcessTerm());
        file_id_term_mapping_dirty_ = true;
    }

    cow_meta_.mapper_->UpdateMapping(page_id, file_page_id);
    wal_builder_.UpdateMapping(page_id, file_page_id);
    return {page_id, file_page_id};
}

void WriteTask::FreePage(PageId page_id)
{
    if (!Options()->data_append_mode)
    {
        // Free file page.
        FilePageId file_page = ToFilePage(page_id);
        cow_meta_.old_mapping_->AddFreeFilePage(file_page);
    }
    cow_meta_.mapper_->FreePage(page_id);
    wal_builder_.DeleteMapping(page_id);
}

FilePageId WriteTask::ToFilePage(PageId page_id)
{
    return cow_meta_.mapper_->GetMapping()->ToFilePage(page_id);
}

KvError WriteTask::FlushManifest()
{
    // If wal_builder_ is empty but roots are MaxPageId and we have existing
    // manifest, we need to write a snapshot to mark the partition as empty
    bool need_empty_snapshot =
        wal_builder_.Empty() && cow_meta_.root_id_ == MaxPageId &&
        cow_meta_.ttl_root_id_ == MaxPageId && cow_meta_.manifest_size_ > 0;

    if (wal_builder_.Empty() && !need_empty_snapshot)
    {
        return KvError::NoError;
    }

    const KvOptions *opts = Options();
    KvError err;
    uint64_t manifest_size = cow_meta_.manifest_size_;
    std::string_view dict_bytes;
    CHECK(cow_meta_.compression_ != nullptr);
    if (cow_meta_.compression_->HasDictionary())
    {
        const std::string &dict_vec = cow_meta_.compression_->DictionaryBytes();
        dict_bytes = {dict_vec.data(), dict_vec.size()};
    }
    const bool dict_dirty = cow_meta_.compression_->Dirty();

    // Serialize FileIdTermMapping for this table.
    std::string term_buf;
    std::shared_ptr<FileIdTermMapping> file_term_mapping =
        IoMgr()->GetOrCreateFileIdTermMapping(tbl_ident_);
    file_term_mapping->insert_or_assign(IouringMgr::LruFD::kManifest,
                                        IoMgr()->ProcessTerm());
    SerializeFileIdTermMapping(*file_term_mapping, term_buf);
    YieldToLowPQ();

    if (need_empty_snapshot)
    {
        // Write a snapshot with empty roots and empty mapping
        MappingSnapshot *mapping = cow_meta_.mapper_->GetMapping();
        FilePageId max_fp_id =
            cow_meta_.mapper_->FilePgAllocator()->MaxFilePageId();
        std::string_view snapshot =
            wal_builder_.Snapshot(cow_meta_.root_id_,
                                  cow_meta_.ttl_root_id_,
                                  mapping,
                                  max_fp_id,
                                  dict_bytes,
                                  term_buf);
        err = IoMgr()->SwitchManifest(tbl_ident_, snapshot);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ = snapshot.size();
        cow_meta_.compression_->ClearDirty();
        return KvError::NoError;
    }

    const size_t alignment = page_align;
    const uint64_t log_physical_size =
        (wal_builder_.CurrentSize() + term_buf.size() + alignment - 1) &
        ~(alignment - 1);

    if (!dict_dirty && manifest_size > 0 &&
        manifest_size + log_physical_size <= opts->manifest_limit)
    {
        wal_builder_.AppendFileIdTermMapping(term_buf);
        std::string_view blob =
            wal_builder_.Finalize(cow_meta_.root_id_, cow_meta_.ttl_root_id_);
        err = IoMgr()->AppendManifest(tbl_ident_, blob, manifest_size);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ += log_physical_size;
    }
    else
    {
        MappingSnapshot *mapping = cow_meta_.mapper_->GetMapping();
        FilePageId max_fp_id =
            cow_meta_.mapper_->FilePgAllocator()->MaxFilePageId();
        std::string_view snapshot =
            wal_builder_.Snapshot(cow_meta_.root_id_,
                                  cow_meta_.ttl_root_id_,
                                  mapping,
                                  max_fp_id,
                                  dict_bytes,
                                  term_buf);
        err = IoMgr()->SwitchManifest(tbl_ident_, snapshot);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ = snapshot.size();
        cow_meta_.compression_->ClearDirty();
        file_id_term_mapping_dirty_ = false;
    }
    return KvError::NoError;
}

KvError WriteTask::UpdateMeta()
{
    // Flush data pages.
    KvError err = WaitWrite();
    CHECK_KV_ERR(err);

    err = IoMgr()->SyncData(tbl_ident_);
    CHECK_KV_ERR(err);

    // Update meta data in storage and then in memory.
    err = FlushManifest();
    CHECK_KV_ERR(err);

    // Hooks after modified partition.
    CompactIfNeeded(cow_meta_.mapper_.get());

    shard->IndexManager()->UpdateRoot(tbl_ident_, std::move(cow_meta_));
    return KvError::NoError;
}

void WriteTask::CompactIfNeeded(PageMapper *mapper) const
{
    const KvOptions *opts = Options();
    if (!opts->data_append_mode || opts->file_amplify_factor == 0 ||
        Type() != TaskType::BatchWrite || shard->HasPendingCompact(tbl_ident_))
    {
        return;
    }

    auto allocator = static_cast<AppendAllocator *>(mapper->FilePgAllocator());
    uint32_t mapping_cnt = mapper->MappingCount();
    size_t space_size = allocator->SpaceSize();
    assert(space_size >= mapping_cnt);
    // When both mapping_cnt and space_size are 0, compaction should NOT be
    // triggered. This indicates that the manifest does not exist yet, or the
    // table has not been initialized.

    // Two cases trigger compaction:
    // (1) The table has been completely cleared (mapping_cnt == 0 but
    // space_size > 0); (2) The space amplification factor has been exceeded.
    if ((mapping_cnt == 0 && space_size != 0) ||
        (space_size >= allocator->PagesPerFile() &&
         static_cast<double>(space_size) / static_cast<double>(mapping_cnt) >
             static_cast<double>(opts->file_amplify_factor)))
    {
        shard->AddPendingCompact(tbl_ident_);
    }
}

void WriteTask::TriggerTTL()
{
    if (shard->HasPendingTTL(tbl_ident_))
    {
        return;
    }

    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    if (err != KvError::NoError)
    {
        return;
    }
    RootMeta *meta = root_handle.Get();
    if (meta->next_expire_ts_ == 0)
    {
        return;
    }
    const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
    if (meta->next_expire_ts_ <= now_ts)
    {
        shard->AddPendingTTL(tbl_ident_);
    }
}

void WriteTask::TriggerFileGC() const
{
    assert(Options()->data_append_mode);

    absl::flat_hash_set<FileId> retained_files;
    std::vector<MappingSnapshot::Ref> snapshot_array;
    KvError build_err =
        BuildRetainedFiles(tbl_ident_, retained_files, snapshot_array);
    if (build_err != KvError::NoError)
    {
        LOG(ERROR) << "BuildRetainedFiles failed for table "
                   << tbl_ident_.ToString()
                   << " err=" << static_cast<int>(build_err);
        return;
    }

    // Check if we're in cloud mode or local mode
    if (!Options()->cloud_store_path.empty())
    {
        // Cloud mode: execute GC directly
        CloudStoreMgr *cloud_mgr =
            static_cast<CloudStoreMgr *>(shard->IoManager());
        if (!cloud_mgr)
        {
            LOG(ERROR) << "CloudStoreMgr not available";
            return;
        }

        KvError gc_err = FileGarbageCollector::ExecuteCloudGC(
            tbl_ident_, retained_files, cloud_mgr);

        if (gc_err != KvError::NoError)
        {
            LOG(ERROR) << "Cloud GC failed for table " << tbl_ident_.ToString();
        }
    }
    else
    {
        // Local mode: execute GC directly
        DLOG(INFO) << "Begin GC in Local mode";
        IouringMgr *io_mgr = static_cast<IouringMgr *>(shard->IoManager());
        KvError gc_err = FileGarbageCollector::ExecuteLocalGC(
            tbl_ident_, retained_files, io_mgr);

        if (gc_err != KvError::NoError)
        {
            LOG(ERROR) << "Local GC failed for table " << tbl_ident_.ToString();
        }
    }
}

KvError WriteTask::TriggerLocalFileGC() const
{
    assert(Options()->data_append_mode);
    absl::flat_hash_set<FileId> retained_files;
    std::vector<MappingSnapshot::Ref> snapshot_array;
    KvError build_err =
        BuildRetainedFiles(tbl_ident_, retained_files, snapshot_array);
    CHECK_KV_ERR(build_err);
    IouringMgr *io_mgr = static_cast<IouringMgr *>(shard->IoManager());
    KvError gc_err = FileGarbageCollector::ExecuteLocalGC(
        tbl_ident_, retained_files, io_mgr);
    if (gc_err != KvError::NoError)
    {
        LOG(ERROR) << "Local GC failed for table " << tbl_ident_.ToString();
    }
    return gc_err;
}

std::pair<DataPage, KvError> WriteTask::LoadDataPage(PageId page_id)
{
    return ::eloqstore::LoadDataPage(tbl_ident_, page_id, ToFilePage(page_id));
}

std::pair<OverflowPage, KvError> WriteTask::LoadOverflowPage(PageId page_id)
{
    return ::eloqstore::LoadOverflowPage(
        tbl_ident_, page_id, ToFilePage(page_id));
}

}  // namespace eloqstore
