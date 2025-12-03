#include "write_task.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "data_page.h"
#include "error.h"
#include "file_gc.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "shard.h"
#include "types.h"
#include "utils.h"

namespace eloqstore
{
const TableIdent &WriteTask::TableId() const
{
    return tbl_ident_;
}

void WriteTask::Reset(const TableIdent &tbl_id)
{
    tbl_ident_ = tbl_id;
    write_err_ = KvError::NoError;
    wal_builder_.Reset();
    batch_pages_.clear();
}

void WriteTask::Abort()
{
    if (!Options()->data_append_mode)
    {
        IoMgr()->AbortWrite(tbl_ident_);
    }

    if (cow_meta_.old_mapping_ != nullptr)
    {
        // Cancel all free file page operations.
        cow_meta_.old_mapping_->ClearFreeFilePage();
        cow_meta_.old_mapping_ = nullptr;
    }
    cow_meta_.mapper_ = nullptr;
    if (cow_meta_.manifest_size_ == 0)
    {
        // MakeCowRoot will create a empty RootMeta if partition not found
        shard->IndexManager()->EvictRootIfEmpty(tbl_ident_);
    }
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

KvError WriteTask::WritePage(MemIndexPage *page)
{
    SetChecksum({page->PagePtr(), Options()->data_page_size});
    auto [page_id, file_page_id] = AllocatePage(page->GetPageId());
    page->SetPageId(page_id);
    page->SetFilePageId(file_page_id);
    return WritePage(page, file_page_id);
}

KvError WriteTask::WritePage(VarPage page, FilePageId file_page_id)
{
    const KvOptions *opts = Options();
    assert(ValidateChecksum({VarPagePtr(page), opts->data_page_size}));
    KvError err;
    if (opts->data_append_mode)
    {
        batch_pages_.emplace_back(std::move(page));
        if (batch_pages_.size() == 1)
        {
            batch_fp_id_ = file_page_id;
        }
        // Flush the current batch when it is full, or when a data file switch
        // is required.
        size_t mask = opts->FilePageOffsetMask();
        if (batch_pages_.size() >= opts->max_write_batch_pages ||
            (file_page_id & mask) == mask)
        {
            err = FlushBatchPages();
            CHECK_KV_ERR(err);
        }
    }
    else
    {
        err = IoMgr()->WritePage(tbl_ident_, std::move(page), file_page_id);
        CHECK_KV_ERR(err);
        if (inflight_io_ >= opts->max_write_batch_pages)
        {
            // Avoid long running WriteTask block ReadTask/ScanTask
            err = WaitWrite();
            CHECK_KV_ERR(err);
        }
    }
    return KvError::NoError;
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
        MemIndexPage *idx_page = std::get<MemIndexPage *>(page);
        if (err == KvError::NoError)
        {
            shard->IndexManager()->FinishIo(cow_meta_.mapper_->GetMapping(),
                                            idx_page);
        }
        else
        {
            shard->IndexManager()->FreeIndexPage(idx_page);
        }
        break;
    }
    case VarPageType::DataPage:
    case VarPageType::OverflowPage:
    case VarPageType::Page:
        break;
    }
}

KvError WriteTask::FlushBatchPages()
{
    assert(!batch_pages_.empty());
    assert(batch_fp_id_ != MaxFilePageId);
    assert(Options()->data_append_mode);
    KvError err = IoMgr()->WritePages(tbl_ident_, batch_pages_, batch_fp_id_);
    for (VarPage &page : batch_pages_)
    {
        WritePageCallback(std::move(page), err);
    }
    batch_pages_.clear();
    batch_fp_id_ = MaxFilePageId;
    return err;
}

KvError WriteTask::WaitWrite()
{
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
    FilePageId file_page_id = cow_meta_.mapper_->FilePgAllocator()->Allocate();
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
    if (wal_builder_.Empty())
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
    if (!dict_dirty && manifest_size > 0 &&
        manifest_size + wal_builder_.CurrentSize() <= opts->manifest_limit)
    {
        std::string_view blob =
            wal_builder_.Finalize(cow_meta_.root_id_, cow_meta_.ttl_root_id_);
        err = IoMgr()->AppendManifest(tbl_ident_, blob, manifest_size);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ += blob.size();
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
                                  dict_bytes);
        err = IoMgr()->SwitchManifest(tbl_ident_, snapshot);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ = snapshot.size();
        cow_meta_.compression_->ClearDirty();
    }
    return KvError::NoError;
}

KvError WriteTask::UpdateMeta()
{
    KvError err;
    const KvOptions *opts = Options();
    // Flush data pages.
    if (opts->data_append_mode)
    {
        if (!batch_pages_.empty())
        {
            err = FlushBatchPages();
            CHECK_KV_ERR(err);
        }
    }
    else
    {
        err = WaitWrite();
        CHECK_KV_ERR(err);
    }

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
        return;
    }
}

void WriteTask::TriggerTTL()
{
    if (shard->HasPendingTTL(tbl_ident_))
    {
        return;
    }

    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    if (err != KvError::NoError)
    {
        return;
    }
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

    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    if (err != KvError::NoError)
    {
        return;
    }

    std::unordered_set<FileId> retained_files;
    const uint8_t shift = Options()->pages_per_file_shift;
    for (MappingSnapshot *mapping : meta->mapping_snapshots_)
    {
        GetRetainedFiles(retained_files, mapping->mapping_tbl_, shift);
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
