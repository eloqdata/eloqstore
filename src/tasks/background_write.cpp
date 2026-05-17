#include "tasks/background_write.h"

#include <algorithm>
#include <array>
#include <memory>  // for std::shared_ptr
#include <string>

#include "global_registered_memory.h"
#include "storage/mem_index_page.h"
#include "storage/shard.h"
#include "utils.h"

namespace eloqstore
{
class MovingCachedPages
{
public:
    MovingCachedPages(size_t cap)
    {
        pages_.reserve(cap);
    }
    ~MovingCachedPages()
    {
        // Moving operations are aborted
        for (auto &entry : pages_)
        {
            entry.handle->SetFilePageId(entry.src_fp_id);
        }
    }
    void Add(MemIndexPage::Handle handle, FilePageId src_fp_id)
    {
        pages_.push_back({std::move(handle), src_fp_id});
    }
    void Finish()
    {
        // Moving operations are succeed
        for (auto &entry : pages_)
        {
            entry.handle.Reset();
        }
        pages_.clear();
    }

private:
    struct Entry
    {
        MemIndexPage::Handle handle;
        FilePageId src_fp_id;
    };
    std::vector<Entry> pages_;
};

namespace
{
bool FilePageLess(const std::pair<FilePageId, PageId> &lhs,
                  const std::pair<FilePageId, PageId> &rhs)
{
    if (lhs.first == rhs.first)
    {
        return lhs.second < rhs.second;
    }
    return lhs.first < rhs.first;
}
}  // namespace

void BackgroundWrite::HeapSortFpIdsWithYield(
    std::vector<std::pair<FilePageId, PageId>> &fp_ids)
{
    if (fp_ids.size() < 2)
    {
        return;
    }

    constexpr size_t push_batch = 1 << 8;
    constexpr size_t pop_batch = 1 << 8;

    size_t push_ops = 0;
    for (size_t next = 1; next < fp_ids.size(); ++next)
    {
        std::push_heap(fp_ids.begin(), fp_ids.begin() + next + 1, FilePageLess);
        push_ops++;
        if ((push_ops & (push_batch - 1)) == 0)
        {
            YieldToLowPQ();
        }
    }

    size_t pop_ops = 0;
    for (size_t count = fp_ids.size(); count > 1; --count)
    {
        std::pop_heap(fp_ids.begin(), fp_ids.begin() + count, FilePageLess);
        pop_ops++;
        if ((pop_ops & (pop_batch - 1)) == 0)
        {
            YieldToLowPQ();
        }
    }
}

KvError BackgroundWrite::Compact()
{
    const KvOptions *opts = Options();

    LOG(INFO) << "begin compaction on " << tbl_ident_;
    KvError err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);

    // Short-circuit when there is nothing to compact and nothing to GC:
    // both mappings empty AND both allocators report zero space. This
    // happens when a stale CompactRequest (queued legitimately by a
    // writer's CompactIfNeeded while the partition still had work) gets
    // dispatched *after* something else already wiped the partition --
    // typically an ArchiveRequest whose CreateArchive->needs_compact ran
    // an inline Compact and TriggerFileGC, including rmdir'ing the
    // partition directory. Without this guard the stale CompactRequest
    // would call TriggerFileGC -> ExecuteLocalGC -> ListLocalFiles on
    // the rmdir'd directory and SIGABRT from a filesystem_error.
    const bool data_clean =
        !opts->data_append_mode ||
        (cow_meta_.mapper_->MappingCount() == 0 &&
         static_cast<AppendAllocator *>(cow_meta_.mapper_->FilePgAllocator())
                 ->SpaceSize() == 0);
    const bool seg_clean = cow_meta_.segment_mapper_ == nullptr ||
                           (cow_meta_.segment_mapper_->MappingCount() == 0 &&
                            static_cast<AppendAllocator *>(
                                cow_meta_.segment_mapper_->FilePgAllocator())
                                    ->SpaceSize() == 0);
    if (data_clean && seg_clean)
    {
        LOG(INFO) << "skip compaction on " << tbl_ident_
                  << " (no work; stale CompactRequest after prior wipe)";
        return KvError::NoError;
    }

    // Data pages only participate when the partition is in data-append mode;
    // otherwise the data allocator is not an AppendAllocator and this
    // compaction model (min_file_id / empty_file_cnt statistics, tail rewrite)
    // does not apply. Segments are independent: EnsureSegmentMapper always
    // installs an AppendAllocator, so segment compaction runs whenever the
    // segment mapping exceeds its amplification threshold, regardless of the
    // data-page mode.
    MovingCachedPages moving_cached(
        opts->data_append_mode ? cow_meta_.mapper_->MappingCount() : 0);
    if (opts->data_append_mode)
    {
        err = DoCompactDataFile(moving_cached);
        CHECK_KV_ERR(err);
    }
    if (cow_meta_.segment_mapper_ != nullptr)
    {
        err = DoCompactSegmentFile();
        CHECK_KV_ERR(err);
    }

    // Suppress the compact-trigger side effect of UpdateMeta: the background
    // compaction just produced this state and must not re-signal itself.
    err = UpdateMeta(/*trigger_compact=*/false);
    CHECK_KV_ERR(err);
    moving_cached.Finish();
    TriggerFileGC();
    LOG(INFO) << "finish compaction on " << tbl_ident_;
    return KvError::NoError;
}

KvError BackgroundWrite::DoCompactDataFile(MovingCachedPages &moving_cached)
{
    const KvOptions *opts = Options();
    PageMapper *mapper = cow_meta_.mapper_.get();
    auto *allocator = static_cast<AppendAllocator *>(mapper->FilePgAllocator());
    const uint32_t mapping_cnt = mapper->MappingCount();
    const uint32_t pages_per_file = allocator->PagesPerFile();
    const double file_saf_limit = opts->file_amplify_factor;

    // Get all file page ids that are used by this version.
    std::vector<std::pair<FilePageId, PageId>> fp_ids;
    fp_ids.reserve(mapping_cnt);
    size_t tbl_size = mapper->GetMapping()->mapping_tbl_.size();
    for (PageId page_id = 0; page_id < tbl_size; page_id++)
    {
        FilePageId fp_id = ToFilePage(page_id);
        if (fp_id != MaxFilePageId)
        {
            fp_ids.emplace_back(fp_id, page_id);
        }
        if ((page_id & 0xFF) == 0)
        {
            YieldToLowPQ();
        }
    }
    YieldToLowPQ();
    assert(fp_ids.size() == mapping_cnt);

    // Empty mapping: forfeit the tail file too. The upcoming TriggerFileGC
    // will delete every data file on disk (retained_files is empty), so
    // advance min_file_id past the tail and let UpdateStat snap max_fp_id_
    // up to that boundary. Without this, SpaceSize stays at the partial
    // tail (max_fp_id mod pages_per_file), MapperExceedsAmplification keeps
    // firing on mapping_cnt==0 && space_size>0, and the next archive tick
    // would re-enter Compact -> TriggerFileGC on a rmdir'd partition.
    if (fp_ids.empty())
    {
        if (allocator->SpaceSize() > 0)
        {
            const uint32_t mask = (1u << opts->pages_per_file_shift) - 1;
            const FileId next_file =
                static_cast<FileId>((allocator->MaxFilePageId() + mask) >>
                                    opts->pages_per_file_shift);
            allocator->UpdateStat(next_file, 0);
        }
        return KvError::NoError;
    }

    HeapSortFpIdsWithYield(fp_ids);
    YieldToLowPQ();

    constexpr uint8_t max_move_batch = max_read_pages_batch;
    std::vector<Page> move_batch_buf;
    move_batch_buf.reserve(max_move_batch);
    YieldToLowPQ();
    std::vector<FilePageId> move_batch_fp_ids;
    move_batch_fp_ids.reserve(max_move_batch);

    KvError err = KvError::NoError;
    auto it_low = fp_ids.begin();
    auto it_high = fp_ids.begin();
    FileId begin_file_id = fp_ids.front().first >> opts->pages_per_file_shift;
    // Do not compact the data file that is currently being written to and is
    // not yet full.
    const FileId end_file_id = allocator->CurrentFileId();
    FileId min_file_id = end_file_id;
    uint32_t empty_file_cnt = 0;
    size_t round_cnt = 0;
    for (FileId file_id = begin_file_id; file_id < end_file_id; file_id++)
    {
        if ((round_cnt & 0xFF) == 0)
        {
            YieldToLowPQ();
            round_cnt = 0;
        }
        FilePageId end_fp_id = (file_id + 1) << opts->pages_per_file_shift;
        while (it_high != fp_ids.end() && it_high->first < end_fp_id)
        {
            ++round_cnt;
            it_high++;
        }
        if (it_low == it_high)
        {
            if (min_file_id != end_file_id)
            {
                empty_file_cnt++;
            }
            // This file has no pages referenced by the latest mapping.
            continue;
        }

        if (double factor = double(pages_per_file) / double(it_high - it_low);
            factor <= file_saf_limit)
        {
            // This file don't need compaction.
            if (min_file_id == end_file_id)
            {
                // Record the oldest file that don't need compaction.
                min_file_id = file_id;
            }
            it_low = it_high;
            continue;
        }

        // Compact this data file, copy all pages in this file to the back.
        for (auto it = it_low; it < it_high; it += max_move_batch)
        {
            YieldToLowPQ();
            uint32_t batch_size = std::min(long(max_move_batch), it_high - it);
            const std::span<std::pair<FilePageId, PageId>> batch_ids(
                it, batch_size);
            // Read original pages.
            move_batch_fp_ids.clear();
            for (auto [fp_id, page_id] : batch_ids)
            {
                MemIndexPage::Handle handle =
                    cow_meta_.old_mapping_->GetSwizzlingHandle(page_id);
                if (handle && !handle->IsDetached())
                {
                    auto [_, new_fp_id] = AllocatePage(page_id);
                    FilePageId src_fp_id = handle->GetFilePageId();
                    handle->SetFilePageId(new_fp_id);
                    err = WritePage(handle, new_fp_id);
                    CHECK_KV_ERR(err);
                    moving_cached.Add(std::move(handle), src_fp_id);
                }
                else
                {
                    move_batch_fp_ids.emplace_back(fp_id);
                }
            }
            if (move_batch_fp_ids.empty())
            {
                continue;
            }
            err = IoMgr()->ReadPages(
                tbl_ident_, move_batch_fp_ids, move_batch_buf);
            CHECK_KV_ERR(err);
            // Write these pages to the new file.
            for (uint32_t i = 0; auto [fp_id, page_id] : batch_ids)
            {
                if (i == move_batch_fp_ids.size())
                {
                    break;
                }
                if (fp_id != move_batch_fp_ids[i])
                {
                    continue;
                }
                auto [_, new_fp_id] = AllocatePage(page_id);
                err = WritePage(std::move(move_batch_buf[i]), new_fp_id);
                CHECK_KV_ERR(err);
                i++;
            }
        }
        if (min_file_id != end_file_id)
        {
            empty_file_cnt++;
        }
        it_low = it_high;
    }
    allocator->UpdateStat(min_file_id, empty_file_cnt);
    assert(mapping_cnt == mapper->MappingCount());
    assert(allocator->SpaceSize() >= mapping_cnt);
    assert(mapper->DebugStat());
    return KvError::NoError;
}

KvError BackgroundWrite::DoCompactSegmentFile()
{
    const KvOptions *opts = Options();
    PageMapper *mapper = cow_meta_.segment_mapper_.get();
    CHECK(mapper != nullptr);
    auto *seg_allocator =
        static_cast<AppendAllocator *>(mapper->FilePgAllocator());
    const uint32_t mapping_cnt = mapper->MappingCount();
    const uint32_t segments_per_file = seg_allocator->PagesPerFile();
    const double saf_limit = opts->segment_file_amplify_factor;

    // ReadSegments / WriteSegments require io_uring fixed-buffer indices, so
    // compaction shares GlobalRegisteredMemory with foreground large-value
    // reads/writes. Foreground impact stays predictable because the
    // background's in-flight segment count is bounded: each batch holds at
    // most max_segments_batch segments and recycles them before the next
    // batch. Tune max_segments_batch / segment_compact_yield_every to shift
    // the bound, not a separate pool.
    GlobalRegisteredMemory *global_mem = IoMgr()->GetGlobalRegisteredMemory();
    CHECK(global_mem != nullptr)
        << "DoCompactSegmentFile: GlobalRegisteredMemory required when segment "
           "mapper is present";
    const uint16_t reg_mem_base = IoMgr()->GlobalRegMemIndexBase();

    // Collect (physical_fp_id, logical_page_id) pairs from the segment
    // mapping.
    MappingSnapshot *mapping = mapper->GetMapping();
    std::vector<std::pair<FilePageId, PageId>> fp_ids;
    fp_ids.reserve(mapping_cnt);
    size_t tbl_size = mapping->mapping_tbl_.size();
    for (PageId page_id = 0; page_id < tbl_size; ++page_id)
    {
        FilePageId fp_id = mapping->ToFilePage(page_id);
        if (fp_id != MaxFilePageId)
        {
            fp_ids.emplace_back(fp_id, page_id);
        }
        if ((page_id & 0xFF) == 0)
        {
            YieldToLowPQ();
        }
    }
    YieldToLowPQ();
    assert(fp_ids.size() == mapping_cnt);

    // Empty segment mapping: forfeit the tail segment file too. See the
    // matching note in DoCompactDataFile — without rounding up, SpaceSize
    // stays at the partial tail and the empty-wipe partition keeps
    // re-entering Compact -> TriggerFileGC on every archive tick.
    if (fp_ids.empty())
    {
        if (seg_allocator->SpaceSize() > 0)
        {
            const uint32_t mask = (1u << opts->segments_per_file_shift) - 1;
            const FileId next_file =
                static_cast<FileId>((seg_allocator->MaxFilePageId() + mask) >>
                                    opts->segments_per_file_shift);
            seg_allocator->UpdateStat(next_file, 0);
        }
        return KvError::NoError;
    }

    HeapSortFpIdsWithYield(fp_ids);
    YieldToLowPQ();

    auto yield_seg_alloc = [this]() { YieldToLowPQ(); };

    // Do not compact the tail segment file that is still receiving writes.
    const FileId end_file_id = seg_allocator->CurrentFileId();
    FileId min_file_id = end_file_id;
    uint32_t empty_file_cnt = 0;
    size_t round_cnt = 0;
    size_t segments_since_yield = 0;
    const uint32_t yield_every = opts->segment_compact_yield_every;

    auto it_low = fp_ids.begin();
    auto it_high = fp_ids.begin();
    FileId begin_file_id =
        fp_ids.front().first >> opts->segments_per_file_shift;
    for (FileId file_id = begin_file_id; file_id < end_file_id; ++file_id)
    {
        if ((round_cnt & 0xFF) == 0)
        {
            YieldToLowPQ();
            round_cnt = 0;
        }
        FilePageId end_fp_id = FilePageId(file_id + 1)
                               << opts->segments_per_file_shift;
        while (it_high != fp_ids.end() && it_high->first < end_fp_id)
        {
            ++round_cnt;
            ++it_high;
        }
        if (it_low == it_high)
        {
            if (min_file_id != end_file_id)
            {
                ++empty_file_cnt;
            }
            continue;
        }

        if (double factor =
                double(segments_per_file) / double(it_high - it_low);
            factor <= saf_limit)
        {
            if (min_file_id == end_file_id)
            {
                min_file_id = file_id;
            }
            it_low = it_high;
            continue;
        }

        // Rewrite every live segment in this file to the tail.
        for (auto it = it_low; it < it_high;)
        {
            uint32_t batch_size = std::min<uint32_t>(
                max_segments_batch, static_cast<uint32_t>(it_high - it));

            std::array<char *, max_segments_batch> bufs;
            std::array<uint32_t, max_segments_batch> chunk_indices;
            std::array<uint16_t, max_segments_batch> buf_indices;
            std::array<FilePageId, max_segments_batch> src_fp_ids;
            std::array<FilePageId, max_segments_batch> dst_fp_ids;
            std::array<char *, max_segments_batch> dst_write_ptrs;
            std::array<const char *, max_segments_batch> src_read_ptrs;

            for (uint32_t i = 0; i < batch_size; ++i)
            {
                auto [ptr, idx] = global_mem->GetSegment(yield_seg_alloc);
                bufs[i] = ptr;
                chunk_indices[i] = idx;
                buf_indices[i] = reg_mem_base + static_cast<uint16_t>(idx);
                src_fp_ids[i] = (it + i)->first;
                dst_write_ptrs[i] = ptr;
                src_read_ptrs[i] = ptr;
            }

            KvError err =
                IoMgr()->ReadSegments(tbl_ident_,
                                      {src_fp_ids.data(), batch_size},
                                      {dst_write_ptrs.data(), batch_size},
                                      {buf_indices.data(), batch_size});
            if (err != KvError::NoError)
            {
                for (uint32_t i = 0; i < batch_size; ++i)
                {
                    global_mem->Recycle(bufs[i], chunk_indices[i]);
                }
                return err;
            }

            for (uint32_t i = 0; i < batch_size; ++i)
            {
                PageId logical_id = (it + i)->second;
                dst_fp_ids[i] = AllocateSegment(logical_id).second;
            }

            err = IoMgr()->WriteSegments(tbl_ident_,
                                         {dst_fp_ids.data(), batch_size},
                                         {src_read_ptrs.data(), batch_size},
                                         {buf_indices.data(), batch_size});
            for (uint32_t i = 0; i < batch_size; ++i)
            {
                global_mem->Recycle(bufs[i], chunk_indices[i]);
            }
            CHECK_KV_ERR(err);

            it += batch_size;
            segments_since_yield += batch_size;
            if (yield_every > 0 && segments_since_yield >= yield_every)
            {
                YieldToLowPQ();
                segments_since_yield = 0;
            }
        }

        if (min_file_id != end_file_id)
        {
            ++empty_file_cnt;
        }
        it_low = it_high;
    }

    seg_allocator->UpdateStat(min_file_id, empty_file_cnt);
    assert(mapping_cnt == mapper->MappingCount());
    assert(seg_allocator->SpaceSize() >= mapping_cnt);
    return KvError::NoError;
}

KvError BackgroundWrite::CreateArchive(std::string_view tag)
{
    assert(Options()->data_append_mode);
    assert(Options()->num_retained_archives > 0);

    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    if (err == KvError::NotFound)
    {
        // Partitions without manifest files (e.g., only term files) are
        // normal, archive is considered complete.
        DLOG(INFO) << "CreateArchive is skipped, table=" << tbl_ident_
                   << ", term=" << IoMgr()->ProcessTerm() << ", tag=" << tag;
        return KvError::NoError;
    }
    CHECK_KV_ERR(err);

    const KvOptions *opts = Options();
    RootMeta *meta = root_handle.Get();
    bool needs_compact =
        (opts->data_append_mode &&
         MapperExceedsAmplification(meta->mapper_.get(),
                                    opts->file_amplify_factor)) ||
        MapperExceedsAmplification(meta->segment_mapper_.get(),
                                   opts->segment_file_amplify_factor);
    if (needs_compact)
    {
        // Release the handle so Compact() can take its own COW root reference
        // without contending, then re-acquire the refreshed root.
        root_handle = {};
        err = Compact();
        CHECK_KV_ERR(err);
        std::tie(root_handle, err) =
            shard->IndexManager()->FindRoot(tbl_ident_);
        CHECK_KV_ERR(err);
        meta = root_handle.Get();
    }
    PageId root = meta->root_id_;

    PageId ttl_root = meta->ttl_root_id_;
    MappingSnapshot *mapping = meta->mapper_->GetMapping();
    FilePageId max_fp_id = meta->mapper_->FilePgAllocator()->MaxFilePageId();
    std::string_view dict_bytes;
    if (meta->compression_->HasDictionary())
    {
        dict_bytes = meta->compression_->DictionaryBytes();
    }
    // Archive snapshot carries BranchManifestMetadata and the segment mapping.
    BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = std::string(IoMgr()->GetActiveBranch());
    branch_metadata.term = IoMgr()->ProcessTerm();
    branch_metadata.file_ranges = IoMgr()->GetBranchFileMapping(tbl_ident_);

    MappingSnapshot *seg_mapping =
        meta->segment_mapper_ ? meta->segment_mapper_->GetMapping() : nullptr;
    FilePageId max_seg_fp_id =
        meta->segment_mapper_
            ? meta->segment_mapper_->FilePgAllocator()->MaxFilePageId()
            : 0;

    std::string_view snapshot = wal_builder_.Snapshot(root,
                                                      ttl_root,
                                                      mapping,
                                                      max_fp_id,
                                                      dict_bytes,
                                                      branch_metadata,
                                                      seg_mapping,
                                                      max_seg_fp_id);

    const std::string generated_tag =
        tag.empty() ? std::to_string(utils::UnixTs<chrono::microseconds>())
                    : std::string();
    if (tag.empty())
    {
        tag = generated_tag;
    }

    DLOG(INFO) << "CreateArchive begin, table=" << tbl_ident_
               << ", term=" << IoMgr()->ProcessTerm() << ", tag=" << tag
               << ", root=" << root << ", ttl_root=" << ttl_root
               << ", max_fp_id=" << max_fp_id
               << ", snapshot_bytes=" << snapshot.size();
    err = IoMgr()->CreateArchive(tbl_ident_,
                                 branch_metadata.branch_name,
                                 branch_metadata.term,
                                 snapshot,
                                 tag);
    CHECK_KV_ERR(err);
    DLOG(INFO) << "CreateArchive done, table=" << tbl_ident_
               << ", term=" << IoMgr()->ProcessTerm() << ", tag=" << tag;

    LOG(INFO) << "created archive for partition " << tbl_ident_ << " with tag "
              << tag;

    return KvError::NoError;
}

KvError BackgroundWrite::CreateBranch(std::string_view branch_name)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return KvError::InvalidArgs;
    }

    // Compact before snapshotting so the branch inherits a dense mapping
    // and does not carry over fragmented files from the parent.  Compact()
    // walks both data and segment mappings; data-file compaction is gated on
    // data_append_mode internally, and segment compaction only fires when a
    // segment mapper exists.
    if (Options()->data_append_mode)
    {
        KvError compact_err = Compact();
        if (compact_err == KvError::NotFound)
        {
            // Partition has no manifest (e.g. only term files).
            // No branch manifest needed — treat as success.
            return KvError::NoError;
        }
        CHECK_KV_ERR(compact_err);
    }

    BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = normalized_branch;
    branch_metadata.term = 0;
    branch_metadata.file_ranges = IoMgr()->GetBranchFileMapping(tbl_ident_);

    wal_builder_.Reset();
    auto [root_handle, root_err] = shard->IndexManager()->FindRoot(tbl_ident_);
    if (root_err == KvError::NotFound)
    {
        // Partition has no manifest yet (empty/unwritten partition).
        // No branch manifest needed — treat as success.
        return KvError::NoError;
    }
    if (root_err != KvError::NoError)
    {
        return root_err;
    }
    RootMeta *meta = root_handle.Get();
    if (!meta || !meta->mapper_)
    {
        // Mapper is null — partition exists as a stub but has no data.
        // Treat as empty partition; no branch manifest needed.
        return KvError::NoError;
    }

    // new branch jump to use the next file id to avoid any collision with
    // parent branch
    FileId parent_branch_max_file_id =
        meta->mapper_->FilePgAllocator()->CurrentFileId();
    FilePageId new_max_fp_id =
        static_cast<FilePageId>(parent_branch_max_file_id + 1)
        << Options()->pages_per_file_shift;

    PageId root = meta->root_id_;
    PageId ttl_root = meta->ttl_root_id_;
    MappingSnapshot *mapping = meta->mapper_->GetMapping();
    std::string_view dict_bytes;
    if (meta->compression_->HasDictionary())
    {
        dict_bytes = meta->compression_->DictionaryBytes();
    }

    std::string_view snapshot = wal_builder_.Snapshot(
        root, ttl_root, mapping, new_max_fp_id, dict_bytes, branch_metadata);

    // The CURRENT_TERM file is NOT created here.  It will be created
    // when a store instance starts with this branch as its active branch
    // (via BootstrapUpsertTermFile in cloud mode).  Branch deletion and
    // GC already handle missing term files gracefully.
    return IoMgr()->WriteBranchManifest(
        tbl_ident_, normalized_branch, 0, snapshot);
}

KvError BackgroundWrite::DeleteBranch(std::string_view branch_name)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return KvError::InvalidArgs;
    }

    if (normalized_branch == MainBranchName)
    {
        LOG(ERROR) << "Cannot delete main branch";
        return KvError::InvalidArgs;
    }

    if (normalized_branch == IoMgr()->GetActiveBranch())
    {
        LOG(ERROR) << "Cannot delete the currently active branch: "
                   << normalized_branch;
        return KvError::InvalidArgs;
    }

    LOG(INFO) << "Deleting branch " << normalized_branch;

    // Delete all manifest files for this branch (all terms) plus CURRENT_TERM.
    // The term argument is ignored; DeleteBranchFiles reads CURRENT_TERM
    // itself.
    KvError del_err =
        IoMgr()->DeleteBranchFiles(tbl_ident_, normalized_branch, 0);
    if (del_err != KvError::NoError && del_err != KvError::NotFound)
    {
        LOG(ERROR) << "DeleteBranch: failed to remove files for branch "
                   << normalized_branch << ": " << ErrorString(del_err);
        return del_err;
    }

    LOG(INFO) << "Successfully deleted branch " << normalized_branch;
    return KvError::NoError;
}

KvError BackgroundWrite::RunLocalFileGc()
{
    return TriggerLocalFileGC();
}

}  // namespace eloqstore
