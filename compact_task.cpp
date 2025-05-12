#include "compact_task.h"

#include "file_gc.h"
#include "shard.h"
namespace kvstore
{
KvError CompactTask::CompactDataFile()
{
    const KvOptions *opts = Options();
    assert(opts->data_append_mode);
    assert(opts->file_amplify_factor != 0);

    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    CHECK_KV_ERR(err);
    if (meta->root_page_ == nullptr)
    {
        return KvError::NotFound;
    }

    auto allocator =
        static_cast<AppendAllocator *>(meta->mapper_->FilePgAllocator());
    uint32_t mapping_cnt = meta->mapper_->MappingCount();
    if (mapping_cnt == 0)
    {
        // Update statistic.
        allocator->UpdateStat(MaxFileId, 0);
        return KvError::NoError;
    }
    const uint32_t pages_per_file = allocator->PagesPerFile();
    const double file_saf_limit = opts->file_amplify_factor;
    size_t space_size = allocator->SpaceSize();
    assert(space_size >= mapping_cnt);
    if (space_size < pages_per_file ||
        double(space_size) / double(mapping_cnt) <= file_saf_limit)
    {
        // No compaction required.
        return KvError::NoError;
    }

    // Begin compaction.

    err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    assert(cow_meta_.root_ != nullptr);
    // Prevent the meta data from being evicted.
    MemIndexPage *root = cow_meta_.root_;
    root->Pin();

    allocator =
        static_cast<AppendAllocator *>(cow_meta_.mapper_->FilePgAllocator());
    assert(mapping_cnt == cow_meta_.mapper_->MappingCount());

    // Get all file page ids that are used by this version.
    std::vector<std::pair<FilePageId, PageId>> fp_ids;
    fp_ids.reserve(mapping_cnt);
    size_t tbl_size = cow_meta_.mapper_->GetMapping()->mapping_tbl_.size();
    for (PageId page_id = 0; page_id < tbl_size; page_id++)
    {
        FilePageId fp_id = ToFilePage(page_id);
        if (fp_id != MaxFilePageId)
        {
            fp_ids.emplace_back(fp_id, page_id);
        }
    }
    assert(fp_ids.size() == mapping_cnt);
    // Sort by file page id.
    std::sort(fp_ids.begin(), fp_ids.end());

    constexpr uint8_t max_move_batch =
        std::min(max_read_pages_batch, max_write_pages_batch);
    std::vector<Page> move_batch_buf;
    move_batch_buf.reserve(max_move_batch);
    std::vector<FilePageId> move_batch_fp_ids;
    move_batch_fp_ids.reserve(max_move_batch);

    auto it_low = fp_ids.begin();
    auto it_high = fp_ids.begin();
    FileId begin_file_id = fp_ids.front().first >> opts->pages_per_file_shift;
    // Do not compact the data file that is currently being written to and is
    // not yet full.
    const FileId end_file_id = allocator->CurrentFileId();
    FileId min_file_id = end_file_id;
    uint32_t empty_file_cnt = 0;
    for (FileId file_id = begin_file_id; file_id < end_file_id; file_id++)
    {
        FilePageId end_fp_id = (file_id + 1) << opts->pages_per_file_shift;
        while (it_high != fp_ids.end() && it_high->first < end_fp_id)
        {
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
            uint32_t batch_size = std::min(long(max_move_batch), it_high - it);
            std::span<std::pair<FilePageId, PageId>> batch_ids(it, batch_size);
            // Read original pages.
            move_batch_fp_ids.clear();
            for (auto [fp_id, _] : batch_ids)
            {
                move_batch_fp_ids.emplace_back(fp_id);
            }
            err = IoMgr()->ReadPages(
                tbl_ident_, move_batch_fp_ids, move_batch_buf);
            if (err != KvError::NoError)
            {
                root->Unpin();
                return err;
            }
            // Write these pages to the new file.
            for (uint32_t i = 0; i < batch_size; i++)
            {
                PageId page_id = batch_ids[i].second;
                auto [_, fp_id] = AllocatePage(page_id);
                err = WritePage(std::move(move_batch_buf[i]), fp_id);
                if (err != KvError::NoError)
                {
                    // TODO: put Page back to page_pool for reusing.
                    root->Unpin();
                    return err;
                }
            }
        }
        if (min_file_id != end_file_id)
        {
            empty_file_cnt++;
        }
        it_low = it_high;
    }
    allocator->UpdateStat(min_file_id, empty_file_cnt);
    assert(mapping_cnt == cow_meta_.mapper_->MappingCount());
    assert(allocator->SpaceSize() >= mapping_cnt);
    assert(meta->mapper_->DebugStat());

    err = UpdateMeta(cow_meta_.root_);
    root->Unpin();
    CHECK_KV_ERR(err);

    if (file_garbage_collector != nullptr)
    {
        err = TriggerFileGC();
        assert(err == KvError::NoError);
    }
    return KvError::NoError;
}
}  // namespace kvstore