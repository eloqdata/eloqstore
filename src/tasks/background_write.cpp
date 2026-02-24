#include "tasks/background_write.h"

#include <algorithm>
#include <memory>  // for std::shared_ptr
#include <string>

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

KvError BackgroundWrite::CompactDataFile()
{
    LOG(INFO) << "begin compaction on " << this->tbl_ident_;
    const KvOptions *opts = Options();
    assert(opts->data_append_mode);
    assert(opts->file_amplify_factor != 0);

    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    CHECK_KV_ERR(err);
    RootMeta *meta = root_handle.Get();

    auto allocator =
        static_cast<AppendAllocator *>(meta->mapper_->FilePgAllocator());
    uint32_t mapping_cnt = meta->mapper_->MappingCount();

    // Ensure consistency between the mapping count and the available trees.
    // mapping_cnt counts both the primary tree and the TTL tree, so we only
    // expect it to be zero when both roots are invalid.
    if (mapping_cnt == 0)
    {
        // Update statistic.
        FilePageId max_fp_id = allocator->MaxFilePageId();
        allocator->UpdateStat(max_fp_id >> opts->pages_per_file_shift, 0);
        TriggerFileGC();
        return KvError::NoError;
    }
    CHECK((meta->root_id_ != MaxPageId) || (meta->ttl_root_id_ != MaxPageId))
        << "mapping_cnt=" << mapping_cnt << " tbl:" << tbl_ident_;

    const uint32_t pages_per_file = allocator->PagesPerFile();
    const double file_saf_limit = opts->file_amplify_factor;
    size_t space_size = allocator->SpaceSize();
    assert(space_size >= mapping_cnt);

    if (space_size < pages_per_file ||
        double(space_size) / double(mapping_cnt) <= file_saf_limit)
    {
        DLOG(INFO) << "CompactDataFile: no compaction required";
        // No compaction required.
        return KvError::NoError;
    }

    // Begin compaction.

    err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    PageMapper *mapper = cow_meta_.mapper_.get();

    allocator = static_cast<AppendAllocator *>(mapper->FilePgAllocator());
    assert(mapping_cnt == mapper->MappingCount());

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
    HeapSortFpIdsWithYield(fp_ids);
    YieldToLowPQ();

    constexpr uint8_t max_move_batch = max_read_pages_batch;
    std::vector<Page> move_batch_buf;
    move_batch_buf.reserve(max_move_batch);
    YieldToLowPQ();
    std::vector<FilePageId> move_batch_fp_ids;
    move_batch_fp_ids.reserve(max_move_batch);
    MovingCachedPages moving_cached(mapping_cnt);

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
                if (handle)
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
    assert(meta->mapper_->DebugStat());

    err = UpdateMeta();
    CHECK_KV_ERR(err);
    moving_cached.Finish();
    TriggerFileGC();
    LOG(INFO) << "finish compaction on " << tbl_ident_;
    return KvError::NoError;
}

KvError BackgroundWrite::CreateArchive(uint64_t provided_ts)
{
    assert(Options()->data_append_mode);
    assert(Options()->num_retained_archives > 0);

    KvError compact_err = CompactDataFile();
    if (compact_err == KvError::NotFound)
    {
        // Partitions without manifest files (e.g., only term files) are
        // normal, archive is considered complete.
        return KvError::NoError;
    }
    CHECK_KV_ERR(compact_err);

    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    CHECK_KV_ERR(err);
    RootMeta *meta = root_handle.Get();
    PageId root = meta->root_id_;
    if (root == MaxPageId)
    {
        return KvError::NotFound;
    }

    PageId ttl_root = meta->ttl_root_id_;
    MappingSnapshot *mapping = meta->mapper_->GetMapping();
    FilePageId max_fp_id = meta->mapper_->FilePgAllocator()->MaxFilePageId();
    std::string_view dict_bytes;
    if (meta->compression_->HasDictionary())
    {
        dict_bytes = meta->compression_->DictionaryBytes();
    }
    // Archive snapshot should also carry FileIdTermMapping for this table
    std::string term_buf;
    std::shared_ptr<FileIdTermMapping> file_term_mapping =
        shard->IoManager()->GetOrCreateFileIdTermMapping(tbl_ident_);
    file_term_mapping->insert_or_assign(IouringMgr::LruFD::kManifest,
                                        IoMgr()->ProcessTerm());
    SerializeFileIdTermMapping(*file_term_mapping, term_buf);
    std::string_view snapshot = wal_builder_.Snapshot(
        root, ttl_root, mapping, max_fp_id, dict_bytes, term_buf);

    uint64_t current_ts =
        provided_ts != 0 ? provided_ts : utils::UnixTs<chrono::microseconds>();
    err = IoMgr()->CreateArchive(tbl_ident_, snapshot, current_ts);
    CHECK_KV_ERR(err);

    // Update the cached max file id.
    FileId max_file_id =
        static_cast<FileId>(max_fp_id >> Options()->pages_per_file_shift);
    IoMgr()->least_not_archived_file_ids_[tbl_ident_] = max_file_id + 1;

    LOG(INFO) << "created archive for partition " << tbl_ident_ << " at "
              << current_ts << ", updated cached max file id to "
              << max_file_id + 1;
    return KvError::NoError;
}

KvError BackgroundWrite::RunLocalFileGc()
{
    return TriggerLocalFileGC();
}

}  // namespace eloqstore
