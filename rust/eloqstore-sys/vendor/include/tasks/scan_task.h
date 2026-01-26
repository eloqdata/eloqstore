#pragma once

#include <array>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "error.h"
#include "storage/data_page.h"
#include "storage/mem_index_page.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class ScanIterator
{
public:
    ~ScanIterator();
    ScanIterator(const TableIdent &tbl_id,
                 std::vector<DataPage> &prefetched_pages,
                 std::vector<Page> &read_pages,
                 size_t prefetch_pages = kDefaultScanPrefetchPageCount);
    KvError Seek(std::string_view key, bool ttl = false);
    KvError Next();

    std::string_view Key() const;
    std::pair<std::string_view, KvError> ResolveValue(std::string &storage);
    uint64_t ExpireTs() const;
    uint64_t Timestamp() const;

    MappingSnapshot *Mapping() const;

private:
    struct IndexFrame
    {
        MemIndexPage *page;
        IndexPageIter iter;
    };

    const TableIdent tbl_id_;
    size_t prefetch_page_num_;
    PageId root_id_{MaxPageId};
    std::shared_ptr<MappingSnapshot> mapping_;
    DataPage data_page_;
    DataPageIter iter_;
    std::vector<DataPage> *prefetched_pages_;
    std::vector<Page> *read_pages_;
    std::array<PageId, max_read_pages_batch> prefetched_leaf_ids_{};
    std::array<FilePageId, max_read_pages_batch> prefetched_file_page_ids_{};
    size_t prefetched_offset_{0};
    size_t prefetched_count_{0};
    const compression::DictCompression *compression_{nullptr};
    std::vector<IndexFrame> index_stack_;

    void ResetPrefetchState();
    void ClearIndexStack();
    KvError BuildIndexStack(std::string_view key);
    KvError AdvanceToNextLeaf();
    KvError PrefetchFromStack();
    KvError ConsumePrefetchedPage();
    KvError LoadNextDataPage();
};

class ScanRequest;
class ScanTask : public KvTask
{
public:
    ScanTask() = default;

    KvError Scan();
    TaskType Type() const override
    {
        return TaskType::Scan;
    }

private:
    // Pooled buffer to avoid repeated allocation in ScanIterator.
    std::vector<DataPage> prefetched_pages_;
    std::vector<Page> read_pages_;
};
}  // namespace eloqstore
