#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <variant>
#include <vector>

#include "data_page.h"
#include "data_page_builder.h"
#include "index_page_builder.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "table_ident.h"
#include "task.h"
#include "write_op.h"
#include "write_tree_stack.h"

namespace kvstore
{
class IndexPageManager;
class AsyncIoManager;
class WriteTask;
class PageMapper;
class MappingSnapshot;

struct WriteReq
{
    uint32_t page_id_{UINT32_MAX};
    uint32_t file_page_id_{UINT32_MAX};
    const TableIdent *tbl_ident_{nullptr};
    std::variant<MemIndexPage *, DataPage *> page_;
    WriteTask *task_{nullptr};

    WriteReq *next_{nullptr};
};

class WriteTask : public KvTask
{
public:
    WriteTask() = delete;
    WriteTask(std::string tbl_name,
              uint32_t partition_id,
              const KvOptions *opt);
    WriteTask(const WriteTask &) = delete;

    void Yield() override;
    void Resume() override;
    void Rollback() override;

    TaskType Type() const override
    {
        return TaskType::Write;
    }

    void Reset(IndexPageManager *idx_page_manager);

    void AddData(std::string key, std::string val, uint64_t ts, WriteOp op);

    /**
     * @brief The index page has been flushed. If the index page has been
     * attached to its parent, enqueues the page into the cache replacement list
    (so that the page is allowed to be evicted). If the index page has not been
    attached, defers enqueuing until it is attached.
     *
     * @param req
     */
    void FinishIo(WriteReq *req);

    void Apply();

private:
    bool IsWriteBufferFull() const
    {
        return free_req_cnt_ == 0;
    }
    bool IsWriteBufferEmpty() const
    {
        return free_req_cnt_ == write_reqs_.size();
    }

    MemIndexPage *Pop();

    void FinishIndexPage(MemIndexPage *new_page,
                         std::string idx_page_key,
                         uint32_t page_id,
                         uint32_t file_page_id,
                         bool elevate);

    void FinishDataPage(std::string_view page_view,
                        std::string page_key,
                        uint32_t page_id,
                        uint32_t file_page_id);

    /**
     * @brief Calculates the left boundary of the data page or the top index
     * page in the stack.
     *
     * @param is_data_page
     * @return std::string_view
     */
    std::string_view LeftBound(bool is_data_page);

    /**
     * @brief Calculates the right boundary of the data page or the top index
     * page in the stack.
     *
     * @param is_data_page
     * @return std::string
     */
    std::string RightBound(bool is_data_page);

    void ApplyOnePage(size_t &cidx);
    void AdvanceDataPageIter(DataPageIter &iter, bool &is_valid);
    void AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid);

    /**
     * @brief Pops up the index stack such that the top index entry contains the
     * search key.
     *
     * @param search_key
     */
    void SeekStack(std::string_view search_key);

    void Seek(std::string_view key);

    void AllocatePage(WriteReq *req, uint32_t page_id, uint32_t file_page_id);

    uint32_t ToFilePage(uint32_t page_id);

    struct WriteDataEntry
    {
        std::string key_;
        std::string val_;
        uint64_t timestamp_;
        WriteOp op_;
    };

    TableIdent tbl_ident_;
    std::vector<WriteDataEntry> data_;

    IndexPageBuilder idx_page_builder_;
    DataPageBuilder data_page_builder_;
    IndexPageManager *idx_page_manager_{nullptr};

    std::vector<std::unique_ptr<IndexStackEntry>> stack_;
    DataPage data_page_;
    std::deque<DataPage> pending_pages_;
    WriteReq *prev_req_{nullptr};

    std::unique_ptr<PageMapper> mapper_{nullptr};
    MappingSnapshot *new_mapping_{nullptr};
    std::shared_ptr<MappingSnapshot> old_mapping_{nullptr};

    void RecycleWriteReq(WriteReq *req);
    WriteReq *GetWriteReq();

    std::vector<WriteReq> write_reqs_;
    WriteReq free_req_head_;
    size_t free_req_cnt_{0};

    friend class AsyncIoManager;
};
}  // namespace kvstore