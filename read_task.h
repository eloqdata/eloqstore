#pragma once

#include <string_view>
#include <unordered_set>

#include "data_page.h"
#include "error.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

// Given the table id, tree root and the input key, returns the logical page id
// of the data page that might contain the key.
uint32_t SeekIndex(IndexPageManager *idx_page_mgr,
                   MappingSnapshot *mapping,
                   const TableIdent &tbl_ident,
                   MemIndexPage *node,
                   std::string_view key);

class ReadTask : public KvTask
{
public:
    ReadTask(IndexPageManager *idx_manager);

    void Yield() override;
    void Resume() override;
    void Rollback() override;

    void Reset(IndexPageManager *idx_page_manager);

    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string_view &value,
                 uint64_t &timestamp);

    TaskType Type() const override
    {
        return TaskType::Read;
    }

private:
    uint32_t SeekIndex(const TableIdent &tbl_ident,
                       MemIndexPage *node,
                       std::string_view key);

    DataPage data_page_;
    IndexPageManager *idx_page_manager_{nullptr};
    std::shared_ptr<MappingSnapshot> page_mapping_;

public:
    /**
     * @brief A collection of pages the task has requested to read. A task
     * usually issues one read request and waits for it (BlockedForOne). When a
     * value spans multiple pages, the task may issue a batch of requests and
     * wait for all of them to finish (BlockedForAll).
     *
     */
    std::unordered_set<uint32_t> read_fps_;
};
}  // namespace kvstore