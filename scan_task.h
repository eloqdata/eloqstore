#pragma once

#include <string_view>
#include <vector>

#include "data_page.h"
#include "error.h"
#include "task.h"
#include "types.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

class ScanTask : public KvTask
{
public:
    ScanTask();

    KvError Scan(const TableIdent &tbl_id,
                 std::string_view begin_key,
                 std::string_view end_key,
                 bool begin_inclusive,
                 size_t page_entries,
                 size_t page_size,
                 std::vector<KvEntry> &result,
                 bool &has_remaining);
    TaskType Type() const override
    {
        return TaskType::Scan;
    }

private:
    KvError Next(MappingSnapshot *m);
    DataPage data_page_;
    DataPageIter iter_;
};
}  // namespace kvstore