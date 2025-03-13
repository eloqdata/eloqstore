#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "data_page.h"
#include "error.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

using KvEntry = std::tuple<std::string, std::string, uint64_t>;

class ScanTask : public KvTask
{
public:
    ScanTask();

    KvError Scan(const TableIdent &tbl_id,
                 std::string_view begin_key,
                 std::string_view end_key,
                 std::vector<KvEntry> &entries);
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