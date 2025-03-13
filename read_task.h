#pragma once

#include <string_view>

#include "error.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

class ReadTask : public KvTask
{
public:
    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string &value,
                 uint64_t &timestamp);

    TaskType Type() const override
    {
        return TaskType::Read;
    }
};
}  // namespace kvstore