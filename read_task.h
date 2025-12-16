#pragma once

#include <string_view>

#include "error.h"
#include "task.h"
#include "types.h"

namespace eloqstore
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
                 uint64_t &timestamp,
                 uint64_t &expire_ts);

    /**
     * @brief Read the biggest key not greater than the search key.
     */
    KvError Floor(const TableIdent &tbl_id,
                  std::string_view search_key,
                  std::string &floor_key,
                  std::string &value,
                  uint64_t &timestamp,
                  uint64_t &expire_ts);

    TaskType Type() const override
    {
        return TaskType::Read;
    }
};
}  // namespace eloqstore