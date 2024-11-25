#pragma once

#include <cstdint>
#include <memory>
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

using Tuple = std::tuple<std::string, std::string, uint64_t>;

class ScanTask : public KvTask
{
public:
    ScanTask(IndexPageManager *idx_manager);

    void Yield() override;
    void Resume() override;
    void Rollback() override;

    KvError ScanVec(const TableIdent &tbl_ident,
                    std::string_view begin_key,
                    std::string_view end_key,
                    std::vector<Tuple> &tuples);
    KvError Scan(const TableIdent &tbl_ident,
                 std::string_view begin_key,
                 std::string_view end_key);
    bool Valid() const;
    KvError Next();

    std::string_view Key() const;
    std::string_view Value() const;
    uint64_t Timestamp() const;

    TaskType Type() const override
    {
        return TaskType::Scan;
    }

private:
    KvError NextPage();
    IndexPageManager *page_mgr_{nullptr};
    std::shared_ptr<MappingSnapshot> mapping_;
    DataPage data_page_;
    DataPageIter iter_;
    std::string end_key_;
    TableIdent tbl_ident_;
};
}  // namespace kvstore