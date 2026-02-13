#pragma once

#include <vector>

#include "kv_options.h"
#include "storage/mem_index_page.h"

namespace eloqstore
{
struct IndexOp
{
    std::string key_;
    uint32_t page_id_;
    WriteOp op_;
};

class IndexStackEntry
{
public:
    IndexStackEntry(MemIndexPage::Handle handle, const KvOptions *opts)
        : idx_page_iter_(handle, opts), handle_(std::move(handle))
    {
    }

    IndexStackEntry(const IndexStackEntry &) = delete;
    IndexStackEntry(IndexStackEntry &&rhs) = delete;

    IndexPageIter idx_page_iter_;
    MemIndexPage::Handle handle_{};
    std::vector<IndexOp> changes_{};
    bool is_leaf_index_{false};
};
}  // namespace eloqstore
