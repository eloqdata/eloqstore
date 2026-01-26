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
    IndexStackEntry(MemIndexPage *page, const KvOptions *opts)
        : idx_page_(page), idx_page_iter_(page, opts)
    {
    }

    IndexStackEntry(const IndexStackEntry &) = delete;
    IndexStackEntry(IndexStackEntry &&rhs) = delete;

    MemIndexPage *idx_page_{nullptr};
    IndexPageIter idx_page_iter_;
    std::vector<IndexOp> changes_{};
    bool is_leaf_index_{false};
};
}  // namespace eloqstore
