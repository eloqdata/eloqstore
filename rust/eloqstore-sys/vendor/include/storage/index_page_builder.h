#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kv_options.h"
#include "types.h"

namespace eloqstore
{
class IndexPageBuilder
{
public:
    explicit IndexPageBuilder(const KvOptions *opt);

    IndexPageBuilder(const IndexPageBuilder &) = delete;
    IndexPageBuilder &operator=(const IndexPageBuilder &) = delete;

    void Reset();

    bool Add(std::string_view key, PageId page_id, bool is_leaf_index);

    std::string_view Finish();

    size_t CurrentSizeEstimate() const;

    bool IsEmpty() const
    {
        return cnt_ == 0;
    }

    std::string Move()
    {
        return std::move(buffer_);
    }
    void Swap(IndexPageBuilder &other);

    static size_t HeaderSize();

private:
    uint16_t TailMetaSize() const;

    const KvOptions *const options_;
    std::string buffer_;
    std::vector<uint16_t> restarts_;
    uint16_t counter_{0};
    uint16_t cnt_{0};
    bool finished_{false};
    std::string last_key_;
    int32_t last_page_id_{0};
};

}  // namespace eloqstore