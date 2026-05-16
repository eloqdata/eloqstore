#pragma once

#include <string_view>

#include "error.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class IndexPageManager;
class IoStringBuffer;
class MemIndexPage;
class MappingSnapshot;

class ReadTask : public KvTask
{
public:
    /**
     * @brief Point read.
     *
     * For a non-large value, @p value receives the decoded inline bytes.
     *
     * For a large value, @p value receives the metadata blob (empty if the
     * entry has no metadata) when @p extract_metadata is true; when false,
     * `value` is cleared and no metadata work is done. If @p large_value
     * is non-null, the very large value is additionally fetched into the
     * IoStringBuffer via zero-copy segment reads.
     */
    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string &value,
                 uint64_t &timestamp,
                 uint64_t &expire_ts,
                 IoStringBuffer *large_value = nullptr,
                 bool extract_metadata = true);

    /**
     * @brief Point read into contiguous pinned memory (KV Cache mode).
     *
     * For a non-large value, @p value receives the decoded inline bytes; the
     * pinned buffer is unused.
     *
     * For a large value, @p value receives the metadata blob and the segments
     * are read directly into the [@p large_value, @p large_value +
     * @p large_value_size) range via io_uring fixed-buffer reads. The pinned
     * range must lie within a single chunk registered through
     * `KvOptions::pinned_memory_chunks`. Pass (nullptr, 0) to extract only
     * metadata.
     */
    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string &value,
                 uint64_t &timestamp,
                 uint64_t &expire_ts,
                 char *large_value,
                 size_t large_value_size);

    /**
     * @brief Point read of a known-large value into contiguous pinned memory,
     * without extracting the metadata trailer.
     *
     * Intended for KV Cache callers that already retrieved metadata via a
     * prior metadata-only read and now need to fetch only the value bytes.
     * Returns KvError::InvalidArgs when the resolved entry is not a large
     * value (the caller's assumption is stale; they should retry with a
     * metadata-emitting overload).
     */
    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 uint64_t &timestamp,
                 uint64_t &expire_ts,
                 char *large_value,
                 size_t large_value_size);

    /**
     * @brief Read the biggest key not greater than the search key.
     */
    KvError Floor(const TableIdent &tbl_id,
                  std::string_view search_key,
                  std::string &floor_key,
                  std::string &value,
                  uint64_t &timestamp,
                  uint64_t &expire_ts,
                  IoStringBuffer *large_value = nullptr);

    TaskType Type() const override
    {
        return TaskType::Read;
    }
};
}  // namespace eloqstore
