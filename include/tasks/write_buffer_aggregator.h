#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "types.h"

namespace eloqstore
{

struct WriteBufferBatch
{
    char *buffer{nullptr};
    uint16_t buffer_index{0};
    bool use_fixed{true};
    FileId file_id{0};
    uint64_t start_offset{0};
    size_t bytes{0};
    std::vector<VarPage> pages;
    std::vector<char *> release_ptrs;
    std::vector<uint16_t> release_indices;
};

class WriteBufferAggregator
{
public:
    explicit WriteBufferAggregator(size_t buffer_size);

    void Reset();
    bool HasBuffer() const;
    bool HasData() const;
    /**
     * @brief Get the file ID of the current buffer.
     *
     * Returns the file_id associated with the current write buffer.
     * Used to detect when file_id switches (indicating a file seal event).
     *
     * @return Current FileId, or invalid if no buffer/data
     */
    FileId CurrentFileId() const
    {
        return file_id_;
    }
    void SetBuffer(char *buffer,
                   uint16_t buffer_index,
                   FileId file_id,
                   uint64_t start_offset,
                   bool use_fixed);
    bool CanAppend(FileId file_id, uint64_t offset, size_t size) const;
    char *TryReserve(FileId file_id, uint64_t offset, size_t size);
    void AddPage(VarPage page, char *release_ptr, uint16_t release_index);
    bool ShouldFlush(size_t next_size) const;
    WriteBufferBatch TakeBatch();

private:
    size_t buffer_size_{0};
    char *buffer_{nullptr};
    uint16_t buffer_index_{0};
    bool use_fixed_{true};
    FileId file_id_{0};
    uint64_t start_offset_{0};
    uint64_t next_offset_{0};
    size_t used_{0};
    std::vector<VarPage> pages_;
    std::vector<char *> release_ptrs_;
    std::vector<uint16_t> release_indices_;
};

}  // namespace eloqstore
