#include "tasks/write_buffer_aggregator.h"

#include <utility>

namespace eloqstore
{
WriteBufferAggregator::WriteBufferAggregator(size_t buffer_size)
    : buffer_size_(buffer_size)
{
}

void WriteBufferAggregator::Reset()
{
    buffer_ = nullptr;
    buffer_index_ = 0;
    use_fixed_ = true;
    file_id_ = 0;
    start_offset_ = 0;
    next_offset_ = 0;
    used_ = 0;
    pages_.clear();
    release_ptrs_.clear();
    release_indices_.clear();
}

bool WriteBufferAggregator::HasBuffer() const
{
    return buffer_ != nullptr;
}

bool WriteBufferAggregator::HasData() const
{
    return buffer_ != nullptr && used_ > 0;
}

void WriteBufferAggregator::SetBuffer(char *buffer,
                                      uint16_t buffer_index,
                                      FileId file_id,
                                      uint64_t start_offset,
                                      bool use_fixed)
{
    buffer_ = buffer;
    buffer_index_ = buffer_index;
    use_fixed_ = use_fixed;
    file_id_ = file_id;
    start_offset_ = start_offset;
    next_offset_ = start_offset;
    used_ = 0;
    pages_.clear();
    release_ptrs_.clear();
    release_indices_.clear();
}

bool WriteBufferAggregator::CanAppend(FileId file_id,
                                      uint64_t offset,
                                      size_t size) const
{
    if (buffer_ == nullptr)
    {
        return false;
    }
    if (file_id != file_id_)
    {
        return false;
    }
    if (offset != next_offset_)
    {
        LOG(INFO) << "WriteBufferAggregator::CanAppend non-contiguous offset="
                  << offset << " expected=" << next_offset_
                  << " file_id=" << file_id_;
        return false;
    }
    if (used_ + size > buffer_size_)
    {
        return false;
    }
    return true;
}

char *WriteBufferAggregator::TryReserve(FileId file_id,
                                        uint64_t offset,
                                        size_t size)
{
    if (!CanAppend(file_id, offset, size))
    {
        return nullptr;
    }
    char *ptr = buffer_ + used_;
    used_ += size;
    next_offset_ += size;
    return ptr;
}

void WriteBufferAggregator::AddPage(VarPage page,
                                    char *release_ptr,
                                    uint16_t release_index)
{
    pages_.push_back(std::move(page));
    release_ptrs_.push_back(release_ptr);
    release_indices_.push_back(release_index);
}

bool WriteBufferAggregator::ShouldFlush(size_t next_size) const
{
    if (buffer_ == nullptr)
    {
        return false;
    }
    return used_ + next_size > buffer_size_;
}

WriteBufferBatch WriteBufferAggregator::TakeBatch()
{
    WriteBufferBatch batch;
    batch.buffer = buffer_;
    batch.buffer_index = buffer_index_;
    batch.use_fixed = use_fixed_;
    batch.file_id = file_id_;
    batch.start_offset = start_offset_;
    batch.bytes = used_;
    batch.pages = std::move(pages_);
    batch.release_ptrs = std::move(release_ptrs_);
    batch.release_indices = std::move(release_indices_);

    buffer_ = nullptr;
    buffer_index_ = 0;
    use_fixed_ = true;
    file_id_ = 0;
    start_offset_ = 0;
    next_offset_ = 0;
    used_ = 0;
    pages_.clear();
    release_ptrs_.clear();
    release_indices_.clear();

    return batch;
}
}  // namespace eloqstore
