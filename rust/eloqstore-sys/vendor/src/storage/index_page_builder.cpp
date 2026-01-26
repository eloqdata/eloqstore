#include "storage/index_page_builder.h"

#include <sys/types.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string_view>

#include "coding.h"
#include "kv_options.h"
#include "storage/mem_index_page.h"

namespace eloqstore
{
IndexPageBuilder::IndexPageBuilder(const KvOptions *opt)
    : options_(opt), counter_(0), cnt_(0), finished_(false)
{
    buffer_.resize(HeaderSize(), 0x0);
    restarts_.emplace_back(buffer_.size());
}

void IndexPageBuilder::Reset()
{
    buffer_.clear();
    buffer_.resize(HeaderSize(), 0x0);
    restarts_.clear();
    restarts_.emplace_back(buffer_.size());
    counter_ = 0;
    cnt_ = 0;
    finished_ = false;
    last_key_.clear();
    last_page_id_ = 0;
}

size_t IndexPageBuilder::CurrentSizeEstimate() const
{
    return buffer_.size() +  // Existing buffer
           TailMetaSize();   // Meta-data at tail
}

std::string_view IndexPageBuilder::Finish()
{
    if (cnt_ == 1)
    {
        // The page only contains the leftmost pointer. The number of restart
        // points is 0.
        restarts_.clear();
    }

    for (auto &offset : restarts_)
    {
        PutFixed16(&buffer_, offset);
    }
    PutFixed16(&buffer_, restarts_.size());

    uint16_t content_size = buffer_.size();
    // Stores the page size at the header after the page type.
    EncodeFixed16(buffer_.data() + MemIndexPage::page_size_offset,
                  content_size);

    assert(buffer_.size() <= options_->data_page_size);
    buffer_.resize(options_->data_page_size);

    finished_ = true;

    return {buffer_.data(), buffer_.size()};
}

bool IndexPageBuilder::Add(std::string_view key,
                           PageId page_id,
                           bool is_leaf_index)
{
    if (IsEmpty())
    {
        assert(buffer_.size() >= HeaderSize());
        // Sets the page type.
        SetPageType(
            buffer_.data(),
            is_leaf_index ? PageType::LeafIndex : PageType::NonLeafIndex);

        // The leftmost pointer can only be set once.
        assert(DecodeFixed32(buffer_.data() +
                             MemIndexPage::leftmost_ptr_offset) == 0);
        char buf[sizeof(uint32_t)];
        EncodeFixed32(buf, page_id);
        memcpy(buffer_.data() + MemIndexPage::leftmost_ptr_offset,
               buf,
               sizeof(uint32_t));
        ++cnt_;
        return true;
    }
    assert(!key.empty());

#ifndef NDEBUG
    size_t buf_prev_size = buffer_.size();
    bool is_restart = false;
#endif

    // If the page type has been set and the input pointer does not match the
    // type, throws an error.
    if (TypeOfPage(buffer_.data()) !=
        (is_leaf_index ? PageType::LeafIndex : PageType::NonLeafIndex))
    {
        throw std::runtime_error(
            "Unmatched storage pointers in the same index page.");
    }

    std::string_view last_key_view{last_key_.data(), last_key_.size()};
    assert(!finished_);
    assert(counter_ <= options_->index_page_restart_interval);
    assert(buffer_.empty() ||
           options_->comparator_->Compare(key, last_key_view) > 0);

    // The number of bytes incurred by adding the key-value pair.
    size_t addition_delta = 0;

    size_t shared = 0;
    if (counter_ < options_->index_page_restart_interval)
    {
        size_t comm_prefix_len = std::min(last_key_view.size(), key.size());
        while (shared < comm_prefix_len && last_key_view[shared] == key[shared])
        {
            shared++;
        }
    }
    else
    {
        // Restarting compression adds one more restart entry.
        addition_delta += sizeof(uint16_t);
        last_page_id_ = 0;
#ifndef NDEBUG
        is_restart = true;
#endif
    }
    size_t non_shared = key.size() - shared;

    // The index entry starts with the tuple <shared, non_shared>
    addition_delta += Varint32Size(shared);
    addition_delta += Varint32Size(non_shared);
    addition_delta += non_shared;

    assert(page_id <= INT32_MAX);
    int32_t delta = (int32_t) page_id - last_page_id_;
    uint32_t p_delta = EncodeInt32Delta(delta);
    addition_delta += Varint32Size(p_delta);

    // Does not add the index entry if it would overflow the page.
    if (CurrentSizeEstimate() + addition_delta > options_->data_page_size)
    {
        return false;
    }

    // Now modifies the page buffer.
    if (counter_ >= options_->index_page_restart_interval)
    {
        restarts_.emplace_back(buffer_.size());
        counter_ = 0;
    }

    // Adds the tuple <shared, non_shared>
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);

    buffer_.append(key.data() + shared, non_shared);
    PutVarint32(&buffer_, p_delta);

#ifndef NDEBUG
    uint16_t minus = is_restart ? sizeof(uint16_t) : 0;
    assert(buffer_.size() - buf_prev_size == addition_delta - minus);
    assert(CurrentSizeEstimate() <= options_->data_page_size);
#endif

    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    last_page_id_ = page_id;
    ++counter_;
    ++cnt_;

    return true;
}

void IndexPageBuilder::Swap(IndexPageBuilder &other)
{
    std::swap(buffer_, other.buffer_);
    std::swap(restarts_, other.restarts_);
    std::swap(counter_, other.counter_);
    std::swap(cnt_, other.cnt_);
    std::swap(finished_, other.finished_);
    std::swap(last_key_, other.last_key_);
    std::swap(last_page_id_, other.last_page_id_);
}

size_t IndexPageBuilder::HeaderSize()
{
    // 1 byte for the page type and the following 8 bytes for the leftmost
    // pointer.
    return checksum_bytes +    // 8 bytes for checksum
           1 +                 // 1 byte for the page type
           sizeof(uint16_t) +  // 2 bytes for content size
           sizeof(uint32_t);   // 4 bytes for leftmost pointer
}

uint16_t IndexPageBuilder::TailMetaSize() const
{
    // restart array + restart array size
    return restarts_.size() * sizeof(uint16_t) + sizeof(uint16_t);
}
}  // namespace eloqstore