#include "data_page_builder.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "coding.h"
#include "data_page.h"
#include "index_page_builder.h"

namespace eloqstore
{
DataPageBuilder::DataPageBuilder(const KvOptions *opt)
    : options_(opt), counter_(0), cnt_(0), finished_(false)
{
    buffer_.resize(HeaderSize(), 0x0);
    // The first restart point is at the offset 0.
    restarts_.emplace_back(buffer_.size());
}

void DataPageBuilder::Reset()
{
    buffer_.clear();
    buffer_.resize(HeaderSize(), 0x0);
    SetPageType(buffer_.data(), PageType::Data);
    restarts_.clear();
    restarts_.emplace_back(buffer_.size());
    counter_ = 0;
    cnt_ = 0;
    finished_ = false;
    last_key_.clear();
    last_timestamp_ = 0;
}

size_t DataPageBuilder::CurrentSizeEstimate() const
{
    return (buffer_.size() +                       // Raw data buffer
            restarts_.size() * sizeof(uint16_t) +  // Restart array
            sizeof(uint16_t));                     // Restart array length
}

bool DataPageBuilder::IsOverflowKV(std::string_view key,
                                   size_t val_size,
                                   uint64_t ts,
                                   uint64_t expire_ts,
                                   const KvOptions *options)
{
    // Minimum reserved space for header and restart array.
    size_t reserved = HeaderSize() + (2 * sizeof(uint16_t));

    if (reserved + val_size > options->data_page_size)
    {
        // Fast path
        return true;
    }

    size_t stored_val_len = val_size << uint8_t(ValLenBit::BitsCount);
    auto ret = CalculateDelta(key, stored_val_len, ts, expire_ts, {}, 0, false);
    return reserved + std::get<0>(ret) > options->data_page_size;
}

std::tuple<size_t, size_t, size_t, uint64_t> DataPageBuilder::CalculateDelta(
    std::string_view key,
    size_t stored_val_len,
    uint64_t ts,
    uint64_t expire_ts,
    std::string_view last_key,
    uint64_t last_ts,
    bool restart)
{
    // The number of bytes incurred by adding the key-value pair.
    size_t addition_delta = 0;
    size_t shared = 0;
    if (restart)
    {
        // Restarting compression adds one more restart point
        addition_delta += sizeof(uint16_t);
        last_ts = 0;
    }
    else
    {
        // See how much sharing to do with previous string
        const size_t min_length = std::min(last_key.size(), key.size());
        while ((shared < min_length) && (last_key[shared] == key[shared]))
        {
            shared++;
        }
    }
    size_t non_shared = key.size() - shared;

    // The data entry starts with the triple <shared><non_shared><value_size>
    addition_delta += Varint32Size(shared);
    addition_delta += Varint32Size(non_shared);
    addition_delta += Varint32Size(stored_val_len);
    // Key
    addition_delta += non_shared;
    // Value
    addition_delta += stored_val_len >> uint8_t(ValLenBit::BitsCount);

    if (expire_ts != 0)
    {
        addition_delta += Varint64Size(expire_ts);
    }

    // Timestamp delta
    assert(ts <= INT64_MAX);
    int64_t ts_delta = (int64_t) ts - last_ts;
    uint64_t p_ts_delta = EncodeInt64Delta(ts_delta);
    addition_delta += Varint64Size(p_ts_delta);
    return {addition_delta, shared, non_shared, p_ts_delta};
}

size_t DataPageBuilder::HeaderSize()
{
    return checksum_bytes +       // 8 bytes for checksum
           1 +                    // 1 byte for the page type.
           sizeof(uint16_t) +     // 2 bytes for content size
           sizeof(uint32_t) * 2;  // 2 * 4 bytes for IDs of prev and next pages
}

std::string_view DataPageBuilder::Finish()
{
    // Append restart array
    for (size_t i = 0; i < restarts_.size(); i++)
    {
        PutFixed16(&buffer_, restarts_[i]);
    }
    PutFixed16(&buffer_, restarts_.size());

    uint16_t content_size = buffer_.size();
    // Stores the page size at the header after the page type.
    EncodeFixed16(buffer_.data() + DataPage::page_size_offset, content_size);

    assert(buffer_.size() <= options_->data_page_size);
    buffer_.resize(options_->data_page_size);

    finished_ = true;
    return {buffer_.data(), buffer_.size()};
}

bool DataPageBuilder::Add(std::string_view key,
                          std::string_view value,
                          bool overflow,
                          uint64_t ts,
                          uint64_t expire_ts,
                          compression::CompressionType compression_kind)
{
#ifndef NDEBUG
    size_t buf_prev_size = buffer_.size();
    bool is_restart = false;
#endif

    assert(!finished_);
    assert(counter_ <= options_->data_page_restart_interval);
    assert(buffer_.empty() ||
           options_->comparator_->Compare(key, last_key_) > 0);

    size_t stored_val_len = value.size() << uint8_t(ValLenBit::BitsCount);
    stored_val_len |= (overflow << uint8_t(ValLenBit::Overflow));
    auto comp_bits = static_cast<uint8_t>(compression_kind);
    assert(comp_bits < 0b11);
    stored_val_len |= comp_bits << uint8_t(ValLenBit::DictionaryCompressed);
    if (expire_ts != 0)
    {
        stored_val_len |= (1 << uint8_t(ValLenBit::Expire));
    }
    auto [size_delta, shared, non_shared, ts_delta] =
        CalculateDelta(key,
                       stored_val_len,
                       ts,
                       expire_ts,
                       last_key_,
                       last_timestamp_,
                       NeedRestart());

    // Does not add the data item if it would overflow the page.
    if (CurrentSizeEstimate() + size_delta > options_->data_page_size)
    {
        return false;
    }

    if (NeedRestart())
    {
        restarts_.emplace_back(buffer_.size());
        counter_ = 0;
        last_timestamp_ = 0;
#ifndef NDEBUG
        is_restart = true;
#endif
    }

    // Adds the triple <shared><non_shared><value_size>
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, stored_val_len);

    // Adds string delta to buffer_ followed by value
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());
    if (expire_ts != 0)
    {
        PutVarint64(&buffer_, expire_ts);
    }
    PutVarint64(&buffer_, ts_delta);

#ifndef NDEBUG
    uint16_t minus = is_restart ? sizeof(uint16_t) : 0;
    assert(buffer_.size() - buf_prev_size == size_delta - minus);
#endif

    // Update state
    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    last_timestamp_ = ts;
    assert(std::string_view(last_key_.data(), last_key_.size()) == key);
    ++counter_;
    ++cnt_;

    return true;
}

FastPageBuilder::FastPageBuilder(const KvOptions *options) : options_(options)
{
}

void FastPageBuilder::Reset(char *ptr, PageType type)
{
    ptr_ = ptr;
    SetPageType(ptr_, type);
    switch (type)
    {
    case PageType::NonLeafIndex:
    case PageType::LeafIndex:
        end_offset_ = IndexPageBuilder::HeaderSize();
        break;
    case PageType::Data:
        end_offset_ = DataPageBuilder::HeaderSize();
        break;
    default:
        assert(false);
        break;
    }
    region_offsets_.clear();
}

bool FastPageBuilder::AddRegion(std::string_view region)
{
    if (CurrentSize() + region.size() + sizeof(uint16_t) >
        options_->data_page_size)
    {
        return false;
    }
    if (ptr_ + end_offset_ != region.data())
    {
        std::memcpy(ptr_ + end_offset_, region.data(), region.size());
    }
    region_offsets_.emplace_back(end_offset_);
    end_offset_ += region.size();
    return true;
}

size_t FastPageBuilder::CurrentSize() const
{
    return (end_offset_ +                                // Raw data buffer
            region_offsets_.size() * sizeof(uint16_t) +  // Restart array
            sizeof(uint16_t));                           // Restart array length
}

void FastPageBuilder::Finish()
{
    // Append restart array
    for (uint16_t offset : region_offsets_)
    {
        EncodeFixed16(ptr_ + end_offset_, offset);
        end_offset_ += sizeof(uint16_t);
    }
    EncodeFixed16(ptr_ + end_offset_, region_offsets_.size());
    end_offset_ += sizeof(uint16_t);

    // Stores the page size at the header after the page type.
    EncodeFixed16(ptr_ + DataPage::page_size_offset, end_offset_);
}

}  // namespace eloqstore