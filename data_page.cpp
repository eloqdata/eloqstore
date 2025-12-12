#include "data_page.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>

#include "coding.h"
#include "compression.h"
#include "eloq_store.h"
#include "kv_options.h"

namespace eloqstore
{
DataPage::DataPage(PageId page_id) : page_id_(page_id)
{
    page_ = Page{true};
}

DataPage::DataPage(DataPage &&rhs)
    : page_id_(rhs.page_id_), page_(std::move(rhs.page_))
{
}

DataPage &DataPage::operator=(DataPage &&other) noexcept
{
    if (this != &other)
    {
        Clear();
        page_id_ = other.page_id_;
        page_ = std::move(other.page_);
    }
    return *this;
}

bool DataPage::IsEmpty() const
{
    return page_.Ptr() == nullptr;
}

uint16_t DataPage::ContentLength() const
{
    return DecodeFixed16(page_.Ptr() + page_size_offset);
}

uint16_t DataPage::RestartNum() const
{
    return DecodeFixed16(page_.Ptr() + ContentLength() - sizeof(uint16_t));
}

PageId DataPage::PrevPageId() const
{
    return DecodeFixed32(page_.Ptr() + prev_page_offset);
}

PageId DataPage::NextPageId() const
{
    return DecodeFixed32(page_.Ptr() + next_page_offset);
}

void DataPage::SetPrevPageId(PageId page_id)
{
    EncodeFixed32(page_.Ptr() + prev_page_offset, page_id);
}

void DataPage::SetNextPageId(PageId page_id)
{
    EncodeFixed32(page_.Ptr() + next_page_offset, page_id);
}

void DataPage::SetPageId(PageId page_id)
{
    page_id_ = page_id;
}

PageId DataPage::GetPageId() const
{
    return page_id_;
}

char *DataPage::PagePtr() const
{
    return page_.Ptr();
}

void DataPage::SetPage(Page page)
{
    page_ = std::move(page);
}

void DataPage::Clear()
{
    page_.Free();
}

std::ostream &operator<<(std::ostream &out, DataPage const &page)
{
    out << "{D" << page.GetPageId() << '|';
    out << page.PrevPageId() << ',' << page.NextPageId() << '}';
    return out;
}

DataPageIter::DataPageIter(const DataPage *data_page, const KvOptions *options)
    : cmp_(options->comparator_),
      page_(data_page == nullptr ? std::string_view{}
                                 : std::string_view{data_page->PagePtr(),
                                                    options->data_page_size}),
      restart_num_(data_page == nullptr ? 0 : data_page->RestartNum()),
      restart_offset_(data_page == nullptr
                          ? 0
                          : data_page->ContentLength() -
                                (1 + restart_num_) * sizeof(uint16_t))
{
}

void DataPageIter::Reset(const DataPage *data_page, uint32_t size)
{
    if (data_page)
    {
        page_ = std::string_view{data_page->PagePtr(), size};
        restart_num_ = data_page->RestartNum();
        restart_offset_ =
            data_page->ContentLength() - (1 + restart_num_) * sizeof(uint16_t);
    }
    else
    {
        page_ = std::string_view{};
        restart_num_ = 0;
        restart_offset_ = 0;
    }
    Reset();
}

void DataPageIter::Reset()
{
    curr_offset_ = DataPage::content_offset;
    curr_restart_idx_ = 0;
    key_.clear();
    value_ = std::string_view{};
    timestamp_ = 0;
    overflow_ = false;
    compression_type_ = compression::CompressionType::None;
}

std::string_view DataPageIter::Key() const
{
    return {key_.data(), key_.size()};
}

std::string_view DataPageIter::Value() const
{
    return value_;
}

bool DataPageIter::IsOverflow() const
{
    return overflow_;
}

compression::CompressionType DataPageIter::CompressionType() const
{
    return compression_type_;
}

uint64_t DataPageIter::ExpireTs() const
{
    return expire_ts_;
}

uint64_t DataPageIter::Timestamp() const
{
    return timestamp_;
}

bool DataPageIter::HasNext() const
{
    return curr_offset_ < restart_offset_;
}

bool DataPageIter::Next()
{
    return ParseNextKey();
}

bool DataPageIter::SeekFloor(std::string_view search_key)
{
    auto [equal, ceil_point] = SearchRegion(search_key);
    if (equal)
    {
        SeekToRestart(ceil_point);
        ParseNextKey();
        return true;
    }
    else if (ceil_point == 0)
    {
        // All keys in this page are bigger than search_key.
        return false;
    }

    uint16_t last_offset = restart_offset_;
    std::string key;
    std::string_view value = {};
    bool overflow = false;
    compression::CompressionType compression_kind =
        compression::CompressionType::None;
    uint64_t timestamp = 0;

    assert(ceil_point <= restart_num_);
    uint16_t limit =
        ceil_point < restart_num_ ? RestartOffset(ceil_point) : restart_offset_;
    // Linear searches the region before the ceiling restart point.
    SeekToRestart(ceil_point - 1);
    while (curr_offset_ < limit)
    {
        ParseNextKey();
        if (cmp_->Compare(Key(), search_key) > 0)
        {
            break;
        }
        last_offset = curr_offset_;
        key = Key();
        value = Value();
        overflow = IsOverflow();
        timestamp = Timestamp();
        compression_kind = compression_type_;
    }
    curr_offset_ = last_offset;
    key_ = std::move(key);
    value_ = value;
    overflow_ = overflow;
    timestamp_ = timestamp;
    compression_type_ = compression_kind;
    return true;
}

bool DataPageIter::Seek(std::string_view search_key)
{
    auto [equal, ceil_point] = SearchRegion(search_key);
    if (equal || ceil_point == 0)
    {
        assert(ceil_point < restart_num_);
        // The search key matches a restart point or is smaller than the first
        // restart point. Positions to the restart point.
        SeekToRestart(ceil_point);
        ParseNextKey();
        return true;
    }
    else
    {
        assert(ceil_point > 0 && ceil_point <= restart_num_);
        uint16_t limit = ceil_point < restart_num_ ? RestartOffset(ceil_point)
                                                   : restart_offset_;
        // Linear searches the region before the ceiling restart point.
        SeekToRestart(ceil_point - 1);
        while (curr_offset_ < limit)
        {
            ParseNextKey();
            std::string_view data_key = Key();
            if (cmp_->Compare(data_key, search_key) >= 0)
            {
                // Finds the ceiling of the search key.
                return true;
            }
        }
        // The search key is greater than all data keys in the region prior to
        // the ceiling restart point. The offset now points to the ceiling
        // restart point or the page end.
        return ParseNextKey();
    }
}

std::pair<bool, uint16_t> DataPageIter::SearchRegion(std::string_view key) const
{
    assert(restart_num_ > 0);

    size_t left = 0;
    size_t right = restart_num_ - 1;
    int cmp_ret = 0;

    // Binary searches the ceiling restart point of the search key.
    size_t cnt = right - left + 1;
    while (cnt > 0)
    {
        size_t step = cnt >> 1;
        size_t mid = left + step;
        uint16_t region_offset = RestartOffset(mid);
        uint32_t shared, non_shared, val_len;
        bool overflow, expire;
        compression::CompressionType compression_kind;
        const char *key_ptr = DecodeEntry(page_.data() + region_offset,
                                          page_.data() + restart_offset_,
                                          &shared,
                                          &non_shared,
                                          &val_len,
                                          &overflow,
                                          &expire,
                                          &compression_kind);
        assert(key_ptr != nullptr && shared == 0);

        std::string_view pivot{key_ptr, non_shared};
        cmp_ret = cmp_->Compare(pivot, key);
        if (cmp_ret == 0)
        {
            return {true, mid};
        }
        else if (cmp_ret < 0)
        {
            left = mid + 1;
            cnt -= step + 1;
        }
        else
        {
            cnt = step;
        }
    }
    return {cmp_ret == 0, left};
}

uint16_t DataPageIter::RestartOffset(uint16_t restart_idx) const
{
    assert(restart_idx < restart_num_);
    return DecodeFixed16(page_.data() + restart_offset_ +
                         restart_idx * sizeof(uint16_t));
}

void DataPageIter::SeekToRestart(uint16_t restart_idx)
{
    curr_restart_idx_ = restart_idx;
    curr_offset_ = RestartOffset(restart_idx);
    key_.clear();
    timestamp_ = 0;
}

bool DataPageIter::ParseNextKey()
{
    const char *pt = page_.data() + curr_offset_;
    const char *limit = page_.data() + restart_offset_;

    if (pt >= limit)
    {
        Invalidate();
        return false;
    }
    else if (curr_offset_ < DataPage::content_offset)
    {
        curr_offset_ = DataPage::content_offset;
        pt = page_.data() + curr_offset_;
    }

    bool is_restart_pointer = curr_offset_ == RestartOffset(curr_restart_idx_);
    uint32_t shared = 0, non_shared = 0, value_len = 0;
    bool has_expire_ts;
    pt = DecodeEntry(pt,
                     limit,
                     &shared,
                     &non_shared,
                     &value_len,
                     &overflow_,
                     &has_expire_ts,
                     &compression_type_);

    if (pt == nullptr || key_.size() < shared)
    {
        Invalidate();
        return false;
    }
    else
    {
        key_.resize(shared);
        key_.append(pt, non_shared);
        pt += non_shared;
        value_ = {pt, value_len};
        pt += value_len;

        if (has_expire_ts)
        {
            pt = GetVarint64Ptr(pt, limit, &expire_ts_);
        }
        else
        {
            expire_ts_ = 0;
        }

        // Parses the timestamp. The stored value is the real value if this is
        // the restarting point, or the numerical delta to the previous
        // timestamp.
        uint64_t ts_val;
        if ((pt = GetVarint64Ptr(pt, limit, &ts_val)) == nullptr)
        {
            Invalidate();
            return false;
        }
        int64_t delta = DecodeInt64Delta(ts_val);
        timestamp_ = is_restart_pointer
                         ? delta
                         : static_cast<int64_t>(timestamp_) + delta;

        curr_offset_ = pt - page_.data();
        if (curr_restart_idx_ + 1 < restart_num_ &&
            curr_offset_ >= RestartOffset(curr_restart_idx_ + 1))
        {
            ++curr_restart_idx_;
        }

        return true;
    }
}

void DataPageIter::Invalidate()
{
    curr_offset_ = restart_offset_;
    curr_restart_idx_ = restart_num_;
    key_.clear();
    value_ = std::string_view{};
    expire_ts_ = 0;
    timestamp_ = 0;
    overflow_ = false;
    compression_type_ = compression::CompressionType::None;
}

const char *DataPageIter::DecodeEntry(
    const char *p,
    const char *limit,
    uint32_t *shared,
    uint32_t *non_shared,
    uint32_t *value_length,
    bool *overflow,
    bool *expire,
    compression::CompressionType *compression_type)
{
    if (limit - p < 3)
        return nullptr;
    *shared = reinterpret_cast<const uint8_t *>(p)[0];
    *non_shared = reinterpret_cast<const uint8_t *>(p)[1];
    *value_length = reinterpret_cast<const uint8_t *>(p)[2];
    if ((*shared | *non_shared | *value_length) < 128)
    {
        // Fast path: all three values are encoded in one byte each
        p += 3;
    }
    else
    {
        if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr)
            return nullptr;
        if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr)
            return nullptr;
        if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr)
            return nullptr;
    }

    *overflow = *value_length & (1 << uint8_t(ValLenBit::Overflow));
    *expire = *value_length & (1 << uint8_t(ValLenBit::Expire));
    uint8_t compressed =
        *value_length >> uint8_t(ValLenBit::DictionaryCompressed) & 0b11;

    CHECK(compressed != 0b11) << "Data is corrupted";
    *compression_type = static_cast<compression::CompressionType>(compressed);

    *value_length >>= uint8_t(ValLenBit::BitsCount);

    if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length))
    {
        return nullptr;
    }
    return p;
}

PageRegionIter::PageRegionIter(std::string_view page)
{
    Reset(page);
}

void PageRegionIter::Reset(std::string_view page)
{
    assert(DecodeFixed16(page.data() + DataPage::page_size_offset) ==
           page.size());
    page_ = page;
    const char *end = page.data() + page.size();
    num_regions_ = DecodeFixed16(end - sizeof(uint16_t));
    restart_array_ = end - sizeof(uint16_t) * (num_regions_ + 1);
    assert(num_regions_ > 0);
    cur_region_idx_ = 0;
}

std::string_view PageRegionIter::Region() const
{
    uint16_t offset = RegionOffset(cur_region_idx_);
    uint16_t size = RegionOffset(cur_region_idx_ + 1) - offset;
    return {page_.data() + offset, size};
}

bool PageRegionIter::Valid() const
{
    return cur_region_idx_ < num_regions_;
}

void PageRegionIter::Next()
{
    assert(Valid());
    cur_region_idx_++;
}

uint16_t PageRegionIter::RegionOffset(uint16_t region_idx) const
{
    if (region_idx < num_regions_)
    {
        return DecodeFixed16(restart_array_ + region_idx * sizeof(uint16_t));
    }
    else
    {
        return restart_array_ - page_.data();
    }
}

OverflowPage::OverflowPage(PageId page_id, Page page)
    : page_id_(page_id), page_(std::move(page))
{
    assert(TypeOfPage(page_.Ptr()) == PageType::Overflow);
}

OverflowPage::OverflowPage(PageId page_id,
                           const KvOptions *opts,
                           std::string_view val,
                           std::span<PageId> pointers)
    : page_id_(page_id)
{
    if (opts->data_page_size == 0)
    {
        return;
    }

    page_ = Page(true);

    SetPageType(page_.Ptr(), PageType::Overflow);

    EncodeFixed16(page_.Ptr() + OverflowPage::page_size_offset, val.size());
    memcpy(page_.Ptr() + OverflowPage::value_offset, val.data(), val.size());

    char *dst = (page_.Ptr() + opts->data_page_size - 1);
    *dst = pointers.size();
    if (!pointers.empty())
    {
        dst -= (pointers.size() * sizeof(PageId));
        for (PageId p : pointers)
        {
            EncodeFixed32(dst, p);
            dst += sizeof(PageId);
        }
    }
    assert(NumPointers(opts) == pointers.size());
}

OverflowPage::OverflowPage(OverflowPage &&rhs)
    : page_id_(rhs.page_id_), page_(std::move(rhs.page_))
{
}

uint16_t OverflowPage::ValueSize() const
{
    return DecodeFixed16(page_.Ptr() + page_size_offset);
}

std::string_view OverflowPage::GetValue() const
{
    return {page_.Ptr() + value_offset, ValueSize()};
}

void OverflowPage::SetPageId(PageId page_id)
{
    page_id_ = page_id;
}

PageId OverflowPage::GetPageId() const
{
    return page_id_;
}

char *OverflowPage::PagePtr() const
{
    return page_.Ptr();
}

uint16_t OverflowPage::Capacity(const KvOptions *options, bool end)
{
    // The last byte is reserved for the number of pointers.
    uint16_t cap = options->data_page_size - header_size - 1;
    if (end)
    {
        // The end page of a overflow group has pointers to the next group.
        cap -= (options->overflow_pointers * sizeof(PageId));
    }
    return cap;
}

uint8_t OverflowPage::NumPointers(const KvOptions *options) const
{
    return *(page_.Ptr() + options->data_page_size - 1);
}

std::string_view OverflowPage::GetEncodedPointers(
    const KvOptions *options) const
{
    uint8_t n = NumPointers(options);
    if (n == 0)
    {
        return {};
    }
    char *ptr =
        page_.Ptr() + options->data_page_size - 1 - (n * sizeof(uint32_t));
    return {ptr, n * sizeof(uint32_t)};
}

}  // namespace eloqstore
