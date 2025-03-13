#include "data_page.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>

#include "coding.h"
#include "kv_options.h"

namespace kvstore
{
DataPage::DataPage(uint32_t page_id, uint32_t page_size) : page_id_(page_id)
{
    if (page_size == 0)
    {
        return;
    }

    if (page_pool != nullptr)
    {
        page_ = page_pool->Allocate();
    }
    else
    {
        char *p = (char *) std::aligned_alloc(page_align, page_size);
        assert(p != nullptr);
        page_ = std::unique_ptr<char[]>(p);
    }
}

DataPage::DataPage(DataPage &&rhs)
    : page_id_(rhs.page_id_), page_(std::move(rhs.page_))
{
}

DataPage &DataPage::operator=(DataPage &&other)
{
    if (this != &other)
    {
        Clear();
        page_id_ = other.page_id_;
        page_ = std::move(other.page_);
    }
    return *this;
}

DataPage::~DataPage()
{
    Clear();
}

bool DataPage::IsEmpty() const
{
    return page_ == nullptr;
}

uint16_t DataPage::ContentLength() const
{
    return DecodeFixed16(page_.get() + page_size_offset);
}

uint16_t DataPage::RestartNum() const
{
    return DecodeFixed16(page_.get() + ContentLength() - sizeof(uint16_t));
}

uint32_t DataPage::PrevPageId() const
{
    return DecodeFixed32(page_.get() + prev_page_offset);
}

uint32_t DataPage::NextPageId() const
{
    return DecodeFixed32(page_.get() + next_page_offset);
}

void DataPage::SetPrevPageId(uint32_t page_id)
{
    EncodeFixed32(page_.get() + prev_page_offset, page_id);
}

void DataPage::SetNextPageId(uint32_t page_id)
{
    EncodeFixed32(page_.get() + next_page_offset, page_id);
}

void DataPage::SetPageId(uint32_t page_id)
{
    page_id_ = page_id;
}

uint32_t DataPage::PageId() const
{
    return page_id_;
}

char *DataPage::PagePtr() const
{
    return page_.get();
}

std::unique_ptr<char[]> DataPage::GetPtr()
{
    return std::move(page_);
}

void DataPage::SetPtr(std::unique_ptr<char[]> ptr)
{
    page_ = std::move(ptr);
}

void DataPage::Clear()
{
    if (page_ != nullptr)
    {
        if (page_pool != nullptr)
        {
            page_pool->Free(std::move(page_));
        }
        else
        {
            page_ = nullptr;
        }
    }
}

std::ostream &operator<<(std::ostream &out, DataPage const &page)
{
    out << "{D" << page.PageId() << '|';
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
                                (1 + restart_num_) * sizeof(uint16_t)),
      curr_offset_(DataPage::content_offset),
      curr_restart_idx_(0)
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
}

std::string_view DataPageIter::Key() const
{
    return {key_.data(), key_.size()};
}

std::string_view DataPageIter::Value() const
{
    return value_;
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

void DataPageIter::Seek(std::string_view search_key)
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
        const char *key_ptr = DecodeEntry(page_.data() + region_offset,
                                          page_.data() + restart_offset_,
                                          &shared,
                                          &non_shared,
                                          &val_len);
        if (key_ptr == nullptr || shared != 0)
        {
            Invalidate();
            return;
        }

        std::string_view pivot{key_ptr, non_shared};
        cmp_ret = cmp_->Compare(pivot, search_key);
        if (cmp_ret < 0)
        {
            left = mid + 1;
            cnt -= step + 1;
        }
        else
        {
            cnt = step;
        }
    }

    if (cmp_ret == 0 || left == 0)
    {
        assert(left < restart_num_);
        // The search key matches a restart point or is smaller than the first
        // restart point. Positions to the restart point.
        SeekToRestart(left);
        ParseNextKey();
    }
    else
    {
        assert(left > 0 && left <= restart_num_);
        uint16_t limit =
            left < restart_num_ ? RestartOffset(left) : restart_offset_;
        // Linear searches the region before the ceiling restart point.
        SeekToRestart(left - 1);
        while (curr_offset_ < limit)
        {
            if (!ParseNextKey())
            {
                Invalidate();
                return;
            }

            std::string_view data_key = Key();
            if (cmp_->Compare(data_key, search_key) >= 0)
            {
                // Finds the ceiling of the search key.
                return;
            }
        }
        // The search key is greater than all data keys in the region prior to
        // the ceiling restart point. The offset now points to the ceiling
        // restart point or the page end.
        ParseNextKey();
    }
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
        curr_offset_ = restart_offset_;
        curr_restart_idx_ = restart_num_;
        key_.clear();
        timestamp_ = 0;
        return false;
    }
    else if (curr_offset_ < DataPage::content_offset)
    {
        curr_offset_ = DataPage::content_offset;
        pt = page_.data() + curr_offset_;
    }

    bool is_restart_pointer = curr_offset_ == RestartOffset(curr_restart_idx_);
    uint32_t shared = 0, non_shared = 0, value_len = 0;
    pt = DecodeEntry(pt, limit, &shared, &non_shared, &value_len);

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
    timestamp_ = 0;
}

const char *DataPageIter::DecodeEntry(const char *p,
                                      const char *limit,
                                      uint32_t *shared,
                                      uint32_t *non_shared,
                                      uint32_t *value_length)
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

    if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length))
    {
        return nullptr;
    }
    return p;
}

PagePool::PagePool(uint16_t page_size) : page_size_(page_size)
{
}

std::unique_ptr<char[]> PagePool::Allocate()
{
    if (pages_.empty())
    {
        char *p = (char *) std::aligned_alloc(page_align, page_size_);
        assert(p != nullptr);
        return std::unique_ptr<char[]>(p);
    }
    else
    {
        std::unique_ptr<char[]> page = std::move(pages_.back());
        pages_.pop_back();
        return page;
    }
}

void PagePool::Free(std::unique_ptr<char[]> page)
{
    pages_.emplace_back(std::move(page));
}
}  // namespace kvstore