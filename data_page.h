#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "comparator.h"
#include "compression.h"
#include "kv_options.h"
#include "page.h"
#include "types.h"

namespace eloqstore
{
class DictCompression;
enum class ValLenBit : uint8_t
{
    Overflow = 0,
    Expire,
    DictionaryCompressed,
    StandaloneCompressed,
    BitsCount
};

/**
 * Format:
 * +------------+--------+------------------+-------------+-------------+
 * |checksum(8B)|type(1B)|content length(2B)|prev page(4B)|next page(4B)|
 * +------------+--------+------------------+-------------+-------------+
 * +---------+-------------------+---------------+-------------+
 * |data blob|restart array(N*2B)|restart num(2B)|padding bytes|
 * +---------+-------------------+---------------+-------------+
 */
class DataPage
{
public:
    DataPage() = default;
    DataPage(PageId page_id);
    DataPage(PageId page_id, Page page)
        : page_id_(page_id), page_(std::move(page)) {};
    DataPage(const DataPage &) = delete;
    DataPage(DataPage &&rhs);
    DataPage &operator=(DataPage &&);

    static uint16_t const page_size_offset = page_type_offset + sizeof(uint8_t);
    static uint16_t const prev_page_offset =
        page_size_offset + sizeof(uint16_t);
    static uint16_t const next_page_offset = prev_page_offset + sizeof(PageId);
    static uint16_t const content_offset = next_page_offset + sizeof(PageId);

    bool IsEmpty() const;
    uint16_t ContentLength() const;
    uint16_t RestartNum() const;
    PageId PrevPageId() const;
    PageId NextPageId() const;
    void SetPrevPageId(PageId page_id);
    void SetNextPageId(PageId page_id);
    void SetPageId(PageId page_id);
    PageId GetPageId() const;
    char *PagePtr() const;
    void SetPage(Page page);
    void Clear();

private:
    PageId page_id_{MaxPageId};
    Page page_{false};
};

std::ostream &operator<<(std::ostream &out, DataPage const &page);

class DataPageIter
{
public:
    DataPageIter() = delete;
    DataPageIter(const DataPage *data_page, const KvOptions *options);

    void Reset(const DataPage *data_page, uint32_t size);
    void Reset();
    std::string_view Key() const;
    std::string_view Value() const;
    bool IsOverflow() const;
    compression::CompressionType CompressionType() const;
    uint64_t ExpireTs() const;
    uint64_t Timestamp() const;

    bool HasNext() const;
    bool Next();

    /**
     * @brief Seeks to the first key in the page equal to or greater than the
     * search key.
     */
    bool Seek(std::string_view search_key);

    /**
     * @brief Seek to the last key in the page not greater than search_key.
     * @return false if such a key not exists.
     */
    bool SeekFloor(std::string_view search_key);

private:
    /**
     * @brief Searches the first region whose start key is no less than the
     * search key.
     * @return <false, region-idx> if the start key of region at region-idx is
     * greater than the search key.
     * @return <true, region-idx> if the search key exactly equal to the start
     * key of the region at region-idx.
     */
    std::pair<bool, uint16_t> SearchRegion(std::string_view key) const;
    uint16_t RestartOffset(uint16_t restart_idx) const;
    void SeekToRestart(uint16_t restart_idx);
    bool ParseNextKey();
    void Invalidate();
    static const char *DecodeEntry(
        const char *p,
        const char *limit,
        uint32_t *shared,
        uint32_t *non_shared,
        uint32_t *value_length,
        bool *overflow,
        bool *expire,
        compression::CompressionType *compression_kind);

    const Comparator *const cmp_;
    std::string_view page_;
    uint16_t restart_num_;
    uint16_t restart_offset_;

    uint16_t curr_offset_{DataPage::content_offset};
    uint16_t curr_restart_idx_{0};

    std::string key_;
    std::string_view value_;
    bool overflow_;
    compression::CompressionType compression_type_{
        compression::CompressionType::None};
    uint64_t timestamp_;
    uint64_t expire_ts_;
};

class PageRegionIter
{
public:
    explicit PageRegionIter(std::string_view page);
    void Reset(std::string_view page);
    std::string_view Region() const;
    bool Valid() const;
    void Next();

private:
    uint16_t RegionOffset(uint16_t region_idx) const;
    std::string_view page_;
    uint16_t cur_region_idx_{0};
    uint16_t num_regions_;
    const char *restart_array_{nullptr};
};

/**
 * @brief The overflow page is used to store the overflow value that can not fit
 * in a DataPage.
 * Format:
 * +---------------+-----------+----------------+-------+
 * | checksum (8B) | type (1B) | value len (2B) | value |
 * +---------------+-----------+----------------+-------+
 * +----------------------+-------------------------+
 * | pointers (N*4 Bytes) | number of pointers (1B) | <End>
 * +----------------------+-------------------------+
 * The pointers are stored at the end of the page.
 */
class OverflowPage
{
public:
    OverflowPage() = default;
    OverflowPage(PageId page_id, Page page);
    OverflowPage(PageId page_id,
                 const KvOptions *opts,
                 std::string_view val,
                 std::span<PageId> pointers = {});
    OverflowPage(const OverflowPage &) = delete;
    OverflowPage(OverflowPage &&rhs);
    void SetPageId(PageId page_id);
    PageId GetPageId() const;
    char *PagePtr() const;
    uint16_t ValueSize() const;
    std::string_view GetValue() const;
    uint8_t NumPointers(const KvOptions *options) const;
    std::string_view GetEncodedPointers(const KvOptions *options) const;

    /**
     * @brief Calculate the capacity of an overflow page.
     * @param options The options of the KV store.
     * @param end Whether the page is the end page of a overflow group.
     */
    static uint16_t Capacity(const KvOptions *options, bool end);

    static const uint16_t page_size_offset = page_type_offset + sizeof(uint8_t);
    static const uint16_t value_offset = page_size_offset + sizeof(uint16_t);
    static const uint16_t header_size = value_offset;

private:
    PageId page_id_{MaxPageId};
    Page page_{false};
};
}  // namespace eloqstore
