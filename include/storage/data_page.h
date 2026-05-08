#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "coding.h"
#include "comparator.h"
#include "compression.h"
#include "kv_options.h"
#include "storage/page.h"
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
 * @brief Check if the compression bits (bits 2-3 of value_length) indicate
 * a very large string stored in segment files. When both bits are set (0b11),
 * the value content is an array of segment IDs rather than inline data.
 */
inline bool IsLargeValueEncoding(uint32_t stored_val_len)
{
    uint8_t comp_bits =
        (stored_val_len >> uint8_t(ValLenBit::DictionaryCompressed)) & 0b11;
    return comp_bits == 0b11;
}

/**
 * @brief Encode large value content: [actual_length(4B) | segment_ids(4B
 * each)].
 * @param actual_length The true byte length of the very large value.
 * @param segment_ids Logical segment IDs allocated from the segment mapping.
 * @param dst Output buffer to append encoded content to.
 */
inline void EncodeLargeValueContent(uint32_t actual_length,
                                    std::span<const PageId> segment_ids,
                                    std::string &dst)
{
    size_t offset = dst.size();
    dst.resize(offset + sizeof(uint32_t) + segment_ids.size() * sizeof(PageId));
    char *p = dst.data() + offset;
    EncodeFixed32(p, actual_length);
    p += sizeof(uint32_t);
    for (PageId id : segment_ids)
    {
        EncodeFixed32(p, id);
        p += sizeof(PageId);
    }
}

/**
 * @brief Decode large value content encoded by EncodeLargeValueContent.
 * @param encoded The encoded value content from the data page.
 * @param[out] actual_length The true byte length of the very large value.
 * @param[out] segment_ids Output span to receive decoded segment IDs.
 * @return Number of segment IDs decoded, or 0 on error.
 */
inline uint32_t DecodeLargeValueContent(std::string_view encoded,
                                        uint32_t &actual_length,
                                        std::span<PageId> segment_ids)
{
    if (encoded.size() < sizeof(uint32_t))
    {
        return 0;
    }
    actual_length = DecodeFixed32(encoded.data());
    encoded = encoded.substr(sizeof(uint32_t));

    uint32_t num_segments =
        static_cast<uint32_t>(encoded.size() / sizeof(PageId));
    if (num_segments > segment_ids.size())
    {
        return 0;
    }

    for (uint32_t i = 0; i < num_segments; ++i)
    {
        segment_ids[i] = DecodeFixed32(encoded.data());
        encoded = encoded.substr(sizeof(PageId));
    }
    return num_segments;
}

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
    DataPage &operator=(DataPage &&) noexcept;
    DataPage &operator=(const DataPage &) = delete;

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
    bool IsRegistered() const
    {
        return page_.IsRegistered();
    }

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
    bool IsLargeValue() const;
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
        bool *large_value,
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
    bool large_value_{false};
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
    uint16_t num_regions_{};
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
    PageId GetPageId() const;
    char *PagePtr() const;
    uint16_t ValueSize() const;
    std::string_view GetValue() const;
    uint8_t NumPointers(const KvOptions *options) const;
    std::string_view GetEncodedPointers(const KvOptions *options) const;
    bool IsRegistered() const
    {
        return page_.IsRegistered();
    }

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
