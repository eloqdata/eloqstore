#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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

// Bit 31 of word0 in the large-value content flags whether a metadata trailer
// is appended after the segment-id array. Bits 30..0 hold actual_length, which
// caps very large values at 2 GiB - 1 bytes (well above the 10 MiB workload).
inline constexpr uint32_t kLargeValueHasMetadataBit = 0x80000000u;
inline constexpr uint32_t kLargeValueLengthMask = 0x7fffffffu;

/**
 * @brief Encode large value content.
 *
 * Layout when @p metadata is empty (backward compatible):
 *   [ word0 (4B) | K x uint32_t segment_ids ]
 *
 * Layout when @p metadata is non-empty:
 *   [ word0 (4B) | K x uint32_t segment_ids | uint16_t metadata_length |
 *     metadata_bytes ]
 *
 * word0 holds the has-metadata flag in bit 31 and actual_length in bits 30..0.
 * K is implied by actual_length and the configured segment_size, so it is not
 * encoded separately. Decoders recover K via
 *   K = ceil(actual_length / segment_size).
 *
 * @param actual_length True byte length of the very large value. Must fit in
 * 31 bits.
 * @param segment_ids Logical segment IDs allocated from the segment mapping.
 * @param dst Output buffer to append encoded content to.
 * @param metadata Optional metadata blob. Must be at most 65535 bytes.
 */
inline void EncodeLargeValueContent(uint32_t actual_length,
                                    std::span<const PageId> segment_ids,
                                    std::string &dst,
                                    std::string_view metadata = {})
{
    assert((actual_length & kLargeValueHasMetadataBit) == 0);
    assert(metadata.size() <= std::numeric_limits<uint16_t>::max());

    const bool has_metadata = !metadata.empty();
    const size_t trailer_bytes =
        has_metadata ? sizeof(uint16_t) + metadata.size() : 0;
    const size_t offset = dst.size();
    dst.resize(offset + sizeof(uint32_t) + segment_ids.size() * sizeof(PageId) +
               trailer_bytes);
    char *p = dst.data() + offset;

    uint32_t word0 = actual_length;
    if (has_metadata)
    {
        word0 |= kLargeValueHasMetadataBit;
    }
    EncodeFixed32(p, word0);
    p += sizeof(uint32_t);

    for (PageId id : segment_ids)
    {
        EncodeFixed32(p, id);
        p += sizeof(PageId);
    }

    if (has_metadata)
    {
        EncodeFixed16(p, static_cast<uint16_t>(metadata.size()));
        p += sizeof(uint16_t);
        std::memcpy(p, metadata.data(), metadata.size());
    }
}

/**
 * @brief Header info parsed from a large-value content blob: actual byte
 * length, segment count, and metadata trailer length. The metadata flag is
 * implicit: `metadata_length > 0` iff a metadata trailer is present (the
 * encoder never emits a flagged-but-zero-length trailer).
 */
struct LargeValueHeader
{
    uint32_t actual_length;
    uint32_t num_segments;
    uint16_t metadata_length;
};

/**
 * @brief Parse the header of a large-value content blob. Decodes word0 to
 * recover actual_length and the has-metadata flag, derives the segment count
 * from actual_length and the configured segment size, and (when metadata is
 * present) reads metadata_length from the trailer.
 *
 * The caller uses this to size the segment-ID buffer before calling
 * DecodeLargeValueContent.
 *
 * @param encoded The encoded value content from the data page.
 * @param segment_size The segment size in bytes from KvOptions.
 * @return Parsed header, or std::nullopt on malformed input (truncated buffer
 *   or trailing bytes don't match the expected size).
 */
inline std::optional<LargeValueHeader> DecodeLargeValueHeader(
    std::string_view encoded, uint32_t segment_size)
{
    assert(segment_size > 0);
    if (encoded.size() < sizeof(uint32_t))
    {
        return std::nullopt;
    }
    const uint32_t word0 = DecodeFixed32(encoded.data());
    const bool has_metadata = (word0 & kLargeValueHasMetadataBit) != 0;

    LargeValueHeader header{};
    header.actual_length = word0 & kLargeValueLengthMask;
    header.num_segments =
        (header.actual_length + segment_size - 1) / segment_size;

    const size_t segments_end =
        sizeof(uint32_t) +
        static_cast<size_t>(header.num_segments) * sizeof(PageId);

    if (!has_metadata)
    {
        if (encoded.size() != segments_end)
        {
            return std::nullopt;
        }
        return header;
    }

    if (encoded.size() < segments_end + sizeof(uint16_t))
    {
        return std::nullopt;
    }
    header.metadata_length = DecodeFixed16(encoded.data() + segments_end);
    const size_t expected =
        segments_end + sizeof(uint16_t) + header.metadata_length;
    if (encoded.size() != expected)
    {
        return std::nullopt;
    }
    return header;
}

/**
 * @brief Decode the segment-ID array and/or the metadata blob from a
 * large-value content blob. The caller must call DecodeLargeValueHeader
 * first and pass the resulting @p header. Both output sinks are independently
 * optional, so callers that need only one half (e.g. metadata-only readers,
 * GC freeing segment IDs) don't have to allocate a buffer for the other.
 *
 * @param encoded The encoded value content from the data page.
 * @param header Header parsed from a prior DecodeLargeValueHeader call.
 * @param segment_ids Output span. Pass an empty span (default) to skip
 *   segment-ID extraction. When non-empty, must be sized to
 *   header.num_segments.
 * @param[out] metadata_out Optional metadata sink. When non-null, receives
 *   the metadata blob (cleared if no trailer is present). Pass nullptr to
 *   skip metadata extraction.
 * @return true on success; false if @p segment_ids is non-empty but its size
 *   does not match header.num_segments.
 */
inline bool DecodeLargeValueContent(std::string_view encoded,
                                    const LargeValueHeader &header,
                                    std::span<PageId> segment_ids = {},
                                    std::string *metadata_out = nullptr)
{
    const bool extract_segments = !segment_ids.empty();
    if (extract_segments && segment_ids.size() != header.num_segments)
    {
        return false;
    }

    const char *seg_ptr = encoded.data() + sizeof(uint32_t);
    if (extract_segments)
    {
        for (uint32_t i = 0; i < header.num_segments; ++i)
        {
            segment_ids[i] = DecodeFixed32(seg_ptr);
            seg_ptr += sizeof(PageId);
        }
    }
    else
    {
        // Skip past the segment-ID array to land on the metadata trailer.
        seg_ptr += static_cast<size_t>(header.num_segments) * sizeof(PageId);
    }

    if (metadata_out != nullptr)
    {
        if (header.metadata_length > 0)
        {
            // Metadata bytes follow the uint16 metadata_length field.
            metadata_out->assign(seg_ptr + sizeof(uint16_t),
                                 header.metadata_length);
        }
        else
        {
            metadata_out->clear();
        }
    }
    return true;
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
