#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "../include/storage/data_page.h"
#include "../include/types.h"

// Test segment size; chosen small so multi-segment cases are cheap to express.
constexpr uint32_t kTestSegSize = 256u * 1024u;  // 256 KiB

TEST_CASE("IsLargeValueEncoding truth table", "[large-value]")
{
    // Build stored_val_len with varying compression bits (bits 2-3) and
    // arbitrary high-order value length. Only 0b11 is large.
    for (uint32_t hi = 0; hi < 4; ++hi)
    {
        uint32_t base = hi << uint8_t(eloqstore::ValLenBit::BitsCount);
        uint32_t b00 = base;  // no compression
        uint32_t b01 =
            base |
            (0b01u << uint8_t(eloqstore::ValLenBit::DictionaryCompressed));
        uint32_t b10 =
            base |
            (0b10u << uint8_t(eloqstore::ValLenBit::DictionaryCompressed));
        uint32_t b11 =
            base |
            (0b11u << uint8_t(eloqstore::ValLenBit::DictionaryCompressed));
        REQUIRE_FALSE(eloqstore::IsLargeValueEncoding(b00));
        REQUIRE_FALSE(eloqstore::IsLargeValueEncoding(b01));
        REQUIRE_FALSE(eloqstore::IsLargeValueEncoding(b10));
        REQUIRE(eloqstore::IsLargeValueEncoding(b11));
    }
}

TEST_CASE("IsLargeValueEncoding ignores overflow and expire bits",
          "[large-value]")
{
    uint32_t v = (0b11u << uint8_t(eloqstore::ValLenBit::DictionaryCompressed));
    // Flip overflow / expire bits and confirm the compression-bit state is
    // what controls the classification.
    uint32_t with_overflow =
        v | (1u << uint8_t(eloqstore::ValLenBit::Overflow));
    uint32_t with_expire = v | (1u << uint8_t(eloqstore::ValLenBit::Expire));
    REQUIRE(eloqstore::IsLargeValueEncoding(with_overflow));
    REQUIRE(eloqstore::IsLargeValueEncoding(with_expire));

    // And that same bits without 0b11 don't falsely flag.
    uint32_t not_large = (1u << uint8_t(eloqstore::ValLenBit::Overflow)) |
                         (1u << uint8_t(eloqstore::ValLenBit::Expire));
    REQUIRE_FALSE(eloqstore::IsLargeValueEncoding(not_large));
}

TEST_CASE("EncodeLargeValueContent + DecodeLargeValueContent round-trip K=1, 2",
          "[large-value]")
{
    // K is derived from actual_length / segment_size, so use lengths that map
    // exactly to the desired K values.
    struct Case
    {
        uint32_t k;
        uint32_t actual_len;
    };
    const Case cases[] = {
        {1u, kTestSegSize},       // exactly fills 1 segment
        {2u, kTestSegSize + 1u},  // spills into a 2nd segment
    };
    for (const Case &c : cases)
    {
        std::vector<eloqstore::PageId> ids;
        for (uint32_t i = 0; i < c.k; ++i)
        {
            ids.push_back(0xdeadbee0 + i);
        }
        std::string dst;
        eloqstore::EncodeLargeValueContent(
            c.actual_len, std::span<const eloqstore::PageId>(ids), dst);
        REQUIRE(dst.size() ==
                sizeof(uint32_t) + ids.size() * sizeof(eloqstore::PageId));

        auto header = eloqstore::DecodeLargeValueHeader(dst, kTestSegSize);
        REQUIRE(header.has_value());
        REQUIRE(header->num_segments == c.k);
        REQUIRE(header->actual_length == c.actual_len);
        REQUIRE(header->metadata_length == 0);

        std::vector<eloqstore::PageId> out(header->num_segments);
        REQUIRE(eloqstore::DecodeLargeValueContent(
            dst, *header, std::span<eloqstore::PageId>(out)));
        for (uint32_t i = 0; i < c.k; ++i)
        {
            REQUIRE(out[i] == ids[i]);
        }
    }
}

TEST_CASE("EncodeLargeValueContent round-trip at max_segments_batch",
          "[large-value]")
{
    constexpr uint32_t k = eloqstore::max_segments_batch;
    std::vector<eloqstore::PageId> ids;
    for (uint32_t i = 0; i < k; ++i)
    {
        ids.push_back(0x10000u + i * 3u);
    }
    // Pick actual_length that maps to exactly k segments.
    const uint32_t actual_len = (k - 1) * kTestSegSize + 1u;
    std::string dst;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), dst);

    auto header = eloqstore::DecodeLargeValueHeader(dst, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->num_segments == k);
    REQUIRE(header->actual_length == actual_len);

    std::vector<eloqstore::PageId> out(header->num_segments);
    REQUIRE(eloqstore::DecodeLargeValueContent(
        dst, *header, std::span<eloqstore::PageId>(out)));
    for (uint32_t i = 0; i < k; ++i)
    {
        REQUIRE(out[i] == ids[i]);
    }
}

TEST_CASE("EncodeLargeValueContent appends to an existing buffer",
          "[large-value]")
{
    std::string dst = "preamble";
    std::vector<eloqstore::PageId> ids = {42, 43};
    const uint32_t actual_len = kTestSegSize + 100u;  // exactly 2 segments
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), dst);
    REQUIRE(dst.substr(0, 8) == "preamble");

    std::string_view payload(dst.data() + 8, dst.size() - 8);
    auto header = eloqstore::DecodeLargeValueHeader(payload, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->num_segments == 2);
    REQUIRE(header->actual_length == actual_len);

    std::vector<eloqstore::PageId> out(2);
    REQUIRE(eloqstore::DecodeLargeValueContent(
        payload, *header, std::span<eloqstore::PageId>(out)));
    REQUIRE(out[0] == 42u);
    REQUIRE(out[1] == 43u);
}

TEST_CASE("DecodeLargeValueHeader rejects truncated prefix", "[large-value]")
{
    // empty
    REQUIRE_FALSE(
        eloqstore::DecodeLargeValueHeader(std::string_view{}, kTestSegSize)
            .has_value());

    // 1 byte (< 4 required for word0)
    std::string s1(1, '\0');
    REQUIRE_FALSE(
        eloqstore::DecodeLargeValueHeader(s1, kTestSegSize).has_value());

    // 3 bytes (still short)
    std::string s3(3, '\0');
    REQUIRE_FALSE(
        eloqstore::DecodeLargeValueHeader(s3, kTestSegSize).has_value());
}

TEST_CASE("DecodeLargeValueContent rejects when output span size is wrong",
          "[large-value]")
{
    std::vector<eloqstore::PageId> ids = {1, 2, 3, 4};
    const uint32_t actual_len = 3u * kTestSegSize + 1u;  // 4 segments
    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded);

    auto header = eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->num_segments == 4);

    // Too small.
    std::vector<eloqstore::PageId> small_out(2);
    REQUIRE_FALSE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(small_out)));

    // Too large.
    std::vector<eloqstore::PageId> big_out(8);
    REQUIRE_FALSE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(big_out)));
}

TEST_CASE("DecodeLargeValueHeader with zero-length value", "[large-value]")
{
    // actual_length == 0 implies zero segments.
    std::string encoded;
    std::vector<eloqstore::PageId> empty;
    eloqstore::EncodeLargeValueContent(
        0u, std::span<const eloqstore::PageId>(empty), encoded);
    REQUIRE(encoded.size() == sizeof(uint32_t));

    auto header = eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->num_segments == 0);
    REQUIRE(header->actual_length == 0u);
    REQUIRE(header->metadata_length == 0);

    std::vector<eloqstore::PageId> out;
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out)));
}

TEST_CASE(
    "EncodeLargeValueContent without metadata matches pre-metadata layout",
    "[large-value]")
{
    // When the metadata blob is empty, the encoded byte layout must be
    // byte-for-byte identical to the pre-metadata encoding so older data on
    // disk continues to decode cleanly.
    std::vector<eloqstore::PageId> ids = {0x10, 0x11, 0x12};
    const uint32_t actual_len = 2u * kTestSegSize + 1u;  // 3 segments
    std::string with_default;
    std::string with_explicit_empty;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), with_default);
    eloqstore::EncodeLargeValueContent(actual_len,
                                       std::span<const eloqstore::PageId>(ids),
                                       with_explicit_empty,
                                       std::string_view{});

    REQUIRE(with_default.size() ==
            sizeof(uint32_t) + ids.size() * sizeof(eloqstore::PageId));
    REQUIRE(with_default == with_explicit_empty);

    // High bit of word0 must be clear when no metadata is present.
    const uint32_t word0 = eloqstore::DecodeFixed32(with_default.data());
    REQUIRE((word0 & eloqstore::kLargeValueHasMetadataBit) == 0);
}

TEST_CASE("EncodeLargeValueContent + DecodeLargeValueContent metadata trailer",
          "[large-value]")
{
    std::vector<eloqstore::PageId> ids = {0xaaaa0000, 0xaaaa0001, 0xaaaa0002};
    const std::string metadata = "torch-shape:[4,16,128];dtype=bf16";
    const uint32_t actual_len = 2u * kTestSegSize + 4u;  // 3 segments

    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded, metadata);
    REQUIRE(encoded.size() == sizeof(uint32_t) +
                                  ids.size() * sizeof(eloqstore::PageId) +
                                  sizeof(uint16_t) + metadata.size());

    // High bit of word0 set; bits 30..0 carry actual_length unchanged.
    const uint32_t word0 = eloqstore::DecodeFixed32(encoded.data());
    REQUIRE((word0 & eloqstore::kLargeValueHasMetadataBit) != 0);
    REQUIRE((word0 & eloqstore::kLargeValueLengthMask) == actual_len);

    // metadata_length immediately follows the segment-id array.
    const size_t segments_end =
        sizeof(uint32_t) + ids.size() * sizeof(eloqstore::PageId);
    const uint16_t trailer_len =
        eloqstore::DecodeFixed16(encoded.data() + segments_end);
    REQUIRE(trailer_len == metadata.size());

    // Header parses cleanly.
    auto header = eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->actual_length == actual_len);
    REQUIRE(header->num_segments == ids.size());
    REQUIRE(header->metadata_length == metadata.size());

    // Decode round-trip with metadata sink.
    std::vector<eloqstore::PageId> out(header->num_segments);
    std::string decoded_metadata = "previous";  // ensure decode overwrites
    REQUIRE(
        eloqstore::DecodeLargeValueContent(encoded,
                                           *header,
                                           std::span<eloqstore::PageId>(out),
                                           &decoded_metadata));
    for (size_t i = 0; i < ids.size(); ++i)
    {
        REQUIRE(out[i] == ids[i]);
    }
    REQUIRE(decoded_metadata == metadata);

    // Decode without a metadata sink also succeeds; segment IDs unchanged.
    std::fill(out.begin(), out.end(), 0u);
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out)));
    for (size_t i = 0; i < ids.size(); ++i)
    {
        REQUIRE(out[i] == ids[i]);
    }
}

TEST_CASE("DecodeLargeValueContent skips segments when span is empty",
          "[large-value]")
{
    std::vector<eloqstore::PageId> ids = {0xc0de01, 0xc0de02, 0xc0de03};
    const std::string metadata = "meta-only-fast-path";
    const uint32_t actual_len = 2u * kTestSegSize + 1u;  // 3 segments

    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded, metadata);

    auto header = eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize);
    REQUIRE(header.has_value());

    // Metadata-only path: caller doesn't supply a segment-id buffer.
    std::string sink;
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, /*segment_ids=*/{}, &sink));
    REQUIRE(sink == metadata);

    // Same call but with both outputs skipped is a structural validation
    // only (returns true without writing anything).
    REQUIRE(eloqstore::DecodeLargeValueContent(encoded, *header));

    // Segment-only path: caller skips metadata.
    std::vector<eloqstore::PageId> out(header->num_segments);
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out)));
    for (size_t i = 0; i < ids.size(); ++i)
    {
        REQUIRE(out[i] == ids[i]);
    }
}

TEST_CASE("DecodeLargeValueContent clears metadata sink when flag is unset",
          "[large-value]")
{
    std::vector<eloqstore::PageId> ids = {1, 2};
    const uint32_t actual_len = kTestSegSize + 1u;  // 2 segments
    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded);

    auto header = eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->metadata_length == 0);

    std::vector<eloqstore::PageId> out(header->num_segments);
    std::string sink = "stale-value";
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out), &sink));
    REQUIRE(sink.empty());
}

TEST_CASE("EncodeLargeValueContent round-trip at 10 MiB actual_length",
          "[large-value]")
{
    // 10 MiB is the upper end of the KV Cache target value size. At a
    // 256 KiB segment size, K = 40 segments; the segment-array length is
    // 160 bytes -- well under data-page capacity -- and actual_length sits
    // far from the 31-bit boundary so neither low- nor high-half encoding
    // bugs would surface on the earlier tests.
    constexpr uint32_t actual_len = 10u * 1024u * 1024u;
    constexpr uint32_t kSegSize = 256u * 1024u;
    constexpr uint32_t k = (actual_len + kSegSize - 1) / kSegSize;
    REQUIRE(k == 40);

    std::vector<eloqstore::PageId> ids;
    ids.reserve(k);
    for (uint32_t i = 0; i < k; ++i)
    {
        ids.push_back(0xc0ffee00u + i);
    }
    const std::string metadata =
        "tensor[10MiB,bf16,key=" + std::to_string(actual_len) + "]";

    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded, metadata);

    auto header = eloqstore::DecodeLargeValueHeader(encoded, kSegSize);
    REQUIRE(header.has_value());
    REQUIRE(header->actual_length == actual_len);
    REQUIRE(header->num_segments == k);
    REQUIRE(header->metadata_length == metadata.size());

    std::vector<eloqstore::PageId> out(header->num_segments);
    std::string sink;
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out), &sink));
    REQUIRE(sink == metadata);
    for (uint32_t i = 0; i < k; ++i)
    {
        REQUIRE(out[i] == ids[i]);
    }
}

TEST_CASE("DecodeLargeValueHeader metadata at actual_length boundary",
          "[large-value]")
{
    // Near 31-bit boundary for actual_length: must not bleed into has_metadata
    // bit. Use a value just under INT32_MAX rounded to a manageable segment
    // count so the encoded buffer is small.
    constexpr uint32_t large_seg_size = 1u << 30;       // 1 GiB
    constexpr uint32_t boundary_len = (1u << 31) - 1u;  // INT32_MAX
    std::vector<eloqstore::PageId> ids = {7, 8};        // 2 segments
    const std::string metadata = "m";

    std::string encoded;
    eloqstore::EncodeLargeValueContent(boundary_len,
                                       std::span<const eloqstore::PageId>(ids),
                                       encoded,
                                       metadata);

    auto header = eloqstore::DecodeLargeValueHeader(encoded, large_seg_size);
    REQUIRE(header.has_value());
    REQUIRE(header->actual_length == boundary_len);
    REQUIRE(header->num_segments == 2);
    REQUIRE(header->metadata_length == metadata.size());

    std::vector<eloqstore::PageId> out(header->num_segments);
    std::string sink;
    REQUIRE(eloqstore::DecodeLargeValueContent(
        encoded, *header, std::span<eloqstore::PageId>(out), &sink));
    REQUIRE(out[0] == 7u);
    REQUIRE(out[1] == 8u);
    REQUIRE(sink == metadata);
}

TEST_CASE("DecodeLargeValueHeader rejects truncated metadata trailer",
          "[large-value]")
{
    // Build a valid encoded blob with metadata, then truncate so the
    // metadata_length field declares more bytes than remain.
    std::vector<eloqstore::PageId> ids = {42};
    const uint32_t actual_len = kTestSegSize;  // exactly 1 segment
    std::string encoded;
    eloqstore::EncodeLargeValueContent(actual_len,
                                       std::span<const eloqstore::PageId>(ids),
                                       encoded,
                                       std::string(8, 'x'));

    // Drop the 8 metadata bytes; keep word0 (with has_metadata=1), segment
    // id, and the metadata_length field claiming 8 bytes follow.
    REQUIRE(encoded.size() == sizeof(uint32_t) +
                                  ids.size() * sizeof(eloqstore::PageId) +
                                  sizeof(uint16_t) + 8u);
    std::string truncated(encoded.data(),
                          encoded.size() - 8u);  // drop the metadata bytes
    REQUIRE_FALSE(
        eloqstore::DecodeLargeValueHeader(truncated, kTestSegSize).has_value());
}

TEST_CASE("DecodeLargeValueHeader rejects mismatched segment-count length",
          "[large-value]")
{
    // An encoded blob whose tail length doesn't match the segment count
    // implied by actual_length must be rejected.
    std::vector<eloqstore::PageId> ids = {1, 2, 3};
    const uint32_t actual_len = 2u * kTestSegSize + 1u;  // 3 segments
    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), encoded);

    // Append a stray byte so the encoded size no longer matches the expected
    // segment area length.
    encoded.push_back('\0');
    REQUIRE_FALSE(
        eloqstore::DecodeLargeValueHeader(encoded, kTestSegSize).has_value());
}
