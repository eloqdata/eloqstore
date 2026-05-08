#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "../include/storage/data_page.h"
#include "../include/types.h"

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
    for (uint32_t k : {1u, 2u})
    {
        std::vector<eloqstore::PageId> ids;
        for (uint32_t i = 0; i < k; ++i)
        {
            ids.push_back(0xdeadbee0 + i);
        }
        const uint32_t actual_len = 3'140'000u;  // arbitrary
        std::string dst;
        eloqstore::EncodeLargeValueContent(
            actual_len, std::span<const eloqstore::PageId>(ids), dst);
        REQUIRE(dst.size() ==
                sizeof(uint32_t) + ids.size() * sizeof(eloqstore::PageId));

        uint32_t decoded_len = 0;
        std::vector<eloqstore::PageId> out(k);
        uint32_t n = eloqstore::DecodeLargeValueContent(
            dst, decoded_len, std::span<eloqstore::PageId>(out));
        REQUIRE(n == k);
        REQUIRE(decoded_len == actual_len);
        for (uint32_t i = 0; i < k; ++i)
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
    const uint32_t actual_len = 8'000'000u;
    std::string dst;
    eloqstore::EncodeLargeValueContent(
        actual_len, std::span<const eloqstore::PageId>(ids), dst);

    uint32_t decoded_len = 0;
    std::vector<eloqstore::PageId> out(k);
    uint32_t n = eloqstore::DecodeLargeValueContent(
        dst, decoded_len, std::span<eloqstore::PageId>(out));
    REQUIRE(n == k);
    REQUIRE(decoded_len == actual_len);
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
    eloqstore::EncodeLargeValueContent(
        0xabcdu, std::span<const eloqstore::PageId>(ids), dst);
    REQUIRE(dst.substr(0, 8) == "preamble");

    std::string_view payload(dst.data() + 8, dst.size() - 8);
    uint32_t decoded_len = 0;
    std::vector<eloqstore::PageId> out(2);
    uint32_t n = eloqstore::DecodeLargeValueContent(
        payload, decoded_len, std::span<eloqstore::PageId>(out));
    REQUIRE(n == 2);
    REQUIRE(decoded_len == 0xabcdu);
    REQUIRE(out[0] == 42u);
    REQUIRE(out[1] == 43u);
}

TEST_CASE("DecodeLargeValueContent rejects truncated prefix", "[large-value]")
{
    uint32_t actual_len = 0;
    std::vector<eloqstore::PageId> out(4);

    // empty
    REQUIRE(eloqstore::DecodeLargeValueContent(
                std::string_view(),
                actual_len,
                std::span<eloqstore::PageId>(out)) == 0);

    // 1 byte (< 4 required for actual_length)
    std::string s1(1, '\0');
    REQUIRE(eloqstore::DecodeLargeValueContent(
                s1, actual_len, std::span<eloqstore::PageId>(out)) == 0);

    // 3 bytes (still short)
    std::string s3(3, '\0');
    REQUIRE(eloqstore::DecodeLargeValueContent(
                s3, actual_len, std::span<eloqstore::PageId>(out)) == 0);
}

TEST_CASE("DecodeLargeValueContent rejects when output span is too small",
          "[large-value]")
{
    std::vector<eloqstore::PageId> ids = {1, 2, 3, 4};
    std::string encoded;
    eloqstore::EncodeLargeValueContent(
        100u, std::span<const eloqstore::PageId>(ids), encoded);

    std::vector<eloqstore::PageId> small_out(2);
    uint32_t actual_len = 0;
    uint32_t n = eloqstore::DecodeLargeValueContent(
        encoded, actual_len, std::span<eloqstore::PageId>(small_out));
    REQUIRE(n == 0);
}

TEST_CASE("DecodeLargeValueContent with zero segments", "[large-value]")
{
    // Four bytes encoding only actual_length; zero segment IDs follow.
    std::string encoded;
    std::vector<eloqstore::PageId> empty;
    eloqstore::EncodeLargeValueContent(
        7u, std::span<const eloqstore::PageId>(empty), encoded);
    REQUIRE(encoded.size() == sizeof(uint32_t));

    std::vector<eloqstore::PageId> out;
    uint32_t actual_len = 0;
    uint32_t n = eloqstore::DecodeLargeValueContent(
        encoded, actual_len, std::span<eloqstore::PageId>(out));
    REQUIRE(n == 0);
    REQUIRE(actual_len == 7u);
}
