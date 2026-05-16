#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "../include/async_io_manager.h"

using ChunkArray = std::vector<std::pair<char *, size_t>>;

TEST_CASE("LookupBufIndex resolves boundary addresses across multiple chunks",
          "[pinned-lookup]")
{
    // Three back-to-back chunks at fixed virtual addresses. We never
    // dereference the pointers, so any non-null base works.
    char *const base0 = reinterpret_cast<char *>(0x10000);
    char *const base1 = reinterpret_cast<char *>(0x20000);
    char *const base2 = reinterpret_cast<char *>(0x30000);
    constexpr size_t kChunkSize = 4096;
    ChunkArray chunks = {
        {base0, kChunkSize},
        {base1, kChunkSize},
        {base2, kChunkSize},
    };
    constexpr uint16_t kBase = 7;  // arbitrary iovec-index base

    // First byte of each chunk maps to its own index.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, kBase, base0) ==
            kBase + 0);
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, kBase, base1) ==
            kBase + 1);
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, kBase, base2) ==
            kBase + 2);

    // Last byte of each chunk still resolves to that chunk.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(
                chunks, kBase, base0 + kChunkSize - 1) == kBase + 0);
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(
                chunks, kBase, base1 + kChunkSize - 1) == kBase + 1);
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(
                chunks, kBase, base2 + kChunkSize - 1) == kBase + 2);

    // Mid-chunk addresses also work.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(
                chunks, kBase, base1 + kChunkSize / 2) == kBase + 1);
}

TEST_CASE("LookupBufIndex returns sentinel for out-of-range pointers",
          "[pinned-lookup]")
{
    char *const base = reinterpret_cast<char *>(0x10000);
    constexpr size_t kChunkSize = 4096;
    ChunkArray chunks = {{base, kChunkSize}};

    // One byte before the chunk -- not contained.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, 0, base - 1) ==
            std::numeric_limits<uint16_t>::max());

    // First byte past the chunk -- exclusive upper bound.
    REQUIRE(
        eloqstore::IouringMgr::LookupBufIndex(chunks, 0, base + kChunkSize) ==
        std::numeric_limits<uint16_t>::max());

    // Empty chunk array.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex({}, 0, base) ==
            std::numeric_limits<uint16_t>::max());
}

TEST_CASE("LookupBufIndex skips gaps between chunks", "[pinned-lookup]")
{
    // Chunks separated by an unmapped region. An address in the gap must not
    // be reported as belonging to either chunk.
    char *const base0 = reinterpret_cast<char *>(0x10000);
    char *const base1 = reinterpret_cast<char *>(0x40000);
    constexpr size_t kChunkSize = 0x1000;
    ChunkArray chunks = {{base0, kChunkSize}, {base1, kChunkSize}};

    // Address in the gap.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(
                chunks, 0, base0 + kChunkSize + 16) ==
            std::numeric_limits<uint16_t>::max());

    // Addresses inside each chunk still resolve correctly.
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, 0, base0) == 0);
    REQUIRE(eloqstore::IouringMgr::LookupBufIndex(chunks, 0, base1) == 1);
}

TEST_CASE("LookupPinnedChunk returns full chunk bounds and buf_index",
          "[pinned-lookup]")
{
    // Three back-to-back chunks. We never dereference the pointers, so any
    // non-null base works.
    char *const base0 = reinterpret_cast<char *>(0x10000);
    char *const base1 = reinterpret_cast<char *>(0x20000);
    char *const base2 = reinterpret_cast<char *>(0x30000);
    constexpr size_t kChunkSize = 4096;
    ChunkArray chunks = {
        {base0, kChunkSize},
        {base1, kChunkSize},
        {base2, kChunkSize},
    };
    constexpr uint16_t kBase = 7;  // arbitrary iovec-index base

    // Mid-chunk lookup returns the full record for the containing chunk.
    auto mid1 = eloqstore::IouringMgr::LookupPinnedChunk(
        chunks, kBase, base1 + kChunkSize / 2);
    REQUIRE(mid1.has_value());
    REQUIRE(mid1->base == base1);
    REQUIRE(mid1->size == kChunkSize);
    REQUIRE(mid1->buf_index == kBase + 1);

    // First byte of a chunk -> that chunk.
    auto first0 =
        eloqstore::IouringMgr::LookupPinnedChunk(chunks, kBase, base0);
    REQUIRE(first0.has_value());
    REQUIRE(first0->base == base0);
    REQUIRE(first0->buf_index == kBase + 0);

    // Last byte (exclusive upper bound minus one) -> same chunk.
    auto last2 = eloqstore::IouringMgr::LookupPinnedChunk(
        chunks, kBase, base2 + kChunkSize - 1);
    REQUIRE(last2.has_value());
    REQUIRE(last2->base == base2);
    REQUIRE(last2->buf_index == kBase + 2);
}

TEST_CASE("LookupPinnedChunk returns nullopt for out-of-range pointers",
          "[pinned-lookup]")
{
    char *const base = reinterpret_cast<char *>(0x10000);
    constexpr size_t kChunkSize = 4096;
    ChunkArray chunks = {{base, kChunkSize}};

    // One byte before the chunk.
    REQUIRE_FALSE(eloqstore::IouringMgr::LookupPinnedChunk(chunks, 0, base - 1)
                      .has_value());

    // First byte past the chunk -- exclusive upper bound.
    REQUIRE_FALSE(
        eloqstore::IouringMgr::LookupPinnedChunk(chunks, 0, base + kChunkSize)
            .has_value());

    // Empty chunk array.
    REQUIRE_FALSE(
        eloqstore::IouringMgr::LookupPinnedChunk({}, 0, base).has_value());
}

TEST_CASE("LookupPinnedChunk skips gaps between chunks", "[pinned-lookup]")
{
    // Chunks separated by an unmapped region. An address in the gap must
    // resolve to nullopt, not to either chunk.
    char *const base0 = reinterpret_cast<char *>(0x10000);
    char *const base1 = reinterpret_cast<char *>(0x40000);
    constexpr size_t kChunkSize = 0x1000;
    ChunkArray chunks = {{base0, kChunkSize}, {base1, kChunkSize}};

    REQUIRE_FALSE(eloqstore::IouringMgr::LookupPinnedChunk(
                      chunks, 0, base0 + kChunkSize + 16)
                      .has_value());

    // Each chunk's own range still resolves to itself.
    auto in0 = eloqstore::IouringMgr::LookupPinnedChunk(chunks, 0, base0);
    REQUIRE(in0.has_value());
    REQUIRE(in0->buf_index == 0);
    auto in1 = eloqstore::IouringMgr::LookupPinnedChunk(chunks, 0, base1);
    REQUIRE(in1.has_value());
    REQUIRE(in1->buf_index == 1);
}
