#include "../include/global_registered_memory.h"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <set>
#include <utility>
#include <vector>

namespace
{
constexpr uint32_t kSegment128K = 128 * 1024;
constexpr uint32_t kSegment256K = 256 * 1024;
constexpr uint32_t kSegment512K = 512 * 1024;

// Confirm a pointer is aligned to `align` bytes.
bool IsAligned(const char *p, size_t align)
{
    return (reinterpret_cast<uintptr_t>(p) % align) == 0;
}
}  // namespace

TEST_CASE(
    "GlobalRegisteredMemory reports chunks with correct size and alignment",
    "[global-registered-memory]")
{
    const size_t chunk_size = kSegment256K * 4;  // 1MB per chunk
    const size_t total_size = chunk_size * 3;    // 3 chunks, 3MB total
    eloqstore::GlobalRegisteredMemory mem(kSegment256K, chunk_size, total_size);

    auto chunks = mem.MemChunks();
    REQUIRE(chunks.size() == 3);
    for (const eloqstore::MemChunk &c : chunks)
    {
        REQUIRE(c.size_ == chunk_size);
        REQUIRE(IsAligned(c.base_, 4096));
    }
    REQUIRE(mem.TotalSegments() == total_size / kSegment256K);
    REQUIRE(mem.FreeSegments() == mem.TotalSegments());
    REQUIRE(mem.SegmentSize() == kSegment256K);
}

TEST_CASE(
    "GlobalRegisteredMemory allocates every segment exactly once, then empties",
    "[global-registered-memory]")
{
    const size_t chunk_size = kSegment128K * 8;  // 1MB
    const size_t total_size = chunk_size * 2;    // 2MB
    eloqstore::GlobalRegisteredMemory mem(kSegment128K, chunk_size, total_size);

    const size_t total = mem.TotalSegments();
    std::set<char *> seen;
    std::vector<std::pair<char *, uint32_t>> allocated;
    allocated.reserve(total);

    int yield_calls = 0;
    auto never_yield = [&yield_calls]() { ++yield_calls; };

    for (size_t i = 0; i < total; ++i)
    {
        auto [seg, cidx] = mem.GetSegment(never_yield);
        REQUIRE(seg != nullptr);
        REQUIRE(cidx < mem.MemChunks().size());
        REQUIRE(IsAligned(seg, 4096));
        // Every segment must be distinct.
        REQUIRE(seen.insert(seg).second);
        allocated.emplace_back(seg, cidx);
    }

    REQUIRE(yield_calls == 0);
    REQUIRE(mem.FreeSegments() == 0);

    // Recycle everything in FIFO order, confirm counter restored.
    for (auto &[seg, cidx] : allocated)
    {
        mem.Recycle(seg, cidx);
    }
    REQUIRE(mem.FreeSegments() == total);
}

TEST_CASE("GlobalRegisteredMemory recycle is LIFO",
          "[global-registered-memory]")
{
    const size_t chunk_size = kSegment256K * 4;
    eloqstore::GlobalRegisteredMemory mem(kSegment256K, chunk_size, chunk_size);

    auto never_yield = []() {};
    auto a = mem.GetSegment(never_yield);
    auto b = mem.GetSegment(never_yield);
    auto c = mem.GetSegment(never_yield);
    REQUIRE(a.first != b.first);
    REQUIRE(b.first != c.first);

    // Recycle in order A, B, C; LIFO means next GetSegment returns C, then B,
    // then A.
    mem.Recycle(a.first, a.second);
    mem.Recycle(b.first, b.second);
    mem.Recycle(c.first, c.second);

    auto r1 = mem.GetSegment(never_yield);
    auto r2 = mem.GetSegment(never_yield);
    auto r3 = mem.GetSegment(never_yield);
    REQUIRE(r1.first == c.first);
    REQUIRE(r2.first == b.first);
    REQUIRE(r3.first == a.first);
}

TEST_CASE(
    "GlobalRegisteredMemory only returns recycled segments when pool was "
    "drained",
    "[global-registered-memory]")
{
    const size_t chunk_size = kSegment256K * 2;  // 2 segments per chunk
    const size_t total_size = chunk_size * 2;    // 4 segments total
    eloqstore::GlobalRegisteredMemory mem(kSegment256K, chunk_size, total_size);

    auto never_yield = []() {};
    std::vector<std::pair<char *, uint32_t>> all;
    for (size_t i = 0; i < mem.TotalSegments(); ++i)
    {
        all.push_back(mem.GetSegment(never_yield));
    }
    REQUIRE(mem.FreeSegments() == 0);

    // Recycle only one; the next allocation must equal that specific segment.
    auto &only = all[2];
    mem.Recycle(only.first, only.second);
    auto next = mem.GetSegment(never_yield);
    REQUIRE(next.first == only.first);
    REQUIRE(next.second == only.second);
    REQUIRE(mem.FreeSegments() == 0);

    // Clean up.
    for (auto &[seg, cidx] : all)
    {
        mem.Recycle(seg, cidx);
    }
}

TEST_CASE(
    "GlobalRegisteredMemory reuses across chunks when only later chunk has "
    "free segments",
    "[global-registered-memory]")
{
    const size_t chunk_size = kSegment128K * 4;  // 4 segments per chunk
    const size_t total_size = chunk_size * 2;    // 2 chunks, 8 segments
    eloqstore::GlobalRegisteredMemory mem(kSegment128K, chunk_size, total_size);

    auto never_yield = []() {};
    std::vector<std::pair<char *, uint32_t>> all;
    for (size_t i = 0; i < mem.TotalSegments(); ++i)
    {
        all.push_back(mem.GetSegment(never_yield));
    }
    REQUIRE(mem.FreeSegments() == 0);

    // Recycle only segments that came from chunk index 1 (the second chunk).
    std::vector<std::pair<char *, uint32_t>> chunk1_segments;
    for (auto &e : all)
    {
        if (e.second == 1)
        {
            chunk1_segments.push_back(e);
        }
    }
    REQUIRE(!chunk1_segments.empty());
    for (auto &e : chunk1_segments)
    {
        mem.Recycle(e.first, e.second);
    }

    // Every subsequent allocation must come from chunk 1.
    for (size_t i = 0; i < chunk1_segments.size(); ++i)
    {
        auto next = mem.GetSegment(never_yield);
        REQUIRE(next.second == 1);
    }

    // Drain again before destruction.
    for (auto &e : all)
    {
        if (e.second == 0)
        {
            mem.Recycle(e.first, e.second);
        }
    }
}

TEST_CASE("GlobalRegisteredMemory eviction hook fires when pool is empty",
          "[global-registered-memory]")
{
    const size_t chunk_size = kSegment256K * 2;  // 2 segments
    eloqstore::GlobalRegisteredMemory mem(kSegment256K, chunk_size, chunk_size);

    auto never_yield = []() {};
    auto held_a = mem.GetSegment(never_yield);
    auto held_b = mem.GetSegment(never_yield);
    REQUIRE(mem.FreeSegments() == 0);

    // Eviction hook recycles held_a on first call.
    int evict_calls = 0;
    mem.SetEvictFunc(
        [&]()
        {
            ++evict_calls;
            if (evict_calls == 1)
            {
                mem.Recycle(held_a.first, held_a.second);
            }
        });

    int yield_calls = 0;
    auto tracking_yield = [&]() { ++yield_calls; };

    auto got = mem.GetSegment(tracking_yield);
    REQUIRE(evict_calls == 1);
    // Yield was invoked once on the empty-then-refill path; after yielding,
    // the loop re-enters TryGetSegment which now returns the evicted segment.
    REQUIRE(yield_calls == 1);
    REQUIRE(got.first == held_a.first);

    // Clean up.
    mem.Recycle(got.first, got.second);
    mem.Recycle(held_b.first, held_b.second);
}

TEST_CASE(
    "GlobalRegisteredMemory yields repeatedly until eviction frees a segment",
    "[global-registered-memory]")
{
    const size_t chunk_size = kSegment256K * 2;
    eloqstore::GlobalRegisteredMemory mem(kSegment256K, chunk_size, chunk_size);

    auto never_yield = []() {};
    auto held_a = mem.GetSegment(never_yield);
    auto held_b = mem.GetSegment(never_yield);
    REQUIRE(mem.FreeSegments() == 0);

    // Eviction hook is a no-op for the first 3 calls, then recycles held_a on
    // the 4th. Each empty loop should fire (evict_func_, yield) exactly once.
    int evict_calls = 0;
    mem.SetEvictFunc(
        [&]()
        {
            ++evict_calls;
            if (evict_calls == 4)
            {
                mem.Recycle(held_a.first, held_a.second);
            }
        });

    int yield_calls = 0;
    auto tracking_yield = [&]() { ++yield_calls; };

    auto got = mem.GetSegment(tracking_yield);
    REQUIRE(evict_calls == 4);
    REQUIRE(yield_calls == 4);
    REQUIRE(got.first == held_a.first);

    // Clean up.
    mem.Recycle(got.first, got.second);
    mem.Recycle(held_b.first, held_b.second);
}
