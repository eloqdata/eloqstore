#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "../include/global_registered_memory.h"
#include "../include/io_buffer_ref.h"
#include "../include/io_string_buffer.h"

namespace
{
constexpr uint32_t kSegment256K = 256 * 1024;
constexpr uint16_t kRegBase = 7;

eloqstore::GlobalRegisteredMemory MakeMem(size_t num_chunks = 2,
                                          size_t segments_per_chunk = 4)
{
    const size_t chunk_size = kSegment256K * segments_per_chunk;
    return eloqstore::GlobalRegisteredMemory(kSegment256K, chunk_size,
                                             chunk_size * num_chunks);
}
}  // namespace

TEST_CASE("IoStringBuffer non-copyable, movable", "[io-string-buffer]")
{
    STATIC_REQUIRE(!std::is_copy_constructible_v<eloqstore::IoStringBuffer>);
    STATIC_REQUIRE(!std::is_copy_assignable_v<eloqstore::IoStringBuffer>);
    STATIC_REQUIRE(std::is_move_constructible_v<eloqstore::IoStringBuffer>);
    STATIC_REQUIRE(std::is_move_assignable_v<eloqstore::IoStringBuffer>);
}

TEST_CASE("IoStringBuffer Append/Fragments/Size accounting",
          "[io-string-buffer]")
{
    eloqstore::IoStringBuffer buf;
    REQUIRE(buf.Size() == 0);
    REQUIRE(buf.Fragments().empty());

    eloqstore::IoBufferRef a{reinterpret_cast<char *>(0x1000), 0};
    eloqstore::IoBufferRef b{reinterpret_cast<char *>(0x2000), 1};
    eloqstore::IoBufferRef c{reinterpret_cast<char *>(0x3000), 2};
    buf.Append(a);
    buf.Append(b);
    buf.Append(c);
    buf.SetSize(3 * kSegment256K - 100);

    const auto &frags = buf.Fragments();
    REQUIRE(frags.size() == 3);
    REQUIRE(frags[0].data_ == a.data_);
    REQUIRE(frags[0].buf_index_ == a.buf_index_);
    REQUIRE(frags[1].data_ == b.data_);
    REQUIRE(frags[2].data_ == c.data_);
    REQUIRE(buf.Size() == 3 * kSegment256K - 100);
}

TEST_CASE("IoStringBuffer move leaves source empty", "[io-string-buffer]")
{
    eloqstore::IoStringBuffer src;
    src.Append({reinterpret_cast<char *>(0x1000), 0});
    src.SetSize(42);

    eloqstore::IoStringBuffer dst(std::move(src));
    REQUIRE(dst.Size() == 42);
    REQUIRE(dst.Fragments().size() == 1);
    REQUIRE(src.Size() == 0);
    REQUIRE(src.Fragments().empty());

    eloqstore::IoStringBuffer assigned;
    assigned.Append({reinterpret_cast<char *>(0x9000), 9});
    assigned.SetSize(99);
    assigned = std::move(dst);
    REQUIRE(assigned.Size() == 42);
    REQUIRE(assigned.Fragments().size() == 1);
    REQUIRE(dst.Size() == 0);
    REQUIRE(dst.Fragments().empty());
}

TEST_CASE("IoStringBuffer Recycle returns every fragment to pool",
          "[io-string-buffer]")
{
    eloqstore::GlobalRegisteredMemory mem = MakeMem(/*num_chunks=*/2,
                                                    /*segments_per_chunk=*/4);
    const size_t total = mem.TotalSegments();
    auto never_yield = []() {};

    // Allocate all segments, wrap each as an IoBufferRef using a fake base.
    std::vector<std::pair<char *, uint32_t>> allocated;
    eloqstore::IoStringBuffer buf;
    for (size_t i = 0; i < total; ++i)
    {
        auto [seg, cidx] = mem.GetSegment(never_yield);
        allocated.emplace_back(seg, cidx);
        buf.Append({seg, static_cast<uint16_t>(cidx + kRegBase)});
    }
    buf.SetSize(total * kSegment256K);
    REQUIRE(mem.FreeSegments() == 0);

    buf.Recycle(&mem, kRegBase);

    REQUIRE(mem.FreeSegments() == total);
    REQUIRE(buf.Size() == 0);
    REQUIRE(buf.Fragments().empty());

    // Re-allocate to confirm every original pointer is reachable again.
    std::vector<char *> reallocated;
    for (size_t i = 0; i < total; ++i)
    {
        reallocated.push_back(mem.GetSegment(never_yield).first);
    }
    for (const auto &orig : allocated)
    {
        bool found = false;
        for (char *p : reallocated)
        {
            if (p == orig.first)
            {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }
}

TEST_CASE("IoStringBuffer Recycle on empty buffer is a no-op",
          "[io-string-buffer]")
{
    eloqstore::GlobalRegisteredMemory mem = MakeMem();
    const size_t total = mem.TotalSegments();

    eloqstore::IoStringBuffer buf;
    buf.Recycle(&mem, kRegBase);  // must not crash or alter pool
    REQUIRE(mem.FreeSegments() == total);
    REQUIRE(buf.Size() == 0);
    REQUIRE(buf.Fragments().empty());
}

TEST_CASE(
    "IoStringBuffer Recycle recovers chunk index across multiple chunks",
    "[io-string-buffer]")
{
    eloqstore::GlobalRegisteredMemory mem = MakeMem(/*num_chunks=*/3,
                                                    /*segments_per_chunk=*/2);
    const size_t total = mem.TotalSegments();
    auto never_yield = []() {};

    eloqstore::IoStringBuffer buf;
    for (size_t i = 0; i < total; ++i)
    {
        auto [seg, cidx] = mem.GetSegment(never_yield);
        REQUIRE(cidx < mem.MemChunks().size());
        // Deliberately vary reg base so a plain cast wouldn't match.
        buf.Append({seg, static_cast<uint16_t>(cidx + kRegBase)});
    }
    buf.SetSize(total * kSegment256K);
    REQUIRE(mem.FreeSegments() == 0);

    buf.Recycle(&mem, kRegBase);
    REQUIRE(mem.FreeSegments() == total);
}
