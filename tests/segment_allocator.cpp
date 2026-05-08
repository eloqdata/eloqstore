#include <catch2/catch_test_macros.hpp>
#include <cstdint>

#include "../include/storage/page_mapper.h"
#include "../include/types.h"

TEST_CASE("AppendAllocator allocates strictly monotonically", "[segment-alloc]")
{
    // 8 segments per file (shift=3).
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3},
                                     /*min_file_id=*/0,
                                     /*max_fp_id=*/0,
                                     /*empty_cnt=*/0);

    eloqstore::FilePageId prev = alloc.Allocate();
    REQUIRE(prev == 0);
    for (int i = 1; i < 100; ++i)
    {
        eloqstore::FilePageId next = alloc.Allocate();
        REQUIRE(next == prev + 1);
        prev = next;
    }
}

TEST_CASE("AppendAllocator reports PagesPerFile matching configured shift",
          "[segment-alloc]")
{
    eloqstore::AppendAllocator a0(/*shift=*/uint8_t{0}, 0, 0, 0);
    eloqstore::AppendAllocator a3(/*shift=*/uint8_t{3}, 0, 0, 0);
    eloqstore::AppendAllocator a7(/*shift=*/uint8_t{7}, 0, 0, 0);
    REQUIRE(a0.PagesPerFile() == 1u);
    REQUIRE(a3.PagesPerFile() == 8u);
    REQUIRE(a7.PagesPerFile() == 128u);
}

TEST_CASE("AppendAllocator CurrentFileId advances at file boundary",
          "[segment-alloc]")
{
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3}, 0, 0, 0);
    // Allocate 8 segments; file id 0 is "current" throughout because the next
    // unallocated id (max_fp_id_) still sits in the next file until we cross.
    for (int i = 0; i < 8; ++i)
    {
        eloqstore::FilePageId fp = alloc.Allocate();
        REQUIRE((fp >> 3) == 0u);
    }
    // max_fp_id_ now == 8 → CurrentFileId = 8 >> 3 == 1.
    REQUIRE(alloc.CurrentFileId() == 1u);
    REQUIRE(alloc.MaxFilePageId() == 8u);

    // Allocate 8 more in file 1, then one into file 2.
    for (int i = 0; i < 8; ++i)
    {
        eloqstore::FilePageId fp = alloc.Allocate();
        REQUIRE((fp >> 3) == 1u);
    }
    REQUIRE(alloc.CurrentFileId() == 2u);
    REQUIRE(alloc.MaxFilePageId() == 16u);

    eloqstore::FilePageId first_in_file_2 = alloc.Allocate();
    REQUIRE((first_in_file_2 >> 3) == 2u);
    REQUIRE(alloc.MaxFilePageId() == 17u);
}

TEST_CASE("AppendAllocator restored state picks up where it left off",
          "[segment-alloc]")
{
    // Simulate a replayer: allocator restored with max_fp_id=17, min_file=0.
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3},
                                     /*min_file_id=*/0,
                                     /*max_fp_id=*/17,
                                     /*empty_cnt=*/0);
    REQUIRE(alloc.MaxFilePageId() == 17u);
    REQUIRE(alloc.CurrentFileId() == (17u >> 3));
    REQUIRE(alloc.MinFileId() == 0u);

    eloqstore::FilePageId next = alloc.Allocate();
    REQUIRE(next == 17u);
    REQUIRE(alloc.MaxFilePageId() == 18u);
}

TEST_CASE("AppendAllocator UpdateStat retires empty low files",
          "[segment-alloc]")
{
    // Simulate the Phase 8 empty-mapper path: allocate into files 0..2, then
    // UpdateStat retires files 0 and 1 as empty. SpaceSize should reflect only
    // the still-occupied range.
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3}, 0, 0, 0);
    for (int i = 0; i < 24; ++i)
    {
        alloc.Allocate();
    }
    // 24 pages allocated across 3 files; nothing retired yet.
    REQUIRE(alloc.MinFileId() == 0u);
    REQUIRE(alloc.SpaceSize() == 24u);

    // Move min_file_id forward to 2, declaring files 0..1 empty / reclaimed
    // (hole_cnt=0 because min jumped past them rather than leaving holes).
    alloc.UpdateStat(/*min_file_id=*/2, /*hole_cnt=*/0);
    REQUIRE(alloc.MinFileId() == 2u);
    // SpaceSize = (max_fp_id - (min_file_id << shift)) - (hole_cnt << shift)
    //           = (24 - 16) - 0 = 8 pages of live space in the tail file.
    REQUIRE(alloc.SpaceSize() == 8u);

    // A subsequent allocation still advances the tail.
    eloqstore::FilePageId next = alloc.Allocate();
    REQUIRE(next == 24u);
    REQUIRE(alloc.MaxFilePageId() == 25u);
}

TEST_CASE("AppendAllocator hole_cnt reduces SpaceSize", "[segment-alloc]")
{
    // Allocate 16 pages (2 files at shift=3). Declare the second file empty
    // via hole_cnt=1; SpaceSize should drop by PagesPerFile().
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3}, 0, 0, 0);
    for (int i = 0; i < 16; ++i)
    {
        alloc.Allocate();
    }
    REQUIRE(alloc.SpaceSize() == 16u);
    alloc.UpdateStat(/*min_file_id=*/0, /*hole_cnt=*/1);
    REQUIRE(alloc.SpaceSize() == 16u - 8u);
}

TEST_CASE("AppendAllocator clone preserves state", "[segment-alloc]")
{
    eloqstore::AppendAllocator alloc(/*shift=*/uint8_t{3},
                                     /*min_file_id=*/1,
                                     /*max_fp_id=*/20,
                                     /*empty_cnt=*/0);
    auto clone = alloc.Clone();
    REQUIRE(clone != nullptr);
    REQUIRE(clone->MaxFilePageId() == 20u);
    REQUIRE(clone->PagesPerFile() == 8u);
    // Clone must allocate independently of the source.
    REQUIRE(clone->Allocate() == 20u);
    REQUIRE(alloc.MaxFilePageId() == 20u);
}
