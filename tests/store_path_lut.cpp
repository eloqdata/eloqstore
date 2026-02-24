#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <vector>

#include "kv_options.h"

using eloqstore::ComputeStorePathLut;

TEST_CASE("ComputeStorePathLut preserves ratios regardless of order",
          "[store_path_lut]")
{
    std::vector<uint64_t> weights{4, 1, 1};
    auto lut = ComputeStorePathLut(weights, 64);

    REQUIRE(lut.size() == 6);
    REQUIRE(std::count(lut.begin(), lut.end(), 0) == 4);
    REQUIRE(std::count(lut.begin(), lut.end(), 1) == 1);
    REQUIRE(std::count(lut.begin(), lut.end(), 2) == 1);
}

TEST_CASE("ComputeStorePathLut normalizes gcd", "[store_path_lut]")
{
    std::vector<uint64_t> weights{2000, 500, 500};
    auto lut = ComputeStorePathLut(weights, 64);

    REQUIRE(lut.size() == 6);
    REQUIRE(std::count(lut.begin(), lut.end(), 0) == 4);
    REQUIRE(std::count(lut.begin(), lut.end(), 1) == 1);
    REQUIRE(std::count(lut.begin(), lut.end(), 2) == 1);
}

TEST_CASE("ComputeStorePathLut respects max entries", "[store_path_lut]")
{
    std::vector<uint64_t> weights{0, 5};
    auto lut = ComputeStorePathLut(weights, 3);

    REQUIRE(lut.size() == 3);
    REQUIRE(std::count(lut.begin(), lut.end(), 0) == 1);
    REQUIRE(std::count(lut.begin(), lut.end(), 1) == 2);
}

TEST_CASE("ComputeStorePathLut handles fractional-style ratios",
          "[store_path_lut]")
{
    std::vector<uint64_t> weights{1, 2000, 1};
    auto lut = ComputeStorePathLut(weights, 4096);

    REQUIRE(lut.size() == 2002);
    REQUIRE(std::count(lut.begin(), lut.end(), 0) == 1);
    REQUIRE(std::count(lut.begin(), lut.end(), 1) == 2000);
    REQUIRE(std::count(lut.begin(), lut.end(), 2) == 1);
}
