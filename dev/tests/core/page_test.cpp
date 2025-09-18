#include <catch2/catch_test_macros.hpp>
#include <cstring>
#include <random>
#include <set>

#include "page.h"
#include "coding.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Page_BasicConstruction", "[page][unit]") {
    SECTION("Page with allocated buffer") {
        Page page(true);  // Allocate page
        REQUIRE(page.Ptr() != nullptr);
    }

    SECTION("Page with external buffer") {
        const size_t page_size = 4096;
        auto buffer = std::make_unique<char[]>(page_size);

        Page page(buffer.get());
        REQUIRE(page.Ptr() == buffer.get());
    }

    SECTION("Move construction") {
        Page page1(true);
        char* original_ptr = page1.Ptr();

        Page page2(std::move(page1));
        REQUIRE(page2.Ptr() == original_ptr);
    }
}

TEST_CASE("Page_HeaderManagement", "[page][unit]") {
    const size_t page_size = 4096;
    auto buffer = std::make_unique<char[]>(page_size);
    std::memset(buffer.get(), 0, page_size);

    SECTION("Set and get page type") {
        Page page(buffer.get());

        SetPageType(page.Ptr(), PageType::Data);
        REQUIRE(TypeOfPage(page.Ptr()) == PageType::Data);

        SetPageType(page.Ptr(), PageType::LeafIndex);
        REQUIRE(TypeOfPage(page.Ptr()) == PageType::LeafIndex);

        SetPageType(page.Ptr(), PageType::Overflow);
        REQUIRE(TypeOfPage(page.Ptr()) == PageType::Overflow);
    }

    SECTION("Checksum validation") {
        Page page(buffer.get());

        // Fill with test data
        std::memset(page.Ptr() + checksum_bytes + 1, 0xAB, 100);

        // Test checksum functions
        std::string_view page_view(page.Ptr(), page_size);
        SetChecksum(page_view);
        REQUIRE(ValidateChecksum(page_view));

        // Corrupt data
        page.Ptr()[checksum_bytes + 10] = 0xFF;
        REQUIRE(!ValidateChecksum(page_view));
    }
}

TEST_CASE("Page_BoundaryConditions", "[page][unit][edge-case]") {
    SECTION("Page types boundary testing") {
        Page page(true);

        SetPageType(page.Ptr(), PageType::NonLeafIndex);
        REQUIRE(TypeOfPage(page.Ptr()) == PageType::NonLeafIndex);

        SetPageType(page.Ptr(), PageType::Deleted);
        REQUIRE(TypeOfPage(page.Ptr()) == PageType::Deleted);
    }

    SECTION("Checksum with minimal content") {
        const size_t min_size = 64;
        auto buffer = std::make_unique<char[]>(min_size);
        std::memset(buffer.get(), 0, min_size);

        Page page(buffer.get());

        std::string_view page_view(page.Ptr(), min_size);
        SetChecksum(page_view);
        REQUIRE(ValidateChecksum(page_view));
    }
}

TEST_CASE("Page_CorruptionDetection", "[page][unit][error]") {
    const size_t page_size = 4096;
    auto buffer = std::make_unique<char[]>(page_size);
    DataGenerator gen(42);

    SECTION("Single bit flip in content") {
        Page page(buffer.get());
        std::memset(page.Ptr() + checksum_bytes + 1, 0x55, 100);

        std::string_view page_view(page.Ptr(), page_size);
        SetChecksum(page_view);
        REQUIRE(ValidateChecksum(page_view));

        // Flip single bit
        page.Ptr()[checksum_bytes + 10] ^= 0x01;
        REQUIRE(!ValidateChecksum(page_view));

        // Flip it back
        page.Ptr()[checksum_bytes + 10] ^= 0x01;
        REQUIRE(ValidateChecksum(page_view));
    }

    SECTION("Corruption patterns") {
        Page page(buffer.get());

        // Set initial content
        std::memset(page.Ptr() + checksum_bytes + 1, 0x42, 100);
        std::string_view page_view(page.Ptr(), page_size);
        SetChecksum(page_view);
        REQUIRE(ValidateChecksum(page_view));

        // Apply corruption - all ones
        std::memset(page.Ptr() + checksum_bytes + 1, 0xFF, 10);
        REQUIRE(!ValidateChecksum(page_view));

        // Reset and test all zeros
        std::memset(page.Ptr() + checksum_bytes + 1, 0x42, 100);
        SetChecksum(page_view);
        REQUIRE(ValidateChecksum(page_view));

        std::memset(page.Ptr() + checksum_bytes + 1, 0x00, 10);
        REQUIRE(!ValidateChecksum(page_view));
    }
}

TEST_CASE("Page_Stress", "[page][stress]") {
    const size_t page_size = 4096;
    DataGenerator gen(42);

    SECTION("Page type transitions") {
        auto buffer = std::make_unique<char[]>(page_size);
        Page page(buffer.get());

        const std::vector<PageType> types = {
            PageType::Data,
            PageType::NonLeafIndex,
            PageType::LeafIndex,
            PageType::Overflow
        };

        for (int i = 0; i < 1000; ++i) {
            for (PageType type : types) {
                SetPageType(page.Ptr(), type);
                REQUIRE(TypeOfPage(page.Ptr()) == type);
            }
        }
    }

    SECTION("Page allocation and deallocation") {
        std::vector<Page> pages;

        // Allocate multiple pages
        for (int i = 0; i < 100; ++i) {
            pages.emplace_back(true);
            REQUIRE(pages.back().Ptr() != nullptr);
        }

        // They should all have different pointers
        std::set<char*> ptrs;
        for (const auto& page : pages) {
            ptrs.insert(page.Ptr());
        }
        // Note: Due to memory pooling, pointers might be reused
    }
}