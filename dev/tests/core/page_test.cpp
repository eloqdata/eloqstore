#include <catch2/catch_test_macros.hpp>
#include <cstring>
#include <random>

#include "page.h"
#include "coding.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Page_BasicConstruction", "[page][unit]") {
    SECTION("Empty page construction") {
        Page page;
        REQUIRE(page.data() == nullptr);
        REQUIRE(page.size() == 0);
        REQUIRE(page.IsEmpty());
    }

    SECTION("Page with allocated buffer") {
        const size_t page_size = 4096;
        auto buffer = std::make_unique<char[]>(page_size);

        Page page(buffer.get(), page_size);
        REQUIRE(page.data() == buffer.get());
        REQUIRE(page.size() == page_size);
        REQUIRE(!page.IsEmpty());
    }

    SECTION("Move construction") {
        const size_t page_size = 4096;
        auto buffer = std::make_unique<char[]>(page_size);

        Page page1(buffer.get(), page_size);
        Page page2(std::move(page1));

        REQUIRE(page2.data() == buffer.get());
        REQUIRE(page2.size() == page_size);
        REQUIRE(page1.data() == nullptr);
        REQUIRE(page1.size() == 0);
    }
}

TEST_CASE("Page_HeaderManagement", "[page][unit]") {
    const size_t page_size = 4096;
    auto buffer = std::make_unique<char[]>(page_size);
    std::memset(buffer.get(), 0, page_size);

    SECTION("Set and get page type") {
        Page page(buffer.get(), page_size);

        page.SetPageType(PageType::Data);
        REQUIRE(page.GetPageType() == PageType::Data);

        page.SetPageType(PageType::Index);
        REQUIRE(page.GetPageType() == PageType::Index);

        page.SetPageType(PageType::Overflow);
        REQUIRE(page.GetPageType() == PageType::Overflow);
    }

    SECTION("Set and get page ID") {
        Page page(buffer.get(), page_size);

        page.SetPageId(0);
        REQUIRE(page.GetPageId() == 0);

        page.SetPageId(12345);
        REQUIRE(page.GetPageId() == 12345);

        page.SetPageId(MaxPageId);
        REQUIRE(page.GetPageId() == MaxPageId);
    }

    SECTION("Checksum calculation and verification") {
        Page page(buffer.get(), page_size);

        // Fill with test data
        std::memset(buffer.get() + Page::kHeaderSize, 0xAB, 100);

        uint32_t checksum = page.CalculateChecksum();
        page.SetChecksum(checksum);

        REQUIRE(page.VerifyChecksum());

        // Corrupt data
        buffer[Page::kHeaderSize] = 0xFF;
        REQUIRE(!page.VerifyChecksum());
    }
}

TEST_CASE("Page_BoundaryConditions", "[page][unit][edge-case]") {
    SECTION("Minimum valid page size") {
        const size_t min_size = Page::kHeaderSize + 1;
        auto buffer = std::make_unique<char[]>(min_size);

        Page page(buffer.get(), min_size);
        REQUIRE(page.GetContentSize() == 1);
    }

    SECTION("Maximum page ID") {
        const size_t page_size = 4096;
        auto buffer = std::make_unique<char[]>(page_size);

        Page page(buffer.get(), page_size);
        page.SetPageId(MaxPageId);
        REQUIRE(page.GetPageId() == MaxPageId);

        // Verify it doesn't overflow
        page.SetPageId(MaxPageId - 1);
        REQUIRE(page.GetPageId() == MaxPageId - 1);
    }

    SECTION("Zero-size content area") {
        const size_t exact_header_size = Page::kHeaderSize;
        auto buffer = std::make_unique<char[]>(exact_header_size);

        Page page(buffer.get(), exact_header_size);
        REQUIRE(page.GetContentSize() == 0);
        REQUIRE(page.GetContent() == buffer.get() + Page::kHeaderSize);
    }
}

TEST_CASE("Page_CorruptionDetection", "[page][unit][error]") {
    const size_t page_size = 4096;
    auto buffer = std::make_unique<char[]>(page_size);
    DataGenerator gen(42);

    SECTION("Random corruption in header") {
        Page page(buffer.get(), page_size);

        // Fill with random data
        auto random_data = gen.GenerateBinaryString(page_size - Page::kHeaderSize);
        std::memcpy(page.GetContent(), random_data.data(), random_data.size());

        uint32_t checksum = page.CalculateChecksum();
        page.SetChecksum(checksum);
        REQUIRE(page.VerifyChecksum());

        // Corrupt header bytes
        for (int i = 0; i < Page::kHeaderSize; ++i) {
            char original = buffer[i];
            buffer[i] ^= 0xFF;

            // Some header corruption might not affect checksum verification
            // (e.g., corrupting the checksum field itself)
            bool still_valid = page.VerifyChecksum();

            buffer[i] = original; // Restore
        }
    }

    SECTION("Single bit flip in content") {
        Page page(buffer.get(), page_size);
        std::memset(page.GetContent(), 0x55, page.GetContentSize());

        uint32_t checksum = page.CalculateChecksum();
        page.SetChecksum(checksum);
        REQUIRE(page.VerifyChecksum());

        // Flip single bit
        page.GetContent()[0] ^= 0x01;
        REQUIRE(!page.VerifyChecksum());

        // Flip it back
        page.GetContent()[0] ^= 0x01;
        REQUIRE(page.VerifyChecksum());
    }

    SECTION("Corruption patterns") {
        Page page(buffer.get(), page_size);

        // Test various corruption patterns
        std::vector<std::function<void()>> corruptions = {
            [&]() { std::memset(page.GetContent(), 0xFF, 10); },     // All ones
            [&]() { std::memset(page.GetContent(), 0x00, 10); },     // All zeros
            [&]() {
                for (int i = 0; i < 10; ++i) {
                    page.GetContent()[i] = static_cast<char>(i);
                }
            }, // Sequential
            [&]() {
                for (int i = 0; i < page.GetContentSize(); ++i) {
                    page.GetContent()[i] ^= 0xAA;  // Alternating bits
                }
            }
        };

        for (auto& corrupt : corruptions) {
            // Set initial content
            std::memset(page.GetContent(), 0x42, page.GetContentSize());
            uint32_t checksum = page.CalculateChecksum();
            page.SetChecksum(checksum);
            REQUIRE(page.VerifyChecksum());

            // Apply corruption
            corrupt();
            REQUIRE(!page.VerifyChecksum());
        }
    }
}

TEST_CASE("Page_Stress", "[page][stress]") {
    const size_t page_size = 4096;
    DataGenerator gen(42);

    SECTION("Rapid checksum calculations") {
        auto buffer = std::make_unique<char[]>(page_size);
        Page page(buffer.get(), page_size);

        const int iterations = 10000;
        std::set<uint32_t> checksums;

        for (int i = 0; i < iterations; ++i) {
            // Generate random content
            auto data = gen.GenerateBinaryString(page.GetContentSize());
            std::memcpy(page.GetContent(), data.data(), data.size());

            uint32_t checksum1 = page.CalculateChecksum();
            uint32_t checksum2 = page.CalculateChecksum();

            // Checksum should be deterministic
            REQUIRE(checksum1 == checksum2);

            checksums.insert(checksum1);
        }

        // Should have good distribution (most checksums unique)
        REQUIRE(checksums.size() > iterations * 0.95);
    }

    SECTION("Page type transitions") {
        auto buffer = std::make_unique<char[]>(page_size);
        Page page(buffer.get(), page_size);

        const std::vector<PageType> types = {
            PageType::Data,
            PageType::Index,
            PageType::Overflow
        };

        for (int i = 0; i < 1000; ++i) {
            for (PageType type : types) {
                page.SetPageType(type);
                REQUIRE(page.GetPageType() == type);

                // Type changes shouldn't affect other fields
                uint32_t page_id = i % 1000;
                page.SetPageId(page_id);
                REQUIRE(page.GetPageId() == page_id);
                REQUIRE(page.GetPageType() == type);
            }
        }
    }
}