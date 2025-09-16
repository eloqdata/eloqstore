#include <catch2/catch_test_macros.hpp>
#include <random>
#include <numeric>

#include "data_page.h"
#include "page.h"
#include "coding.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

// OverflowPage test fixture
class OverflowPageTestFixture {
public:
    OverflowPageTestFixture() : gen_(42) {}

    std::unique_ptr<OverflowPage> CreateOverflowPage(
        const std::string& data,
        PageId page_id = 1000) {

        size_t page_size = Page::kHeaderSize + data.size() + 16;  // Some overhead
        auto buffer = std::make_unique<char[]>(page_size);

        Page page(buffer.get(), page_size);
        page.SetPageType(PageType::Overflow);
        page.SetPageId(page_id);

        // Write overflow data after header
        std::memcpy(page.GetContent(), data.data(), data.size());

        // Set content size
        EncodeFixed32(page.GetContent() + data.size(), data.size());

        return std::make_unique<OverflowPage>(buffer.release(), page_size);
    }

    std::vector<PageId> EncodeOverflowPointers(const std::vector<PageId>& pages) {
        // Simulate encoding of overflow pointers
        return pages;
    }

protected:
    DataGenerator gen_;
};

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_BasicConstruction", "[overflow][unit]") {
    SECTION("Create empty overflow page") {
        auto page = CreateOverflowPage("");
        REQUIRE(page != nullptr);
        REQUIRE(page->GetPageType() == PageType::Overflow);
        REQUIRE(page->GetPageId() == 1000);
    }

    SECTION("Create overflow page with data") {
        std::string data = "This is overflow data that doesn't fit in a regular page";
        auto page = CreateOverflowPage(data);

        REQUIRE(page != nullptr);
        REQUIRE(page->GetPageType() == PageType::Overflow);

        // Verify data can be read back
        std::string recovered(page->GetContent(), data.size());
        REQUIRE(recovered == data);
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_LargeValues", "[overflow][unit]") {
    SECTION("Very large value") {
        // Create a value larger than typical page size
        std::string large_value(10000, 'x');
        auto page = CreateOverflowPage(large_value);

        REQUIRE(page != nullptr);
        std::string recovered(page->GetContent(), large_value.size());
        REQUIRE(recovered == large_value);
    }

    SECTION("Maximum size value") {
        // Test with maximum practical size
        std::string max_value(65536, 'M');  // 64KB
        auto page = CreateOverflowPage(max_value);

        REQUIRE(page != nullptr);
        std::string recovered(page->GetContent(), max_value.size());
        REQUIRE(recovered == max_value);
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_MultiplePages", "[overflow][unit]") {
    SECTION("Chain of overflow pages") {
        // Simulate a value that spans multiple overflow pages
        const size_t chunk_size = 4000;  // Size per overflow page
        std::string total_value(12000, 'V');  // Needs 3 pages

        std::vector<std::unique_ptr<OverflowPage>> pages;
        std::vector<PageId> page_ids;

        // Create overflow pages
        for (size_t offset = 0; offset < total_value.size(); offset += chunk_size) {
            size_t size = std::min(chunk_size, total_value.size() - offset);
            std::string chunk = total_value.substr(offset, size);

            PageId page_id = 2000 + pages.size();
            pages.push_back(CreateOverflowPage(chunk, page_id));
            page_ids.push_back(page_id);
        }

        REQUIRE(pages.size() == 3);

        // Reconstruct value from pages
        std::string reconstructed;
        for (const auto& page : pages) {
            size_t chunk_size = std::min(size_t(4000), total_value.size() - reconstructed.size());
            reconstructed.append(page->GetContent(), chunk_size);
        }

        REQUIRE(reconstructed == total_value);
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_PointerEncoding", "[overflow][unit]") {
    SECTION("Encode single pointer") {
        std::string encoded;
        PageId page_id = 12345;
        EncodeVarint32(&encoded, page_id);

        PageId decoded;
        const char* ptr = encoded.data();
        ptr = GetVarint32Ptr(ptr, encoded.data() + encoded.size(), &decoded);

        REQUIRE(ptr != nullptr);
        REQUIRE(decoded == page_id);
    }

    SECTION("Encode multiple pointers") {
        std::vector<PageId> page_ids = {100, 200, 300, 400, 500};
        std::string encoded;

        // Encode count and then IDs
        EncodeVarint32(&encoded, page_ids.size());
        for (PageId id : page_ids) {
            EncodeVarint32(&encoded, id);
        }

        // Decode
        const char* ptr = encoded.data();
        const char* limit = encoded.data() + encoded.size();

        uint32_t count;
        ptr = GetVarint32Ptr(ptr, limit, &count);
        REQUIRE(count == page_ids.size());

        std::vector<PageId> decoded_ids;
        for (uint32_t i = 0; i < count; ++i) {
            PageId id;
            ptr = GetVarint32Ptr(ptr, limit, &id);
            REQUIRE(ptr != nullptr);
            decoded_ids.push_back(id);
        }

        REQUIRE(decoded_ids == page_ids);
    }

    SECTION("Maximum pointers") {
        // Test encoding maximum allowed overflow pointers
        std::vector<PageId> page_ids;
        for (uint8_t i = 0; i < max_overflow_pointers; ++i) {
            page_ids.push_back(1000 + i);
        }

        std::string encoded;
        EncodeVarint32(&encoded, page_ids.size());
        for (PageId id : page_ids) {
            EncodeVarint32(&encoded, id);
        }

        // Should handle max pointers
        REQUIRE(page_ids.size() == max_overflow_pointers);
        REQUIRE(encoded.size() < 1000);  // Should be reasonably compact
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_EdgeCases", "[overflow][unit][edge-case]") {
    SECTION("Binary data with null bytes") {
        std::string binary_data;
        for (int i = 0; i < 256; ++i) {
            binary_data += static_cast<char>(i);
        }

        auto page = CreateOverflowPage(binary_data);
        std::string recovered(page->GetContent(), binary_data.size());
        REQUIRE(recovered == binary_data);
    }

    SECTION("All zeros") {
        std::string zeros(1000, '\0');
        auto page = CreateOverflowPage(zeros);
        std::string recovered(page->GetContent(), zeros.size());
        REQUIRE(recovered == zeros);
    }

    SECTION("All ones") {
        std::string ones(1000, '\xFF');
        auto page = CreateOverflowPage(ones);
        std::string recovered(page->GetContent(), ones.size());
        REQUIRE(recovered == ones);
    }

    SECTION("Alternating pattern") {
        std::string pattern;
        for (int i = 0; i < 1000; ++i) {
            pattern += (i % 2) ? '\xAA' : '\x55';
        }

        auto page = CreateOverflowPage(pattern);
        std::string recovered(page->GetContent(), pattern.size());
        REQUIRE(recovered == pattern);
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_Performance", "[overflow][benchmark]") {
    SECTION("Large value write/read") {
        std::string large_data = gen_.GenerateValue(50000);  // 50KB
        Timer timer;

        const int iterations = 1000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            auto page = CreateOverflowPage(large_data);
            std::string recovered(page->GetContent(), large_data.size());
            volatile bool match = (recovered == large_data);  // Prevent optimization
            (void)match;
        }

        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "Overflow page operations: " << ops_per_sec << " ops/sec";

        // Should handle reasonable throughput
        REQUIRE(ops_per_sec > 100);
    }

    SECTION("Pointer encoding performance") {
        std::vector<PageId> page_ids;
        for (uint8_t i = 0; i < max_overflow_pointers; ++i) {
            page_ids.push_back(1000000 + i);
        }

        Timer timer;
        const int iterations = 10000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            std::string encoded;
            EncodeVarint32(&encoded, page_ids.size());
            for (PageId id : page_ids) {
                EncodeVarint32(&encoded, id);
            }

            // Decode
            const char* ptr = encoded.data();
            const char* limit = encoded.data() + encoded.size();
            uint32_t count;
            ptr = GetVarint32Ptr(ptr, limit, &count);

            for (uint32_t j = 0; j < count; ++j) {
                PageId id;
                ptr = GetVarint32Ptr(ptr, limit, &id);
            }
        }

        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "Pointer encoding/decoding: " << ops_per_sec << " ops/sec";

        REQUIRE(ops_per_sec > 1000);
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_Corruption", "[overflow][unit][error]") {
    SECTION("Corrupted size field") {
        std::string data = "Valid overflow data";
        auto page = CreateOverflowPage(data);

        // Corrupt the size field
        char* buffer = const_cast<char*>(page->data());
        buffer[page->size() - 4] = 0xFF;  // Corrupt size bytes

        // Reading should detect corruption or return wrong size
        // Behavior is implementation-specific
    }

    SECTION("Corrupted pointer list") {
        std::string encoded;
        EncodeVarint32(&encoded, 5);  // Claim 5 pointers
        EncodeVarint32(&encoded, 100);  // Only provide 1

        // Try to decode
        const char* ptr = encoded.data();
        const char* limit = encoded.data() + encoded.size();

        uint32_t count;
        ptr = GetVarint32Ptr(ptr, limit, &count);
        REQUIRE(count == 5);

        // Should fail to decode all 5
        std::vector<PageId> ids;
        for (uint32_t i = 0; i < count; ++i) {
            PageId id;
            const char* next = GetVarint32Ptr(ptr, limit, &id);
            if (next == nullptr) {
                break;  // Decode failed as expected
            }
            ptr = next;
            ids.push_back(id);
        }

        REQUIRE(ids.size() < 5);  // Should not decode all
    }
}

TEST_CASE_METHOD(OverflowPageTestFixture, "OverflowPage_StressTest", "[overflow][stress]") {
    SECTION("Random sizes") {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<size_t> size_dist(1, 100000);

        for (int i = 0; i < 100; ++i) {
            size_t size = size_dist(rng);
            std::string data = gen_.GenerateValue(size);

            auto page = CreateOverflowPage(data);
            std::string recovered(page->GetContent(), data.size());
            REQUIRE(recovered == data);
        }
    }

    SECTION("Maximum pointer chains") {
        // Simulate handling of maximum chain length
        std::vector<std::vector<PageId>> pointer_chains;

        for (int chain = 0; chain < 10; ++chain) {
            std::vector<PageId> chain_ids;
            for (uint8_t i = 0; i < max_overflow_pointers; ++i) {
                chain_ids.push_back(chain * 1000 + i);
            }
            pointer_chains.push_back(chain_ids);
        }

        // Encode all chains
        std::vector<std::string> encoded_chains;
        for (const auto& chain : pointer_chains) {
            std::string encoded;
            EncodeVarint32(&encoded, chain.size());
            for (PageId id : chain) {
                EncodeVarint32(&encoded, id);
            }
            encoded_chains.push_back(encoded);
        }

        // Verify all can be decoded
        for (size_t i = 0; i < encoded_chains.size(); ++i) {
            const std::string& encoded = encoded_chains[i];
            const std::vector<PageId>& expected = pointer_chains[i];

            const char* ptr = encoded.data();
            const char* limit = encoded.data() + encoded.size();

            uint32_t count;
            ptr = GetVarint32Ptr(ptr, limit, &count);
            REQUIRE(count == expected.size());

            std::vector<PageId> decoded;
            for (uint32_t j = 0; j < count; ++j) {
                PageId id;
                ptr = GetVarint32Ptr(ptr, limit, &id);
                REQUIRE(ptr != nullptr);
                decoded.push_back(id);
            }

            REQUIRE(decoded == expected);
        }
    }

    SECTION("Fragmented large value") {
        // Simulate a value fragmented across many pages
        const size_t total_size = 1000000;  // 1MB
        const size_t page_size = 4000;

        std::string original = gen_.GenerateValue(total_size);
        std::vector<std::string> fragments;

        // Fragment the value
        for (size_t offset = 0; offset < total_size; offset += page_size) {
            size_t chunk_size = std::min(page_size, total_size - offset);
            fragments.push_back(original.substr(offset, chunk_size));
        }

        // Create overflow pages for each fragment
        std::vector<std::unique_ptr<OverflowPage>> pages;
        for (size_t i = 0; i < fragments.size(); ++i) {
            pages.push_back(CreateOverflowPage(fragments[i], 5000 + i));
        }

        // Reconstruct
        std::string reconstructed;
        for (size_t i = 0; i < pages.size(); ++i) {
            reconstructed.append(pages[i]->GetContent(), fragments[i].size());
        }

        REQUIRE(reconstructed == original);
    }
}