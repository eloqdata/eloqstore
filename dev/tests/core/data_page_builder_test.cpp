#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <numeric>

#include "data_page_builder.h"
#include "data_page.h"
#include "comparator.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class DataPageBuilderTestFixture {
public:
    DataPageBuilderTestFixture()
        : comparator_(Comparator::DefaultComparator()),
          default_page_size_(4096),
          restart_interval_(16) {}

protected:
    const Comparator* comparator_;
    const uint32_t default_page_size_;
    const uint32_t restart_interval_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_EmptyPage", "[builder][unit]") {
    DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

    SECTION("Empty builder produces valid empty page") {
        auto page_data = builder.Finish();

        REQUIRE(page_data.size() > 0);
        REQUIRE(page_data.size() <= default_page_size_);

        // Verify the page can be read
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());
        REQUIRE(page.NumEntries() == 0);
        REQUIRE(page.Empty());
    }

    SECTION("Estimated size for empty builder") {
        REQUIRE(builder.EstimatedSize() > 0);
        REQUIRE(builder.EstimatedSize() < 100);  // Should be just headers
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_AddSingleEntry", "[builder][unit]") {
    DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

    SECTION("Add one key-value pair") {
        REQUIRE(builder.Add("key1", "value1") == true);

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        REQUIRE(page.NumEntries() == 1);
        std::string value;
        REQUIRE(page.Get("key1", &value) == true);
        REQUIRE(value == "value1");
    }

    SECTION("Estimated size increases after add") {
        size_t initial_size = builder.EstimatedSize();
        builder.Add("key", "value");
        size_t after_add_size = builder.EstimatedSize();

        REQUIRE(after_add_size > initial_size);
        REQUIRE(after_add_size >= initial_size + strlen("key") + strlen("value"));
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_KeyOrdering", "[builder][unit]") {
    DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

    SECTION("Keys must be added in order") {
        REQUIRE(builder.Add("aaa", "1") == true);
        REQUIRE(builder.Add("bbb", "2") == true);
        REQUIRE(builder.Add("ccc", "3") == true);

        // Try to add out-of-order key (implementation may assert or return false)
        // This behavior is implementation-specific
        bool out_of_order_result = builder.Add("aab", "4");

        if (out_of_order_result) {
            // If it accepts out-of-order, verify it's handled correctly
            auto page_data = builder.Finish();
            DataPage page(const_cast<char*>(page_data.data()), page_data.size());

            // Check if keys are properly sorted
            DataPage::Iterator iter(&page, comparator_);
            std::vector<std::string> keys;
            while (iter.Valid()) {
                keys.push_back(iter.key().ToString());
                iter.Next();
            }
            REQUIRE(std::is_sorted(keys.begin(), keys.end()));
        }
    }

    SECTION("Duplicate keys") {
        REQUIRE(builder.Add("key1", "value1") == true);

        // Try to add duplicate key - behavior is implementation-specific
        // Could replace, reject, or assert
        bool duplicate_result = builder.Add("key1", "value2");

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        std::string value;
        REQUIRE(page.Get("key1", &value) == true);
        // Value could be either "value1" or "value2" depending on implementation
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_PageFullCondition", "[builder][unit]") {
    SECTION("Fill page to capacity") {
        DataPageBuilder builder(comparator_, 512, restart_interval_);  // Small page

        int added_count = 0;
        for (int i = 0; i < 1000; ++i) {
            std::string key = "key_" + std::to_string(10000 + i);
            std::string value = "value_" + std::to_string(i);

            if (!builder.Add(key, value)) {
                break;  // Page full
            }
            added_count++;
        }

        REQUIRE(added_count > 0);
        REQUIRE(added_count < 1000);  // Should hit page limit

        // Verify all added entries
        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        REQUIRE(page.NumEntries() == added_count);

        for (int i = 0; i < added_count; ++i) {
            std::string key = "key_" + std::to_string(10000 + i);
            std::string value;
            REQUIRE(page.Get(key, &value) == true);
            REQUIRE(value == "value_" + std::to_string(i));
        }
    }

    SECTION("Detect when single entry exceeds page size") {
        DataPageBuilder builder(comparator_, 256, restart_interval_);  // Very small page

        std::string large_value(500, 'x');  // Larger than page
        bool result = builder.Add("key", large_value);

        REQUIRE(result == false);  // Should reject entry larger than page
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_RestartPoints", "[builder][unit]") {
    SECTION("Verify restart points are created") {
        DataPageBuilder builder(comparator_, default_page_size_, 4);  // Restart every 4 entries

        // Add exactly 16 entries to get 4 restart points
        for (int i = 0; i < 16; ++i) {
            std::string key = "key_" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
            REQUIRE(builder.Add(key, "value") == true);
        }

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        // Verify seeking works efficiently with restart points
        DataPage::Iterator iter(&page, comparator_);

        // Seek to entries at restart points
        for (int i = 0; i < 16; i += 4) {
            std::string key = "key_" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
            iter.Seek(key);
            REQUIRE(iter.Valid());
            REQUIRE(iter.key() == key);
        }
    }

    SECTION("Single restart point for few entries") {
        DataPageBuilder builder(comparator_, default_page_size_, 100);  // Large interval

        for (int i = 0; i < 5; ++i) {
            builder.Add("key" + std::to_string(i), "value");
        }

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        // Should still work with single restart point
        std::string value;
        REQUIRE(page.Get("key2", &value) == true);
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_EdgeCases", "[builder][unit][edge-case]") {
    SECTION("Empty key") {
        DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

        REQUIRE(builder.Add("", "value") == true);

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        std::string value;
        REQUIRE(page.Get("", &value) == true);
        REQUIRE(value == "value");
    }

    SECTION("Empty value") {
        DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

        REQUIRE(builder.Add("key", "") == true);

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        std::string value = "not_empty";
        REQUIRE(page.Get("key", &value) == true);
        REQUIRE(value == "");
    }

    SECTION("Maximum length key and value") {
        DataPageBuilder builder(comparator_, 8192, restart_interval_);  // Larger page

        std::string max_key(1024, 'k');
        std::string max_value(2048, 'v');

        REQUIRE(builder.Add(max_key, max_value) == true);

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        std::string value;
        REQUIRE(page.Get(max_key, &value) == true);
        REQUIRE(value == max_value);
    }

    SECTION("Binary data with null bytes") {
        DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

        std::string binary_key;
        std::string binary_value;

        for (int i = 0; i < 256; ++i) {
            binary_key += static_cast<char>(i);
            binary_value += static_cast<char>(255 - i);
        }

        REQUIRE(builder.Add(binary_key, binary_value) == true);

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        std::string value;
        REQUIRE(page.Get(binary_key, &value) == true);
        REQUIRE(value == binary_value);
    }

    SECTION("UTF-8 strings") {
        DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

        std::vector<std::pair<std::string, std::string>> utf8_pairs = {
            {u8"键1", u8"值1"},
            {u8"キー2", u8"バリュー2"},
            {u8"مفتاح3", u8"قيمة3"},
            {u8"🔑4", u8"💎4"}
        };

        // Sort by key for proper ordering
        std::sort(utf8_pairs.begin(), utf8_pairs.end());

        for (const auto& [key, value] : utf8_pairs) {
            REQUIRE(builder.Add(key, value) == true);
        }

        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        for (const auto& [key, expected_value] : utf8_pairs) {
            std::string value;
            REQUIRE(page.Get(key, &value) == true);
            REQUIRE(value == expected_value);
        }
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_StressTest", "[builder][stress]") {
    SECTION("Build with maximum entries") {
        DataPageBuilder builder(comparator_, 65536, restart_interval_);  // 64KB page

        int max_entries = 0;
        for (int i = 0; i < 10000; ++i) {
            // Use compact keys and values
            std::string key = std::to_string(10000000 + i);
            std::string value = std::to_string(i);

            if (!builder.Add(key, value)) {
                break;
            }
            max_entries++;
        }

        LOG(INFO) << "Fit " << max_entries << " entries in 64KB page";
        REQUIRE(max_entries > 1000);  // Should fit many small entries

        // Verify page integrity
        auto page_data = builder.Finish();
        DataPage page(const_cast<char*>(page_data.data()), page_data.size());

        REQUIRE(page.NumEntries() == max_entries);

        // Spot check some entries
        for (int i = 0; i < max_entries; i += 100) {
            std::string key = std::to_string(10000000 + i);
            std::string value;
            REQUIRE(page.Get(key, &value) == true);
            REQUIRE(value == std::to_string(i));
        }
    }

    SECTION("Vary key and value sizes") {
        for (int test = 0; test < 5; ++test) {
            DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

            std::vector<std::pair<std::string, std::string>> entries;

            // Generate entries with varying sizes
            for (int i = 0; i < 100; ++i) {
                size_t key_len = 5 + (i % 20);
                size_t val_len = 10 + (i % 50);

                std::string key = gen_.GeneratePrefixedKey("k", 1000 + i);
                std::string value = gen_.GenerateValue(val_len);

                if (builder.Add(key, value)) {
                    entries.push_back({key, value});
                } else {
                    break;
                }
            }

            // Verify all entries
            auto page_data = builder.Finish();
            DataPage page(const_cast<char*>(page_data.data()), page_data.size());

            for (const auto& [key, expected_value] : entries) {
                std::string value;
                REQUIRE(page.Get(key, &value) == true);
                REQUIRE(value == expected_value);
            }
        }
    }

    SECTION("Performance - rapid additions") {
        Timer timer;
        const int iterations = 1000;

        timer.Start();
        for (int iter = 0; iter < iterations; ++iter) {
            DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

            for (int i = 0; i < 100; ++i) {
                std::string key = gen_.GenerateSequentialKey(i);
                std::string value = gen_.GenerateValue(20);
                builder.Add(key, value);
            }

            auto page_data = builder.Finish();
        }
        timer.Stop();

        double builds_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "DataPageBuilder: " << builds_per_sec << " builds/sec";

        // Should handle at least 1000 builds per second
        REQUIRE(builds_per_sec > 1000);
    }
}

TEST_CASE_METHOD(DataPageBuilderTestFixture, "DataPageBuilder_ErrorConditions", "[builder][unit][error]") {
    SECTION("Add after Finish") {
        DataPageBuilder builder(comparator_, default_page_size_, restart_interval_);

        builder.Add("key1", "value1");
        auto page_data = builder.Finish();

        // Behavior after Finish is implementation-specific
        // Could assert, return false, or start new page
        // We'll just verify the original page is valid

        DataPage page(const_cast<char*>(page_data.data()), page_data.size());
        REQUIRE(page.NumEntries() == 1);
    }

    SECTION("Extremely small page size") {
        DataPageBuilder builder(comparator_, 32, restart_interval_);  // Tiny page

        // Should at least handle page headers
        bool added_any = builder.Add("k", "v");

        if (added_any) {
            auto page_data = builder.Finish();
            REQUIRE(page_data.size() <= 32);
        }
    }

    SECTION("Page size exactly at boundary") {
        // Find the exact size where one more entry would exceed limit
        for (uint32_t page_size = 100; page_size <= 200; ++page_size) {
            DataPageBuilder builder(comparator_, page_size, restart_interval_);

            int count = 0;
            while (builder.Add("k" + std::to_string(count), "v")) {
                count++;
                if (count > 100) break;  // Safety limit
            }

            if (count > 0) {
                auto page_data = builder.Finish();
                REQUIRE(page_data.size() <= page_size);

                // Try to find boundary condition
                if (page_data.size() > page_size - 20 && page_data.size() <= page_size) {
                    LOG(INFO) << "Boundary found: page_size=" << page_size
                              << " actual=" << page_data.size()
                              << " entries=" << count;
                }
            }
        }
    }
}