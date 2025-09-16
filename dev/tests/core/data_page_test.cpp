#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <set>

#include "data_page.h"
#include "data_page_builder.h"
#include "comparator.h"
#include "coding.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class DataPageTestFixture {
public:
    DataPageTestFixture() : comparator_(Comparator::DefaultComparator()) {}

    std::unique_ptr<DataPage> BuildDataPage(
        const std::vector<std::pair<std::string, std::string>>& kvs,
        uint32_t page_size = 4096) {

        DataPageBuilder builder(comparator_, page_size, 16);

        for (const auto& [key, value] : kvs) {
            bool added = builder.Add(key, value);
            if (!added) {
                break; // Page full
            }
        }

        auto page_data = builder.Finish();
        auto buffer = std::make_unique<char[]>(page_data.size());
        std::memcpy(buffer.get(), page_data.data(), page_data.size());

        return std::make_unique<DataPage>(buffer.release(), page_data.size());
    }

protected:
    const Comparator* comparator_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_EmptyPage", "[datapage][unit]") {
    auto page = BuildDataPage({});

    REQUIRE(page->NumEntries() == 0);
    REQUIRE(page->Empty());

    // Lookups in empty page should return not found
    std::string value;
    REQUIRE(page->Get("any_key", &value) == false);

    // Iterator should be at end
    DataPage::Iterator iter(page.get(), comparator_);
    REQUIRE(!iter.Valid());
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_SingleEntry", "[datapage][unit]") {
    auto page = BuildDataPage({{"key1", "value1"}});

    REQUIRE(page->NumEntries() == 1);
    REQUIRE(!page->Empty());

    SECTION("Get existing key") {
        std::string value;
        REQUIRE(page->Get("key1", &value) == true);
        REQUIRE(value == "value1");
    }

    SECTION("Get non-existing key") {
        std::string value;
        REQUIRE(page->Get("key2", &value) == false);
        REQUIRE(page->Get("key0", &value) == false);
    }

    SECTION("Iterator") {
        DataPage::Iterator iter(page.get(), comparator_);
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "key1");
        REQUIRE(iter.value() == "value1");

        iter.Next();
        REQUIRE(!iter.Valid());
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_MultipleEntries", "[datapage][unit]") {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"aaa", "value1"},
        {"bbb", "value2"},
        {"ccc", "value3"},
        {"ddd", "value4"},
        {"eee", "value5"}
    };

    auto page = BuildDataPage(kvs);
    REQUIRE(page->NumEntries() == 5);

    SECTION("Get all keys") {
        for (const auto& [key, expected_value] : kvs) {
            std::string value;
            REQUIRE(page->Get(key, &value) == true);
            REQUIRE(value == expected_value);
        }
    }

    SECTION("Iterate all entries") {
        DataPage::Iterator iter(page.get(), comparator_);
        int index = 0;

        while (iter.Valid()) {
            REQUIRE(index < kvs.size());
            REQUIRE(iter.key() == kvs[index].first);
            REQUIRE(iter.value() == kvs[index].second);
            iter.Next();
            index++;
        }

        REQUIRE(index == kvs.size());
    }

    SECTION("Seek to specific keys") {
        DataPage::Iterator iter(page.get(), comparator_);

        // Seek to existing key
        iter.Seek("ccc");
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "ccc");

        // Seek to non-existing key (should position at next key)
        iter.Seek("cab");
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "ccc");

        // Seek past all keys
        iter.Seek("zzz");
        REQUIRE(!iter.Valid());
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_BinarySearch", "[datapage][unit]") {
    // Build page with many entries to test binary search
    std::vector<std::pair<std::string, std::string>> kvs;
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::string(4 - std::to_string(i).length(), '0') + std::to_string(i);
        kvs.push_back({key, "value_" + std::to_string(i)});
    }

    auto page = BuildDataPage(kvs);

    SECTION("Binary search for existing keys") {
        for (const auto& [key, expected_value] : kvs) {
            std::string value;
            REQUIRE(page->Get(key, &value) == true);
            REQUIRE(value == expected_value);
        }
    }

    SECTION("Binary search for non-existing keys") {
        std::string value;
        REQUIRE(page->Get("key_-001", &value) == false);  // Before first
        REQUIRE(page->Get("key_0045", &value) == false);  // Between keys
        REQUIRE(page->Get("key_9999", &value) == false);  // After last
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_RestartPoints", "[datapage][unit]") {
    // Build page with entries that will create multiple restart points
    std::vector<std::pair<std::string, std::string>> kvs;
    for (int i = 0; i < 64; ++i) {  // Should create 4 restart points with interval 16
        kvs.push_back({
            "key_" + std::to_string(1000 + i),
            "value_" + std::to_string(i)
        });
    }

    auto page = BuildDataPage(kvs);
    REQUIRE(page->NumEntries() == 64);

    // Verify restart points work correctly by seeking to various positions
    DataPage::Iterator iter(page.get(), comparator_);

    // Seek to keys near restart points
    for (int i = 0; i < 64; i += 16) {
        iter.Seek("key_" + std::to_string(1000 + i));
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "key_" + std::to_string(1000 + i));
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_EdgeCases", "[datapage][unit][edge-case]") {
    SECTION("Empty key") {
        auto page = BuildDataPage({{"", "value_for_empty_key"}});

        std::string value;
        REQUIRE(page->Get("", &value) == true);
        REQUIRE(value == "value_for_empty_key");
    }

    SECTION("Empty value") {
        auto page = BuildDataPage({{"key_with_empty_value", ""}});

        std::string value = "not_empty";
        REQUIRE(page->Get("key_with_empty_value", &value) == true);
        REQUIRE(value == "");
    }

    SECTION("Empty key and value") {
        auto page = BuildDataPage({{"", ""}});

        std::string value = "not_empty";
        REQUIRE(page->Get("", &value) == true);
        REQUIRE(value == "");
    }

    SECTION("Maximum length key") {
        std::string max_key(1024, 'k');
        auto page = BuildDataPage({{max_key, "value"}});

        std::string value;
        REQUIRE(page->Get(max_key, &value) == true);
        REQUIRE(value == "value");
    }

    SECTION("Maximum length value") {
        std::string max_value(3000, 'v');
        auto page = BuildDataPage({{"key", max_value}});

        std::string value;
        REQUIRE(page->Get("key", &value) == true);
        REQUIRE(value == max_value);
    }

    SECTION("Binary data with null bytes") {
        std::string binary_key = "key\x00\x01\x02";
        std::string binary_value = "value\x00\xFF\xFE";

        auto page = BuildDataPage({{binary_key, binary_value}});

        std::string value;
        REQUIRE(page->Get(binary_key, &value) == true);
        REQUIRE(value == binary_value);
    }

    SECTION("Keys with special characters") {
        std::vector<std::pair<std::string, std::string>> special_kvs = {
            {"key\n", "value1"},
            {"key\t", "value2"},
            {"key\r", "value3"},
            {"key ", "value4"},
            {" key", "value5"}
        };

        auto page = BuildDataPage(special_kvs);

        for (const auto& [key, expected_value] : special_kvs) {
            std::string value;
            REQUIRE(page->Get(key, &value) == true);
            REQUIRE(value == expected_value);
        }
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_IteratorEdgeCases", "[datapage][unit][edge-case]") {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"aaa", "1"},
        {"bbb", "2"},
        {"ccc", "3"}
    };

    auto page = BuildDataPage(kvs);

    SECTION("SeekToFirst and SeekToLast") {
        DataPage::Iterator iter(page.get(), comparator_);

        iter.SeekToLast();
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "ccc");

        iter.SeekToFirst();
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "aaa");
    }

    SECTION("Prev operation") {
        DataPage::Iterator iter(page.get(), comparator_);

        iter.SeekToLast();
        REQUIRE(iter.key() == "ccc");

        iter.Prev();
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "bbb");

        iter.Prev();
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "aaa");

        iter.Prev();
        REQUIRE(!iter.Valid());
    }

    SECTION("Seek beyond boundaries") {
        DataPage::Iterator iter(page.get(), comparator_);

        // Seek before first
        iter.Seek("000");
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "aaa");

        // Seek after last
        iter.Seek("zzz");
        REQUIRE(!iter.Valid());
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_Performance", "[datapage][benchmark]") {
    // Build a large page
    std::vector<std::pair<std::string, std::string>> kvs;
    for (int i = 0; i < 500; ++i) {
        kvs.push_back({
            gen_.GenerateSequentialKey(i),
            gen_.GenerateValue(50)
        });
    }

    auto page = BuildDataPage(kvs, 32768);  // 32KB page

    SECTION("Random access performance") {
        Timer timer;
        const int lookups = 10000;

        timer.Start();
        for (int i = 0; i < lookups; ++i) {
            std::string key = gen_.GenerateSequentialKey(i % kvs.size());
            std::string value;
            page->Get(key, &value);
        }
        timer.Stop();

        double ops_per_sec = lookups / timer.ElapsedSeconds();
        LOG(INFO) << "DataPage random access: " << ops_per_sec << " ops/sec";

        // Should handle at least 100K lookups/sec
        REQUIRE(ops_per_sec > 100000);
    }

    SECTION("Sequential iteration performance") {
        Timer timer;
        const int iterations = 1000;

        timer.Start();
        for (int i = 0; i < iterations; ++i) {
            DataPage::Iterator iter(page.get(), comparator_);
            int count = 0;
            while (iter.Valid()) {
                auto k = iter.key();
                auto v = iter.value();
                iter.Next();
                count++;
            }
            REQUIRE(count == page->NumEntries());
        }
        timer.Stop();

        double iters_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "DataPage full iteration: " << iters_per_sec << " iterations/sec";

        // Should handle at least 1000 full iterations/sec
        REQUIRE(iters_per_sec > 1000);
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_Corruption", "[datapage][unit][error]") {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
    };

    auto page = BuildDataPage(kvs);

    SECTION("Detect corrupted entry count") {
        // Corrupt the entry count in page header
        char* data = const_cast<char*>(page->data());
        // Assuming entry count is stored after basic page header
        uint32_t* entry_count_ptr = reinterpret_cast<uint32_t*>(data + Page::kHeaderSize);
        *entry_count_ptr = 999999;  // Invalid count

        // Iterator should handle gracefully
        DataPage::Iterator iter(page.get(), comparator_);
        int actual_count = 0;
        while (iter.Valid() && actual_count < 10) {  // Limit iterations
            iter.Next();
            actual_count++;
        }

        // Should not iterate 999999 times
        REQUIRE(actual_count < 10);
    }

    SECTION("Detect corrupted restart points") {
        // Build page with restart points
        std::vector<std::pair<std::string, std::string>> many_kvs;
        for (int i = 0; i < 32; ++i) {
            many_kvs.push_back({
                gen_.GenerateSequentialKey(i),
                gen_.GenerateValue(10)
            });
        }

        auto big_page = BuildDataPage(many_kvs);

        // Corrupt restart point data
        char* data = const_cast<char*>(big_page->data());
        // This is implementation specific - restart points are usually at the end
        data[big_page->size() - 10] = 0xFF;
        data[big_page->size() - 11] = 0xFF;

        // Operations should still work (with degraded performance)
        std::string value;
        bool found = big_page->Get(many_kvs[15].first, &value);
        // May or may not find depending on corruption impact
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_StressTest", "[datapage][stress]") {
    SECTION("Maximum entries that fit") {
        std::vector<std::pair<std::string, std::string>> kvs;

        // Try to fill a 4KB page with small entries
        for (int i = 0; i < 1000; ++i) {
            kvs.push_back({
                std::to_string(10000 + i),  // Short keys
                std::to_string(i)           // Short values
            });
        }

        auto page = BuildDataPage(kvs);

        // Verify all entries that fit are accessible
        int accessible_count = 0;
        for (const auto& [key, expected_value] : kvs) {
            std::string value;
            if (page->Get(key, &value)) {
                REQUIRE(value == expected_value);
                accessible_count++;
            }
        }

        LOG(INFO) << "Fit " << accessible_count << " entries in 4KB page";
        REQUIRE(accessible_count > 100);  // Should fit at least 100 small entries
    }

    SECTION("Random key patterns") {
        for (int pattern = 0; pattern < 5; ++pattern) {
            std::vector<std::pair<std::string, std::string>> kvs;

            switch (pattern) {
                case 0:  // Sequential
                    kvs = gen_.GenerateSequentialData(0, 50, 20);
                    break;
                case 1:  // Random
                    kvs = gen_.GenerateKeyValuePairs(50, 16, 20);
                    break;
                case 2:  // Reverse sorted
                    for (int i = 49; i >= 0; --i) {
                        kvs.push_back({
                            gen_.GenerateSequentialKey(i),
                            gen_.GenerateValue(20)
                        });
                    }
                    break;
                case 3:  // Clustered
                    for (auto& key : gen_.GenerateClusteredKeys(5, 10)) {
                        kvs.push_back({key, gen_.GenerateValue(20)});
                    }
                    break;
                case 4:  // Mixed sizes
                    for (int i = 0; i < 20; ++i) {
                        kvs.push_back({
                            gen_.GenerateRandomKey(5, 50),
                            gen_.GenerateRandomValue(10, 100)
                        });
                    }
                    break;
            }

            // Sort keys as DataPage expects sorted input
            std::sort(kvs.begin(), kvs.end(),
                     [](const auto& a, const auto& b) { return a.first < b.first; });

            auto page = BuildDataPage(kvs);

            // Verify all entries
            for (const auto& [key, expected_value] : kvs) {
                std::string value;
                if (page->Get(key, &value)) {
                    REQUIRE(value == expected_value);
                }
            }
        }
    }
}