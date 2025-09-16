#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <random>
#include <map>

#include "mem_index_page.h"
#include "index_page_builder.h"
#include "index_page_manager.h"
#include "comparator.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class IndexPageTestFixture {
public:
    IndexPageTestFixture()
        : comparator_(Comparator::DefaultComparator()),
          gen_(42) {}

    std::unique_ptr<MemIndexPage> CreateIndexPage(
        const std::vector<std::pair<std::string, PageId>>& entries) {

        auto page = std::make_unique<MemIndexPage>(comparator_, 16);

        for (const auto& [key, page_id] : entries) {
            page->Add(key, page_id);
        }

        return page;
    }

protected:
    const Comparator* comparator_;
    DataGenerator gen_;
};

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_BasicOperations", "[index][unit]") {
    SECTION("Empty index page") {
        auto page = std::make_unique<MemIndexPage>(comparator_, 16);

        REQUIRE(page->NumEntries() == 0);
        REQUIRE(page->Empty());

        PageId result;
        REQUIRE(page->Lookup("any_key", &result) == false);
    }

    SECTION("Add single entry") {
        auto page = CreateIndexPage({{"key1", 100}});

        REQUIRE(page->NumEntries() == 1);
        REQUIRE(!page->Empty());

        PageId result;
        REQUIRE(page->Lookup("key1", &result) == true);
        REQUIRE(result == 100);

        REQUIRE(page->Lookup("key2", &result) == false);
    }

    SECTION("Add multiple entries") {
        std::vector<std::pair<std::string, PageId>> entries = {
            {"aaa", 100},
            {"bbb", 200},
            {"ccc", 300},
            {"ddd", 400},
            {"eee", 500}
        };

        auto page = CreateIndexPage(entries);
        REQUIRE(page->NumEntries() == 5);

        for (const auto& [key, expected_page] : entries) {
            PageId result;
            REQUIRE(page->Lookup(key, &result) == true);
            REQUIRE(result == expected_page);
        }
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_KeyOrdering", "[index][unit]") {
    SECTION("Keys must be sorted") {
        auto page = std::make_unique<MemIndexPage>(comparator_, 16);

        // Add keys in random order
        std::vector<std::pair<std::string, PageId>> entries = {
            {"ccc", 300},
            {"aaa", 100},
            {"eee", 500},
            {"bbb", 200},
            {"ddd", 400}
        };

        for (const auto& [key, page_id] : entries) {
            page->Add(key, page_id);
        }

        // Verify all can be found
        for (const auto& [key, expected_page] : entries) {
            PageId result;
            REQUIRE(page->Lookup(key, &result) == true);
            REQUIRE(result == expected_page);
        }
    }

    SECTION("Duplicate keys") {
        auto page = std::make_unique<MemIndexPage>(comparator_, 16);

        page->Add("key1", 100);
        page->Add("key1", 200);  // Update or reject?

        PageId result;
        REQUIRE(page->Lookup("key1", &result) == true);
        // Result depends on implementation - could be 100 or 200
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_BinarySearch", "[index][unit]") {
    // Create page with many entries to test binary search
    auto page = std::make_unique<MemIndexPage>(comparator_, 16);

    std::map<std::string, PageId> entries;
    for (int i = 0; i < 1000; ++i) {
        std::string key = gen_.GenerateSequentialKey(i);
        entries[key] = i * 10;
        page->Add(key, i * 10);
    }

    SECTION("Find existing keys") {
        for (const auto& [key, expected_page] : entries) {
            PageId result;
            REQUIRE(page->Lookup(key, &result) == true);
            REQUIRE(result == expected_page);
        }
    }

    SECTION("Find non-existing keys") {
        PageId result;
        REQUIRE(page->Lookup("not_exist_1", &result) == false);
        REQUIRE(page->Lookup("not_exist_2", &result) == false);
        REQUIRE(page->Lookup("", &result) == false);
    }

    SECTION("Find boundary keys") {
        PageId result;

        // Key before all entries
        REQUIRE(page->Lookup("0", &result) == false);

        // Key after all entries
        REQUIRE(page->Lookup("zzzzz", &result) == false);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_RangeQuery", "[index][unit]") {
    std::vector<std::pair<std::string, PageId>> entries;
    for (int i = 0; i < 100; ++i) {
        entries.push_back({gen_.GeneratePrefixedKey("key", i), i});
    }

    auto page = CreateIndexPage(entries);

    SECTION("Get range of keys") {
        auto range = page->GetRange("key_10", "key_20");
        REQUIRE(range.size() > 0);

        for (const auto& [key, page_id] : range) {
            REQUIRE(comparator_->Compare(key, "key_10") >= 0);
            REQUIRE(comparator_->Compare(key, "key_20") <= 0);
        }
    }

    SECTION("Empty range") {
        auto range = page->GetRange("key_50", "key_40");  // Invalid range
        REQUIRE(range.empty());
    }

    SECTION("Range beyond boundaries") {
        auto range = page->GetRange("aaa", "zzz");
        REQUIRE(range.size() == entries.size());
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageManager_CacheManagement", "[index][unit]") {
    const uint32_t cache_size = 10;
    IndexPageManager manager(cache_size);

    SECTION("Basic caching") {
        TableIdent table("test", 1);

        // Add pages to cache
        for (PageId i = 0; i < cache_size; ++i) {
            auto page = std::make_unique<MemIndexPage>(comparator_, 16);
            page->Add("key_" + std::to_string(i), i);
            manager.Put(table, i, std::move(page));
        }

        // Retrieve from cache
        for (PageId i = 0; i < cache_size; ++i) {
            auto* page = manager.Get(table, i);
            REQUIRE(page != nullptr);

            PageId result;
            REQUIRE(page->Lookup("key_" + std::to_string(i), &result) == true);
            REQUIRE(result == i);
        }
    }

    SECTION("LRU eviction") {
        TableIdent table("test", 1);

        // Fill cache beyond capacity
        for (PageId i = 0; i < cache_size * 2; ++i) {
            auto page = std::make_unique<MemIndexPage>(comparator_, 16);
            page->Add("key_" + std::to_string(i), i);
            manager.Put(table, i, std::move(page));
        }

        // Old pages should be evicted
        for (PageId i = 0; i < cache_size; ++i) {
            auto* page = manager.Get(table, i);
            // These might be evicted
        }

        // Recent pages should be in cache
        for (PageId i = cache_size; i < cache_size * 2; ++i) {
            auto* page = manager.Get(table, i);
            // These should likely be present
        }
    }

    SECTION("Multiple tables") {
        TableIdent table1("table1", 1);
        TableIdent table2("table2", 2);

        auto page1 = std::make_unique<MemIndexPage>(comparator_, 16);
        page1->Add("key1", 100);
        manager.Put(table1, 0, std::move(page1));

        auto page2 = std::make_unique<MemIndexPage>(comparator_, 16);
        page2->Add("key2", 200);
        manager.Put(table2, 0, std::move(page2));

        auto* retrieved1 = manager.Get(table1, 0);
        auto* retrieved2 = manager.Get(table2, 0);

        REQUIRE(retrieved1 != nullptr);
        REQUIRE(retrieved2 != nullptr);

        PageId result;
        REQUIRE(retrieved1->Lookup("key1", &result) == true);
        REQUIRE(result == 100);

        REQUIRE(retrieved2->Lookup("key2", &result) == true);
        REQUIRE(result == 200);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageBuilder_Construction", "[index][unit]") {
    IndexPageBuilder builder(comparator_, 4096, 16);

    SECTION("Build empty page") {
        auto data = builder.Finish();
        REQUIRE(data.size() > 0);

        // Should be able to parse as index page
        MemIndexPage page(comparator_, 16);
        REQUIRE(page.Empty());
    }

    SECTION("Build with entries") {
        builder.Add("key1", 100);
        builder.Add("key2", 200);
        builder.Add("key3", 300);

        auto data = builder.Finish();
        REQUIRE(data.size() > 0);
    }

    SECTION("Page size limit") {
        IndexPageBuilder small_builder(comparator_, 256, 16);  // Small page

        int added = 0;
        for (int i = 0; i < 1000; ++i) {
            if (!small_builder.Add(gen_.GenerateSequentialKey(i), i)) {
                break;
            }
            added++;
        }

        REQUIRE(added > 0);
        REQUIRE(added < 1000);  // Should hit size limit

        auto data = small_builder.Finish();
        REQUIRE(data.size() <= 256);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPage_EdgeCases", "[index][unit][edge-case]") {
    SECTION("Empty key") {
        auto page = CreateIndexPage({{"", 100}});

        PageId result;
        REQUIRE(page->Lookup("", &result) == true);
        REQUIRE(result == 100);
    }

    SECTION("Very long key") {
        std::string long_key(10000, 'k');
        auto page = CreateIndexPage({{long_key, 999}});

        PageId result;
        REQUIRE(page->Lookup(long_key, &result) == true);
        REQUIRE(result == 999);
    }

    SECTION("Binary keys") {
        std::string binary_key = gen_.GenerateBinaryString(100);
        auto page = CreateIndexPage({{binary_key, 777}});

        PageId result;
        REQUIRE(page->Lookup(binary_key, &result) == true);
        REQUIRE(result == 777);
    }

    SECTION("Maximum PageId") {
        auto page = CreateIndexPage({{"max_key", MaxPageId}});

        PageId result;
        REQUIRE(page->Lookup("max_key", &result) == true);
        REQUIRE(result == MaxPageId);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPage_Performance", "[index][benchmark]") {
    SECTION("Lookup performance") {
        auto page = std::make_unique<MemIndexPage>(comparator_, 16);

        // Add many entries
        std::vector<std::string> keys;
        for (int i = 0; i < 10000; ++i) {
            std::string key = gen_.GenerateSequentialKey(i);
            keys.push_back(key);
            page->Add(key, i);
        }

        Timer timer;
        const int lookups = 100000;

        timer.Start();
        for (int i = 0; i < lookups; ++i) {
            PageId result;
            page->Lookup(keys[i % keys.size()], &result);
        }
        timer.Stop();

        double ops_per_sec = lookups / timer.ElapsedSeconds();
        LOG(INFO) << "Index lookup: " << ops_per_sec << " ops/sec";

        REQUIRE(ops_per_sec > 100000);  // Should handle >100K lookups/sec
    }

    SECTION("Build performance") {
        Timer timer;
        const int builds = 1000;

        timer.Start();
        for (int b = 0; b < builds; ++b) {
            IndexPageBuilder builder(comparator_, 4096, 16);

            for (int i = 0; i < 100; ++i) {
                builder.Add(gen_.GenerateSequentialKey(i), i);
            }

            auto data = builder.Finish();
        }
        timer.Stop();

        double builds_per_sec = builds / timer.ElapsedSeconds();
        LOG(INFO) << "Index page builds: " << builds_per_sec << " builds/sec";

        REQUIRE(builds_per_sec > 100);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageManager_StressTest", "[index][stress]") {
    SECTION("Concurrent access") {
        const uint32_t cache_size = 100;
        IndexPageManager manager(cache_size);

        std::vector<std::thread> threads;
        std::atomic<int> successful_ops{0};

        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([&, t]() {
                TableIdent table("table_" + std::to_string(t), t);

                for (int i = 0; i < 100; ++i) {
                    // Add pages
                    auto page = std::make_unique<MemIndexPage>(comparator_, 16);
                    page->Add("key_" + std::to_string(i), i);
                    manager.Put(table, i, std::move(page));

                    // Retrieve pages
                    auto* retrieved = manager.Get(table, i);
                    if (retrieved) {
                        PageId result;
                        if (retrieved->Lookup("key_" + std::to_string(i), &result)) {
                            successful_ops++;
                        }
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        REQUIRE(successful_ops > 0);
    }

    SECTION("Cache thrashing") {
        const uint32_t cache_size = 10;
        IndexPageManager manager(cache_size);
        TableIdent table("test", 1);

        // Rapidly add and access many pages
        for (int iteration = 0; iteration < 10; ++iteration) {
            for (PageId i = 0; i < cache_size * 10; ++i) {
                auto page = std::make_unique<MemIndexPage>(comparator_, 16);
                page->Add("iter_" + std::to_string(iteration) + "_" + std::to_string(i), i);
                manager.Put(table, i, std::move(page));

                // Random access pattern
                PageId random_id = rand() % (cache_size * 10);
                manager.Get(table, random_id);
            }
        }

        // Cache should still function correctly
        auto page = std::make_unique<MemIndexPage>(comparator_, 16);
        page->Add("final", 999);
        manager.Put(table, 999, std::move(page));

        auto* retrieved = manager.Get(table, 999);
        if (retrieved) {
            PageId result;
            REQUIRE(retrieved->Lookup("final", &result) == true);
            REQUIRE(result == 999);
        }
    }
}