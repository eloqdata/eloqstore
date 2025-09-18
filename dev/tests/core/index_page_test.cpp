#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <random>
#include <map>

#include "mem_index_page.h"
#include "index_page_builder.h"
#include "index_page_manager.h"
#include "comparator.h"
#include "kv_options.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class IndexPageTestFixture : public TestFixture {
public:
    IndexPageTestFixture() : gen_(42) {
        InitStoreWithDefaults();
    }

protected:
    DataGenerator gen_;
    KvOptions options_;
};

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_BasicOperations", "[index][unit]") {
    SECTION("Create empty index page") {
        auto page = std::make_unique<MemIndexPage>(true);

        REQUIRE(page->PagePtr() != nullptr);
        REQUIRE(page->GetPageId() == MaxPageId);  // Uninitialized
        REQUIRE(page->IsDetached() == true);
        REQUIRE(!page->IsPinned());
    }

    SECTION("Page ID management") {
        auto page = std::make_unique<MemIndexPage>(true);

        page->SetPageId(12345);
        REQUIRE(page->GetPageId() == 12345);
        REQUIRE(page->IsPageIdValid());

        page->SetFilePageId(67890);
        REQUIRE(page->GetFilePageId() == 67890);
    }

    SECTION("Page pinning") {
        auto page = std::make_unique<MemIndexPage>(true);

        REQUIRE(!page->IsPinned());

        page->Pin();
        REQUIRE(page->IsPinned());

        page->Pin();
        REQUIRE(page->IsPinned());

        page->Unpin();
        REQUIRE(page->IsPinned());

        page->Unpin();
        REQUIRE(!page->IsPinned());
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageBuilder_BasicUsage", "[index][unit]") {

    SECTION("Empty builder") {
        IndexPageBuilder builder(&options_);

        REQUIRE(builder.IsEmpty());
        REQUIRE(builder.CurrentSizeEstimate() == IndexPageBuilder::HeaderSize());
    }

    SECTION("Add single entry") {
        IndexPageBuilder builder(&options_);

        bool success = builder.Add("key1", 100, true);
        REQUIRE(success);
        REQUIRE(!builder.IsEmpty());

        std::string_view page_data = builder.Finish();
        REQUIRE(page_data.size() > 0);
    }

    SECTION("Add multiple entries") {
        IndexPageBuilder builder(&options_);

        std::vector<std::pair<std::string, PageId>> entries = {
            {"aaa", 100},
            {"bbb", 200},
            {"ccc", 300}
        };

        for (const auto& [key, page_id] : entries) {
            bool success = builder.Add(key, page_id, true);
            REQUIRE(success);
        }

        std::string_view page_data = builder.Finish();
        REQUIRE(page_data.size() > 0);
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageIter_Iteration", "[index][unit]") {
    SECTION("Build and iterate page") {
        IndexPageBuilder builder(&options_);

        std::vector<std::pair<std::string, PageId>> entries = {
            {"aaa", 100},
            {"bbb", 200},
            {"ccc", 300}
        };

        for (const auto& [key, page_id] : entries) {
            builder.Add(key, page_id, true);
        }

        std::string_view page_data = builder.Finish();

        // Create iterator
        IndexPageIter iter(page_data, &options_);

        // Iterate through entries
        std::vector<std::pair<std::string, PageId>> found_entries;
        while (iter.HasNext()) {
            std::string_view key;
            uint32_t page_id;
            iter.Advance(key, page_id);
            found_entries.emplace_back(std::string(key), page_id);
        }

        REQUIRE(found_entries.size() == entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
            REQUIRE(found_entries[i].first == entries[i].first);
            REQUIRE(found_entries[i].second == entries[i].second);
        }
    }

    SECTION("Seek functionality") {
        IndexPageBuilder builder(&options_);

        for (int i = 0; i < 10; ++i) {
            std::string key = "key_" + std::to_string(i * 10);
            builder.Add(key, i * 10, true);
        }

        std::string_view page_data = builder.Finish();
        IndexPageIter iter(page_data, &options_);

        // Seek to specific key
        iter.Seek("key_30");

        if (iter.HasNext()) {
            std::string_view key;
            uint32_t page_id;
            iter.Advance(key, page_id);

            // Should find key_30 or the next key
            REQUIRE(key >= "key_30");
        }
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "MemIndexPage_LinkList", "[index][unit]") {
    SECTION("Page linking operations") {
        auto page1 = std::make_unique<MemIndexPage>(true);
        auto page2 = std::make_unique<MemIndexPage>(true);

        REQUIRE(page1->IsDetached());
        REQUIRE(page2->IsDetached());

        // Link pages
        page1->EnqueNext(page2.get());

        // Test dequeue operations
        page1->Deque();
        page2->Deque();

        REQUIRE(page1->IsDetached());
        REQUIRE(page2->IsDetached());
    }
}

TEST_CASE_METHOD(IndexPageTestFixture, "IndexPageBuilder_EdgeCases", "[index][unit]") {
    SECTION("Reset builder") {
        IndexPageBuilder builder(&options_);

        builder.Add("key1", 100, true);
        REQUIRE(!builder.IsEmpty());

        builder.Reset();
        REQUIRE(builder.IsEmpty());
        REQUIRE(builder.CurrentSizeEstimate() == IndexPageBuilder::HeaderSize());
    }

    SECTION("Move data from builder") {
        IndexPageBuilder builder(&options_);

        builder.Add("key1", 100, true);
        builder.Finish();

        std::string data = builder.Move();
        REQUIRE(!data.empty());
    }

}