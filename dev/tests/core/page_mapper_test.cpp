#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <set>
#include <thread>
#include <random>

#include "page_mapper.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "kvoptions.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class PageMapperTestFixture {
public:
    PageMapperTestFixture() {
        // Initialize test options
        InitOptions();
    }

    void InitOptions() {
        options_ = std::make_unique<KvOptions>();
        options_->page_size = 4096;
        options_->pages_per_file = 256;
        options_->use_append_mode = false;
        options_->local_data_dirs = {"/tmp/test_pagemapper"};
    }

    std::unique_ptr<PageMapper> CreatePageMapper() {
        index_manager_ = std::make_unique<IndexPageManager>(100);
        table_ident_ = std::make_unique<TableIdent>("test_table", 1);
        return std::make_unique<PageMapper>(index_manager_.get(), table_ident_.get());
    }

protected:
    std::unique_ptr<KvOptions> options_;
    std::unique_ptr<IndexPageManager> index_manager_;
    std::unique_ptr<TableIdent> table_ident_;
};

TEST_CASE_METHOD(PageMapperTestFixture, "MappingSnapshot_Encoding", "[pagemapper][unit]") {
    SECTION("Encode and decode FilePageId") {
        FilePageId file_page_id = 12345;
        uint64_t encoded = MappingSnapshot::EncodeFilePageId(file_page_id);

        REQUIRE(MappingSnapshot::IsFilePageId(encoded) == true);
        REQUIRE(MappingSnapshot::IsSwizzlingPointer(encoded) == false);
        REQUIRE(MappingSnapshot::GetValType(encoded) == MappingSnapshot::ValType::FilePageId);
        REQUIRE(MappingSnapshot::DecodeId(encoded) == file_page_id);
    }

    SECTION("Encode and decode PageId") {
        PageId page_id = 67890;
        uint64_t encoded = MappingSnapshot::EncodePageId(page_id);

        REQUIRE(MappingSnapshot::IsFilePageId(encoded) == false);
        REQUIRE(MappingSnapshot::GetValType(encoded) == MappingSnapshot::ValType::PageId);
        REQUIRE(MappingSnapshot::DecodeId(encoded) == page_id);
    }

    SECTION("Invalid value detection") {
        uint64_t invalid = MappingSnapshot::InvalidValue;
        REQUIRE(MappingSnapshot::GetValType(invalid) == MappingSnapshot::ValType::Invalid);
    }

    SECTION("Swizzling pointer detection") {
        // Create a fake pointer value
        MemIndexPage* fake_ptr = reinterpret_cast<MemIndexPage*>(0x123456789ABC);
        uint64_t ptr_val = reinterpret_cast<uint64_t>(fake_ptr);

        // Swizzling pointers have specific encoding
        if ((ptr_val & MappingSnapshot::TypeMask) == static_cast<uint8_t>(MappingSnapshot::ValType::SwizzlingPointer)) {
            REQUIRE(MappingSnapshot::IsSwizzlingPointer(ptr_val) == true);
        }
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "MappingSnapshot_Operations", "[pagemapper][unit]") {
    auto mapper = CreatePageMapper();
    auto snapshot = mapper->GetMappingSnapshot();

    SECTION("Add and retrieve free file pages") {
        std::vector<FilePageId> free_pages = {10, 20, 30, 40, 50};

        for (FilePageId fp : free_pages) {
            snapshot->AddFreeFilePage(fp);
        }

        REQUIRE(snapshot->to_free_file_pages_.size() == free_pages.size());

        // Clear free pages
        snapshot->ClearFreeFilePage();
        REQUIRE(snapshot->to_free_file_pages_.empty());
    }

    SECTION("Swizzling operations") {
        PageId page_id = 100;
        auto index_page = std::make_unique<MemIndexPage>(nullptr, 16);

        // Add swizzling pointer
        snapshot->AddSwizzling(page_id, index_page.get());

        // Retrieve swizzling pointer
        auto retrieved = snapshot->GetSwizzlingPointer(page_id);
        REQUIRE(retrieved == index_page.get());

        // Unswizzling
        snapshot->Unswizzling(index_page.get());
    }

    SECTION("ToFilePage conversions") {
        // Add some mappings
        snapshot->mapping_tbl_.resize(10);
        for (size_t i = 0; i < 10; ++i) {
            snapshot->mapping_tbl_[i] = MappingSnapshot::EncodeFilePageId(i * 100);
        }

        // Convert PageId to FilePageId
        PageId page_id = 5;
        FilePageId file_page = snapshot->ToFilePage(page_id);
        REQUIRE(file_page == 500);
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "FilePageAllocator_Basic", "[pagemapper][unit]") {
    SECTION("Sequential allocation") {
        FilePageAllocator allocator(options_.get(), 0);

        std::set<FilePageId> allocated;
        for (int i = 0; i < 100; ++i) {
            FilePageId fp = allocator.Allocate();
            REQUIRE(allocated.find(fp) == allocated.end());  // Should be unique
            allocated.insert(fp);
        }

        REQUIRE(allocated.size() == 100);
    }

    SECTION("File ID calculation") {
        FilePageAllocator allocator(options_.get(), 1000);

        REQUIRE(allocator.MaxFilePageId() == 1000);
        REQUIRE(allocator.PagesPerFile() == options_->pages_per_file);

        // Current file ID should be based on max file page ID
        FileId expected_file = 1000 / options_->pages_per_file;
        REQUIRE(allocator.CurrentFileId() == expected_file);
    }

    SECTION("Clone allocator") {
        FilePageAllocator allocator(options_.get(), 500);

        // Allocate some pages
        for (int i = 0; i < 10; ++i) {
            allocator.Allocate();
        }

        // Clone should have same state
        auto cloned = allocator.Clone();
        REQUIRE(cloned->MaxFilePageId() == allocator.MaxFilePageId());
        REQUIRE(cloned->CurrentFileId() == allocator.CurrentFileId());
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "AppendAllocator_Operations", "[pagemapper][unit]") {
    SECTION("Basic allocation") {
        AppendAllocator allocator(options_.get());

        // Allocations should be sequential
        FilePageId prev = 0;
        for (int i = 0; i < 100; ++i) {
            FilePageId fp = allocator.Allocate();
            if (i > 0) {
                REQUIRE(fp == prev + 1);
            }
            prev = fp;
        }
    }

    SECTION("Space calculation") {
        AppendAllocator allocator(options_.get(), 10, 1000, 2);

        REQUIRE(allocator.MinFileId() == 10);

        // Space size should account for files used
        size_t space = allocator.SpaceSize();
        REQUIRE(space > 0);
    }

    SECTION("Update statistics") {
        AppendAllocator allocator(options_.get());

        allocator.UpdateStat(5, 3);
        REQUIRE(allocator.MinFileId() == 5);

        // Clone should preserve statistics
        auto cloned = dynamic_cast<AppendAllocator*>(allocator.Clone().get());
        REQUIRE(cloned != nullptr);
        REQUIRE(cloned->MinFileId() == 5);
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "PooledFilePages_Operations", "[pagemapper][unit]") {
    SECTION("Allocation from pool") {
        std::vector<uint32_t> free_ids = {100, 200, 300, 400, 500};
        PooledFilePages allocator(options_.get(), 1000, free_ids);

        // Should allocate from free list first
        std::set<FilePageId> allocated;
        for (size_t i = 0; i < free_ids.size(); ++i) {
            FilePageId fp = allocator.Allocate();
            allocated.insert(fp);
        }

        // All free IDs should be used
        for (uint32_t id : free_ids) {
            REQUIRE(allocated.find(id) != allocated.end());
        }

        // Next allocation should be from max_fp_id
        FilePageId next = allocator.Allocate();
        REQUIRE(next >= 1000);
    }

    SECTION("Free pages back to pool") {
        PooledFilePages allocator(options_.get());

        // Allocate some pages
        std::vector<FilePageId> allocated;
        for (int i = 0; i < 10; ++i) {
            allocated.push_back(allocator.Allocate());
        }

        // Free them back
        allocator.Free(allocated);

        // Should be able to reallocate same IDs
        std::set<FilePageId> reallocated;
        for (int i = 0; i < 10; ++i) {
            reallocated.insert(allocator.Allocate());
        }

        for (FilePageId fp : allocated) {
            REQUIRE(reallocated.find(fp) != reallocated.end());
        }
    }

    SECTION("Clone with free list") {
        std::vector<uint32_t> free_ids = {10, 20, 30};
        PooledFilePages allocator(options_.get(), 100, free_ids);

        auto cloned = allocator.Clone();
        REQUIRE(cloned != nullptr);

        // Both should allocate same free IDs
        FilePageId orig = allocator.Allocate();
        FilePageId clone = cloned->Allocate();

        std::set<uint32_t> free_set(free_ids.begin(), free_ids.end());
        REQUIRE(free_set.find(orig) != free_set.end());
        REQUIRE(free_set.find(clone) != free_set.end());
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "PageMapper_Basic", "[pagemapper][unit]") {
    auto mapper = CreatePageMapper();

    SECTION("Get and free pages") {
        std::vector<PageId> pages;

        // Get multiple pages
        for (int i = 0; i < 100; ++i) {
            PageId page = mapper->GetPage();
            REQUIRE(page != MaxPageId);
            pages.push_back(page);
        }

        // All should be unique
        std::set<PageId> unique_pages(pages.begin(), pages.end());
        REQUIRE(unique_pages.size() == pages.size());

        // Free some pages
        for (size_t i = 0; i < 50; ++i) {
            mapper->FreePage(pages[i]);
        }

        // Should be able to reuse freed pages
        for (int i = 0; i < 50; ++i) {
            PageId page = mapper->GetPage();
            REQUIRE(page != MaxPageId);
        }
    }

    SECTION("Update mapping") {
        PageId page_id = mapper->GetPage();
        FilePageId file_page_id = 12345;

        mapper->UpdateMapping(page_id, file_page_id);

        auto snapshot = mapper->GetMappingSnapshot();
        FilePageId retrieved = snapshot->ToFilePage(page_id);
        REQUIRE(retrieved == file_page_id);
    }

    SECTION("Mapping count") {
        uint32_t initial_count = mapper->MappingCount();

        // Allocate pages
        for (int i = 0; i < 10; ++i) {
            PageId page = mapper->GetPage();
            mapper->UpdateMapping(page, i * 100);
        }

        REQUIRE(mapper->MappingCount() == initial_count + 10);
    }

    SECTION("Copy constructor") {
        // Allocate and map some pages
        std::vector<PageId> pages;
        for (int i = 0; i < 10; ++i) {
            PageId page = mapper->GetPage();
            mapper->UpdateMapping(page, i * 100);
            pages.push_back(page);
        }

        // Copy mapper
        PageMapper copied(*mapper);

        // Both should have same mappings
        auto orig_snapshot = mapper->GetMappingSnapshot();
        auto copy_snapshot = copied.GetMappingSnapshot();

        for (PageId page : pages) {
            REQUIRE(orig_snapshot->ToFilePage(page) == copy_snapshot->ToFilePage(page));
        }
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "PageMapper_FreeList", "[pagemapper][unit]") {
    auto mapper = CreatePageMapper();

    SECTION("Free list reuse") {
        std::vector<PageId> first_batch;

        // Allocate first batch
        for (int i = 0; i < 100; ++i) {
            first_batch.push_back(mapper->GetPage());
        }

        // Free all
        for (PageId page : first_batch) {
            mapper->FreePage(page);
        }

        // Reallocate - should reuse same pages
        std::set<PageId> second_batch;
        for (int i = 0; i < 100; ++i) {
            second_batch.insert(mapper->GetPage());
        }

        // Check all first batch pages are reused
        for (PageId page : first_batch) {
            REQUIRE(second_batch.find(page) != second_batch.end());
        }
    }

    SECTION("Mixed allocation and freeing") {
        std::vector<PageId> pages;
        std::set<PageId> freed;

        // Allocate some
        for (int i = 0; i < 50; ++i) {
            pages.push_back(mapper->GetPage());
        }

        // Free every other one
        for (size_t i = 0; i < pages.size(); i += 2) {
            mapper->FreePage(pages[i]);
            freed.insert(pages[i]);
        }

        // Allocate more - should reuse freed ones first
        std::vector<PageId> new_pages;
        for (size_t i = 0; i < freed.size(); ++i) {
            new_pages.push_back(mapper->GetPage());
        }

        // Check freed pages are reused
        for (PageId page : new_pages) {
            REQUIRE(freed.find(page) != freed.end());
        }
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "PageMapper_EdgeCases", "[pagemapper][unit][edge-case]") {
    SECTION("Empty mapper") {
        auto mapper = CreatePageMapper();

        REQUIRE(mapper->MappingCount() == 0);
        REQUIRE(mapper->GetMappingSnapshot() != nullptr);
        REQUIRE(mapper->UseCount() >= 1);
    }

    SECTION("Single page operations") {
        auto mapper = CreatePageMapper();

        PageId page = mapper->GetPage();
        REQUIRE(page != MaxPageId);

        mapper->FreePage(page);

        PageId reused = mapper->GetPage();
        REQUIRE(reused == page);
    }

    SECTION("Maximum page ID boundary") {
        auto mapper = CreatePageMapper();

        // Free an invalid page ID
        mapper->FreePage(MaxPageId);  // Should handle gracefully

        // Get should still work
        PageId page = mapper->GetPage();
        REQUIRE(page != MaxPageId);
    }

    SECTION("Zero file page ID") {
        auto mapper = CreatePageMapper();

        PageId page = mapper->GetPage();
        mapper->UpdateMapping(page, 0);  // File page ID 0

        auto snapshot = mapper->GetMappingSnapshot();
        REQUIRE(snapshot->ToFilePage(page) == 0);
    }

    SECTION("Rapid allocation and deallocation") {
        auto mapper = CreatePageMapper();

        for (int iteration = 0; iteration < 100; ++iteration) {
            std::vector<PageId> pages;

            // Rapid allocation
            for (int i = 0; i < 10; ++i) {
                pages.push_back(mapper->GetPage());
            }

            // Rapid deallocation
            for (PageId page : pages) {
                mapper->FreePage(page);
            }
        }

        // Should still be functional
        PageId page = mapper->GetPage();
        REQUIRE(page != MaxPageId);
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "PageMapper_Concurrency", "[pagemapper][stress]") {
    SECTION("Concurrent page allocation") {
        auto mapper = CreatePageMapper();
        std::atomic<int> success_count{0};
        std::vector<std::thread> threads;

        const int thread_count = 8;
        const int pages_per_thread = 100;

        std::mutex pages_mutex;
        std::set<PageId> all_pages;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&mapper, &success_count, &pages_mutex, &all_pages, pages_per_thread]() {
                std::vector<PageId> local_pages;

                for (int i = 0; i < pages_per_thread; ++i) {
                    PageId page = mapper->GetPage();
                    if (page != MaxPageId) {
                        local_pages.push_back(page);
                        success_count++;
                    }
                }

                std::lock_guard<std::mutex> lock(pages_mutex);
                all_pages.insert(local_pages.begin(), local_pages.end());
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // All allocated pages should be unique
        REQUIRE(all_pages.size() == static_cast<size_t>(success_count.load()));
    }

    SECTION("Concurrent allocation and freeing") {
        auto mapper = CreatePageMapper();
        std::atomic<bool> stop{false};
        std::atomic<int> allocations{0};
        std::atomic<int> deallocations{0};

        std::vector<std::thread> threads;

        // Allocator threads
        for (int t = 0; t < 4; ++t) {
            threads.emplace_back([&mapper, &stop, &allocations]() {
                while (!stop) {
                    PageId page = mapper->GetPage();
                    if (page != MaxPageId) {
                        allocations++;
                        std::this_thread::sleep_for(std::chrono::microseconds(10));
                        mapper->FreePage(page);
                    }
                }
            });
        }

        // Deallocator threads
        for (int t = 0; t < 4; ++t) {
            threads.emplace_back([&mapper, &stop, &deallocations]() {
                std::vector<PageId> pages;
                while (!stop) {
                    // Allocate batch
                    for (int i = 0; i < 10; ++i) {
                        PageId page = mapper->GetPage();
                        if (page != MaxPageId) {
                            pages.push_back(page);
                        }
                    }

                    // Free batch
                    for (PageId page : pages) {
                        mapper->FreePage(page);
                        deallocations++;
                    }
                    pages.clear();

                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        stop = true;

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(allocations > 0);
        REQUIRE(deallocations > 0);
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "MappingSnapshot_Serialization", "[pagemapper][unit]") {
    auto mapper = CreatePageMapper();

    SECTION("Serialize empty snapshot") {
        auto snapshot = mapper->GetMappingSnapshot();

        std::string serialized;
        snapshot->Serialize(serialized);

        REQUIRE(!serialized.empty());
    }

    SECTION("Serialize with mappings") {
        // Add some mappings
        for (int i = 0; i < 100; ++i) {
            PageId page = mapper->GetPage();
            mapper->UpdateMapping(page, i * 1000);
        }

        auto snapshot = mapper->GetMappingSnapshot();

        std::string serialized;
        snapshot->Serialize(serialized);

        REQUIRE(!serialized.empty());
        // Serialized data should contain mapping information
        REQUIRE(serialized.size() > 100);
    }
}

TEST_CASE_METHOD(PageMapperTestFixture, "FilePageAllocator_Modes", "[pagemapper][unit]") {
    SECTION("Append mode allocator") {
        options_->use_append_mode = true;
        auto allocator = FilePageAllocator::Instance(options_.get());

        REQUIRE(dynamic_cast<AppendAllocator*>(allocator.get()) != nullptr);

        // Should allocate sequentially
        FilePageId first = allocator->Allocate();
        FilePageId second = allocator->Allocate();
        REQUIRE(second == first + 1);
    }

    SECTION("Pooled mode allocator") {
        options_->use_append_mode = false;
        auto allocator = FilePageAllocator::Instance(options_.get());

        REQUIRE(dynamic_cast<PooledFilePages*>(allocator.get()) != nullptr);

        // Should allocate from pool
        std::set<FilePageId> allocated;
        for (int i = 0; i < 10; ++i) {
            allocated.insert(allocator->Allocate());
        }
        REQUIRE(allocated.size() == 10);
    }
}

TEST_CASE("PageMapper_Performance", "[pagemapper][benchmark]") {
    PageMapperTestFixture fixture;
    auto mapper = fixture.CreatePageMapper();

    SECTION("Allocation performance") {
        Timer timer;
        const int num_pages = 100000;

        timer.Start();
        for (int i = 0; i < num_pages; ++i) {
            volatile PageId page = mapper->GetPage();
            (void)page;
        }
        timer.Stop();

        double ops_per_sec = num_pages / timer.ElapsedSeconds();
        LOG(INFO) << "Page allocation: " << ops_per_sec << " ops/sec";

        REQUIRE(ops_per_sec > 100000);  // Should handle >100K allocations/sec
    }

    SECTION("Mapping update performance") {
        std::vector<PageId> pages;
        for (int i = 0; i < 1000; ++i) {
            pages.push_back(mapper->GetPage());
        }

        Timer timer;
        timer.Start();
        for (size_t i = 0; i < pages.size(); ++i) {
            for (int j = 0; j < 100; ++j) {
                mapper->UpdateMapping(pages[i], i * 100 + j);
            }
        }
        timer.Stop();

        double updates_per_sec = (pages.size() * 100) / timer.ElapsedSeconds();
        LOG(INFO) << "Mapping updates: " << updates_per_sec << " ops/sec";

        REQUIRE(updates_per_sec > 10000);
    }
}

#ifndef NDEBUG
TEST_CASE_METHOD(PageMapperTestFixture, "PageMapper_Debug", "[pagemapper][debug]") {
    auto mapper = CreatePageMapper();

    SECTION("Debug statistics") {
        // Allocate some pages
        for (int i = 0; i < 10; ++i) {
            mapper->GetPage();
        }

        // Check debug stats
        REQUIRE(mapper->DebugStat() == true);
    }
}
#endif