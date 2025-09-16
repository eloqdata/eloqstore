#include <catch2/catch_test_macros.hpp>
#include <sstream>
#include <filesystem>

#include "types.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Types_TableIdent_Construction", "[types][unit]") {
    SECTION("Default construction") {
        TableIdent tid;
        REQUIRE(!tid.IsValid());
        REQUIRE(tid.tbl_name_.empty());
        REQUIRE(tid.partition_id_ == 0);
    }

    SECTION("Parameterized construction") {
        TableIdent tid("test_table", 42);
        REQUIRE(tid.IsValid());
        REQUIRE(tid.tbl_name_ == "test_table");
        REQUIRE(tid.partition_id_ == 42);
    }

    SECTION("Copy construction") {
        TableIdent tid1("table", 10);
        TableIdent tid2(tid1);
        REQUIRE(tid2.tbl_name_ == tid1.tbl_name_);
        REQUIRE(tid2.partition_id_ == tid1.partition_id_);
    }
}

TEST_CASE("Types_TableIdent_StringConversion", "[types][unit]") {
    SECTION("ToString") {
        TableIdent tid("my_table", 123);
        std::string str = tid.ToString();
        REQUIRE(str == "my_table.123");
    }

    SECTION("FromString - valid") {
        std::string input = "test_table.456";
        TableIdent tid = TableIdent::FromString(input);
        REQUIRE(tid.tbl_name_ == "test_table");
        REQUIRE(tid.partition_id_ == 456);
        REQUIRE(tid.IsValid());
    }

    SECTION("FromString - edge cases") {
        // Empty string
        TableIdent tid1 = TableIdent::FromString("");
        REQUIRE(!tid1.IsValid());

        // No separator
        TableIdent tid2 = TableIdent::FromString("noseparator");
        REQUIRE(tid2.tbl_name_ == "noseparator");
        REQUIRE(tid2.partition_id_ == 0);

        // Multiple separators
        TableIdent tid3 = TableIdent::FromString("table.name.123");
        REQUIRE(tid3.tbl_name_ == "table.name");
        REQUIRE(tid3.partition_id_ == 123);

        // Invalid partition number
        TableIdent tid4 = TableIdent::FromString("table.abc");
        REQUIRE(tid4.tbl_name_ == "table");
        // partition_id_ behavior with invalid number is implementation-specific
    }

    SECTION("Round-trip conversion") {
        TableIdent original("round_trip_table", 789);
        std::string str = original.ToString();
        TableIdent recovered = TableIdent::FromString(str);

        REQUIRE(recovered == original);
        REQUIRE(recovered.tbl_name_ == original.tbl_name_);
        REQUIRE(recovered.partition_id_ == original.partition_id_);
    }
}

TEST_CASE("Types_TableIdent_Equality", "[types][unit]") {
    SECTION("Equal tables") {
        TableIdent tid1("table", 100);
        TableIdent tid2("table", 100);
        REQUIRE(tid1 == tid2);
    }

    SECTION("Different table names") {
        TableIdent tid1("table1", 100);
        TableIdent tid2("table2", 100);
        REQUIRE(!(tid1 == tid2));
    }

    SECTION("Different partition IDs") {
        TableIdent tid1("table", 100);
        TableIdent tid2("table", 200);
        REQUIRE(!(tid1 == tid2));
    }

    SECTION("Both different") {
        TableIdent tid1("table1", 100);
        TableIdent tid2("table2", 200);
        REQUIRE(!(tid1 == tid2));
    }
}

TEST_CASE("Types_TableIdent_Sharding", "[types][unit]") {
    SECTION("Shard distribution") {
        const uint16_t num_shards = 16;
        std::vector<uint16_t> shard_counts(num_shards, 0);

        // Create many tables and check distribution
        for (uint32_t i = 0; i < 1000; ++i) {
            TableIdent tid("table", i);
            uint16_t shard = tid.ShardIndex(num_shards);
            REQUIRE(shard < num_shards);
            shard_counts[shard]++;
        }

        // Check for reasonable distribution
        for (uint16_t count : shard_counts) {
            // Each shard should get roughly 1000/16 ≈ 62 tables
            REQUIRE(count > 40);
            REQUIRE(count < 85);
        }
    }

    SECTION("Deterministic sharding") {
        TableIdent tid("table", 12345);

        // Same table should always map to same shard
        uint16_t shard1 = tid.ShardIndex(8);
        uint16_t shard2 = tid.ShardIndex(8);
        REQUIRE(shard1 == shard2);

        // Different shard counts should give different results
        uint16_t shard_8 = tid.ShardIndex(8);
        uint16_t shard_16 = tid.ShardIndex(16);
        REQUIRE(shard_8 < 8);
        REQUIRE(shard_16 < 16);
    }
}

TEST_CASE("Types_TableIdent_DiskIndex", "[types][unit]") {
    SECTION("Disk distribution") {
        const uint8_t num_disks = 4;
        std::vector<uint8_t> disk_counts(num_disks, 0);

        for (uint32_t i = 0; i < 400; ++i) {
            TableIdent tid("table", i);
            uint8_t disk = tid.DiskIndex(num_disks);
            REQUIRE(disk < num_disks);
            disk_counts[disk]++;
        }

        // Check for even distribution
        for (uint8_t count : disk_counts) {
            REQUIRE(count > 80);
            REQUIRE(count < 120);
        }
    }

    SECTION("Single disk") {
        TableIdent tid("table", 999);
        REQUIRE(tid.DiskIndex(1) == 0);
    }
}

TEST_CASE("Types_TableIdent_StorePath", "[types][unit]") {
    SECTION("Single disk path") {
        std::vector<std::string> disks = {"/data/disk1"};
        TableIdent tid("my_table", 42);

        fs::path path = tid.StorePath(disks);
        REQUIRE(path.string().find("/data/disk1") != std::string::npos);
        REQUIRE(path.string().find("my_table.42") != std::string::npos);
    }

    SECTION("Multiple disk paths") {
        std::vector<std::string> disks = {
            "/data/disk1",
            "/data/disk2",
            "/data/disk3",
            "/data/disk4"
        };

        TableIdent tid1("table", 1);
        TableIdent tid2("table", 2);

        fs::path path1 = tid1.StorePath(disks);
        fs::path path2 = tid2.StorePath(disks);

        // Should use different disks for different partitions
        // (though not guaranteed, highly likely with good hash)
    }

    SECTION("Empty disk list") {
        std::vector<std::string> disks;
        TableIdent tid("table", 1);

        // Should handle empty disk list gracefully
        // Behavior is implementation-specific
    }
}

TEST_CASE("Types_FileKey", "[types][unit]") {
    SECTION("FileKey equality") {
        FileKey key1{TableIdent("table", 1), 100};
        FileKey key2{TableIdent("table", 1), 100};
        FileKey key3{TableIdent("table", 1), 200};
        FileKey key4{TableIdent("table", 2), 100};

        REQUIRE(key1 == key2);
        REQUIRE(!(key1 == key3));
        REQUIRE(!(key1 == key4));
    }

    SECTION("FileKey hash") {
        FileKey key1{TableIdent("table", 1), 100};
        FileKey key2{TableIdent("table", 1), 100};
        FileKey key3{TableIdent("table", 2), 200};

        std::hash<FileKey> hasher;
        REQUIRE(hasher(key1) == hasher(key2));
        // Different keys should (likely) have different hashes
        REQUIRE(hasher(key1) != hasher(key3));
    }

    SECTION("FileKey in container") {
        std::unordered_map<FileKey, std::string> file_map;

        FileKey key1{TableIdent("table1", 1), 100};
        FileKey key2{TableIdent("table2", 2), 200};

        file_map[key1] = "file1";
        file_map[key2] = "file2";

        REQUIRE(file_map.size() == 2);
        REQUIRE(file_map[key1] == "file1");
        REQUIRE(file_map[key2] == "file2");
    }
}

TEST_CASE("Types_PageId", "[types][unit]") {
    SECTION("Page ID limits") {
        PageId min_id = 0;
        PageId max_id = MaxPageId;

        REQUIRE(min_id < max_id);
        REQUIRE(max_id == UINT32_MAX);

        // Arithmetic near boundaries
        PageId near_max = max_id - 1;
        REQUIRE(near_max < max_id);
        REQUIRE(near_max == UINT32_MAX - 1);
    }

    SECTION("Page ID operations") {
        PageId id1 = 1000;
        PageId id2 = 2000;

        REQUIRE(id1 < id2);
        REQUIRE(id1 + 1000 == id2);

        // Overflow behavior
        PageId overflow = MaxPageId;
        overflow++;  // Will wrap to 0
        REQUIRE(overflow == 0);
    }
}

TEST_CASE("Types_FilePageId", "[types][unit]") {
    SECTION("FilePageId limits") {
        FilePageId min_id = 0;
        FilePageId max_id = MaxFilePageId;

        REQUIRE(min_id < max_id);
        REQUIRE(max_id == UINT64_MAX);
    }

    SECTION("FilePageId encoding") {
        // FilePageId might encode both file and page info
        FileId file_id = 100;
        PageId page_id = 200;

        // Common pattern: high bits for file, low bits for page
        FilePageId combined = (static_cast<FilePageId>(file_id) << 32) | page_id;

        FileId extracted_file = combined >> 32;
        PageId extracted_page = combined & 0xFFFFFFFF;

        REQUIRE(extracted_file == file_id);
        REQUIRE(extracted_page == page_id);
    }
}

TEST_CASE("Types_FileId", "[types][unit]") {
    SECTION("FileId limits") {
        FileId min_id = 0;
        FileId max_id = MaxFileId;

        REQUIRE(min_id < max_id);
        REQUIRE(max_id == UINT64_MAX);
    }

    SECTION("FileId generation") {
        // Simulate sequential file ID generation
        FileId current = 1000;
        std::vector<FileId> file_ids;

        for (int i = 0; i < 100; ++i) {
            file_ids.push_back(current++);
        }

        // All IDs should be unique and sequential
        for (size_t i = 1; i < file_ids.size(); ++i) {
            REQUIRE(file_ids[i] == file_ids[i-1] + 1);
        }
    }
}

TEST_CASE("Types_FileNaming", "[types][unit]") {
    SECTION("File name constants") {
        REQUIRE(FileNameSeparator == '_');
        REQUIRE(std::string(FileNameData) == "data");
        REQUIRE(std::string(FileNameManifest) == "manifest");
        REQUIRE(std::string(TmpSuffix) == ".tmp");
    }

    SECTION("Construct file names") {
        TableIdent tid("mytable", 42);
        FileId file_id = 1234;

        // Construct data file name
        std::stringstream ss;
        ss << tid.ToString() << FileNameSeparator
           << FileNameData << FileNameSeparator
           << file_id;
        std::string data_file = ss.str();

        REQUIRE(data_file == "mytable.42_data_1234");

        // Add temp suffix
        std::string temp_file = data_file + TmpSuffix;
        REQUIRE(temp_file == "mytable.42_data_1234.tmp");
    }

    SECTION("Parse file names") {
        std::string filename = "table.10_data_500";

        // Find separators
        size_t first_sep = filename.find(FileNameSeparator);
        size_t second_sep = filename.find(FileNameSeparator, first_sep + 1);

        REQUIRE(first_sep != std::string::npos);
        REQUIRE(second_sep != std::string::npos);

        // Extract components
        std::string table_part = filename.substr(0, first_sep);
        std::string type_part = filename.substr(first_sep + 1, second_sep - first_sep - 1);
        std::string id_part = filename.substr(second_sep + 1);

        REQUIRE(table_part == "table.10");
        REQUIRE(type_part == "data");
        REQUIRE(id_part == "500");
    }
}

TEST_CASE("Types_EdgeCases", "[types][unit][edge-case]") {
    SECTION("TableIdent with empty name") {
        TableIdent tid("", 100);
        REQUIRE(!tid.IsValid());  // Empty name should be invalid
    }

    SECTION("TableIdent with special characters") {
        TableIdent tid1("table-with-dash", 1);
        TableIdent tid2("table.with.dots", 2);
        TableIdent tid3("table/with/slash", 3);
        TableIdent tid4("table with spaces", 4);

        // All should be valid (unless implementation restricts)
        // But ToString/FromString might not round-trip correctly
        std::string str1 = tid1.ToString();
        TableIdent recovered1 = TableIdent::FromString(str1);

        // Special chars might cause issues with separator
        if (tid2.tbl_name_.find('.') != std::string::npos) {
            // Dots conflict with separator
            std::string str2 = tid2.ToString();
            TableIdent recovered2 = TableIdent::FromString(str2);
            // May not round-trip correctly
        }
    }

    SECTION("Maximum values") {
        TableIdent tid(std::string(1000, 'x'), UINT32_MAX);
        REQUIRE(tid.partition_id_ == UINT32_MAX);

        // Test boundary operations
        uint16_t shard = tid.ShardIndex(65535);  // Max uint16
        REQUIRE(shard < 65535);

        uint8_t disk = tid.DiskIndex(255);  // Max uint8
        REQUIRE(disk < 255);
    }

    SECTION("Hash collisions") {
        // Try to find hash collisions (unlikely but possible)
        std::unordered_set<FileKey> key_set;
        int collisions = 0;

        for (uint32_t i = 0; i < 10000; ++i) {
            FileKey key{TableIdent("table", i), i};
            auto [it, inserted] = key_set.insert(key);
            if (!inserted) {
                collisions++;
            }
        }

        REQUIRE(collisions == 0);  // Should have no collisions for different keys
    }
}

TEST_CASE("Types_Stress", "[types][stress]") {
    DataGenerator gen(42);

    SECTION("Many TableIdents") {
        const int num_tables = 10000;
        std::vector<TableIdent> tables;

        for (int i = 0; i < num_tables; ++i) {
            tables.emplace_back(gen.GenerateRandomKey(5, 20), i);
        }

        // Convert all to strings
        std::vector<std::string> strings;
        for (const auto& tid : tables) {
            strings.push_back(tid.ToString());
        }

        // Convert all back
        std::vector<TableIdent> recovered;
        for (const auto& str : strings) {
            recovered.push_back(TableIdent::FromString(str));
        }

        // Verify round-trip
        for (size_t i = 0; i < tables.size(); ++i) {
            if (!tables[i].tbl_name_.empty() &&
                tables[i].tbl_name_.find('.') == std::string::npos) {
                REQUIRE(recovered[i] == tables[i]);
            }
        }
    }

    SECTION("Concurrent access patterns") {
        // Simulate concurrent access to shared table ID
        std::atomic<uint32_t> next_partition{0};
        const std::string table_name = "concurrent_table";

        std::vector<std::thread> threads;
        std::vector<std::vector<TableIdent>> thread_tables(8);

        for (size_t t = 0; t < 8; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < 100; ++i) {
                    uint32_t part_id = next_partition.fetch_add(1);
                    thread_tables[t].emplace_back(table_name, part_id);
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        // Verify all partition IDs are unique
        std::set<uint32_t> all_partitions;
        for (const auto& tables : thread_tables) {
            for (const auto& tid : tables) {
                auto [it, inserted] = all_partitions.insert(tid.partition_id_);
                REQUIRE(inserted);  // Should be unique
            }
        }

        REQUIRE(all_partitions.size() == 800);
    }
}