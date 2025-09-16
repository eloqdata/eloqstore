#include <catch2/catch_test_macros.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>
#include <map>
#include <chrono>
#include <ctime>

#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/random_generator.h"

namespace fs = boost::filesystem;
using namespace eloqstore::test;

class ArchiveModeTestFixture {
public:
    ArchiveModeTestFixture() : gen_(42) {
        archive_dir_ = "/mnt/ramdisk/archive_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(archive_dir_);
        current_archive_id_ = 0;
    }

    ~ArchiveModeTestFixture() {
        Cleanup();
    }

    struct ArchiveEntry {
        std::string key;
        std::string value;
        uint64_t timestamp;
        uint64_t sequence_number;
    };

    struct Archive {
        uint64_t id;
        std::string filename;
        std::vector<ArchiveEntry> entries;
        size_t size_bytes;
        std::time_t creation_time;
        bool is_sealed;
    };

    // Create new archive file
    Archive CreateArchive() {
        Archive archive;
        archive.id = current_archive_id_++;
        archive.filename = archive_dir_ + "/archive_" + std::to_string(archive.id) + ".arc";
        archive.size_bytes = 0;
        archive.creation_time = std::time(nullptr);
        archive.is_sealed = false;

        archives_[archive.id] = archive;
        return archive;
    }

    // Append entry to archive (sequential write)
    bool AppendToArchive(uint64_t archive_id, const ArchiveEntry& entry) {
        auto it = archives_.find(archive_id);
        if (it == archives_.end() || it->second.is_sealed) {
            return false;
        }

        Archive& archive = it->second;

        // Write to file in append mode
        std::ofstream file(archive.filename, std::ios::binary | std::ios::app);
        if (!file) {
            return false;
        }

        // Write entry (simple format for testing)
        uint32_t key_len = entry.key.size();
        uint32_t value_len = entry.value.size();

        file.write(reinterpret_cast<const char*>(&entry.sequence_number), sizeof(entry.sequence_number));
        file.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        file.write(entry.key.data(), key_len);
        file.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        file.write(entry.value.data(), value_len);

        size_t entry_size = sizeof(entry.sequence_number) + sizeof(entry.timestamp) +
                           sizeof(key_len) + key_len + sizeof(value_len) + value_len;

        archive.entries.push_back(entry);
        archive.size_bytes += entry_size;

        return true;
    }

    // Seal archive (make read-only)
    void SealArchive(uint64_t archive_id) {
        auto it = archives_.find(archive_id);
        if (it != archives_.end()) {
            it->second.is_sealed = true;
        }
    }

    // Read archive entries
    std::vector<ArchiveEntry> ReadArchive(uint64_t archive_id) {
        std::vector<ArchiveEntry> entries;

        auto it = archives_.find(archive_id);
        if (it == archives_.end()) {
            return entries;
        }

        std::ifstream file(it->second.filename, std::ios::binary);
        if (!file) {
            return entries;
        }

        while (file.good()) {
            ArchiveEntry entry;

            uint32_t key_len, value_len;

            if (!file.read(reinterpret_cast<char*>(&entry.sequence_number), sizeof(entry.sequence_number)))
                break;

            file.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
            file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));

            entry.key.resize(key_len);
            file.read(entry.key.data(), key_len);

            file.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            entry.value.resize(value_len);
            file.read(entry.value.data(), value_len);

            entries.push_back(entry);
        }

        return entries;
    }

    // Rotate archives based on size or age
    uint64_t RotateArchive(uint64_t current_archive_id, size_t max_size) {
        auto it = archives_.find(current_archive_id);
        if (it != archives_.end() && it->second.size_bytes >= max_size) {
            SealArchive(current_archive_id);
            Archive new_archive = CreateArchive();
            return new_archive.id;
        }
        return current_archive_id;
    }

    // Delete old archives
    void DeleteOldArchives(int max_age_seconds) {
        std::time_t now = std::time(nullptr);
        std::vector<uint64_t> to_delete;

        for (const auto& [id, archive] : archives_) {
            if (archive.is_sealed && (now - archive.creation_time) > max_age_seconds) {
                to_delete.push_back(id);
            }
        }

        for (uint64_t id : to_delete) {
            DeleteArchive(id);
        }
    }

    void DeleteArchive(uint64_t archive_id) {
        auto it = archives_.find(archive_id);
        if (it != archives_.end()) {
            fs::remove(it->second.filename);
            archives_.erase(it);
        }
    }

    size_t GetArchiveCount() const {
        return archives_.size();
    }

    size_t GetTotalArchiveSize() const {
        size_t total = 0;
        for (const auto& [id, archive] : archives_) {
            total += archive.size_bytes;
        }
        return total;
    }

    Archive* GetArchive(uint64_t archive_id) {
        auto it = archives_.find(archive_id);
        return it != archives_.end() ? &it->second : nullptr;
    }

    std::map<uint64_t, Archive>& GetArchives() { return archives_; }

private:
    void Cleanup() {
        try {
            if (fs::exists(archive_dir_)) {
                fs::remove_all(archive_dir_);
            }
        } catch (...) {
            // Ignore cleanup errors
        }
    }

    std::string archive_dir_;
    std::map<uint64_t, Archive> archives_;
    uint64_t current_archive_id_;
    RandomGenerator gen_;
};

TEST_CASE_METHOD(ArchiveModeTestFixture, "Archive mode sequential writes", "[storage][archive]") {
    SECTION("Basic append operations") {
        auto archive = CreateArchive();

        std::vector<ArchiveEntry> test_entries;
        for (int i = 0; i < 100; ++i) {
            ArchiveEntry entry;
            entry.key = "key_" + std::to_string(i);
            entry.value = std::string(100, 'a' + (i % 26));
            entry.timestamp = 1000 + i;
            entry.sequence_number = i;

            test_entries.push_back(entry);
            REQUIRE(AppendToArchive(archive.id, entry));
        }

        // Read back and verify
        auto read_entries = ReadArchive(archive.id);
        REQUIRE(read_entries.size() == test_entries.size());

        for (size_t i = 0; i < test_entries.size(); ++i) {
            REQUIRE(read_entries[i].key == test_entries[i].key);
            REQUIRE(read_entries[i].value == test_entries[i].value);
            REQUIRE(read_entries[i].timestamp == test_entries[i].timestamp);
            REQUIRE(read_entries[i].sequence_number == test_entries[i].sequence_number);
        }
    }

    SECTION("Sequential write performance") {
        auto archive = CreateArchive();
        const int num_entries = 10000;

        auto start = std::chrono::steady_clock::now();

        for (int i = 0; i < num_entries; ++i) {
            ArchiveEntry entry;
            entry.key = "seq_key_" + std::to_string(i);
            entry.value = std::string(200, 'x');
            entry.timestamp = i;
            entry.sequence_number = i;

            AppendToArchive(archive.id, entry);
        }

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // Calculate throughput
        double entries_per_second = (num_entries * 1000.0) / duration.count();
        REQUIRE(entries_per_second > 1000); // Should handle at least 1000 entries/sec
    }

    SECTION("Append after seal fails") {
        auto archive = CreateArchive();

        ArchiveEntry entry;
        entry.key = "test_key";
        entry.value = "test_value";
        entry.timestamp = 1000;
        entry.sequence_number = 1;

        REQUIRE(AppendToArchive(archive.id, entry));

        SealArchive(archive.id);

        // Append should fail after sealing
        ArchiveEntry new_entry;
        new_entry.key = "new_key";
        new_entry.value = "new_value";
        new_entry.timestamp = 2000;
        new_entry.sequence_number = 2;

        REQUIRE_FALSE(AppendToArchive(archive.id, new_entry));
    }
}

TEST_CASE_METHOD(ArchiveModeTestFixture, "Archive rotation", "[storage][archive]") {
    SECTION("Size-based rotation") {
        const size_t max_archive_size = 10000;
        uint64_t current_archive = CreateArchive().id;

        int total_entries = 0;
        std::vector<uint64_t> archive_ids;
        archive_ids.push_back(current_archive);

        // Write until rotation happens
        for (int i = 0; i < 1000; ++i) {
            ArchiveEntry entry;
            entry.key = "rotation_key_" + std::to_string(i);
            entry.value = std::string(100, 'r');
            entry.timestamp = i;
            entry.sequence_number = i;

            AppendToArchive(current_archive, entry);
            total_entries++;

            // Check if rotation needed
            auto* archive = GetArchive(current_archive);
            if (archive && archive->size_bytes >= max_archive_size) {
                current_archive = RotateArchive(current_archive, max_archive_size);
                archive_ids.push_back(current_archive);
            }
        }

        // Should have multiple archives
        REQUIRE(archive_ids.size() > 1);

        // All archives except last should be sealed
        for (size_t i = 0; i < archive_ids.size() - 1; ++i) {
            auto* archive = GetArchive(archive_ids[i]);
            REQUIRE(archive->is_sealed);
        }
    }

    SECTION("Time-based archive deletion") {
        // Create old archives
        std::vector<uint64_t> old_archives;
        for (int i = 0; i < 5; ++i) {
            auto archive = CreateArchive();

            // Add some data
            ArchiveEntry entry;
            entry.key = "old_key_" + std::to_string(i);
            entry.value = "old_value";
            entry.timestamp = i;
            entry.sequence_number = i;

            AppendToArchive(archive.id, entry);
            SealArchive(archive.id);

            // Manually set old creation time
            GetArchives()[archive.id].creation_time = std::time(nullptr) - 3600; // 1 hour old
            old_archives.push_back(archive.id);
        }

        // Create recent archives
        std::vector<uint64_t> recent_archives;
        for (int i = 0; i < 3; ++i) {
            auto archive = CreateArchive();
            recent_archives.push_back(archive.id);
        }

        size_t initial_count = GetArchiveCount();
        REQUIRE(initial_count == 8);

        // Delete archives older than 30 minutes
        DeleteOldArchives(1800);

        // Old archives should be deleted
        REQUIRE(GetArchiveCount() == recent_archives.size());

        for (uint64_t id : old_archives) {
            REQUIRE(GetArchive(id) == nullptr);
        }

        for (uint64_t id : recent_archives) {
            REQUIRE(GetArchive(id) != nullptr);
        }
    }
}

TEST_CASE_METHOD(ArchiveModeTestFixture, "Archive recovery", "[storage][archive]") {
    SECTION("Read from multiple archives") {
        std::map<uint64_t, std::vector<ArchiveEntry>> archive_data;

        // Create multiple archives with data
        for (int a = 0; a < 3; ++a) {
            auto archive = CreateArchive();
            std::vector<ArchiveEntry> entries;

            for (int i = 0; i < 50; ++i) {
                ArchiveEntry entry;
                entry.key = "archive_" + std::to_string(a) + "_key_" + std::to_string(i);
                entry.value = "value_" + std::to_string(a * 100 + i);
                entry.timestamp = a * 1000 + i;
                entry.sequence_number = a * 100 + i;

                entries.push_back(entry);
                AppendToArchive(archive.id, entry);
            }

            SealArchive(archive.id);
            archive_data[archive.id] = entries;
        }

        // Read and verify all archives
        for (const auto& [archive_id, expected_entries] : archive_data) {
            auto read_entries = ReadArchive(archive_id);
            REQUIRE(read_entries.size() == expected_entries.size());

            for (size_t i = 0; i < expected_entries.size(); ++i) {
                REQUIRE(read_entries[i].key == expected_entries[i].key);
                REQUIRE(read_entries[i].value == expected_entries[i].value);
            }
        }
    }

    SECTION("Archive compaction") {
        // Create fragmented archives
        std::vector<uint64_t> source_archives;
        std::vector<ArchiveEntry> all_entries;

        for (int a = 0; a < 5; ++a) {
            auto archive = CreateArchive();
            source_archives.push_back(archive.id);

            // Add only a few entries per archive
            for (int i = 0; i < 10; ++i) {
                ArchiveEntry entry;
                entry.key = "compact_key_" + std::to_string(a * 10 + i);
                entry.value = "compact_value_" + std::to_string(a * 10 + i);
                entry.timestamp = a * 10 + i;
                entry.sequence_number = a * 10 + i;

                AppendToArchive(archive.id, entry);
                all_entries.push_back(entry);
            }

            SealArchive(archive.id);
        }

        // Compact into single archive
        auto compacted = CreateArchive();
        for (const auto& entry : all_entries) {
            AppendToArchive(compacted.id, entry);
        }
        SealArchive(compacted.id);

        // Delete source archives
        for (uint64_t id : source_archives) {
            DeleteArchive(id);
        }

        // Verify compacted archive has all data
        auto compacted_entries = ReadArchive(compacted.id);
        REQUIRE(compacted_entries.size() == all_entries.size());

        // Should have only one archive now
        REQUIRE(GetArchiveCount() == 1);
    }
}

TEST_CASE_METHOD(ArchiveModeTestFixture, "Archive mode edge cases", "[storage][archive]") {
    SECTION("Empty archive") {
        auto archive = CreateArchive();
        SealArchive(archive.id);

        auto entries = ReadArchive(archive.id);
        REQUIRE(entries.empty());
    }

    SECTION("Large entries") {
        auto archive = CreateArchive();

        ArchiveEntry large_entry;
        large_entry.key = "large_key";
        large_entry.value = std::string(1024 * 1024, 'L'); // 1MB value
        large_entry.timestamp = 1000;
        large_entry.sequence_number = 1;

        REQUIRE(AppendToArchive(archive.id, large_entry));

        auto entries = ReadArchive(archive.id);
        REQUIRE(entries.size() == 1);
        REQUIRE(entries[0].value.size() == 1024 * 1024);
    }

    SECTION("Special characters in keys/values") {
        auto archive = CreateArchive();

        std::vector<ArchiveEntry> special_entries;
        special_entries.push_back({"key\0with\0null", "value\0null", 1, 1});
        special_entries.push_back({"key\nwith\nnewlines", "value\n\n", 2, 2});
        special_entries.push_back({"key\twith\ttabs", "value\t\t", 3, 3});
        special_entries.push_back({std::string(1, 0xFF), std::string(1, 0xFF), 4, 4});

        for (const auto& entry : special_entries) {
            REQUIRE(AppendToArchive(archive.id, entry));
        }

        auto read_entries = ReadArchive(archive.id);
        REQUIRE(read_entries.size() == special_entries.size());

        for (size_t i = 0; i < special_entries.size(); ++i) {
            REQUIRE(read_entries[i].key == special_entries[i].key);
            REQUIRE(read_entries[i].value == special_entries[i].value);
        }
    }
}