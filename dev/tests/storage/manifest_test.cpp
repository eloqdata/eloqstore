#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <random>

#include "fixtures/test_fixtures.h"
#include "fixtures/data_generator.h"

namespace fs = std::filesystem;

class ManifestTestFixture {
public:
    ManifestTestFixture() : gen_(42), rng_(42) {
        test_dir_ = "/mnt/ramdisk/manifest_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_dir_);
        manifest_path_ = test_dir_ + "/MANIFEST";
    }

    ~ManifestTestFixture() {
        try {
            if (fs::exists(test_dir_)) {
                fs::remove_all(test_dir_);
            }
        } catch (...) {
            // Ignore cleanup errors
        }
    }

    struct ManifestEntry {
        uint64_t sequence_number;
        std::string table_name;
        uint32_t table_id;
        uint64_t file_number;
        uint64_t file_size;
        std::string min_key;
        std::string max_key;
        uint64_t entry_count;
        uint64_t timestamp;
    };

    void WriteManifest(const std::vector<ManifestEntry>& entries) {
        std::ofstream out(manifest_path_, std::ios::binary);

        // Write header
        uint32_t magic = 0x4D414E49; // "MANI"
        uint32_t version = 1;
        uint64_t entry_count = entries.size();

        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
        out.write(reinterpret_cast<const char*>(&version), sizeof(version));
        out.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

        // Write entries
        for (const auto& entry : entries) {
            out.write(reinterpret_cast<const char*>(&entry.sequence_number), sizeof(entry.sequence_number));

            uint32_t name_len = entry.table_name.size();
            out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
            out.write(entry.table_name.data(), name_len);

            out.write(reinterpret_cast<const char*>(&entry.table_id), sizeof(entry.table_id));
            out.write(reinterpret_cast<const char*>(&entry.file_number), sizeof(entry.file_number));
            out.write(reinterpret_cast<const char*>(&entry.file_size), sizeof(entry.file_size));

            uint32_t min_key_len = entry.min_key.size();
            out.write(reinterpret_cast<const char*>(&min_key_len), sizeof(min_key_len));
            out.write(entry.min_key.data(), min_key_len);

            uint32_t max_key_len = entry.max_key.size();
            out.write(reinterpret_cast<const char*>(&max_key_len), sizeof(max_key_len));
            out.write(entry.max_key.data(), max_key_len);

            out.write(reinterpret_cast<const char*>(&entry.entry_count), sizeof(entry.entry_count));
            out.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        }

        // Write checksum
        uint32_t checksum = CalculateChecksum(entries);
        out.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

        out.close();
    }

    std::vector<ManifestEntry> ReadManifest() {
        std::ifstream in(manifest_path_, std::ios::binary);
        std::vector<ManifestEntry> entries;

        if (!in) {
            return entries;
        }

        // Read header
        uint32_t magic;
        uint32_t version;
        uint64_t entry_count;

        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        in.read(reinterpret_cast<char*>(&version), sizeof(version));
        in.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));

        if (magic != 0x4D414E49 || version != 1) {
            throw std::runtime_error("Invalid manifest header");
        }

        // Read entries
        for (uint64_t i = 0; i < entry_count; ++i) {
            ManifestEntry entry;

            in.read(reinterpret_cast<char*>(&entry.sequence_number), sizeof(entry.sequence_number));

            uint32_t name_len;
            in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
            entry.table_name.resize(name_len);
            in.read(entry.table_name.data(), name_len);

            in.read(reinterpret_cast<char*>(&entry.table_id), sizeof(entry.table_id));
            in.read(reinterpret_cast<char*>(&entry.file_number), sizeof(entry.file_number));
            in.read(reinterpret_cast<char*>(&entry.file_size), sizeof(entry.file_size));

            uint32_t min_key_len;
            in.read(reinterpret_cast<char*>(&min_key_len), sizeof(min_key_len));
            entry.min_key.resize(min_key_len);
            in.read(entry.min_key.data(), min_key_len);

            uint32_t max_key_len;
            in.read(reinterpret_cast<char*>(&max_key_len), sizeof(max_key_len));
            entry.max_key.resize(max_key_len);
            in.read(entry.max_key.data(), max_key_len);

            in.read(reinterpret_cast<char*>(&entry.entry_count), sizeof(entry.entry_count));
            in.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));

            entries.push_back(entry);
        }

        // Verify checksum
        uint32_t stored_checksum;
        in.read(reinterpret_cast<char*>(&stored_checksum), sizeof(stored_checksum));

        uint32_t calculated_checksum = CalculateChecksum(entries);
        if (stored_checksum != calculated_checksum) {
            throw std::runtime_error("Manifest checksum mismatch");
        }

        return entries;
    }

    void CorruptManifest(size_t offset, size_t size) {
        std::fstream file(manifest_path_, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) return;

        file.seekp(offset);
        std::vector<char> garbage(size);
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (auto& byte : garbage) {
            byte = static_cast<char>(dis(rng));
        }

        file.write(garbage.data(), garbage.size());
        file.close();
    }

    ManifestEntry CreateTestEntry(uint64_t seq, uint64_t file_num) {
        ManifestEntry entry;
        entry.sequence_number = seq;
        entry.table_name = "test_table_" + std::to_string(seq % 10);
        entry.table_id = seq % 100;
        entry.file_number = file_num;
        std::uniform_int_distribution<uint64_t> file_size_dist(1024, 1024 * 1024);
        entry.file_size = file_size_dist(rng_);
        entry.min_key = gen_.GenerateRandomKey(10, 20);
        entry.max_key = gen_.GenerateRandomKey(10, 20);
        std::uniform_int_distribution<uint64_t> count_dist(100, 10000);
        entry.entry_count = count_dist(rng_);
        entry.timestamp = std::time(nullptr) + seq;
        return entry;
    }

protected:
    uint32_t CalculateChecksum(const std::vector<ManifestEntry>& entries) {
        uint32_t checksum = 0;
        for (const auto& entry : entries) {
            checksum ^= entry.sequence_number;
            checksum ^= entry.file_number;
            checksum ^= entry.entry_count;
        }
        return checksum;
    }

    std::string test_dir_;
    std::string manifest_path_;
    eloqstore::test::DataGenerator gen_;
    std::mt19937 rng_;
};

TEST_CASE_METHOD(ManifestTestFixture, "Manifest basic operations", "[storage][manifest]") {
    SECTION("Create and write manifest") {
        std::vector<ManifestEntry> entries;
        for (int i = 0; i < 10; ++i) {
            entries.push_back(CreateTestEntry(i, 100 + i));
        }

        WriteManifest(entries);

        REQUIRE(fs::exists(manifest_path_));
        REQUIRE(fs::file_size(manifest_path_) > 0);
    }

    SECTION("Read manifest") {
        std::vector<ManifestEntry> original;
        for (int i = 0; i < 10; ++i) {
            original.push_back(CreateTestEntry(i, 200 + i));
        }

        WriteManifest(original);

        auto loaded = ReadManifest();

        REQUIRE(loaded.size() == original.size());

        for (size_t i = 0; i < original.size(); ++i) {
            REQUIRE(loaded[i].sequence_number == original[i].sequence_number);
            REQUIRE(loaded[i].table_name == original[i].table_name);
            REQUIRE(loaded[i].file_number == original[i].file_number);
            REQUIRE(loaded[i].min_key == original[i].min_key);
            REQUIRE(loaded[i].max_key == original[i].max_key);
        }
    }

    SECTION("Empty manifest") {
        std::vector<ManifestEntry> empty;
        WriteManifest(empty);

        auto loaded = ReadManifest();
        REQUIRE(loaded.empty());
    }

    SECTION("Large manifest") {
        std::vector<ManifestEntry> large;
        for (int i = 0; i < 10000; ++i) {
            large.push_back(CreateTestEntry(i, 1000 + i));
        }

        WriteManifest(large);

        auto loaded = ReadManifest();
        REQUIRE(loaded.size() == large.size());
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "Manifest corruption handling", "[storage][manifest]") {
    std::vector<ManifestEntry> entries;
    for (int i = 0; i < 10; ++i) {
        entries.push_back(CreateTestEntry(i, 300 + i));
    }

    SECTION("Corrupted header") {
        WriteManifest(entries);

        // Corrupt magic number
        CorruptManifest(0, 4);

        REQUIRE_THROWS_AS(ReadManifest(), std::runtime_error);
    }

    SECTION("Corrupted entry data") {
        WriteManifest(entries);

        // Corrupt somewhere in the middle of entries
        size_t file_size = fs::file_size(manifest_path_);
        CorruptManifest(file_size / 2, 16);

        // Should either throw or return partial/incorrect data
        try {
            auto loaded = ReadManifest();
            // If it doesn't throw, data should be different
            REQUIRE(loaded.size() != entries.size());
        } catch (const std::exception&) {
            // Expected - corruption detected
            REQUIRE(true);
        }
    }

    SECTION("Corrupted checksum") {
        WriteManifest(entries);

        // Corrupt last 4 bytes (checksum)
        size_t file_size = fs::file_size(manifest_path_);
        CorruptManifest(file_size - 4, 4);

        REQUIRE_THROWS_AS(ReadManifest(), std::runtime_error);
    }

    SECTION("Truncated manifest") {
        WriteManifest(entries);

        // Truncate file
        {
            std::ofstream out(manifest_path_, std::ios::binary | std::ios::trunc);
            out.write("partial", 7);
        }

        REQUIRE_THROWS(ReadManifest());
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "Manifest versioning", "[storage][manifest]") {
    SECTION("Version upgrade simulation") {
        // Write v1 manifest
        std::vector<ManifestEntry> v1_entries;
        for (int i = 0; i < 5; ++i) {
            v1_entries.push_back(CreateTestEntry(i, 400 + i));
        }
        WriteManifest(v1_entries);

        // Read and verify v1
        auto loaded_v1 = ReadManifest();
        REQUIRE(loaded_v1.size() == v1_entries.size());

        // Simulate upgrade by adding more entries
        std::vector<ManifestEntry> v2_entries = v1_entries;
        for (int i = 5; i < 10; ++i) {
            v2_entries.push_back(CreateTestEntry(i, 400 + i));
        }

        // Backup old manifest
        std::string backup_path = manifest_path_ + ".backup";
        fs::copy_file(manifest_path_, backup_path);

        // Write new version
        WriteManifest(v2_entries);

        // Verify upgrade
        auto loaded_v2 = ReadManifest();
        REQUIRE(loaded_v2.size() == v2_entries.size());

        // Verify backward compatibility (restore and read old version)
        fs::remove(manifest_path_);
        fs::rename(backup_path, manifest_path_);

        auto restored = ReadManifest();
        REQUIRE(restored.size() == v1_entries.size());
    }

    SECTION("Concurrent manifest updates") {
        // Simulate multiple writers (sequentially for test safety)
        std::vector<ManifestEntry> base_entries;
        for (int i = 0; i < 5; ++i) {
            base_entries.push_back(CreateTestEntry(i, 500 + i));
        }

        // Writer 1
        auto writer1_entries = base_entries;
        writer1_entries.push_back(CreateTestEntry(100, 600));
        WriteManifest(writer1_entries);

        // Writer 2 (overwrites)
        auto writer2_entries = base_entries;
        writer2_entries.push_back(CreateTestEntry(200, 700));
        WriteManifest(writer2_entries);

        // Final state should be from writer 2
        auto final_state = ReadManifest();
        REQUIRE(final_state.size() == writer2_entries.size());
        REQUIRE(final_state.back().sequence_number == 200);
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "Manifest recovery scenarios", "[storage][manifest]") {
    SECTION("Recovery after incomplete write") {
        std::vector<ManifestEntry> entries;
        for (int i = 0; i < 10; ++i) {
            entries.push_back(CreateTestEntry(i, 800 + i));
        }

        // Write complete manifest
        WriteManifest(entries);
        std::string good_manifest = manifest_path_ + ".good";
        fs::copy_file(manifest_path_, good_manifest);

        // Simulate incomplete write (truncate)
        {
            std::ifstream in(manifest_path_, std::ios::binary);
            std::vector<char> data((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
            in.close();

            std::ofstream out(manifest_path_, std::ios::binary | std::ios::trunc);
            out.write(data.data(), data.size() / 2); // Write only half
        }

        // Try to read incomplete manifest
        bool read_failed = false;
        try {
            ReadManifest();
        } catch (...) {
            read_failed = true;
        }
        REQUIRE(read_failed);

        // Recovery: restore from backup
        fs::remove(manifest_path_);
        fs::rename(good_manifest, manifest_path_);

        // Should read successfully after recovery
        auto recovered = ReadManifest();
        REQUIRE(recovered.size() == entries.size());
    }

    SECTION("Manifest compaction") {
        // Simulate manifest growing over time
        std::vector<ManifestEntry> all_entries;

        // Add entries in batches
        for (int batch = 0; batch < 10; ++batch) {
            for (int i = 0; i < 100; ++i) {
                all_entries.push_back(CreateTestEntry(batch * 100 + i, 1000 + batch * 100 + i));
            }
            WriteManifest(all_entries);
        }

        // Manifest should have 1000 entries
        auto before_compact = ReadManifest();
        REQUIRE(before_compact.size() == 1000);

        // Simulate compaction: keep only recent entries
        std::vector<ManifestEntry> compacted;
        for (size_t i = 800; i < all_entries.size(); ++i) {
            compacted.push_back(all_entries[i]);
        }

        WriteManifest(compacted);

        // Verify compacted manifest
        auto after_compact = ReadManifest();
        REQUIRE(after_compact.size() == 200);
        REQUIRE(after_compact.front().sequence_number == 800);
    }
}