#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <random>

#include "utils.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Utils_Hash_Functions", "[utils][unit]") {
    SECTION("Hash consistency") {
        std::string data = "test data for hashing";

        uint32_t hash1 = Hash(data.data(), data.size(), 0);
        uint32_t hash2 = Hash(data.data(), data.size(), 0);

        REQUIRE(hash1 == hash2);  // Same input should produce same hash
    }

    SECTION("Different seeds produce different hashes") {
        std::string data = "test data";

        uint32_t hash_seed0 = Hash(data.data(), data.size(), 0);
        uint32_t hash_seed1 = Hash(data.data(), data.size(), 1);
        uint32_t hash_seed42 = Hash(data.data(), data.size(), 42);

        REQUIRE(hash_seed0 != hash_seed1);
        REQUIRE(hash_seed0 != hash_seed42);
        REQUIRE(hash_seed1 != hash_seed42);
    }

    SECTION("Empty data hash") {
        uint32_t empty_hash = Hash("", 0, 0);
        REQUIRE(empty_hash != 0);  // Should still produce a hash value

        // Empty with different seeds
        uint32_t empty_hash1 = Hash("", 0, 1);
        REQUIRE(empty_hash != empty_hash1);
    }

    SECTION("Single byte variations") {
        // Changing a single bit should change the hash
        std::string data1 = "aaaaaaaa";
        std::string data2 = "aaaaaaab";

        uint32_t hash1 = Hash(data1.data(), data1.size(), 0);
        uint32_t hash2 = Hash(data2.data(), data2.size(), 0);

        REQUIRE(hash1 != hash2);
    }

    SECTION("Hash distribution") {
        DataGenerator gen(42);
        const int num_keys = 10000;
        std::set<uint32_t> hashes;

        for (int i = 0; i < num_keys; ++i) {
            std::string key = gen.GenerateRandomKey(10, 20);
            uint32_t hash = Hash(key.data(), key.size(), 0);
            hashes.insert(hash);
        }

        // Should have good distribution (few collisions)
        double collision_rate = 1.0 - (double(hashes.size()) / num_keys);
        REQUIRE(collision_rate < 0.01);  // Less than 1% collisions
    }
}

TEST_CASE("Utils_CRC32_Functions", "[utils][unit]") {
    SECTION("CRC32 basic") {
        std::string data = "The quick brown fox jumps over the lazy dog";
        uint32_t crc = CRC32(data.data(), data.size());

        // CRC should be deterministic
        uint32_t crc2 = CRC32(data.data(), data.size());
        REQUIRE(crc == crc2);
    }

    SECTION("CRC32 with seed") {
        std::string data = "test data";
        uint32_t crc_noseed = CRC32(data.data(), data.size());
        uint32_t crc_seed0 = CRC32(data.data(), data.size(), 0);
        uint32_t crc_seed1 = CRC32(data.data(), data.size(), 1);

        REQUIRE(crc_noseed == crc_seed0);
        REQUIRE(crc_seed0 != crc_seed1);
    }

    SECTION("CRC32 incremental") {
        std::string data = "abcdefghijklmnopqrstuvwxyz";

        // Compute CRC all at once
        uint32_t crc_full = CRC32(data.data(), data.size());

        // Compute CRC incrementally
        uint32_t crc_incremental = 0;
        for (size_t i = 0; i < data.size(); i += 5) {
            size_t chunk_size = std::min(size_t(5), data.size() - i);
            crc_incremental = CRC32(data.data() + i, chunk_size, crc_incremental);
        }

        REQUIRE(crc_full == crc_incremental);
    }

    SECTION("CRC32 error detection") {
        std::string data = std::string(1000, 'x');
        uint32_t original_crc = CRC32(data.data(), data.size());

        // Single bit flip
        data[500] = 'y';
        uint32_t modified_crc = CRC32(data.data(), data.size());

        REQUIRE(original_crc != modified_crc);

        // Multiple bit flips
        data[100] = 'z';
        data[900] = 'w';
        uint32_t multi_modified_crc = CRC32(data.data(), data.size());

        REQUIRE(multi_modified_crc != original_crc);
        REQUIRE(multi_modified_crc != modified_crc);
    }
}

TEST_CASE("Utils_Time_Functions", "[utils][unit]") {
    SECTION("Monotonic time") {
        uint64_t time1 = GetMonotonicTime();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        uint64_t time2 = GetMonotonicTime();

        REQUIRE(time2 > time1);

        // Should be monotonically increasing
        uint64_t prev = GetMonotonicTime();
        for (int i = 0; i < 100; ++i) {
            uint64_t curr = GetMonotonicTime();
            REQUIRE(curr >= prev);
            prev = curr;
        }
    }

    SECTION("Time resolution") {
        // Check that we can measure small time differences
        std::vector<uint64_t> times;
        for (int i = 0; i < 1000; ++i) {
            times.push_back(GetMonotonicTime());
        }

        // Should see some different values
        std::set<uint64_t> unique_times(times.begin(), times.end());
        REQUIRE(unique_times.size() > 1);
    }

    SECTION("Sleep accuracy") {
        uint64_t start = GetMonotonicTime();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        uint64_t end = GetMonotonicTime();

        uint64_t elapsed = end - start;
        // Should be approximately 100ms (allow 10% tolerance)
        REQUIRE(elapsed >= 90000);  // Assuming microsecond resolution
        REQUIRE(elapsed <= 150000);
    }
}

TEST_CASE("Utils_Random_Functions", "[utils][unit]") {
    SECTION("Random number generation") {
        std::set<uint32_t> values;

        for (int i = 0; i < 1000; ++i) {
            values.insert(Random32());
        }

        // Should generate different values
        REQUIRE(values.size() > 900);  // Allow for some duplicates
    }

    SECTION("Random range") {
        const uint32_t max = 100;

        for (int i = 0; i < 1000; ++i) {
            uint32_t val = Random32() % max;
            REQUIRE(val < max);
        }
    }

    SECTION("Random distribution") {
        const int buckets = 10;
        std::vector<int> counts(buckets, 0);
        const int samples = 10000;

        for (int i = 0; i < samples; ++i) {
            uint32_t val = Random32() % buckets;
            counts[val]++;
        }

        // Check for reasonable distribution
        int expected = samples / buckets;
        for (int count : counts) {
            // Allow 20% deviation
            REQUIRE(count > expected * 0.8);
            REQUIRE(count < expected * 1.2);
        }
    }
}

TEST_CASE("Utils_Alignment_Functions", "[utils][unit]") {
    SECTION("Align to power of 2") {
        REQUIRE(AlignUp(0, 4) == 0);
        REQUIRE(AlignUp(1, 4) == 4);
        REQUIRE(AlignUp(3, 4) == 4);
        REQUIRE(AlignUp(4, 4) == 4);
        REQUIRE(AlignUp(5, 4) == 8);

        REQUIRE(AlignUp(1000, 64) == 1024);
        REQUIRE(AlignUp(1024, 64) == 1024);
    }

    SECTION("Align down") {
        REQUIRE(AlignDown(0, 4) == 0);
        REQUIRE(AlignDown(1, 4) == 0);
        REQUIRE(AlignDown(3, 4) == 0);
        REQUIRE(AlignDown(4, 4) == 4);
        REQUIRE(AlignDown(7, 4) == 4);
        REQUIRE(AlignDown(8, 4) == 8);

        REQUIRE(AlignDown(1000, 64) == 960);
        REQUIRE(AlignDown(1024, 64) == 1024);
    }

    SECTION("Is aligned check") {
        REQUIRE(IsAligned(0, 4) == true);
        REQUIRE(IsAligned(4, 4) == true);
        REQUIRE(IsAligned(8, 4) == true);
        REQUIRE(IsAligned(1024, 512) == true);

        REQUIRE(IsAligned(1, 4) == false);
        REQUIRE(IsAligned(7, 4) == false);
        REQUIRE(IsAligned(1000, 64) == false);
    }

    SECTION("Various alignments") {
        std::vector<size_t> alignments = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096};

        for (size_t align : alignments) {
            REQUIRE(IsAligned(0, align));
            REQUIRE(IsAligned(align, align));
            REQUIRE(IsAligned(align * 2, align));

            REQUIRE(AlignUp(1, align) == align);
            REQUIRE(AlignDown(align - 1, align) == 0);
        }
    }
}

TEST_CASE("Utils_Power_Of_Two", "[utils][unit]") {
    SECTION("Is power of two") {
        REQUIRE(IsPowerOfTwo(1) == true);
        REQUIRE(IsPowerOfTwo(2) == true);
        REQUIRE(IsPowerOfTwo(4) == true);
        REQUIRE(IsPowerOfTwo(1024) == true);
        REQUIRE(IsPowerOfTwo(1ULL << 32) == true);

        REQUIRE(IsPowerOfTwo(0) == false);
        REQUIRE(IsPowerOfTwo(3) == false);
        REQUIRE(IsPowerOfTwo(5) == false);
        REQUIRE(IsPowerOfTwo(1000) == false);
    }

    SECTION("Next power of two") {
        REQUIRE(NextPowerOfTwo(0) == 1);
        REQUIRE(NextPowerOfTwo(1) == 1);
        REQUIRE(NextPowerOfTwo(2) == 2);
        REQUIRE(NextPowerOfTwo(3) == 4);
        REQUIRE(NextPowerOfTwo(5) == 8);
        REQUIRE(NextPowerOfTwo(1000) == 1024);
        REQUIRE(NextPowerOfTwo(1024) == 1024);
        REQUIRE(NextPowerOfTwo(1025) == 2048);
    }

    SECTION("Log2 of power of two") {
        REQUIRE(Log2(1) == 0);
        REQUIRE(Log2(2) == 1);
        REQUIRE(Log2(4) == 2);
        REQUIRE(Log2(8) == 3);
        REQUIRE(Log2(1024) == 10);
        REQUIRE(Log2(1ULL << 32) == 32);
    }
}

TEST_CASE("Utils_String_Functions", "[utils][unit]") {
    SECTION("String trim") {
        REQUIRE(Trim("  hello  ") == "hello");
        REQUIRE(Trim("\t\nworld\r\n") == "world");
        REQUIRE(Trim("   ") == "");
        REQUIRE(Trim("") == "");
        REQUIRE(Trim("no_trim") == "no_trim");
    }

    SECTION("String split") {
        std::vector<std::string> parts;

        parts = Split("a,b,c", ',');
        REQUIRE(parts.size() == 3);
        REQUIRE(parts[0] == "a");
        REQUIRE(parts[1] == "b");
        REQUIRE(parts[2] == "c");

        parts = Split("one::two::three", "::");
        REQUIRE(parts.size() == 3);

        parts = Split("single", ',');
        REQUIRE(parts.size() == 1);
        REQUIRE(parts[0] == "single");

        parts = Split("", ',');
        REQUIRE(parts.size() == 0);
    }

    SECTION("String join") {
        std::vector<std::string> parts = {"a", "b", "c"};
        REQUIRE(Join(parts, ",") == "a,b,c");
        REQUIRE(Join(parts, "::") == "a::b::c");

        parts = {"single"};
        REQUIRE(Join(parts, ",") == "single");

        parts = {};
        REQUIRE(Join(parts, ",") == "");
    }

    SECTION("String replace") {
        REQUIRE(Replace("hello world", "world", "universe") == "hello universe");
        REQUIRE(Replace("aaa", "a", "b") == "bbb");
        REQUIRE(Replace("test", "not_found", "x") == "test");
        REQUIRE(Replace("", "a", "b") == "");
    }
}

TEST_CASE("Utils_EdgeCases", "[utils][unit][edge-case]") {
    SECTION("Hash with null bytes") {
        std::string data_with_null = "abc\0def";
        uint32_t hash1 = Hash(data_with_null.c_str(), 3, 0);  // Only "abc"
        uint32_t hash2 = Hash(data_with_null.c_str(), 7, 0);  // Full string with null

        REQUIRE(hash1 != hash2);
    }

    SECTION("CRC with all zeros") {
        std::vector<char> zeros(1000, 0);
        uint32_t crc = CRC32(zeros.data(), zeros.size());
        REQUIRE(crc != 0);  // CRC of zeros should not be zero
    }

    SECTION("CRC with all ones") {
        std::vector<char> ones(1000, 0xFF);
        uint32_t crc = CRC32(ones.data(), ones.size());
        REQUIRE(crc != 0xFFFFFFFF);  // CRC of all ones should not be all ones
    }

    SECTION("Alignment with zero") {
        REQUIRE(AlignUp(0, 1) == 0);
        REQUIRE(AlignDown(0, 1) == 0);
        REQUIRE(IsAligned(0, 1) == true);
    }

    SECTION("Maximum alignment") {
        size_t max_align = 1ULL << 30;  // 1GB alignment
        size_t value = max_align + 1;

        REQUIRE(AlignUp(value, max_align) == max_align * 2);
        REQUIRE(AlignDown(value, max_align) == max_align);
        REQUIRE(!IsAligned(value, max_align));
    }
}

TEST_CASE("Utils_Performance", "[utils][benchmark]") {
    DataGenerator gen(42);

    SECTION("Hash performance") {
        std::string data = gen.GenerateValue(1000);
        Timer timer;

        const int iterations = 1000000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            volatile uint32_t hash = Hash(data.data(), data.size(), i);
            (void)hash;
        }

        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "Hash performance: " << ops_per_sec << " ops/sec";

        REQUIRE(ops_per_sec > 100000);  // Should handle >100K ops/sec
    }

    SECTION("CRC32 performance") {
        std::string data = gen.GenerateValue(4096);
        Timer timer;

        const int iterations = 100000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            volatile uint32_t crc = CRC32(data.data(), data.size());
            (void)crc;
        }

        timer.Stop();

        double mb_per_sec = (iterations * data.size()) / (timer.ElapsedSeconds() * 1024 * 1024);
        LOG(INFO) << "CRC32 throughput: " << mb_per_sec << " MB/sec";

        REQUIRE(mb_per_sec > 100);  // Should handle >100 MB/sec
    }
}