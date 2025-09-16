#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <random>
#include <limits>

#include "coding.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Coding_EncodeFixed32_BoundaryValues", "[coding][unit][edge-case]") {
    SECTION("Zero value") {
        char dst[4];
        EncodeFixed32(dst, 0);

        // Verify all bytes are zero
        for (int i = 0; i < 4; i++) {
            REQUIRE(static_cast<uint8_t>(dst[i]) == 0);
        }
    }

    SECTION("Maximum value") {
        char dst[4];
        EncodeFixed32(dst, UINT32_MAX);

        // Verify all bytes are 0xFF
        for (char c : dst) {
            REQUIRE(static_cast<uint8_t>(c) == 0xFF);
        }
    }

    SECTION("Power of two values") {
        for (uint32_t i = 0; i < 32; ++i) {
            char dst[4];
            uint32_t value = 1U << i;
            EncodeFixed32(dst, value);

            // Decode and verify
            uint32_t decoded = DecodeFixed32(dst);
            REQUIRE(decoded == value);
        }
    }

    SECTION("Alternating bit patterns") {
        uint32_t pattern1 = 0xAAAAAAAA;
        uint32_t pattern2 = 0x55555555;

        char dst1[4];
        EncodeFixed32(dst1, pattern1);
        REQUIRE(DecodeFixed32(dst1) == pattern1);

        char dst2[4];
        EncodeFixed32(dst2, pattern2);
        REQUIRE(DecodeFixed32(dst2) == pattern2);
    }
}

TEST_CASE("Coding_EncodeFixed64_BoundaryValues", "[coding][unit][edge-case]") {
    SECTION("Zero value") {
        std::string dst;
        PutFixed64(&dst, 0);
        REQUIRE(dst.size() == 8);

        for (char c : dst) {
            REQUIRE(static_cast<uint8_t>(c) == 0);
        }
    }

    SECTION("Maximum value") {
        std::string dst;
        PutFixed64(&dst, UINT64_MAX);
        REQUIRE(dst.size() == 8);

        for (char c : dst) {
            REQUIRE(static_cast<uint8_t>(c) == 0xFF);
        }
    }

    SECTION("32-bit boundary crossing") {
        std::string dst;
        uint64_t value = (1ULL << 32) - 1; // Max 32-bit value
        PutFixed64(&dst, value);
        REQUIRE(DecodeFixed64(dst.data()) == value);

        dst.clear();
        value = 1ULL << 32; // Just over 32-bit
        PutFixed64(&dst, value);
        REQUIRE(DecodeFixed64(dst.data()) == value);
    }

    SECTION("Power of two values") {
        std::string dst;
        for (uint32_t i = 0; i < 64; ++i) {
            dst.clear();
            uint64_t value = 1ULL << i;
            PutFixed64(&dst, value);
            REQUIRE(dst.size() == 8);

            uint64_t decoded = DecodeFixed64(dst.data());
            REQUIRE(decoded == value);
        }
    }
}

TEST_CASE("Coding_Varint32_Encoding", "[coding][unit]") {
    SECTION("Single byte values (0-127)") {
        std::string dst;
        for (uint32_t value = 0; value <= 127; ++value) {
            dst.clear();
            PutVarint32(&dst, value);
            REQUIRE(dst.size() == 1);

            const char* ptr = dst.data();
            const char* limit = ptr + dst.size();
            uint32_t decoded;
            ptr = GetVarint32Ptr(ptr, limit, &decoded);
            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
        }
    }

    SECTION("Two byte values (128-16383)") {
        std::string dst;
        for (uint32_t value = 128; value <= 16383; value += 100) {
            dst.clear();
            PutVarint32(&dst, value);
            REQUIRE(dst.size() == 2);

            const char* ptr = dst.data();
            const char* limit = ptr + dst.size();
            uint32_t decoded;
            ptr = GetVarint32Ptr(ptr, limit, &decoded);
            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
        }
    }

    SECTION("Maximum varint32 value") {
        std::string dst;
        PutVarint32(&dst, UINT32_MAX);
        REQUIRE(dst.size() == 5); // Max varint32 is 5 bytes

        const char* ptr = dst.data();
        const char* limit = ptr + dst.size();
        uint32_t decoded;
        ptr = GetVarint32Ptr(ptr, limit, &decoded);
        REQUIRE(ptr != nullptr);
        REQUIRE(decoded == UINT32_MAX);
    }

    SECTION("Boundary values") {
        std::vector<uint32_t> boundaries = {
            0, 1, 127, 128, 255, 256,
            16383, 16384, 65535, 65536,
            (1U << 20) - 1, 1U << 20,
            (1U << 28) - 1, 1U << 28,
            UINT32_MAX - 1, UINT32_MAX
        };

        for (uint32_t value : boundaries) {
            std::string dst;
            PutVarint32(&dst, value);

            const char* ptr = dst.data();
            const char* limit = ptr + dst.size();
            uint32_t decoded;
            ptr = GetVarint32Ptr(ptr, limit, &decoded);
            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
        }
    }
}

TEST_CASE("Coding_Varint64_Encoding", "[coding][unit]") {
    SECTION("Single byte values") {
        std::string dst;
        for (uint64_t value = 0; value <= 127; ++value) {
            dst.clear();
            PutVarint64(&dst, value);
            REQUIRE(dst.size() == 1);

            const char* ptr = dst.data();
            const char* limit = ptr + dst.size();
            uint64_t decoded;
            ptr = GetVarint64Ptr(ptr, limit, &decoded);
            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
        }
    }

    SECTION("Maximum varint64 value") {
        std::string dst;
        PutVarint64(&dst, UINT64_MAX);
        REQUIRE(dst.size() == 10); // Max varint64 is 10 bytes

        const char* ptr = dst.data();
        const char* limit = ptr + dst.size();
        uint64_t decoded;
        ptr = GetVarint64Ptr(ptr, limit, &decoded);
        REQUIRE(ptr != nullptr);
        REQUIRE(decoded == UINT64_MAX);
    }

    SECTION("64-bit specific values") {
        std::vector<uint64_t> values = {
            (1ULL << 32) - 1,  // Max 32-bit
            1ULL << 32,        // Just over 32-bit
            (1ULL << 40) - 1,
            1ULL << 40,
            (1ULL << 48) - 1,
            1ULL << 48,
            (1ULL << 56) - 1,
            1ULL << 56,
            UINT64_MAX
        };

        for (uint64_t value : values) {
            std::string dst;
            PutVarint64(&dst, value);

            const char* ptr = dst.data();
            const char* limit = ptr + dst.size();
            uint64_t decoded;
            ptr = GetVarint64Ptr(ptr, limit, &decoded);
            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
        }
    }
}

TEST_CASE("Coding_LengthPrefixedSlice", "[coding][unit]") {
    SECTION("Empty slice") {
        std::string dst;
        PutLengthPrefixedSlice(&dst, "");

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted.empty());
        REQUIRE(extracted.size() == 0);
    }

    SECTION("Single character") {
        std::string dst;
        PutLengthPrefixedSlice(&dst, "a");

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted == "a");
    }

    SECTION("Maximum single-byte length (127 chars)") {
        std::string data(127, 'x');
        std::string dst;
        PutLengthPrefixedSlice(&dst, data);

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted == data);
    }

    SECTION("Two-byte length (128 chars)") {
        std::string data(128, 'y');
        std::string dst;
        PutLengthPrefixedSlice(&dst, data);

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted == data);
    }

    SECTION("Large data") {
        std::string data(10000, 'z');
        std::string dst;
        PutLengthPrefixedSlice(&dst, data);

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted == data);
    }

    SECTION("Binary data with null bytes") {
        std::string data;
        for (int i = 0; i < 256; ++i) {
            data += static_cast<char>(i);
        }

        std::string dst;
        PutLengthPrefixedSlice(&dst, data);

        std::string_view input(dst);
        std::string_view extracted;
        REQUIRE(GetLengthPrefixedSlice(&input, &extracted));
        REQUIRE(extracted == data);
    }

    SECTION("Multiple slices in sequence") {
        std::string dst;
        std::vector<std::string> test_data = {"first", "second", "", "third"};

        for (const auto& s : test_data) {
            PutLengthPrefixedSlice(&dst, s);
        }

        const char* ptr = dst.data();
        const char* limit = dst.data() + dst.size();

        for (const auto& expected : test_data) {
            std::string_view extracted;
            REQUIRE(GetLengthPrefixedSlice(&ptr, limit, &extracted));
            REQUIRE(extracted == expected);
        }

        REQUIRE(ptr == limit); // All data consumed
    }
}

TEST_CASE("Coding_ErrorCases", "[coding][unit][error]") {
    SECTION("GetVarint32 with truncated data") {
        std::string dst;
        PutVarint32(&dst, 1000000);

        // Try to decode with truncated buffer
        for (size_t len = 0; len < dst.size(); ++len) {
            const char* ptr = dst.data();
            const char* limit = ptr + len;
            uint32_t value;
            const char* result = GetVarint32Ptr(ptr, limit, &value);
            REQUIRE(result == nullptr);
        }
    }

    SECTION("GetVarint64 with truncated data") {
        std::string dst;
        PutVarint64(&dst, UINT64_MAX);

        // Try to decode with truncated buffer
        for (size_t len = 0; len < dst.size(); ++len) {
            const char* ptr = dst.data();
            const char* limit = ptr + len;
            uint64_t value;
            const char* result = GetVarint64Ptr(ptr, limit, &value);
            REQUIRE(result == nullptr);
        }
    }

    SECTION("GetLengthPrefixedSlice with truncated length") {
        std::string dst;
        PutVarint32(&dst, 1000); // Length header
        // Don't add the actual data

        std::string_view extracted;
        const char* ptr = dst.data();
        bool result = GetLengthPrefixedSlice(&ptr, dst.data() + dst.size(), &extracted);
        REQUIRE(result == false);
    }

    SECTION("GetLengthPrefixedSlice with partial data") {
        std::string data(100, 'x');
        std::string dst;
        PutLengthPrefixedSlice(&dst, data);

        // Try to extract with truncated buffer
        for (size_t len = 1; len < dst.size(); ++len) {
            std::string_view extracted;
            const char* ptr = dst.data();
            bool result = GetLengthPrefixedSlice(&ptr, dst.data() + len, &extracted);
            if (len < dst.size()) {
                REQUIRE(result == false);
            }
        }
    }
}

TEST_CASE("Coding_StressTest", "[coding][stress]") {
    DataGenerator gen(42);

    SECTION("Random varint32 encoding/decoding") {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);

        for (int i = 0; i < 10000; ++i) {
            uint32_t value = dist(rng);
            std::string encoded;
            PutVarint32(&encoded, value);

            const char* ptr = encoded.data();
            const char* limit = ptr + encoded.size();
            uint32_t decoded;
            ptr = GetVarint32Ptr(ptr, limit, &decoded);

            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
            REQUIRE(ptr == limit); // All bytes consumed
        }
    }

    SECTION("Random varint64 encoding/decoding") {
        std::random_device rd;
        std::mt19937_64 rng(rd());
        std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

        for (int i = 0; i < 10000; ++i) {
            uint64_t value = dist(rng);
            std::string encoded;
            PutVarint64(&encoded, value);

            const char* ptr = encoded.data();
            const char* limit = ptr + encoded.size();
            uint64_t decoded;
            ptr = GetVarint64Ptr(ptr, limit, &decoded);

            REQUIRE(ptr != nullptr);
            REQUIRE(decoded == value);
            REQUIRE(ptr == limit);
        }
    }

    SECTION("Mixed encoding in single buffer") {
        std::string buffer;
        std::vector<uint32_t> values32;
        std::vector<uint64_t> values64;
        std::vector<std::string> strings;

        // Generate random test data
        std::random_device rd;
        std::mt19937 rng(rd());

        for (int i = 0; i < 100; ++i) {
            uint32_t v32 = std::uniform_int_distribution<uint32_t>()(rng);
            uint64_t v64 = std::uniform_int_distribution<uint64_t>()(rng);
            std::string str = gen.GenerateRandomValue(0, 100);

            values32.push_back(v32);
            values64.push_back(v64);
            strings.push_back(str);

            // Encode everything
            PutFixed32(&buffer, v32);
            PutFixed64(&buffer, v64);
            PutVarint32(&buffer, v32);
            PutVarint64(&buffer, v64);
            PutLengthPrefixedSlice(&buffer, str);
        }

        // Decode and verify
        const char* ptr = buffer.data();
        const char* limit = buffer.data() + buffer.size();

        for (size_t i = 0; i < values32.size(); ++i) {
            // Fixed32
            REQUIRE(ptr + 4 <= limit);
            uint32_t f32 = DecodeFixed32(ptr);
            REQUIRE(f32 == values32[i]);
            ptr += 4;

            // Fixed64
            REQUIRE(ptr + 8 <= limit);
            uint64_t f64 = DecodeFixed64(ptr);
            REQUIRE(f64 == values64[i]);
            ptr += 8;

            // Varint32
            uint32_t v32;
            ptr = GetVarint32Ptr(ptr, limit, &v32);
            REQUIRE(ptr != nullptr);
            REQUIRE(v32 == values32[i]);

            // Varint64
            uint64_t v64;
            ptr = GetVarint64Ptr(ptr, limit, &v64);
            REQUIRE(ptr != nullptr);
            REQUIRE(v64 == values64[i]);

            // String
            std::string_view input(ptr, limit - ptr);
            std::string_view slice;
            REQUIRE(GetLengthPrefixedSlice(&input, &slice));
            REQUIRE(slice == strings[i]);
            ptr = input.data();  // Update ptr to new position
        }

        REQUIRE(ptr == limit); // All data consumed
    }
}

TEST_CASE("Coding_Performance", "[coding][benchmark]") {
    const int iterations = 100000;

    SECTION("Varint32 encoding performance") {
        Timer timer;
        std::string buffer;
        buffer.reserve(iterations * 5);

        timer.Start();
        for (int i = 0; i < iterations; ++i) {
            PutVarint32(&buffer, i);
        }
        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "Varint32 encoding: " << ops_per_sec << " ops/sec";

        // Performance assertion - should encode at least 1M ops/sec
        REQUIRE(ops_per_sec > 1000000);
    }

    SECTION("Varint32 decoding performance") {
        // Prepare encoded data
        std::string buffer;
        for (int i = 0; i < iterations; ++i) {
            PutVarint32(&buffer, i);
        }

        Timer timer;
        const char* ptr = buffer.data();
        const char* limit = buffer.data() + buffer.size();

        timer.Start();
        for (int i = 0; i < iterations; ++i) {
            uint32_t value;
            ptr = GetVarint32Ptr(ptr, limit, &value);
            if (ptr == limit) ptr = buffer.data(); // Wrap around
        }
        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        LOG(INFO) << "Varint32 decoding: " << ops_per_sec << " ops/sec";

        // Performance assertion
        REQUIRE(ops_per_sec > 1000000);
    }
}