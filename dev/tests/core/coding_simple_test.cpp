#include <catch2/catch_test_macros.hpp>
#include "coding.h"
#include <string>
#include <cstring>

using namespace eloqstore;

TEST_CASE("Coding_Fixed_Encoding", "[coding][unit]") {
    SECTION("Fixed32 roundtrip") {
        char buffer[4];
        uint32_t test_values[] = {0, 1, 127, 128, 255, 256, 12345, UINT32_MAX};

        for (uint32_t value : test_values) {
            EncodeFixed32(buffer, value);
            uint32_t decoded = DecodeFixed32(buffer);
            REQUIRE(decoded == value);
        }
    }

    SECTION("Fixed64 roundtrip") {
        char buffer[8];
        uint64_t test_values[] = {0, 1, 127, 128, 255, 256, 12345, UINT32_MAX, UINT64_MAX};

        for (uint64_t value : test_values) {
            EncodeFixed64(buffer, value);
            uint64_t decoded = DecodeFixed64(buffer);
            REQUIRE(decoded == value);
        }
    }

    SECTION("PutFixed32 to string") {
        std::string dst;
        uint32_t value = 12345;
        PutFixed32(&dst, value);
        REQUIRE(dst.size() == 4);
        REQUIRE(DecodeFixed32(dst.data()) == value);
    }

    SECTION("PutFixed64 to string") {
        std::string dst;
        uint64_t value = 1234567890123ULL;
        PutFixed64(&dst, value);
        REQUIRE(dst.size() == 8);
        REQUIRE(DecodeFixed64(dst.data()) == value);
    }
}

TEST_CASE("Coding_Varint_Encoding", "[coding][unit]") {
    SECTION("Varint32 roundtrip") {
        uint32_t test_values[] = {0, 1, 127, 128, 255, 256, 16383, 16384, UINT32_MAX};

        for (uint32_t value : test_values) {
            std::string dst;
            PutVarint32(&dst, value);

            std::string_view input(dst);
            uint32_t decoded = 0;
            REQUIRE(GetVarint32(&input, &decoded));
            REQUIRE(decoded == value);
            REQUIRE(input.empty()); // All consumed
        }
    }

    SECTION("Varint64 roundtrip") {
        uint64_t test_values[] = {0, 1, 127, 128, 255, 256, 16383, 16384,
                                  UINT32_MAX, 1ULL << 32, UINT64_MAX};

        for (uint64_t value : test_values) {
            std::string dst;
            PutVarint64(&dst, value);

            std::string_view input(dst);
            uint64_t decoded = 0;
            REQUIRE(GetVarint64(&input, &decoded));
            REQUIRE(decoded == value);
            REQUIRE(input.empty()); // All consumed
        }
    }
}

TEST_CASE("Coding_LengthPrefixed", "[coding][unit]") {
    SECTION("Empty string") {
        std::string dst;
        PutLengthPrefixedSlice(&dst, "");

        std::string_view input(dst);
        std::string_view result;
        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result.empty());
        REQUIRE(input.empty());
    }

    SECTION("Small string") {
        std::string dst;
        PutLengthPrefixedSlice(&dst, "hello");

        std::string_view input(dst);
        std::string_view result;
        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result == "hello");
        REQUIRE(input.empty());
    }

    SECTION("Large string") {
        std::string large_str(1000, 'x');
        std::string dst;
        PutLengthPrefixedSlice(&dst, large_str);

        std::string_view input(dst);
        std::string_view result;
        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result == large_str);
        REQUIRE(input.empty());
    }

    SECTION("Multiple strings") {
        std::string dst;
        PutLengthPrefixedSlice(&dst, "first");
        PutLengthPrefixedSlice(&dst, "second");
        PutLengthPrefixedSlice(&dst, "third");

        std::string_view input(dst);
        std::string_view result;

        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result == "first");

        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result == "second");

        REQUIRE(GetLengthPrefixedSlice(&input, &result));
        REQUIRE(result == "third");

        REQUIRE(input.empty());
    }
}

TEST_CASE("Coding_Mixed_Operations", "[coding][unit]") {
    SECTION("Complex encoding and decoding") {
        std::string buffer;

        // Encode various types
        PutFixed32(&buffer, 42);
        PutVarint64(&buffer, 1234567890ULL);
        PutLengthPrefixedSlice(&buffer, "test string");
        PutFixed64(&buffer, UINT64_MAX);

        // Decode
        const char* ptr = buffer.data();
        REQUIRE(DecodeFixed32(ptr) == 42);
        ptr += 4;

        std::string_view remaining(ptr, buffer.size() - 4);
        uint64_t varint_val = 0;
        REQUIRE(GetVarint64(&remaining, &varint_val));
        REQUIRE(varint_val == 1234567890ULL);

        std::string_view str_val;
        REQUIRE(GetLengthPrefixedSlice(&remaining, &str_val));
        REQUIRE(str_val == "test string");

        REQUIRE(remaining.size() == 8);
        REQUIRE(DecodeFixed64(remaining.data()) == UINT64_MAX);
    }
}