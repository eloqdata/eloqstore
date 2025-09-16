#include <catch2/catch_test_macros.hpp>
#include "coding.h"

using namespace eloqstore;

TEST_CASE("Simple_Encoding_Test", "[coding][smoke]") {
    SECTION("Fixed32 encoding") {
        char buffer[4];
        EncodeFixed32(buffer, 12345);
        uint32_t decoded = DecodeFixed32(buffer);
        REQUIRE(decoded == 12345);
    }

    SECTION("Fixed64 encoding") {
        char buffer[8];
        EncodeFixed64(buffer, 123456789);
        uint64_t decoded = DecodeFixed64(buffer);
        REQUIRE(decoded == 123456789);
    }

    SECTION("Varint32 encoding") {
        std::string dst;
        PutVarint32(&dst, 300);
        std::string_view input(dst);
        uint32_t value = 0;
        REQUIRE(GetVarint32(&input, &value));
        REQUIRE(value == 300);
    }

    SECTION("Varint64 encoding") {
        std::string dst;
        PutVarint64(&dst, 123456789012345ULL);
        std::string_view input(dst);
        uint64_t value = 0;
        REQUIRE(GetVarint64(&input, &value));
        REQUIRE(value == 123456789012345ULL);
    }
}