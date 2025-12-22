#include "../common.h"

#include <catch2/catch_test_macros.hpp>
#include <string>

using namespace eloqstore;

TEST_CASE("FileIdTermMapping serialize/deserialize roundtrip", "[fileid-term]")
{
    FileIdTermMapping mapping;
    mapping[1] = 10;
    mapping[2] = 20;
    mapping[123456789] = 987654321;

    std::string buf;
    SerializeFileIdTermMapping(mapping, buf);

    std::string_view view(buf);
    FileIdTermMapping parsed;
    REQUIRE(DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.size() == mapping.size());
    for (const auto &[k, v] : mapping)
    {
        REQUIRE(parsed.at(k) == v);
    }
}

TEST_CASE("FileIdTermMapping empty mapping", "[fileid-term]")
{
    FileIdTermMapping mapping;
    std::string buf;
    SerializeFileIdTermMapping(mapping, buf);

    std::string_view view(buf);
    FileIdTermMapping parsed;
    REQUIRE(DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.empty());
}

TEST_CASE("FileIdTermMapping malformed data", "[fileid-term]")
{
    // Count=2 but only one pair provided -> should fail and clear mapping.
    std::string buf;
    PutVarint64(&buf, 2);          // count
    PutVarint64(&buf, 1);          // file_id
    PutVarint64(&buf, 10);         // term
    // Missing second pair data

    std::string_view view(buf);
    FileIdTermMapping parsed;
    parsed[99] = 99;  // pre-fill to ensure it gets cleared on failure
    REQUIRE_FALSE(DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.empty());
}

TEST_CASE("FileIdTermMapping truncated count", "[fileid-term]")
{
    // Buffer too short to read count fully.
    std::string buf;
    buf.push_back(static_cast<char>(0x80));  // incomplete varint

    std::string_view view(buf);
    FileIdTermMapping parsed;
    REQUIRE_FALSE(DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.empty());
}

