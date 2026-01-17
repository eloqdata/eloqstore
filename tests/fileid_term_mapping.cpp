#include <catch2/catch_test_macros.hpp>
#include <string>

#include "../include/common.h"
#include "../include/manifest_buffer.h"
#include "coding.h"

TEST_CASE("FileIdTermMapping serialize/deserialize roundtrip", "[fileid-term]")
{
    eloqstore::FileIdTermMapping mapping;
    mapping[1] = 10;
    mapping[2] = 20;
    mapping[123456789] = 987654321;

    std::string buf;
    eloqstore::SerializeFileIdTermMapping(mapping, buf);

    std::string_view view(buf);
    eloqstore::FileIdTermMapping parsed;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.size() == mapping.size());
    for (const auto &[k, v] : mapping)
    {
        REQUIRE(parsed.at(k) == v);
    }
}

TEST_CASE("FileIdTermMapping empty mapping", "[fileid-term]")
{
    eloqstore::FileIdTermMapping mapping;
    std::string buf;
    eloqstore::SerializeFileIdTermMapping(mapping, buf);

    std::string_view view(buf);
    eloqstore::FileIdTermMapping parsed;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(view, parsed));
    REQUIRE(parsed.empty());
}

TEST_CASE("FileIdTermMapping malformed data", "[fileid-term]")
{
    // Count=2 but only one pair provided -> should fail.
    std::string buf;
    eloqstore::PutFixed32(&buf, 0);    // count
    eloqstore::PutVarint64(&buf, 1);   // file_id
    eloqstore::PutVarint64(&buf, 10);  // term
    eloqstore::EncodeFixed32(buf.data(), buf.size() - 4 + 2);
    // Missing second pair data

    std::string_view view(buf);
    eloqstore::FileIdTermMapping parsed;
    parsed[99] = 99;  // pre-fill to ensure it gets cleared on failure
    REQUIRE_FALSE(eloqstore::DeserializeFileIdTermMapping(view, parsed));
}

TEST_CASE("FileIdTermMapping truncated count", "[fileid-term]")
{
    // Buffer too short to read count fully.
    std::string buf;
    eloqstore::PutFixed32(&buf, 20);  // bytes_len
    eloqstore::PutVarint64(&buf, 1);  // file_id
    // incomplete varint

    std::string_view view(buf);
    eloqstore::FileIdTermMapping parsed;
    REQUIRE_FALSE(eloqstore::DeserializeFileIdTermMapping(view, parsed));
}
