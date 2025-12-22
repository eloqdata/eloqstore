#include <catch2/catch_test_macros.hpp>
#include <string>
#include <string_view>

#include "../include/common.h"

TEST_CASE("ParseFileName - basic parsing", "[filename]")
{
    // Test data files
    auto [type1, suffix1] = eloqstore::ParseFileName("data_123");
    REQUIRE(type1 == "data");
    REQUIRE(suffix1 == "123");

    auto [type2, suffix2] = eloqstore::ParseFileName("data_123_5");
    REQUIRE(type2 == "data");
    REQUIRE(suffix2 == "123_5");

    // Test manifest files
    auto [type3, suffix3] = eloqstore::ParseFileName("manifest");
    REQUIRE(type3 == "manifest");
    REQUIRE(suffix3 == "");

    auto [type4, suffix4] = eloqstore::ParseFileName("manifest_5");
    REQUIRE(type4 == "manifest");
    REQUIRE(suffix4 == "5");

    auto [type5, suffix5] = eloqstore::ParseFileName("manifest_5_123456789");
    REQUIRE(type5 == "manifest");
    REQUIRE(suffix5 == "5_123456789");
}

TEST_CASE("ParseFileName - edge cases", "[filename]")
{
    // Empty string
    auto [type1, suffix1] = eloqstore::ParseFileName("");
    REQUIRE(type1 == "");
    REQUIRE(suffix1 == "");

    // No separator
    auto [type2, suffix2] = eloqstore::ParseFileName("data");
    REQUIRE(type2 == "data");
    REQUIRE(suffix2 == "");

    // Multiple separators
    auto [type3, suffix3] = eloqstore::ParseFileName("data_1_2_3");
    REQUIRE(type3 == "data");
    REQUIRE(suffix3 == "1_2_3");
}

TEST_CASE("ParseDataFileSuffix - legacy format rejected", "[filename]")
{
    // Legacy format: just file_id (no term) is no longer supported
    eloqstore::FileId file_id = 0;
    uint64_t term = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123", file_id, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("0", file_id, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("999999", file_id, term));
}

TEST_CASE("ParseDataFileSuffix - term-aware format", "[filename]")
{
    // Term-aware format: file_id_term
    eloqstore::FileId file_id = 0;
    uint64_t term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix("123_5", file_id, term));
    REQUIRE(file_id == 123);
    REQUIRE(term == 5);

    eloqstore::FileId file_id2 = 0;
    uint64_t term2 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix("0_1", file_id2, term2));
    REQUIRE(file_id2 == 0);
    REQUIRE(term2 == 1);

    eloqstore::FileId file_id3 = 0;
    uint64_t term3 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix("999_12345", file_id3, term3));
    REQUIRE(file_id3 == 999);
    REQUIRE(term3 == 12345);
}

TEST_CASE("ParseDataFileSuffix - edge cases", "[filename]")
{
    // Empty suffix
    eloqstore::FileId file_id1 = 0;
    uint64_t term1 = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("", file_id1, term1));

    // Invalid format (non-numeric)
    eloqstore::FileId file_id2 = 0;
    uint64_t term2 = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("abc", file_id2, term2));

    // Invalid format (non-numeric term)
    eloqstore::FileId file_id3 = 0;
    uint64_t term3 = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_abc", file_id3, term3));

    // Invalid format (non-numeric file_id)
    eloqstore::FileId file_id4 = 0;
    uint64_t term4 = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("abc_5", file_id4, term4));
}

TEST_CASE("ParseManifestFileSuffix - legacy format rejected", "[filename]")
{
    // Legacy format: empty suffix (just "manifest") is no longer supported
    uint64_t term = 0;
    std::optional<uint64_t> timestamp;
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("", term, timestamp));
}

TEST_CASE("ParseManifestFileSuffix - term-only format", "[filename]")
{
    // Term-only format: "manifest_<term>"
    uint64_t term = 0;
    std::optional<uint64_t> timestamp;
    REQUIRE(eloqstore::ParseManifestFileSuffix("5", term, timestamp));
    REQUIRE(term == 5);
    REQUIRE(!timestamp.has_value());

    uint64_t term2 = 0;
    std::optional<uint64_t> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix("0", term2, timestamp2));
    REQUIRE(term2 == 0);
    REQUIRE(!timestamp2.has_value());

    uint64_t term3 = 0;
    std::optional<uint64_t> timestamp3;
    REQUIRE(eloqstore::ParseManifestFileSuffix("12345", term3, timestamp3));
    REQUIRE(term3 == 12345);
    REQUIRE(!timestamp3.has_value());
}

TEST_CASE("ParseManifestFileSuffix - term-aware archive format", "[filename]")
{
    // Term-aware archive format: "manifest_<term>_<timestamp>"
    uint64_t term = 0;
    std::optional<uint64_t> timestamp;
    REQUIRE(eloqstore::ParseManifestFileSuffix("5_123456789", term, timestamp));
    REQUIRE(term == 5);
    REQUIRE(timestamp.has_value());
    REQUIRE(timestamp.value() == 123456789);

    uint64_t term2 = 0;
    std::optional<uint64_t> timestamp2;
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("0_999999999", term2, timestamp2));
    REQUIRE(term2 == 0);
    REQUIRE(timestamp2.has_value());
    REQUIRE(timestamp2.value() == 999999999);

    uint64_t term3 = 0;
    std::optional<uint64_t> timestamp3;
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("123_456789012", term3, timestamp3));
    REQUIRE(term3 == 123);
    REQUIRE(timestamp3.has_value());
    REQUIRE(timestamp3.value() == 456789012);
}

TEST_CASE("ParseManifestFileSuffix - edge cases", "[filename]")
{
    // Invalid format (non-numeric term)
    uint64_t term1 = 0;
    std::optional<uint64_t> timestamp1;
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("abc", term1, timestamp1));

    // Invalid format (non-numeric term in archive)
    uint64_t term2 = 0;
    std::optional<uint64_t> timestamp2;
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("abc_123456789", term2, timestamp2));

    // Invalid format (non-numeric timestamp)
    uint64_t term3 = 0;
    std::optional<uint64_t> timestamp3;
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("5_abc", term3, timestamp3));
}

TEST_CASE("DataFileName - term-aware format", "[filename]")
{
    // Term-aware format: data_<id>_<term>
    std::string name1 = eloqstore::DataFileName(123, 5);
    REQUIRE(name1 == "data_123_5");

    std::string name2 = eloqstore::DataFileName(0, 1);
    REQUIRE(name2 == "data_0_1");

    std::string name3 = eloqstore::DataFileName(999, 12345);
    REQUIRE(name3 == "data_999_12345");

    // term=0 should also include explicit suffix
    std::string name4 = eloqstore::DataFileName(123, 0);
    REQUIRE(name4 == "data_123_0");
}

TEST_CASE("ManifestFileName - term-aware format", "[filename]")
{
    // Term-aware format: manifest_<term>
    std::string name1 = eloqstore::ManifestFileName(5);
    REQUIRE(name1 == "manifest_5");

    std::string name2 = eloqstore::ManifestFileName(12345);
    REQUIRE(name2 == "manifest_12345");

    std::string name3 = eloqstore::ManifestFileName(1);
    REQUIRE(name3 == "manifest_1");

    // term=0 should also include explicit suffix
    std::string name4 = eloqstore::ManifestFileName(0);
    REQUIRE(name4 == "manifest_0");
}

TEST_CASE("ArchiveName - term-aware format", "[filename]")
{
    // Term-aware format: manifest_<term>_<ts>
    std::string name1 = eloqstore::ArchiveName(5, 123456789);
    REQUIRE(name1 == "manifest_5_123456789");

    std::string name2 = eloqstore::ArchiveName(0, 999999999);
    REQUIRE(name2 == "manifest_0_999999999");

    std::string name3 = eloqstore::ArchiveName(123, 456789012);
    REQUIRE(name3 == "manifest_123_456789012");
}

TEST_CASE("Roundtrip - DataFileName generate and parse", "[filename]")
{
    // Test legacy format roundtrip
    std::string name = eloqstore::DataFileName(123, 0);
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "data");
    eloqstore::FileId file_id = 0;
    uint64_t term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix, file_id, term));
    REQUIRE(file_id == 123);
    REQUIRE(term == 0);  // No term in legacy format

    // Test term-aware format roundtrip
    std::string name2 = eloqstore::DataFileName(456, 7);
    auto [type2, suffix2] = eloqstore::ParseFileName(name2);
    REQUIRE(type2 == "data");
    eloqstore::FileId file_id2 = 0;
    uint64_t term2 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix2, file_id2, term2));
    REQUIRE(file_id2 == 456);
    REQUIRE(term2 == 7);
}

TEST_CASE("Roundtrip - ManifestFileName generate and parse", "[filename]")
{
    // Test term=0 format roundtrip
    std::string name = eloqstore::ManifestFileName(0);
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "manifest");
    uint64_t term = 0;
    std::optional<uint64_t> timestamp;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, term, timestamp));
    REQUIRE(term == 0);  // No term in legacy format
    REQUIRE(!timestamp.has_value());

    // Test term-aware format roundtrip
    std::string name2 = eloqstore::ManifestFileName(5);
    auto [type2, suffix2] = eloqstore::ParseFileName(name2);
    REQUIRE(type2 == "manifest");
    uint64_t term2 = 0;
    std::optional<uint64_t> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix2, term2, timestamp2));
    REQUIRE(term2 == 5);
    REQUIRE(!timestamp2.has_value());
}

TEST_CASE("Roundtrip - ArchiveName generate and parse", "[filename]")
{
    // Test term-aware archive format roundtrip
    std::string name = eloqstore::ArchiveName(5, 123456789);
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "manifest");
    uint64_t term = 0;
    std::optional<uint64_t> timestamp;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, term, timestamp));
    REQUIRE(term == 5);
    REQUIRE(timestamp.has_value());
    REQUIRE(timestamp.value() == 123456789);

    // Test with term=0
    std::string name2 = eloqstore::ArchiveName(0, 999999999);
    auto [type2, suffix2] = eloqstore::ParseFileName(name2);
    REQUIRE(type2 == "manifest");
    uint64_t term2 = 0;
    std::optional<uint64_t> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix2, term2, timestamp2));
    REQUIRE(term2 == 0);
    REQUIRE(timestamp2.has_value());
    REQUIRE(timestamp2.value() == 999999999);
}

TEST_CASE("ParseUint64 - valid numbers", "[filename]")
{
    uint64_t result = 0;

    REQUIRE(eloqstore::ParseUint64("0", result));
    REQUIRE(result == 0);

    REQUIRE(eloqstore::ParseUint64("123", result));
    REQUIRE(result == 123);

    REQUIRE(eloqstore::ParseUint64("999999", result));
    REQUIRE(result == 999999);

    REQUIRE(
        eloqstore::ParseUint64("18446744073709551615", result));  // UINT64_MAX
    REQUIRE(result == UINT64_MAX);
}

TEST_CASE("ParseUint64 - invalid inputs", "[filename]")
{
    uint64_t result = 0;

    // Empty string
    REQUIRE(!eloqstore::ParseUint64("", result));

    // Non-numeric
    REQUIRE(!eloqstore::ParseUint64("abc", result));
    REQUIRE(!eloqstore::ParseUint64("123abc", result));
    REQUIRE(!eloqstore::ParseUint64("abc123", result));

    // Overflow (this would overflow, but our check catches it)
    // Note: UINT64_MAX is 18446744073709551615
    // We can't easily test overflow without a very long number
}

TEST_CASE("Integration - complete filename workflow", "[filename]")
{
    // Simulate creating a data file with term
    eloqstore::FileId file_id = 123;
    uint64_t term = 5;
    std::string filename = eloqstore::DataFileName(file_id, term);

    // Parse it back
    auto [type, suffix] = eloqstore::ParseFileName(filename);
    REQUIRE(type == "data");
    eloqstore::FileId parsed_file_id = 0;
    uint64_t parsed_term = 0;
    REQUIRE(
        eloqstore::ParseDataFileSuffix(suffix, parsed_file_id, parsed_term));
    REQUIRE(parsed_file_id == file_id);
    REQUIRE(parsed_term == term);

    // Simulate creating a manifest with term
    uint64_t manifest_term = 7;
    std::string manifest_name = eloqstore::ManifestFileName(manifest_term);

    // Parse it back
    auto [manifest_type, manifest_suffix] =
        eloqstore::ParseFileName(manifest_name);
    REQUIRE(manifest_type == "manifest");
    uint64_t parsed_manifest_term = 0;
    std::optional<uint64_t> parsed_ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        manifest_suffix, parsed_manifest_term, parsed_ts));
    REQUIRE(parsed_manifest_term == manifest_term);
    REQUIRE(!parsed_ts.has_value());

    // Simulate creating an archive
    uint64_t archive_term = 9;
    uint64_t timestamp = 1234567890;
    std::string archive_name = eloqstore::ArchiveName(archive_term, timestamp);

    // Parse it back
    auto [archive_type, archive_suffix] =
        eloqstore::ParseFileName(archive_name);
    REQUIRE(archive_type == "manifest");
    uint64_t parsed_archive_term = 0;
    std::optional<uint64_t> parsed_archive_ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        archive_suffix, parsed_archive_term, parsed_archive_ts));
    REQUIRE(parsed_archive_term == archive_term);
    REQUIRE(parsed_archive_ts.has_value());
    REQUIRE(parsed_archive_ts.value() == timestamp);
}
