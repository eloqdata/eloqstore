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

    auto [type2, suffix2] = eloqstore::ParseFileName("data_123_main_5");
    REQUIRE(type2 == "data");
    REQUIRE(suffix2 == "123_main_5");

    // Test manifest files
    auto [type3, suffix3] = eloqstore::ParseFileName("manifest");
    REQUIRE(type3 == "manifest");
    REQUIRE(suffix3 == "");

    auto [type4, suffix4] = eloqstore::ParseFileName("manifest_main_5");
    REQUIRE(type4 == "manifest");
    REQUIRE(suffix4 == "main_5");

    auto [type5, suffix5] =
        eloqstore::ParseFileName("manifest_main_5_123456789");
    REQUIRE(type5 == "manifest");
    REQUIRE(suffix5 == "main_5_123456789");
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

TEST_CASE("ParseDataFileSuffix - old format rejected", "[filename]")
{
    // Old format (no branch): just file_id is rejected
    eloqstore::FileId file_id = 0;
    std::string_view branch;
    uint64_t term = 0;
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123", file_id, branch, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("0", file_id, branch, term));

    // Old two-part format file_id_term (no branch) is also rejected
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123_5", file_id, branch, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("0_1", file_id, branch, term));
}

TEST_CASE("ParseDataFileSuffix - branch-aware format", "[filename]")
{
    // Branch-aware format: file_id_branch_term
    eloqstore::FileId file_id = 0;
    std::string_view branch;
    uint64_t term = 0;
    REQUIRE(
        eloqstore::ParseDataFileSuffix("123_main_5", file_id, branch, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch == "main");
    REQUIRE(term == 5);

    eloqstore::FileId file_id2 = 0;
    std::string_view branch2;
    uint64_t term2 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "0_feature_1", file_id2, branch2, term2));
    REQUIRE(file_id2 == 0);
    REQUIRE(branch2 == "feature");
    REQUIRE(term2 == 1);

    eloqstore::FileId file_id3 = 0;
    std::string_view branch3;
    uint64_t term3 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "999_dev-branch_12345", file_id3, branch3, term3));
    REQUIRE(file_id3 == 999);
    REQUIRE(branch3 == "dev-branch");
    REQUIRE(term3 == 12345);
}

TEST_CASE("ParseDataFileSuffix - edge cases", "[filename]")
{
    eloqstore::FileId file_id = 0;
    std::string_view branch;
    uint64_t term = 0;

    // Empty suffix
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("", file_id, branch, term));

    // Non-numeric file_id
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("abc_main_5", file_id, branch, term));

    // Non-numeric term
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123_main_abc", file_id, branch, term));

    // Missing term (only two parts)
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123_main", file_id, branch, term));
}

TEST_CASE("ParseManifestFileSuffix - old format rejected", "[filename]")
{
    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;

    // Empty suffix
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("", branch, term, timestamp));

    // Old format: purely numeric term with no branch (e.g. "5", "12345")
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("5", branch, term, timestamp));
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("12345", branch, term, timestamp));

    // Old archive format: term_ts with no branch (e.g. "5_123456789")
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix(
        "5_123456789", branch, term, timestamp));
}

TEST_CASE("ParseManifestFileSuffix - branch-aware manifest format",
          "[filename]")
{
    // Branch-aware format: branch_term
    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;

    REQUIRE(
        eloqstore::ParseManifestFileSuffix("main_5", branch, term, timestamp));
    REQUIRE(branch == "main");
    REQUIRE(term == 5);
    REQUIRE(!timestamp.has_value());

    std::string_view branch2;
    uint64_t term2 = 0;
    std::optional<std::string> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "feature_0", branch2, term2, timestamp2));
    REQUIRE(branch2 == "feature");
    REQUIRE(term2 == 0);
    REQUIRE(!timestamp2.has_value());

    std::string_view branch3;
    uint64_t term3 = 0;
    std::optional<std::string> timestamp3;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "dev-branch_12345", branch3, term3, timestamp3));
    REQUIRE(branch3 == "dev-branch");
    REQUIRE(term3 == 12345);
    REQUIRE(!timestamp3.has_value());
}

TEST_CASE("ParseManifestFileSuffix - branch-aware archive format", "[filename]")
{
    // Branch-aware archive format: branch_term_timestamp
    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;

    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "main_5_123456789", branch, term, timestamp));
    REQUIRE(branch == "main");
    REQUIRE(term == 5);
    REQUIRE(timestamp.has_value());
    REQUIRE(timestamp.value() == "123456789");

    std::string_view branch2;
    uint64_t term2 = 0;
    std::optional<std::string> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "feature_0_999999999", branch2, term2, timestamp2));
    REQUIRE(branch2 == "feature");
    REQUIRE(term2 == 0);
    REQUIRE(timestamp2.has_value());
    REQUIRE(timestamp2.value() == "999999999");
}

TEST_CASE("ParseManifestFileSuffix - edge cases", "[filename]")
{
    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;

    // Invalid branch name (starts with digit — would be mistaken for old
    // format)
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("123_5", branch, term, timestamp));

    // Non-numeric term
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix(
        "main_abc", branch, term, timestamp));

    // Tag can be non-numeric (HEAD's string tags).
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "main_5_abc", branch, term, timestamp));
    REQUIRE(branch == "main");
    REQUIRE(term == 5);
    REQUIRE(timestamp.has_value());
    REQUIRE(timestamp.value() == "abc");
}

TEST_CASE("BranchDataFileName - generate branch-aware data filenames",
          "[filename]")
{
    REQUIRE(eloqstore::BranchDataFileName(123, "main", 5) == "data_123_main_5");
    REQUIRE(eloqstore::BranchDataFileName(0, "feature", 1) ==
            "data_0_feature_1");
    REQUIRE(eloqstore::BranchDataFileName(999, "dev-branch", 12345) ==
            "data_999_dev-branch_12345");
    REQUIRE(eloqstore::BranchDataFileName(123, "main", 0) == "data_123_main_0");
}

TEST_CASE("BranchManifestFileName - generate branch-aware manifest filenames",
          "[filename]")
{
    REQUIRE(eloqstore::BranchManifestFileName("main", 5) == "manifest_main_5");
    REQUIRE(eloqstore::BranchManifestFileName("feature", 0) ==
            "manifest_feature_0");
    REQUIRE(eloqstore::BranchManifestFileName("dev-branch", 12345) ==
            "manifest_dev-branch_12345");
}

TEST_CASE("BranchArchiveName - generate branch-aware archive filenames",
          "[filename]")
{
    REQUIRE(eloqstore::BranchArchiveName("main", 5, "123456789") ==
            "manifest_main_5_123456789");
    REQUIRE(eloqstore::BranchArchiveName("feature", 0, "999999999") ==
            "manifest_feature_0_999999999");
    REQUIRE(eloqstore::BranchArchiveName("dev-branch", 123, "456789012") ==
            "manifest_dev-branch_123_456789012");
}

TEST_CASE("Roundtrip - BranchDataFileName generate and parse", "[filename]")
{
    std::string name = eloqstore::BranchDataFileName(123, "main", 5);
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "data");

    eloqstore::FileId file_id = 0;
    std::string_view branch;
    uint64_t term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix, file_id, branch, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch == "main");
    REQUIRE(term == 5);

    // Different branch and term
    std::string name2 = eloqstore::BranchDataFileName(456, "feature", 7);
    auto [type2, suffix2] = eloqstore::ParseFileName(name2);
    REQUIRE(type2 == "data");
    eloqstore::FileId file_id2 = 0;
    std::string_view branch2;
    uint64_t term2 = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix2, file_id2, branch2, term2));
    REQUIRE(file_id2 == 456);
    REQUIRE(branch2 == "feature");
    REQUIRE(term2 == 7);
}

TEST_CASE("Roundtrip - BranchManifestFileName generate and parse", "[filename]")
{
    std::string name = eloqstore::BranchManifestFileName("main", 5);
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "manifest");

    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;
    REQUIRE(
        eloqstore::ParseManifestFileSuffix(suffix, branch, term, timestamp));
    REQUIRE(branch == "main");
    REQUIRE(term == 5);
    REQUIRE(!timestamp.has_value());
}

TEST_CASE("Roundtrip - BranchArchiveName generate and parse", "[filename]")
{
    std::string name = eloqstore::BranchArchiveName("main", 5, "123456789");
    auto [type, suffix] = eloqstore::ParseFileName(name);
    REQUIRE(type == "manifest");

    std::string_view branch;
    uint64_t term = 0;
    std::optional<std::string> timestamp;
    REQUIRE(
        eloqstore::ParseManifestFileSuffix(suffix, branch, term, timestamp));
    REQUIRE(branch == "main");
    REQUIRE(term == 5);
    REQUIRE(timestamp.has_value());
    REQUIRE(timestamp.value() == "123456789");

    // With term=0
    std::string name2 = eloqstore::BranchArchiveName("feature", 0, "999999999");
    auto [type2, suffix2] = eloqstore::ParseFileName(name2);
    REQUIRE(type2 == "manifest");
    std::string_view branch2;
    uint64_t term2 = 0;
    std::optional<std::string> timestamp2;
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        suffix2, branch2, term2, timestamp2));
    REQUIRE(branch2 == "feature");
    REQUIRE(term2 == 0);
    REQUIRE(timestamp2.has_value());
    REQUIRE(timestamp2.value() == "999999999");
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

    REQUIRE(!eloqstore::ParseUint64("", result));
    REQUIRE(!eloqstore::ParseUint64("abc", result));
    REQUIRE(!eloqstore::ParseUint64("123abc", result));
    REQUIRE(!eloqstore::ParseUint64("abc123", result));
}

TEST_CASE("Integration - complete branch-aware filename workflow", "[filename]")
{
    // Data file: create → parse
    eloqstore::FileId file_id = 123;
    std::string branch_str = "main";
    uint64_t term = 5;
    std::string filename =
        eloqstore::BranchDataFileName(file_id, branch_str, term);
    REQUIRE(filename == "data_123_main_5");

    auto [type, suffix] = eloqstore::ParseFileName(filename);
    REQUIRE(type == "data");
    eloqstore::FileId parsed_file_id = 0;
    std::string_view parsed_branch;
    uint64_t parsed_term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(
        suffix, parsed_file_id, parsed_branch, parsed_term));
    REQUIRE(parsed_file_id == file_id);
    REQUIRE(parsed_branch == branch_str);
    REQUIRE(parsed_term == term);

    // Manifest: create → parse
    std::string manifest_name = eloqstore::BranchManifestFileName("main", 7);
    REQUIRE(manifest_name == "manifest_main_7");

    auto [manifest_type, manifest_suffix] =
        eloqstore::ParseFileName(manifest_name);
    REQUIRE(manifest_type == "manifest");
    std::string_view parsed_manifest_branch;
    uint64_t parsed_manifest_term = 0;
    std::optional<std::string> parsed_ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(manifest_suffix,
                                               parsed_manifest_branch,
                                               parsed_manifest_term,
                                               parsed_ts));
    REQUIRE(parsed_manifest_branch == "main");
    REQUIRE(parsed_manifest_term == 7);
    REQUIRE(!parsed_ts.has_value());

    // Archive: create → parse
    std::string archive_name =
        eloqstore::BranchArchiveName("main", 9, "1234567890");
    REQUIRE(archive_name == "manifest_main_9_1234567890");

    auto [archive_type, archive_suffix] =
        eloqstore::ParseFileName(archive_name);
    REQUIRE(archive_type == "manifest");
    std::string_view parsed_archive_branch;
    uint64_t parsed_archive_term = 0;
    std::optional<std::string> parsed_archive_ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(archive_suffix,
                                               parsed_archive_branch,
                                               parsed_archive_term,
                                               parsed_archive_ts));
    REQUIRE(parsed_archive_branch == "main");
    REQUIRE(parsed_archive_term == 9);
    REQUIRE(parsed_archive_ts.has_value());
    REQUIRE(parsed_archive_ts.value() == "1234567890");
}
