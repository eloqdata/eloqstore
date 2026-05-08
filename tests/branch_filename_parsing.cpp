#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "../include/common.h"
#include "../include/types.h"

// ============================================================================
// Branch Name Validation Tests
// ============================================================================

TEST_CASE("NormalizeBranchName - valid names", "[branch][validation]")
{
    // Lowercase names
    REQUIRE(eloqstore::NormalizeBranchName("main") == "main");
    REQUIRE(eloqstore::NormalizeBranchName("feature") == "feature");
    REQUIRE(eloqstore::NormalizeBranchName("dev") == "dev");

    // With numbers
    REQUIRE(eloqstore::NormalizeBranchName("feature123") == "feature123");
    REQUIRE(eloqstore::NormalizeBranchName("v2") == "v2");
    REQUIRE(eloqstore::NormalizeBranchName("123") == "123");

    // With hyphens (valid)
    REQUIRE(eloqstore::NormalizeBranchName("feature-branch") ==
            "feature-branch");
    REQUIRE(eloqstore::NormalizeBranchName("my-feature-123") ==
            "my-feature-123");

    // Mixed valid characters
    REQUIRE(eloqstore::NormalizeBranchName("feat-123-dev") == "feat-123-dev");
}

TEST_CASE("NormalizeBranchName - case normalization", "[branch][validation]")
{
    // Uppercase to lowercase
    REQUIRE(eloqstore::NormalizeBranchName("MAIN") == "main");
    REQUIRE(eloqstore::NormalizeBranchName("FEATURE") == "feature");
    REQUIRE(eloqstore::NormalizeBranchName("DEV") == "dev");

    // Mixed case to lowercase
    REQUIRE(eloqstore::NormalizeBranchName("Feature") == "feature");
    REQUIRE(eloqstore::NormalizeBranchName("MyFeature") == "myfeature");
    REQUIRE(eloqstore::NormalizeBranchName("FeAtUrE") == "feature");
    REQUIRE(eloqstore::NormalizeBranchName("Feature-Branch") ==
            "feature-branch");
    REQUIRE(eloqstore::NormalizeBranchName("Feature-123") == "feature-123");
}

TEST_CASE("NormalizeBranchName - invalid characters", "[branch][validation]")
{
    // Empty string
    REQUIRE(eloqstore::NormalizeBranchName("") == "");

    // Invalid special characters
    REQUIRE(eloqstore::NormalizeBranchName("feature branch") == "");  // space
    REQUIRE(eloqstore::NormalizeBranchName("feature.branch") == "");  // dot
    REQUIRE(eloqstore::NormalizeBranchName("feature@branch") == "");  // @
    REQUIRE(eloqstore::NormalizeBranchName("feature#branch") == "");  // #
    REQUIRE(eloqstore::NormalizeBranchName("feature$branch") == "");  // $
    REQUIRE(eloqstore::NormalizeBranchName("feature/branch") == "");  // /
    REQUIRE(eloqstore::NormalizeBranchName("feature\\branch") ==
            "");  // backslash
    REQUIRE(eloqstore::NormalizeBranchName("feature:branch") == "");  // colon

    // Underscore is INVALID (reserved as FileNameSeparator)
    REQUIRE(eloqstore::NormalizeBranchName("feature_branch") ==
            "");                                                  // underscore
    REQUIRE(eloqstore::NormalizeBranchName("my_feature") == "");  // underscore
}

TEST_CASE("NormalizeBranchName - edge cases", "[branch][validation]")
{
    // Single character
    REQUIRE(eloqstore::NormalizeBranchName("a") == "a");
    REQUIRE(eloqstore::NormalizeBranchName("A") == "a");
    REQUIRE(eloqstore::NormalizeBranchName("1") == "1");
    REQUIRE(eloqstore::NormalizeBranchName("-") == "-");
    // Underscore is INVALID (reserved as separator)
    REQUIRE(eloqstore::NormalizeBranchName("_") == "");

    // Long name
    std::string long_name(100, 'a');
    REQUIRE(eloqstore::NormalizeBranchName(long_name) == long_name);

    // Reserved name "main" in different cases
    REQUIRE(eloqstore::NormalizeBranchName("main") == "main");
    REQUIRE(eloqstore::NormalizeBranchName("Main") == "main");
    REQUIRE(eloqstore::NormalizeBranchName("MAIN") == "main");
}

TEST_CASE("IsValidBranchName - wrapper validation", "[branch][validation]")
{
    // Valid names
    REQUIRE(eloqstore::IsValidBranchName("main"));
    REQUIRE(eloqstore::IsValidBranchName("feature"));
    REQUIRE(eloqstore::IsValidBranchName("Feature123"));
    REQUIRE(eloqstore::IsValidBranchName("my-feature"));

    // Invalid names
    REQUIRE_FALSE(eloqstore::IsValidBranchName(""));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature branch"));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature.branch"));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature@123"));
    REQUIRE_FALSE(
        eloqstore::IsValidBranchName("my_feature"));  // underscore invalid
}

// ============================================================================
// File Generation Tests
// ============================================================================

TEST_CASE("BranchDataFileName - format verification", "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchDataFileName(123, "main", 5) == "data_123_main_5");
    REQUIRE(eloqstore::BranchDataFileName(456, "feature", 10) ==
            "data_456_feature_10");

    // Zero values
    REQUIRE(eloqstore::BranchDataFileName(0, "main", 0) == "data_0_main_0");
    REQUIRE(eloqstore::BranchDataFileName(0, "feature", 1) ==
            "data_0_feature_1");

    // Large values
    REQUIRE(eloqstore::BranchDataFileName(999999, "main", 123456) ==
            "data_999999_main_123456");

    // Different branch names
    REQUIRE(eloqstore::BranchDataFileName(10, "dev", 1) == "data_10_dev_1");
    REQUIRE(eloqstore::BranchDataFileName(10, "feature-123", 1) ==
            "data_10_feature-123_1");
    REQUIRE(eloqstore::BranchDataFileName(10, "hotfix-456", 1) ==
            "data_10_hotfix-456_1");
}

TEST_CASE("BranchManifestFileName - format verification",
          "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchManifestFileName("main", 5) == "manifest_main_5");
    REQUIRE(eloqstore::BranchManifestFileName("feature", 10) ==
            "manifest_feature_10");

    // Zero term
    REQUIRE(eloqstore::BranchManifestFileName("main", 0) == "manifest_main_0");

    // Large term
    REQUIRE(eloqstore::BranchManifestFileName("main", 123456789) ==
            "manifest_main_123456789");

    // Different branch names
    REQUIRE(eloqstore::BranchManifestFileName("dev", 1) == "manifest_dev_1");
    REQUIRE(eloqstore::BranchManifestFileName("feature-123", 2) ==
            "manifest_feature-123_2");
}

TEST_CASE("BranchArchiveName - format verification", "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchArchiveName("main", 5, "123456") ==
            "manifest_main_5_123456");
    REQUIRE(eloqstore::BranchArchiveName("feature", 10, "789012") ==
            "manifest_feature_10_789012");

    // Zero values
    REQUIRE(eloqstore::BranchArchiveName("main", 0, "0") ==
            "manifest_main_0_0");

    // Large values
    REQUIRE(eloqstore::BranchArchiveName("main", 999, "1234567890123") ==
            "manifest_main_999_1234567890123");
}

TEST_CASE("CurrentTermFileNameForBranchAndPartitionGroup",
          "[branch][generation]")
{
    // Verify underscore separator with pg_id
    REQUIRE(eloqstore::CurrentTermFileNameForBranchAndPartitionGroup(
                "main", 0) == "CURRENT_TERM_main_0");
    REQUIRE(eloqstore::CurrentTermFileNameForBranchAndPartitionGroup(
                "feature", 3) == "CURRENT_TERM_feature_3");
    REQUIRE(eloqstore::CurrentTermFileNameForBranchAndPartitionGroup(
                "dev", 42) == "CURRENT_TERM_dev_42");
    REQUIRE(eloqstore::CurrentTermFileNameForBranchAndPartitionGroup(
                "feature-123", 0) == "CURRENT_TERM_feature-123_0");

    // Verify it starts with CURRENT_TERM constant
    std::string result =
        eloqstore::CurrentTermFileNameForBranchAndPartitionGroup("main", 0);
    REQUIRE(result.find(eloqstore::CurrentTermFileName) == 0);
}

// ============================================================================
// Parsing Tests - ParseDataFileSuffix
// ============================================================================

TEST_CASE("ParseDataFileSuffix - branch format", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;

    // Valid format: file_id_branch_term
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "123_main_5", file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);

    // Different branch
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "456_feature_10", file_id, branch_name, term));
    REQUIRE(file_id == 456);
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);

    // Zero values
    REQUIRE(
        eloqstore::ParseDataFileSuffix("0_main_0", file_id, branch_name, term));
    REQUIRE(file_id == 0);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 0);

    // Branch with hyphen
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "10_feature-123_5", file_id, branch_name, term));
    REQUIRE(file_id == 10);
    REQUIRE(branch_name == "feature-123");
    REQUIRE(term == 5);
}

TEST_CASE("ParseDataFileSuffix - case normalization during parse",
          "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;

    // Note: Normalization happens at file creation time (BranchDataFileName)
    // Parsing extracts branch as-is from filename
    // These tests use lowercase since new files should have lowercase names
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "123_main_5", file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);

    // Mixed case in filename will be returned as-is
    REQUIRE(eloqstore::ParseDataFileSuffix(
        "456_Feature_10", file_id, branch_name, term));
    REQUIRE(file_id == 456);
    REQUIRE(branch_name == "Feature");  // Not normalized, returned as-is
    REQUIRE(term == 10);
}

TEST_CASE("ParseDataFileSuffix - old format rejected", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;

    // Old format: file_id_term (no branch) should fail
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123_5", file_id, branch_name, term));
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("0_1", file_id, branch_name, term));

    // Even older format: just file_id
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123", file_id, branch_name, term));
}

TEST_CASE("ParseDataFileSuffix - invalid formats", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;

    // Empty
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("", file_id, branch_name, term));

    // Non-numeric file_id
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix(
        "abc_main_5", file_id, branch_name, term));

    // Non-numeric term
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix(
        "123_main_abc", file_id, branch_name, term));

    // Invalid branch name (contains dot)
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix(
        "123_main.branch_5", file_id, branch_name, term));

    // Invalid branch name (contains space)
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix(
        "123_main branch_5", file_id, branch_name, term));

    // Missing components
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix(
        "123_main_", file_id, branch_name, term));
    REQUIRE_FALSE(
        eloqstore::ParseDataFileSuffix("123__5", file_id, branch_name, term));
}

// ============================================================================
// Parsing Tests - ParseManifestFileSuffix
// ============================================================================

TEST_CASE("ParseManifestFileSuffix - branch format without tag",
          "[branch][parsing]")
{
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;

    // Valid format: branch_term
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("main_5", branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE_FALSE(tag.has_value());

    // Different branch
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "feature_10", branch_name, term, tag));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE_FALSE(tag.has_value());

    // Zero term
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("main_0", branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 0);
    REQUIRE_FALSE(tag.has_value());
}

TEST_CASE("ParseManifestFileSuffix - branch format with tag",
          "[branch][parsing]")
{
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;

    // Valid archive format: branch_term_tag
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "main_5_123456", branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "123456");

    // Different values
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "feature_10_789012", branch_name, term, tag));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "789012");

    // Zero tag
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("main_5_0", branch_name, term, tag));
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "0");
}

TEST_CASE("ParseManifestFileSuffix - case normalization", "[branch][parsing]")
{
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;

    // Note: Normalization happens at file creation time
    // Parsing extracts branch as-is from filename
    // These tests use lowercase since new files should have lowercase names
    REQUIRE(
        eloqstore::ParseManifestFileSuffix("main_5", branch_name, term, tag));
    REQUIRE(branch_name == "main");

    // Mixed case in filename will be returned as-is
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "Feature_10_123", branch_name, term, tag));
    REQUIRE(branch_name == "Feature");  // Not normalized, returned as-is
}

TEST_CASE("ParseManifestFileSuffix - old format rejected", "[branch][parsing]")
{
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;

    // Old format: just term (no branch)
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("5", branch_name, term, tag));
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("0", branch_name, term, tag));

    // Old archive format: term_timestamp (no branch)
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("5_123456", branch_name, term, tag));
}

TEST_CASE("ParseManifestFileSuffix - invalid formats", "[branch][parsing]")
{
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;

    // Empty
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("", branch_name, term, tag));

    // Non-numeric term
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("main_abc", branch_name, term, tag));

    // Non-numeric tag (now valid since tags are strings)
    REQUIRE(eloqstore::ParseManifestFileSuffix(
        "main_5_abc", branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "abc");

    // Invalid branch name
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix(
        "main.branch_5", branch_name, term, tag));

    // Missing components
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("main_", branch_name, term, tag));
    REQUIRE_FALSE(
        eloqstore::ParseManifestFileSuffix("_5", branch_name, term, tag));
}

// ============================================================================
// Parsing Tests - ParseCurrentTermFilename
// ============================================================================

TEST_CASE("ParseCurrentTermFilename - valid formats", "[branch][parsing]")
{
    std::string_view branch_name;
    eloqstore::PartitonGroupId pg_id = 0;

    // Valid format: CURRENT_TERM_<branch>_<pg_id>
    REQUIRE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_main_0", branch_name, pg_id));
    REQUIRE(branch_name == "main");
    REQUIRE(pg_id == 0);

    REQUIRE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_feature_3", branch_name, pg_id));
    REQUIRE(branch_name == "feature");
    REQUIRE(pg_id == 3);

    REQUIRE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_dev_42", branch_name, pg_id));
    REQUIRE(branch_name == "dev");
    REQUIRE(pg_id == 42);

    // Branch with hyphen
    REQUIRE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_feature-123_0", branch_name, pg_id));
    REQUIRE(branch_name == "feature-123");
    REQUIRE(pg_id == 0);

    // Convenience overload (branch only)
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM_main_0",
                                                branch_name));
    REQUIRE(branch_name == "main");
}

TEST_CASE("ParseCurrentTermFilename - case normalization", "[branch][parsing]")
{
    std::string_view branch_name;

    // Note: Normalization happens at file creation time
    // Parsing extracts branch as-is from filename
    // These tests use lowercase since new files should have lowercase names
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM_main_0",
                                                branch_name));
    REQUIRE(branch_name == "main");

    // Mixed case in filename will be returned as-is
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM_Feature_0",
                                                branch_name));
    REQUIRE(branch_name == "Feature");
}

TEST_CASE("ParseCurrentTermFilename - invalid formats", "[branch][parsing]")
{
    std::string_view branch_name;

    // Old format without branch/pg_id (no separators after prefix)
    REQUIRE_FALSE(
        eloqstore::ParseCurrentTermFilename("CURRENT_TERM", branch_name));

    // Old dot separator format
    REQUIRE_FALSE(
        eloqstore::ParseCurrentTermFilename("CURRENT_TERM.main", branch_name));

    // Missing pg_id (only one segment after prefix)
    REQUIRE_FALSE(
        eloqstore::ParseCurrentTermFilename("CURRENT_TERM_main", branch_name));

    // Empty branch name
    REQUIRE_FALSE(
        eloqstore::ParseCurrentTermFilename("CURRENT_TERM__0", branch_name));

    // Invalid branch name (contains invalid chars)
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_main.branch_0", branch_name));
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename(
        "CURRENT_TERM_main branch_0", branch_name));

    // Wrong prefix
    REQUIRE_FALSE(
        eloqstore::ParseCurrentTermFilename("TERM_main_0", branch_name));
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("current_term_main_0",
                                                      branch_name));

    // Empty string
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("", branch_name));

    // Non-numeric pg_id
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM_main_abc",
                                                      branch_name));
}

// ============================================================================
// Roundtrip Tests
// ============================================================================

TEST_CASE("Roundtrip - data files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchDataFileName(123, "main", 5);
    auto [type, suffix] = eloqstore::ParseFileName(filename);

    eloqstore::FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix, file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);

    // Test with different values
    filename = eloqstore::BranchDataFileName(999, "feature-123", 456);
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(
        eloqstore::ParseDataFileSuffix(suffix2, file_id, branch_name, term));
    REQUIRE(file_id == 999);
    REQUIRE(branch_name == "feature-123");
    REQUIRE(term == 456);

    // Test case normalization at creation time (not during parse)
    // BranchDataFileName normalizes, so parsed result should already be
    // lowercase
    filename = eloqstore::BranchDataFileName(10, "Feature", 1);
    auto [type3, suffix3] = eloqstore::ParseFileName(filename);
    REQUIRE(
        eloqstore::ParseDataFileSuffix(suffix3, file_id, branch_name, term));
    REQUIRE(file_id == 10);
    REQUIRE(branch_name ==
            "feature");  // BranchDataFileName normalizes to lowercase
    REQUIRE(term == 1);
}

TEST_CASE("Roundtrip - manifest files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchManifestFileName("main", 5);
    auto [type, suffix] = eloqstore::ParseFileName(filename);

    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE_FALSE(tag.has_value());

    // Different branch
    filename = eloqstore::BranchManifestFileName("feature", 10);
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(
        eloqstore::ParseManifestFileSuffix(suffix2, branch_name, term, tag));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE_FALSE(tag.has_value());
}

TEST_CASE("Roundtrip - archive files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchArchiveName("main", 5, "123456");
    auto [type, suffix] = eloqstore::ParseFileName(filename);

    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, branch_name, term, tag));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "123456");

    // Different values
    filename = eloqstore::BranchArchiveName("feature", 10, "789012");
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(
        eloqstore::ParseManifestFileSuffix(suffix2, branch_name, term, tag));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE(tag.has_value());
    REQUIRE(tag.value() == "789012");
}

TEST_CASE("Roundtrip - CURRENT_TERM files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename =
        eloqstore::CurrentTermFileNameForBranchAndPartitionGroup("main", 0);
    std::string_view branch_name;
    eloqstore::PartitonGroupId pg_id = 0;
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name, pg_id));
    REQUIRE(branch_name == "main");
    REQUIRE(pg_id == 0);

    // Different branch and pg_id
    filename =
        eloqstore::CurrentTermFileNameForBranchAndPartitionGroup("feature", 7);
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name, pg_id));
    REQUIRE(branch_name == "feature");
    REQUIRE(pg_id == 7);

    // Branch with special chars
    filename = eloqstore::CurrentTermFileNameForBranchAndPartitionGroup(
        "feature-123", 0);
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name, pg_id));
    REQUIRE(branch_name == "feature-123");
    REQUIRE(pg_id == 0);
}

// ============================================================================
// Helper Function Tests
// ============================================================================

TEST_CASE("IsBranchManifest - detection", "[branch][helpers]")
{
    // Manifest files (no timestamp)
    REQUIRE(eloqstore::IsBranchManifest("manifest_main_5"));
    REQUIRE(eloqstore::IsBranchManifest("manifest_feature_10"));

    // Archive files (with timestamp) should return false
    REQUIRE_FALSE(eloqstore::IsBranchManifest("manifest_main_5_123456"));
    REQUIRE_FALSE(eloqstore::IsBranchManifest("manifest_feature_10_789012"));

    // Non-manifest files
    REQUIRE_FALSE(eloqstore::IsBranchManifest("data_123_main_5"));
    REQUIRE_FALSE(eloqstore::IsBranchManifest("CURRENT_TERM.main"));
    REQUIRE_FALSE(eloqstore::IsBranchManifest("invalid"));
}

TEST_CASE("IsBranchArchive - detection", "[branch][helpers]")
{
    // Archive files (with timestamp)
    REQUIRE(eloqstore::IsBranchArchive("manifest_main_5_123456"));
    REQUIRE(eloqstore::IsBranchArchive("manifest_feature_10_789012"));

    // Manifest files (no timestamp) should return false
    REQUIRE_FALSE(eloqstore::IsBranchArchive("manifest_main_5"));
    REQUIRE_FALSE(eloqstore::IsBranchArchive("manifest_feature_10"));

    // Non-manifest files
    REQUIRE_FALSE(eloqstore::IsBranchArchive("data_123_main_5"));
    REQUIRE_FALSE(eloqstore::IsBranchArchive("CURRENT_TERM.main"));
    REQUIRE_FALSE(eloqstore::IsBranchArchive("invalid"));
}

TEST_CASE("IsBranchDataFile - detection", "[branch][helpers]")
{
    // Valid data files
    REQUIRE(eloqstore::IsBranchDataFile("data_123_main_5"));
    REQUIRE(eloqstore::IsBranchDataFile("data_456_feature_10"));
    REQUIRE(eloqstore::IsBranchDataFile("data_0_main_0"));

    // Non-data files
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("manifest_main_5"));
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("manifest_main_5_123456"));
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("CURRENT_TERM.main"));
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("invalid"));

    // Old format (should fail)
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("data_123_5"));
    REQUIRE_FALSE(eloqstore::IsBranchDataFile("data_123"));
}

// ============================================================================
// Integration Tests - Updated Existing Functions
// ============================================================================

TEST_CASE("ManifestTermFromFilename - branch aware", "[branch][integration]")
{
    // Should extract term from new branch format
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_main_5") == 5);
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_feature_10") == 10);
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_main_0") == 0);

    // With timestamp (archive)
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_main_5_123456") == 5);

    // Invalid formats should return 0
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_5") ==
            0);  // old format
    REQUIRE(eloqstore::ManifestTermFromFilename("invalid") == 0);
    REQUIRE(eloqstore::ManifestTermFromFilename("") == 0);
}

TEST_CASE("IsArchiveFile - branch aware", "[branch][integration]")
{
    // Archive files (with timestamp)
    REQUIRE(eloqstore::IsArchiveFile("manifest_main_5_123456"));
    REQUIRE(eloqstore::IsArchiveFile("manifest_feature_10_789012"));

    // Non-archive manifest files
    REQUIRE_FALSE(eloqstore::IsArchiveFile("manifest_main_5"));
    REQUIRE_FALSE(eloqstore::IsArchiveFile("manifest_feature_10"));

    // Old format should fail
    REQUIRE_FALSE(eloqstore::IsArchiveFile("manifest_5_123456"));
    REQUIRE_FALSE(eloqstore::IsArchiveFile("manifest_5"));

    // Other files
    REQUIRE_FALSE(eloqstore::IsArchiveFile("data_123_main_5"));
    REQUIRE_FALSE(eloqstore::IsArchiveFile("invalid"));
}

// ============================================================================
// BranchFileMapping Tests
// ============================================================================

TEST_CASE("BranchFileRange - sorting and comparison", "[branch][mapping]")
{
    eloqstore::BranchFileRange r1{"main", 5, 100};
    eloqstore::BranchFileRange r2{"feature", 3, 50};
    eloqstore::BranchFileRange r3{"hotfix", 1, 200};

    // Sort by max_file_id
    std::vector<eloqstore::BranchFileRange> ranges = {r1, r2, r3};
    std::sort(ranges.begin(), ranges.end());

    REQUIRE(ranges[0].branch_name_ == "feature");
    REQUIRE(ranges[0].max_file_id_ == 50);
    REQUIRE(ranges[1].branch_name_ == "main");
    REQUIRE(ranges[1].max_file_id_ == 100);
    REQUIRE(ranges[2].branch_name_ == "hotfix");
    REQUIRE(ranges[2].max_file_id_ == 200);
}

TEST_CASE("BranchFileMapping - binary search lookup", "[branch][mapping]")
{
    eloqstore::BranchFileMapping mapping;

    // Add ranges (must be sorted by max_file_id)
    mapping.push_back({"main", 5, 100});
    mapping.push_back({"feature", 3, 50});
    mapping.push_back({"hotfix", 1, 200});

    // Sort by max_file_id
    std::sort(mapping.begin(), mapping.end());

    // Test FindBranchRange
    auto it1 = eloqstore::FindBranchRange(mapping, eloqstore::DataFileKey(25));
    REQUIRE(it1 != mapping.end());
    REQUIRE(it1->branch_name_ == "feature");
    REQUIRE(it1->term_ == 3);

    auto it2 = eloqstore::FindBranchRange(mapping, eloqstore::DataFileKey(75));
    REQUIRE(it2 != mapping.end());
    REQUIRE(it2->branch_name_ == "main");

    auto it3 = eloqstore::FindBranchRange(mapping, eloqstore::DataFileKey(150));
    REQUIRE(it3 != mapping.end());
    REQUIRE(it3->branch_name_ == "hotfix");

    // Beyond max range
    auto it4 = eloqstore::FindBranchRange(mapping, eloqstore::DataFileKey(300));
    REQUIRE(it4 == mapping.end());
}

TEST_CASE("BranchFileMapping - GetBranchNameAndTerm", "[branch][mapping]")
{
    eloqstore::BranchFileMapping mapping;
    mapping.push_back({"main", 5, 100});
    mapping.push_back({"feature", 3, 50});
    mapping.push_back({"hotfix", 1, 200});
    std::sort(mapping.begin(), mapping.end());

    std::string branch;
    uint64_t term;

    REQUIRE(eloqstore::GetBranchNameAndTerm(
                mapping, eloqstore::DataFileKey(25), branch, term) == true);
    REQUIRE(branch == "feature");
    REQUIRE(term == 3);

    REQUIRE(eloqstore::GetBranchNameAndTerm(
                mapping, eloqstore::DataFileKey(75), branch, term) == true);
    REQUIRE(branch == "main");
    REQUIRE(term == 5);

    REQUIRE(eloqstore::GetBranchNameAndTerm(
                mapping, eloqstore::DataFileKey(150), branch, term) == true);
    REQUIRE(branch == "hotfix");
    REQUIRE(term == 1);

    REQUIRE(eloqstore::GetBranchNameAndTerm(
                mapping, eloqstore::DataFileKey(300), branch, term) ==
            false);  // Beyond range
}

TEST_CASE("BranchFileMapping - FileIdInBranch", "[branch][mapping]")
{
    eloqstore::BranchFileMapping mapping;
    mapping.push_back({"main", 5, 100});
    mapping.push_back({"feature", 3, 50});
    mapping.push_back({"hotfix", 1, 200});
    std::sort(mapping.begin(), mapping.end());

    // Test FileIdInBranch
    REQUIRE(eloqstore::FileIdInBranch(
                mapping, eloqstore::DataFileKey(25), "feature") == true);
    REQUIRE(eloqstore::FileIdInBranch(
                mapping, eloqstore::DataFileKey(25), "main") == false);
    REQUIRE(eloqstore::FileIdInBranch(
                mapping, eloqstore::DataFileKey(75), "main") == true);
    REQUIRE(eloqstore::FileIdInBranch(
                mapping, eloqstore::DataFileKey(75), "feature") == false);
    REQUIRE(eloqstore::FileIdInBranch(
                mapping, eloqstore::DataFileKey(150), "hotfix") == true);
    REQUIRE(eloqstore::FileIdInBranch(mapping,
                                      eloqstore::DataFileKey(300),
                                      "hotfix") == false);  // Beyond range
}

TEST_CASE("BranchFileMapping - serialization roundtrip", "[branch][mapping]")
{
    eloqstore::BranchFileMapping original;
    original.push_back({"main", 5, 100});
    original.push_back({"feature", 3, 50});
    original.push_back({"hotfix", 1, 200});

    // Serialize
    std::string serialized = eloqstore::SerializeBranchFileMapping(original);

    // Deserialize
    eloqstore::BranchFileMapping deserialized =
        eloqstore::DeserializeBranchFileMapping(serialized);

    // Verify
    REQUIRE(deserialized.size() == 3);
    REQUIRE(deserialized[0].branch_name_ == "main");
    REQUIRE(deserialized[0].term_ == 5);
    REQUIRE(deserialized[0].max_file_id_ == 100);
    REQUIRE(deserialized[1].branch_name_ == "feature");
    REQUIRE(deserialized[1].term_ == 3);
    REQUIRE(deserialized[1].max_file_id_ == 50);
    REQUIRE(deserialized[2].branch_name_ == "hotfix");
    REQUIRE(deserialized[2].term_ == 1);
    REQUIRE(deserialized[2].max_file_id_ == 200);
}

TEST_CASE("BranchFileMapping - empty mapping", "[branch][mapping]")
{
    eloqstore::BranchFileMapping empty;

    // Serialize empty
    std::string serialized = eloqstore::SerializeBranchFileMapping(empty);
    eloqstore::BranchFileMapping deserialized =
        eloqstore::DeserializeBranchFileMapping(serialized);
    REQUIRE(deserialized.size() == 0);

    // Lookup in empty mapping
    REQUIRE(eloqstore::FindBranchRange(empty, eloqstore::DataFileKey(50)) ==
            empty.end());
    std::string branch;
    uint64_t term;
    REQUIRE(eloqstore::GetBranchNameAndTerm(
                empty, eloqstore::DataFileKey(50), branch, term) == false);
    REQUIRE(eloqstore::FileIdInBranch(
                empty, eloqstore::DataFileKey(50), "main") == false);
}
