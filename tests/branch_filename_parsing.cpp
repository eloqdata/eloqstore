#include <catch2/catch_test_macros.hpp>
#include <string>
#include <string_view>

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
    
    // With hyphens
    REQUIRE(eloqstore::NormalizeBranchName("feature-branch") == "feature-branch");
    REQUIRE(eloqstore::NormalizeBranchName("my-feature-123") == "my-feature-123");
    
    // With underscores
    REQUIRE(eloqstore::NormalizeBranchName("feature_branch") == "feature_branch");
    REQUIRE(eloqstore::NormalizeBranchName("my_feature_123") == "my_feature_123");
    
    // Mixed valid characters
    REQUIRE(eloqstore::NormalizeBranchName("feat_123-dev") == "feat_123-dev");
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
    REQUIRE(eloqstore::NormalizeBranchName("Feature-Branch") == "feature-branch");
    REQUIRE(eloqstore::NormalizeBranchName("Feature_123") == "feature_123");
}

TEST_CASE("NormalizeBranchName - invalid characters", "[branch][validation]")
{
    // Empty string
    REQUIRE(eloqstore::NormalizeBranchName("") == "");
    
    // Invalid special characters
    REQUIRE(eloqstore::NormalizeBranchName("feature branch") == ""); // space
    REQUIRE(eloqstore::NormalizeBranchName("feature.branch") == ""); // dot
    REQUIRE(eloqstore::NormalizeBranchName("feature@branch") == ""); // @
    REQUIRE(eloqstore::NormalizeBranchName("feature#branch") == ""); // #
    REQUIRE(eloqstore::NormalizeBranchName("feature$branch") == ""); // $
    REQUIRE(eloqstore::NormalizeBranchName("feature/branch") == ""); // /
    REQUIRE(eloqstore::NormalizeBranchName("feature\\branch") == ""); // backslash
    REQUIRE(eloqstore::NormalizeBranchName("feature:branch") == ""); // colon
}

TEST_CASE("NormalizeBranchName - edge cases", "[branch][validation]")
{
    // Single character
    REQUIRE(eloqstore::NormalizeBranchName("a") == "a");
    REQUIRE(eloqstore::NormalizeBranchName("A") == "a");
    REQUIRE(eloqstore::NormalizeBranchName("1") == "1");
    REQUIRE(eloqstore::NormalizeBranchName("-") == "-");
    REQUIRE(eloqstore::NormalizeBranchName("_") == "_");
    
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
    REQUIRE(eloqstore::IsValidBranchName("my_feature"));
    
    // Invalid names
    REQUIRE_FALSE(eloqstore::IsValidBranchName(""));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature branch"));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature.branch"));
    REQUIRE_FALSE(eloqstore::IsValidBranchName("feature@123"));
}

// ============================================================================
// File Generation Tests
// ============================================================================

TEST_CASE("BranchDataFileName - format verification", "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchDataFileName(123, "main", 5) == "data_123_main_5");
    REQUIRE(eloqstore::BranchDataFileName(456, "feature", 10) == "data_456_feature_10");
    
    // Zero values
    REQUIRE(eloqstore::BranchDataFileName(0, "main", 0) == "data_0_main_0");
    REQUIRE(eloqstore::BranchDataFileName(0, "feature", 1) == "data_0_feature_1");
    
    // Large values
    REQUIRE(eloqstore::BranchDataFileName(999999, "main", 123456) == "data_999999_main_123456");
    
    // Different branch names
    REQUIRE(eloqstore::BranchDataFileName(10, "dev", 1) == "data_10_dev_1");
    REQUIRE(eloqstore::BranchDataFileName(10, "feature-123", 1) == "data_10_feature-123_1");
    REQUIRE(eloqstore::BranchDataFileName(10, "my_branch", 1) == "data_10_my_branch_1");
}

TEST_CASE("BranchManifestFileName - format verification", "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchManifestFileName("main", 5) == "manifest_main_5");
    REQUIRE(eloqstore::BranchManifestFileName("feature", 10) == "manifest_feature_10");
    
    // Zero term
    REQUIRE(eloqstore::BranchManifestFileName("main", 0) == "manifest_main_0");
    
    // Large term
    REQUIRE(eloqstore::BranchManifestFileName("main", 123456789) == "manifest_main_123456789");
    
    // Different branch names
    REQUIRE(eloqstore::BranchManifestFileName("dev", 1) == "manifest_dev_1");
    REQUIRE(eloqstore::BranchManifestFileName("feature-123", 2) == "manifest_feature-123_2");
}

TEST_CASE("BranchArchiveName - format verification", "[branch][generation]")
{
    // Basic format
    REQUIRE(eloqstore::BranchArchiveName("main", 5, 123456) == "manifest_main_5_123456");
    REQUIRE(eloqstore::BranchArchiveName("feature", 10, 789012) == "manifest_feature_10_789012");
    
    // Zero values
    REQUIRE(eloqstore::BranchArchiveName("main", 0, 0) == "manifest_main_0_0");
    
    // Large values
    REQUIRE(eloqstore::BranchArchiveName("main", 999, 1234567890123ULL) == 
            "manifest_main_999_1234567890123");
}

TEST_CASE("BranchCurrentTermFileName - dot separator", "[branch][generation]")
{
    // Verify dot separator (not underscore)
    REQUIRE(eloqstore::BranchCurrentTermFileName("main") == "CURRENT_TERM.main");
    REQUIRE(eloqstore::BranchCurrentTermFileName("feature") == "CURRENT_TERM.feature");
    REQUIRE(eloqstore::BranchCurrentTermFileName("dev") == "CURRENT_TERM.dev");
    REQUIRE(eloqstore::BranchCurrentTermFileName("feature-123") == "CURRENT_TERM.feature-123");
    
    // Verify it starts with CURRENT_TERM constant
    std::string result = eloqstore::BranchCurrentTermFileName("main");
    REQUIRE(result.find(eloqstore::CurrentTermFileName) == 0);
    REQUIRE(result.find('.') != std::string::npos);
}

// ============================================================================
// Parsing Tests - ParseDataFileSuffix
// ============================================================================

TEST_CASE("ParseDataFileSuffix - branch format", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string branch_name;
    uint64_t term = 0;
    
    // Valid format: file_id_branch_term
    REQUIRE(eloqstore::ParseDataFileSuffix("123_main_5", file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    
    // Different branch
    REQUIRE(eloqstore::ParseDataFileSuffix("456_feature_10", file_id, branch_name, term));
    REQUIRE(file_id == 456);
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    
    // Zero values
    REQUIRE(eloqstore::ParseDataFileSuffix("0_main_0", file_id, branch_name, term));
    REQUIRE(file_id == 0);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 0);
    
    // Branch with hyphen
    REQUIRE(eloqstore::ParseDataFileSuffix("10_feature-123_5", file_id, branch_name, term));
    REQUIRE(file_id == 10);
    REQUIRE(branch_name == "feature-123");
    REQUIRE(term == 5);
    
    // Branch with underscore
    REQUIRE(eloqstore::ParseDataFileSuffix("10_my_branch_5", file_id, branch_name, term));
    REQUIRE(file_id == 10);
    REQUIRE(branch_name == "my_branch");
    REQUIRE(term == 5);
}

TEST_CASE("ParseDataFileSuffix - case normalization during parse", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string branch_name;
    uint64_t term = 0;
    
    // Uppercase branch name should be normalized to lowercase
    REQUIRE(eloqstore::ParseDataFileSuffix("123_MAIN_5", file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main"); // normalized
    REQUIRE(term == 5);
    
    // Mixed case
    REQUIRE(eloqstore::ParseDataFileSuffix("456_Feature_10", file_id, branch_name, term));
    REQUIRE(file_id == 456);
    REQUIRE(branch_name == "feature"); // normalized
    REQUIRE(term == 10);
}

TEST_CASE("ParseDataFileSuffix - old format rejected", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string branch_name;
    uint64_t term = 0;
    
    // Old format: file_id_term (no branch) should fail
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_5", file_id, branch_name, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("0_1", file_id, branch_name, term));
    
    // Even older format: just file_id
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123", file_id, branch_name, term));
}

TEST_CASE("ParseDataFileSuffix - invalid formats", "[branch][parsing]")
{
    eloqstore::FileId file_id = 0;
    std::string branch_name;
    uint64_t term = 0;
    
    // Empty
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("", file_id, branch_name, term));
    
    // Non-numeric file_id
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("abc_main_5", file_id, branch_name, term));
    
    // Non-numeric term
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_main_abc", file_id, branch_name, term));
    
    // Invalid branch name (contains dot)
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_main.branch_5", file_id, branch_name, term));
    
    // Invalid branch name (contains space)
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_main branch_5", file_id, branch_name, term));
    
    // Missing components
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123_main_", file_id, branch_name, term));
    REQUIRE_FALSE(eloqstore::ParseDataFileSuffix("123__5", file_id, branch_name, term));
}

// ============================================================================
// Parsing Tests - ParseManifestFileSuffix
// ============================================================================

TEST_CASE("ParseManifestFileSuffix - branch format without timestamp", "[branch][parsing]")
{
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    
    // Valid format: branch_term
    REQUIRE(eloqstore::ParseManifestFileSuffix("main_5", branch_name, term, ts));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE_FALSE(ts.has_value());
    
    // Different branch
    REQUIRE(eloqstore::ParseManifestFileSuffix("feature_10", branch_name, term, ts));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE_FALSE(ts.has_value());
    
    // Zero term
    REQUIRE(eloqstore::ParseManifestFileSuffix("main_0", branch_name, term, ts));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 0);
    REQUIRE_FALSE(ts.has_value());
}

TEST_CASE("ParseManifestFileSuffix - branch format with timestamp", "[branch][parsing]")
{
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    
    // Valid archive format: branch_term_timestamp
    REQUIRE(eloqstore::ParseManifestFileSuffix("main_5_123456", branch_name, term, ts));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE(ts.has_value());
    REQUIRE(ts.value() == 123456);
    
    // Different values
    REQUIRE(eloqstore::ParseManifestFileSuffix("feature_10_789012", branch_name, term, ts));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE(ts.has_value());
    REQUIRE(ts.value() == 789012);
    
    // Zero timestamp
    REQUIRE(eloqstore::ParseManifestFileSuffix("main_5_0", branch_name, term, ts));
    REQUIRE(ts.has_value());
    REQUIRE(ts.value() == 0);
}

TEST_CASE("ParseManifestFileSuffix - case normalization", "[branch][parsing]")
{
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    
    // Uppercase branch name
    REQUIRE(eloqstore::ParseManifestFileSuffix("MAIN_5", branch_name, term, ts));
    REQUIRE(branch_name == "main"); // normalized
    
    // Mixed case
    REQUIRE(eloqstore::ParseManifestFileSuffix("Feature_10_123", branch_name, term, ts));
    REQUIRE(branch_name == "feature"); // normalized
}

TEST_CASE("ParseManifestFileSuffix - old format rejected", "[branch][parsing]")
{
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    
    // Old format: just term (no branch)
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("5", branch_name, term, ts));
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("0", branch_name, term, ts));
    
    // Old archive format: term_timestamp (no branch)
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("5_123456", branch_name, term, ts));
}

TEST_CASE("ParseManifestFileSuffix - invalid formats", "[branch][parsing]")
{
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    
    // Empty
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("", branch_name, term, ts));
    
    // Non-numeric term
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("main_abc", branch_name, term, ts));
    
    // Non-numeric timestamp
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("main_5_abc", branch_name, term, ts));
    
    // Invalid branch name
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("main.branch_5", branch_name, term, ts));
    
    // Missing components
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("main_", branch_name, term, ts));
    REQUIRE_FALSE(eloqstore::ParseManifestFileSuffix("_5", branch_name, term, ts));
}

// ============================================================================
// Parsing Tests - ParseCurrentTermFilename
// ============================================================================

TEST_CASE("ParseCurrentTermFilename - valid formats", "[branch][parsing]")
{
    std::string branch_name;
    
    // Valid format with dot separator
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.main", branch_name));
    REQUIRE(branch_name == "main");
    
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.feature", branch_name));
    REQUIRE(branch_name == "feature");
    
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.dev", branch_name));
    REQUIRE(branch_name == "dev");
    
    // Branch with hyphen/underscore
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.feature-123", branch_name));
    REQUIRE(branch_name == "feature-123");
    
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.my_branch", branch_name));
    REQUIRE(branch_name == "my_branch");
}

TEST_CASE("ParseCurrentTermFilename - case normalization", "[branch][parsing]")
{
    std::string branch_name;
    
    // Uppercase branch name should be normalized
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.MAIN", branch_name));
    REQUIRE(branch_name == "main");
    
    REQUIRE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.Feature", branch_name));
    REQUIRE(branch_name == "feature");
}

TEST_CASE("ParseCurrentTermFilename - invalid formats", "[branch][parsing]")
{
    std::string branch_name;
    
    // Old format without branch (no dot separator)
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM", branch_name));
    
    // Wrong separator (underscore instead of dot)
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM_main", branch_name));
    
    // Empty branch name
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.", branch_name));
    
    // Invalid branch name (contains invalid char)
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.main.branch", branch_name));
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("CURRENT_TERM.main branch", branch_name));
    
    // Wrong prefix
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("TERM.main", branch_name));
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("current_term.main", branch_name));
    
    // Empty string
    REQUIRE_FALSE(eloqstore::ParseCurrentTermFilename("", branch_name));
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
    std::string branch_name;
    uint64_t term = 0;
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix, file_id, branch_name, term));
    REQUIRE(file_id == 123);
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    
    // Test with different values
    filename = eloqstore::BranchDataFileName(999, "feature-123", 456);
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix2, file_id, branch_name, term));
    REQUIRE(file_id == 999);
    REQUIRE(branch_name == "feature-123");
    REQUIRE(term == 456);
    
    // Test case normalization in roundtrip
    filename = eloqstore::BranchDataFileName(10, "Feature", 1);
    auto [type3, suffix3] = eloqstore::ParseFileName(filename);
    REQUIRE(eloqstore::ParseDataFileSuffix(suffix3, file_id, branch_name, term));
    REQUIRE(file_id == 10);
    REQUIRE(branch_name == "feature"); // Uppercase 'F' in generated, normalized in parse
    REQUIRE(term == 1);
}

TEST_CASE("Roundtrip - manifest files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchManifestFileName("main", 5);
    auto [type, suffix] = eloqstore::ParseFileName(filename);
    
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, branch_name, term, ts));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE_FALSE(ts.has_value());
    
    // Different branch
    filename = eloqstore::BranchManifestFileName("feature", 10);
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix2, branch_name, term, ts));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE_FALSE(ts.has_value());
}

TEST_CASE("Roundtrip - archive files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchArchiveName("main", 5, 123456);
    auto [type, suffix] = eloqstore::ParseFileName(filename);
    
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, branch_name, term, ts));
    REQUIRE(branch_name == "main");
    REQUIRE(term == 5);
    REQUIRE(ts.has_value());
    REQUIRE(ts.value() == 123456);
    
    // Different values
    filename = eloqstore::BranchArchiveName("feature", 10, 789012);
    auto [type2, suffix2] = eloqstore::ParseFileName(filename);
    REQUIRE(eloqstore::ParseManifestFileSuffix(suffix2, branch_name, term, ts));
    REQUIRE(branch_name == "feature");
    REQUIRE(term == 10);
    REQUIRE(ts.has_value());
    REQUIRE(ts.value() == 789012);
}

TEST_CASE("Roundtrip - CURRENT_TERM files", "[branch][roundtrip]")
{
    // Generate -> Parse -> Verify
    std::string filename = eloqstore::BranchCurrentTermFileName("main");
    std::string branch_name;
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name));
    REQUIRE(branch_name == "main");
    
    // Different branch
    filename = eloqstore::BranchCurrentTermFileName("feature");
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name));
    REQUIRE(branch_name == "feature");
    
    // Branch with special chars
    filename = eloqstore::BranchCurrentTermFileName("feature-123");
    REQUIRE(eloqstore::ParseCurrentTermFilename(filename, branch_name));
    REQUIRE(branch_name == "feature-123");
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
    REQUIRE(eloqstore::ManifestTermFromFilename("manifest_5") == 0); // old format
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
