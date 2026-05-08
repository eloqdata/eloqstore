#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "manifest_buffer.h"
#include "types.h"

namespace eloqstore
{
constexpr uint32_t num_reserved_fd = 100;

// ParseFileName: splits filename into type and suffix
// Returns {type, suffix} where:
//   - type is the prefix before first separator (e.g., "data", "manifest")
//   - suffix is everything after first separator (or empty if no separator)
// Examples:
//   "data_123" -> {"data", "123"}
//   "data_123_5" -> {"data", "123_5"}
//   "manifest" -> {"manifest", ""}
//   "manifest_5" -> {"manifest", "5"}
//   "manifest_5_123456789" -> {"manifest", "5_123456789"}
inline std::pair<std::string_view, std::string_view> ParseFileName(
    std::string_view name)
{
    size_t pos = name.find(FileNameSeparator);
    std::string_view file_type;
    std::string_view suffix;

    if (pos == std::string::npos)
    {
        file_type = name;
        suffix = std::string_view{};
    }
    else
    {
        file_type = name.substr(0, pos);
        suffix = name.substr(pos + 1);
    }

    return {file_type, suffix};
}

// Helper function to parse a number from string_view
inline bool ParseUint64(std::string_view str, uint64_t &out)
{
    if (str.empty())
    {
        return false;
    }
    errno = 0;
    char *end = nullptr;
    out = std::strtoull(str.data(), &end, 10);
    if (errno != 0 || end != str.data() + str.size())
    {
        return false;
    }
    return true;
}

// Validates and normalizes branch name
// Valid pattern: [a-zA-Z0-9-]+ (alphanumeric and hyphen only, NO underscore)
// Underscore is reserved as FileNameSeparator
// Converts to lowercase
// Returns normalized name if valid, empty string if invalid
inline std::string NormalizeBranchName(std::string_view branch_name)
{
    if (branch_name.empty())
    {
        LOG(WARNING) << "Branch name is empty";
        return "";
    }

    std::string normalized;
    normalized.reserve(branch_name.size());

    for (char c : branch_name)
    {
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')
        {
            normalized.push_back(c);
        }
        else if (c >= 'A' && c <= 'Z')
        {
            // Convert uppercase to lowercase
            normalized.push_back(c + ('a' - 'A'));
        }
        else
        {
            // Invalid character (including underscore which is reserved as
            // separator)
            LOG(WARNING) << "Invalid character in branch name: '" << branch_name
                         << "' (contains '" << c << "')";
            return "";
        }
    }

    return normalized;
}

// Validates branch name (case-insensitive)
// Returns true if valid, false otherwise
// Valid pattern: [a-zA-Z0-9-]+ (alphanumeric and hyphen, case-insensitive)
inline bool IsValidBranchName(std::string_view branch_name)
{
    if (branch_name.empty())
    {
        return false;
    }

    for (char c : branch_name)
    {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-')
        {
            continue;  // Valid character
        }
        else
        {
            return false;  // Invalid character (dot, space, underscore, etc.)
        }
    }

    return true;  // All characters valid and not empty
}

// ParseDataFileSuffix: parses suffix from data file name
// Input suffix formats:
//   "123_main_5" -> file_id=123, branch_name="main", term=5
//   "123_feature_5" -> file_id=123, branch_name="feature", term=5
// Returns true on success, false on parse error
// Note: branch_name is output as string_view (no allocation)
inline bool ParseDataFileSuffix(std::string_view suffix,
                                FileId &file_id,
                                std::string_view &branch_name,
                                uint64_t &term)
{
    file_id = 0;
    term = 0;

    if (suffix.empty())
    {
        return false;
    }

    // Format: <file_id>_<branch_name>_<term>
    // Since underscore is reserved as separator, branch_name cannot contain
    // underscores Simple left-to-right parsing: find first two separators

    // Find first separator (after file_id)
    size_t first_sep = suffix.find(FileNameSeparator);
    if (first_sep == std::string::npos)
    {
        return false;
    }

    // Find second separator (after branch_name)
    size_t second_sep = suffix.find(FileNameSeparator, first_sep + 1);
    if (second_sep == std::string::npos)
    {
        return false;
    }

    // Extract components
    std::string_view file_id_str = suffix.substr(0, first_sep);
    std::string_view branch_str =
        suffix.substr(first_sep + 1, second_sep - first_sep - 1);
    std::string_view term_str = suffix.substr(second_sep + 1);

    // Validate and parse file_id
    uint64_t parsed_id = 0;
    if (!ParseUint64(file_id_str, parsed_id))
    {
        return false;
    }

    // Validate branch_name - files contain already-normalized names
    if (!IsValidBranchName(branch_str))
    {
        return false;  // Invalid branch name
    }

    // Validate and parse term
    uint64_t parsed_term = 0;
    if (!ParseUint64(term_str, parsed_term))
    {
        return false;
    }

    // Success
    file_id = static_cast<FileId>(parsed_id);
    branch_name = branch_str;
    term = parsed_term;
    return true;
}

// ParseManifestFileSuffix: parses suffix from manifest file name
// Input suffix formats:
//   "main_5" -> branch_name="main", term=5, tag=nullopt
//   "feature_5" -> branch_name="feature", term=5, tag=nullopt
//   "main_5_backup-2026-03-05" -> branch_name="main", term=5,
//   tag="backup-2026-03-05"
// Returns true on success, false on parse error
// Note: branch_name is output as string_view (no allocation)
inline bool ParseManifestFileSuffix(std::string_view suffix,
                                    std::string_view &branch_name,
                                    uint64_t &term,
                                    std::optional<std::string> &tag)
{
    term = 0;
    tag.reset();

    if (suffix.empty())
    {
        return false;
    }

    // Format: <branch_name>_<term> or <branch_name>_<term>_<tag>
    // Since underscore is reserved as separator, branch_name cannot contain
    // underscores. Simple left-to-right parsing.

    // Find first separator (after branch_name)
    size_t first_sep = suffix.find(FileNameSeparator);
    if (first_sep == std::string::npos)
    {
        return false;
    }

    // Extract and validate branch_name
    std::string_view branch_str = suffix.substr(0, first_sep);
    // Validate branch_name - files contain already-normalized names
    if (!IsValidBranchName(branch_str))
    {
        return false;  // Invalid branch name
    }

    // Reject old format: If branch_str is purely numeric, it's old format
    uint64_t dummy = 0;
    if (ParseUint64(branch_str, dummy))
    {
        // Branch name cannot be purely numeric - this is old format
        return false;
    }

    // Find second separator (for tag, if present)
    std::string_view remainder = suffix.substr(first_sep + 1);
    size_t second_sep = remainder.find(FileNameSeparator);

    if (second_sep == std::string::npos)
    {
        // Format: <branch_name>_<term>
        uint64_t parsed_term = 0;
        if (!ParseUint64(remainder, parsed_term))
        {
            return false;
        }
        branch_name = branch_str;
        term = parsed_term;
        return true;
    }

    // Format: <branch_name>_<term>_<tag>
    std::string_view term_str = remainder.substr(0, second_sep);
    std::string_view tag_str = remainder.substr(second_sep + 1);

    uint64_t parsed_term = 0;
    if (!ParseUint64(term_str, parsed_term) || tag_str.empty())
    {
        return false;
    }

    branch_name = branch_str;
    term = parsed_term;
    tag = std::string(tag_str);
    return true;
}

// Helper: extract manifest term directly from full filename.
// - For non-manifest files, returns 0.
// - For manifest filenames, uses ParseFileName + ParseManifestFileSuffix.
// - On parse error, returns 0.
inline uint64_t ManifestTermFromFilename(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameManifest)
    {
        return 0;
    }

    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, tag))
    {
        return 0;
    }
    return term;
}

// Term-aware DataFileName
inline std::string DataFileName(FileId file_id, uint64_t term)
{
    // Always use term-aware format: data_<id>_<term> (including term=0).
    std::string name;
    name.reserve(std::size(FileNameData) + 22);
    name.append(FileNameData);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// Branch-aware SegmentFileName: segment_<id>_<branch>_<term>
inline std::string SegmentFileName(FileId file_id,
                                   std::string_view branch_name,
                                   uint64_t term)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return "";
    }
    std::string name;
    name.reserve(std::size(FileNameSegment) + normalized_branch.size() + 32);
    name.append(FileNameSegment);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    name.push_back(FileNameSeparator);
    name.append(normalized_branch);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// ParseSegmentFileSuffix: parses suffix from segment file name
// Same format as data file suffix: "<file_id>_<branch_name>_<term>"
inline bool ParseSegmentFileSuffix(std::string_view suffix,
                                   FileId &file_id,
                                   std::string_view &branch_name,
                                   uint64_t &term)
{
    return ParseDataFileSuffix(suffix, file_id, branch_name, term);
}

// ManifestFileName - generates manifest filename with term suffix
inline std::string ManifestFileName(uint64_t term)
{
    // Always use term-aware format: manifest_<term> (including term=0).
    std::string name;
    name.reserve(std::size(FileNameManifest) + 11);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// ArchiveName: generates term-aware archive filename
// Format: manifest_<term>_<ts>
// Note: term must be provided (use 0 for legacy compatibility if needed)
inline std::string ArchiveName(uint64_t term, uint64_t ts)
{
    std::string name;
    name.reserve(std::size(FileNameManifest) + 31);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    name.push_back(FileNameSeparator);
    name.append(std::to_string(ts));
    return name;
}

inline bool IsArchiveFile(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameManifest)
    {
        return false;
    }
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> tag;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, tag))
    {
        return false;
    }
    return tag.has_value();
}

// ParseCurrentTermFilename: parses CURRENT_TERM_<branch>_<pg_id> filename
// Input formats:
//   "CURRENT_TERM_main_0" -> branch_name="main", pg_id=0
//   "CURRENT_TERM_feature_3" -> branch_name="feature", pg_id=3
// Returns true on success, false on parse error
// Note: branch_name is output as string_view (no allocation)
inline bool ParseCurrentTermFilename(std::string_view filename,
                                     std::string_view &branch_name,
                                     PartitonGroupId &pg_id)
{
    // Check if filename starts with CURRENT_TERM prefix
    constexpr std::string_view prefix = CurrentTermFileName;
    if (filename.size() <= prefix.size() ||
        filename.substr(0, prefix.size()) != prefix)
    {
        return false;
    }

    // Check for underscore separator after CURRENT_TERM
    if (filename[prefix.size()] != FileNameSeparator)
    {
        return false;
    }

    // Remainder is "<branch>_<pg_id>"
    std::string_view remainder = filename.substr(prefix.size() + 1);

    // Find the last underscore to split branch from pg_id
    // Branch names don't contain underscores, so last '_' is the separator
    auto last_sep = remainder.rfind(FileNameSeparator);
    if (last_sep == std::string_view::npos || last_sep == 0 ||
        last_sep == remainder.size() - 1)
    {
        return false;
    }

    std::string_view branch_str = remainder.substr(0, last_sep);
    std::string_view pg_id_str = remainder.substr(last_sep + 1);

    // Validate branch name
    if (!IsValidBranchName(branch_str))
    {
        return false;
    }

    // Parse pg_id
    uint32_t parsed_pg_id = 0;
    for (char c : pg_id_str)
    {
        if (c >= '0' && c <= '9')
        {
            uint32_t digit = static_cast<uint32_t>(c - '0');
            if (parsed_pg_id > (UINT32_MAX - digit) / 10)
            {
                return false;  // Overflow
            }
            parsed_pg_id = parsed_pg_id * 10 + digit;
        }
        else
        {
            return false;  // Invalid character in pg_id
        }
    }

    branch_name = branch_str;
    pg_id = parsed_pg_id;
    return true;
}

// Convenience overload: parse only the branch name (ignore pg_id)
inline bool ParseCurrentTermFilename(std::string_view filename,
                                     std::string_view &branch_name)
{
    PartitonGroupId pg_id = 0;
    return ParseCurrentTermFilename(filename, branch_name, pg_id);
}

// Branch-aware data file naming: data_<file_id>_<branch_name>_<term>
inline std::string BranchDataFileName(FileId file_id,
                                      std::string_view branch_name,
                                      uint64_t term)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return "";  // Invalid branch name
    }

    std::string name;
    name.reserve(std::size(FileNameData) + normalized_branch.size() + 32);
    name.append(FileNameData);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    name.push_back(FileNameSeparator);
    name.append(normalized_branch);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// Branch-aware manifest file naming: manifest_<branch_name>_<term>
inline std::string BranchManifestFileName(std::string_view branch_name,
                                          uint64_t term)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return "";  // Invalid branch name
    }

    std::string name;
    name.reserve(std::size(FileNameManifest) + normalized_branch.size() + 16);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(normalized_branch);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// Branch-aware archive naming: manifest_<branch_name>_<term>_<tag>
inline std::string BranchArchiveName(std::string_view branch_name,
                                     uint64_t term,
                                     std::string_view tag)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return "";  // Invalid branch name
    }

    std::string name;
    name.reserve(std::size(FileNameManifest) + normalized_branch.size() + 32);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(normalized_branch);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    name.push_back(FileNameSeparator);
    name.append(tag);
    return name;
}

// Store-level, branch-aware, partition-group-aware CURRENT_TERM file naming:
// CURRENT_TERM_<branch_name>_<pg_id>
inline std::string CurrentTermFileNameForBranchAndPartitionGroup(
    std::string_view branch_name, PartitonGroupId partition_group_id)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    if (normalized_branch.empty())
    {
        return "";  // Invalid branch name
    }

    std::string name;
    name.reserve(std::size(CurrentTermFileName) + normalized_branch.size() +
                 16);
    name.append(CurrentTermFileName);
    name.push_back(FileNameSeparator);
    name.append(normalized_branch);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(partition_group_id));
    return name;
}

// Parse branch term from CURRENT_TERM.<branch_name> file content.
// Returns true on success (parsed value written to `term`), false on
// empty/invalid/overflow input (`term` is left unchanged).
inline bool ParseBranchTerm(std::string_view content, uint64_t &term)
{
    if (content.empty())
    {
        return false;
    }
    uint64_t result = 0;
    // Content should be numeric string (e.g., "0", "5", "10")
    for (char c : content)
    {
        if (c >= '0' && c <= '9')
        {
            uint64_t digit = static_cast<uint64_t>(c - '0');
            if (result > (UINT64_MAX - digit) / 10)
            {
                return false;  // Overflow
            }
            result = result * 10 + digit;
        }
        else
        {
            return false;  // Invalid content
        }
    }
    term = result;
    return true;
}

// Convert term to string for CURRENT_TERM file content
inline std::string TermToString(uint64_t term)
{
    return std::to_string(term);
}

// Check if filename is a branch manifest (not an archive)
inline bool IsBranchManifest(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameManifest)
    {
        return false;
    }
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> ts;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, ts))
    {
        return false;
    }
    return !ts.has_value();
}

// Check if filename is a branch archive
inline bool IsBranchArchive(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameManifest)
    {
        return false;
    }
    std::string_view branch_name;
    uint64_t term = 0;
    std::optional<std::string> ts;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, ts))
    {
        return false;
    }
    return ts.has_value();
}

// Check if filename is a branch data file
inline bool IsBranchDataFile(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameData)
    {
        return false;
    }
    FileId file_id = 0;
    std::string_view branch_name;
    uint64_t term = 0;
    return ParseDataFileSuffix(suffix, file_id, branch_name, term);
}

// Find branch range for a TypedFileId using binary search. Dispatches by
// file class via BranchFileRange::operator<(TypedFileId): data files compare
// against max_file_id, segment files against max_segment_file_id. Both fields
// are non-decreasing across entries, so std::lower_bound is valid for either.
inline BranchFileMapping::const_iterator FindBranchRange(
    const BranchFileMapping &mapping, TypedFileId file_id)
{
    return std::lower_bound(mapping.begin(), mapping.end(), file_id);
}

// Check if file_id belongs to a specific branch.
inline bool FileIdInBranch(const BranchFileMapping &mapping,
                           TypedFileId file_id,
                           std::string_view branch_name)
{
    auto it = FindBranchRange(mapping, file_id);
    if (it == mapping.end())
    {
        return false;
    }
    return it->branch_name_ == branch_name;
}

// Get branch_name and term for a TypedFileId in one lookup.
inline bool GetBranchNameAndTerm(const BranchFileMapping &mapping,
                                 TypedFileId file_id,
                                 std::string &branch_name,
                                 uint64_t &term)
{
    auto it = FindBranchRange(mapping, file_id);
    if (it == mapping.end())
    {
        return false;
    }
    branch_name = it->branch_name_;
    term = it->term_;
    return true;
}

// Serialize BranchFileMapping to string
// Format:
// [num_entries(8B)][per entry: name_len(4B) | name | term(8B) |
//  max_file_id(8B) | max_segment_file_id(8B)]
inline std::string SerializeBranchFileMapping(const BranchFileMapping &mapping)
{
    std::string result;

    uint64_t num_entries = static_cast<uint64_t>(mapping.size());
    result.append(reinterpret_cast<const char *>(&num_entries),
                  sizeof(uint64_t));

    for (const auto &range : mapping)
    {
        uint32_t name_len = static_cast<uint32_t>(range.branch_name_.size());
        result.append(reinterpret_cast<const char *>(&name_len),
                      sizeof(uint32_t));
        result.append(range.branch_name_);

        uint64_t term = range.term_;
        result.append(reinterpret_cast<const char *>(&term), sizeof(uint64_t));

        uint64_t max_file_id = range.max_file_id_;
        result.append(reinterpret_cast<const char *>(&max_file_id),
                      sizeof(uint64_t));

        uint64_t max_segment_file_id = range.max_segment_file_id_;
        result.append(reinterpret_cast<const char *>(&max_segment_file_id),
                      sizeof(uint64_t));
    }

    return result;
}

// Deserialize BranchFileMapping from string_view; returns empty on error.
inline BranchFileMapping DeserializeBranchFileMapping(std::string_view data)
{
    BranchFileMapping mapping;

    if (data.size() < sizeof(uint64_t))
    {
        return mapping;
    }

    uint64_t num_entries = 0;
    std::memcpy(&num_entries, data.data(), sizeof(uint64_t));
    data = data.substr(sizeof(uint64_t));

    for (uint64_t i = 0; i < num_entries; ++i)
    {
        if (data.size() < sizeof(uint32_t))
        {
            return BranchFileMapping{};
        }

        uint32_t name_len = 0;
        std::memcpy(&name_len, data.data(), sizeof(uint32_t));
        data = data.substr(sizeof(uint32_t));

        // term + max_file_id + max_segment_file_id = 3 * uint64_t
        if (data.size() < name_len + sizeof(uint64_t) * 3)
        {
            return BranchFileMapping{};
        }

        BranchFileRange range;
        range.branch_name_ = std::string(data.substr(0, name_len));
        data = data.substr(name_len);

        std::memcpy(&range.term_, data.data(), sizeof(uint64_t));
        data = data.substr(sizeof(uint64_t));

        std::memcpy(&range.max_file_id_, data.data(), sizeof(uint64_t));
        data = data.substr(sizeof(uint64_t));

        std::memcpy(&range.max_segment_file_id_, data.data(), sizeof(uint64_t));
        data = data.substr(sizeof(uint64_t));

        mapping.push_back(std::move(range));
    }

    return mapping;
}

// Serialize BranchManifestMetadata to string
// Format: [branch_name_len(4B)][branch_name][term(8B)][BranchFileMapping]
inline std::string SerializeBranchManifestMetadata(
    const BranchManifestMetadata &metadata)
{
    std::string result;

    // Branch name length (4 bytes)
    uint32_t name_len = static_cast<uint32_t>(metadata.branch_name.size());
    result.append(reinterpret_cast<const char *>(&name_len), sizeof(uint32_t));

    // Branch name
    result.append(metadata.branch_name);

    // Term (8 bytes)
    uint64_t term = metadata.term;
    result.append(reinterpret_cast<const char *>(&term), sizeof(uint64_t));

    // BranchFileMapping
    std::string mapping_str = SerializeBranchFileMapping(metadata.file_ranges);
    result.append(mapping_str);

    return result;
}

// Deserialize BranchManifestMetadata from string_view
// Returns true on success, false on error (metadata is left default-initialized
// on failure).
inline bool DeserializeBranchManifestMetadata(std::string_view data,
                                              BranchManifestMetadata &metadata)
{
    metadata = {};

    if (data.size() < sizeof(uint32_t))
    {
        return false;
    }

    // Branch name length
    uint32_t name_len = 0;
    std::memcpy(&name_len, data.data(), sizeof(uint32_t));
    data = data.substr(sizeof(uint32_t));

    if (data.size() < name_len + sizeof(uint64_t))
    {
        return false;
    }

    // Branch name
    metadata.branch_name = std::string(data.substr(0, name_len));
    data = data.substr(name_len);

    // Term
    std::memcpy(&metadata.term, data.data(), sizeof(uint64_t));
    data = data.substr(sizeof(uint64_t));

    // BranchFileMapping
    metadata.file_ranges = DeserializeBranchFileMapping(data);

    return true;
}

}  // namespace eloqstore
