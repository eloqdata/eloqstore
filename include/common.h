#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "absl/container/flat_hash_map.h"
#include "coding.h"
#include "manifest_buffer.h"
#include "types.h"

namespace eloqstore
{
constexpr uint32_t num_reserved_fd = 100;

// FileId -> term mapping
using FileIdTermMapping = absl::flat_hash_map<FileId, uint64_t>;

// Serialize FileIdTermMapping to dst (appends to dst)
// Format: Fixed32(bytes length) + pairs of {varint64(file_id) and
// varint64(term)}
inline void SerializeFileIdTermMapping(const FileIdTermMapping &mapping,
                                       std::string &dst)
{
    dst.reserve(mapping.size() << 3);
    // bytes_len(4B)
    dst.resize(4);
    for (const auto &[file_id, term] : mapping)
    {
        PutVarint64(&dst, file_id);
        PutVarint64(&dst, term);
    }
    // update the bytes_len
    uint32_t bytes_len = static_cast<uint32_t>(dst.size() - 4);
    EncodeFixed32(dst.data(), bytes_len);
}

// Deserialize FileIdTermMapping from input; clears mapping on failure
// Returns true on success, false on parse error
inline bool DeserializeFileIdTermMapping(std::string_view input,
                                         FileIdTermMapping &mapping)
{
    if (input.size() < 4)
    {
        return false;
    }
    uint32_t bytes_len = DecodeFixed32(input.data());
    input = input.substr(4, bytes_len);
    if (input.size() != bytes_len)
    {
        return false;
    }
    while (!input.empty())
    {
        uint64_t file_id = 0;
        uint64_t term = 0;
        if (!GetVarint64(&input, &file_id) || !GetVarint64(&input, &term))
        {
            mapping.clear();
            return false;
        }
        mapping[static_cast<FileId>(file_id)] = term;
    }
    return true;
}

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
// Valid pattern: [a-zA-Z0-9_-]+
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
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || 
            c == '_' || c == '-')
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
            // Invalid character
            LOG(WARNING) << "Invalid character in branch name: '" << branch_name 
                         << "' (contains '" << c << "')";
            return "";
        }
    }
    
    return normalized;
}

// Validates branch name without normalization
// Returns true if valid, false otherwise
inline bool IsValidBranchName(std::string_view branch_name)
{
    return !NormalizeBranchName(branch_name).empty();
}

// ParseDataFileSuffix: parses suffix from data file name
// Input suffix formats:
//   "123_main_5" -> file_id=123, branch_name="main", term=5
//   "123_feature_5" -> file_id=123, branch_name="feature", term=5
// Returns true on success, false on parse error
inline bool ParseDataFileSuffix(std::string_view suffix,
                                FileId &file_id,
                                std::string &branch_name,
                                uint64_t &term)
{
    file_id = 0;
    branch_name.clear();
    term = 0;

    if (suffix.empty())
    {
        return false;
    }

    // Format: <file_id>_<branch_name>_<term>
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
    std::string_view branch_str = suffix.substr(first_sep + 1, second_sep - first_sep - 1);
    std::string_view term_str = suffix.substr(second_sep + 1);

    // Validate and parse file_id
    uint64_t parsed_id = 0;
    if (!ParseUint64(file_id_str, parsed_id))
    {
        return false;
    }

    // Validate branch_name
    std::string normalized_branch = NormalizeBranchName(branch_str);
    if (normalized_branch.empty())
    {
        return false;
    }

    // Validate and parse term
    uint64_t parsed_term = 0;
    if (!ParseUint64(term_str, parsed_term))
    {
        return false;
    }

    // Success
    file_id = static_cast<FileId>(parsed_id);
    branch_name = std::move(normalized_branch);
    term = parsed_term;
    return true;
}

// ParseManifestFileSuffix: parses suffix from manifest file name
// Input suffix formats:
//   "main_5" -> branch_name="main", term=5, timestamp=nullopt
//   "feature_5" -> branch_name="feature", term=5, timestamp=nullopt
//   "main_5_123456789" -> branch_name="main", term=5, timestamp=123456789
// Returns true on success, false on parse error
inline bool ParseManifestFileSuffix(std::string_view suffix,
                                    std::string &branch_name,
                                    uint64_t &term,
                                    std::optional<uint64_t> &timestamp)
{
    branch_name.clear();
    term = 0;
    timestamp.reset();

    if (suffix.empty())
    {
        return false;
    }

    // Format: <branch_name>_<term> or <branch_name>_<term>_<timestamp>
    // Find first separator (after branch_name)
    size_t first_sep = suffix.find(FileNameSeparator);
    if (first_sep == std::string::npos)
    {
        return false;
    }

    // Extract branch_name and validate
    std::string_view branch_str = suffix.substr(0, first_sep);
    std::string normalized_branch = NormalizeBranchName(branch_str);
    if (normalized_branch.empty())
    {
        return false;
    }

    // Find second separator (for timestamp, if present)
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
        branch_name = std::move(normalized_branch);
        term = parsed_term;
        return true;
    }

    // Format: <branch_name>_<term>_<timestamp>
    std::string_view term_str = remainder.substr(0, second_sep);
    std::string_view ts_str = remainder.substr(second_sep + 1);

    uint64_t parsed_term = 0;
    uint64_t parsed_ts = 0;
    if (!ParseUint64(term_str, parsed_term) || !ParseUint64(ts_str, parsed_ts))
    {
        return false;
    }

    branch_name = std::move(normalized_branch);
    term = parsed_term;
    timestamp = parsed_ts;
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

    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, ts))
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
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    if (!ParseManifestFileSuffix(suffix, branch_name, term, ts))
    {
        return false;
    }
    return ts.has_value();
}

// ParseCurrentTermFilename: parses CURRENT_TERM filename
// Input formats:
//   "CURRENT_TERM.main" -> branch_name="main"
//   "CURRENT_TERM.feature" -> branch_name="feature"
// Returns true on success, false on parse error
inline bool ParseCurrentTermFilename(std::string_view filename,
                                     std::string &branch_name)
{
    branch_name.clear();

    // Check if filename starts with CURRENT_TERM prefix
    constexpr std::string_view prefix = CurrentTermFileName;
    if (filename.size() <= prefix.size() ||
        filename.substr(0, prefix.size()) != prefix)
    {
        return false;
    }

    // Check for separator (dot)
    if (filename[prefix.size()] != '.')
    {
        return false;
    }

    // Extract branch name after separator
    std::string_view branch_str = filename.substr(prefix.size() + 1);
    std::string normalized_branch = NormalizeBranchName(branch_str);
    if (normalized_branch.empty())
    {
        return false;
    }

    branch_name = std::move(normalized_branch);
    return true;
}

// Branch-aware data file naming: data_<file_id>_<branch_name>_<term>
inline std::string BranchDataFileName(FileId file_id,
                                      std::string_view branch_name,
                                      uint64_t term)
{
    std::string name;
    name.reserve(std::size(FileNameData) + branch_name.size() + 32);
    name.append(FileNameData);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    name.push_back(FileNameSeparator);
    name.append(branch_name);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// Branch-aware manifest file naming: manifest_<branch_name>_<term>
inline std::string BranchManifestFileName(std::string_view branch_name,
                                          uint64_t term)
{
    std::string name;
    name.reserve(std::size(FileNameManifest) + branch_name.size() + 16);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(branch_name);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

// Branch-aware archive naming: manifest_<branch_name>_<term>_<ts>
inline std::string BranchArchiveName(std::string_view branch_name,
                                     uint64_t term,
                                     uint64_t ts)
{
    std::string name;
    name.reserve(std::size(FileNameManifest) + branch_name.size() + 32);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(branch_name);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    name.push_back(FileNameSeparator);
    name.append(std::to_string(ts));
    return name;
}

// Branch-aware CURRENT_TERM file naming: CURRENT_TERM.<branch_name>
inline std::string BranchCurrentTermFileName(std::string_view branch_name)
{
    std::string name;
    name.reserve(std::size(CurrentTermFileName) + branch_name.size() + 1);
    name.append(CurrentTermFileName);
    name.push_back('.');
    name.append(branch_name);
    return name;
}

// Check if filename is a branch manifest (not an archive)
inline bool IsBranchManifest(std::string_view filename)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type != FileNameManifest)
    {
        return false;
    }
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
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
    std::string branch_name;
    uint64_t term = 0;
    std::optional<uint64_t> ts;
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
    std::string branch_name;
    uint64_t term = 0;
    return ParseDataFileSuffix(suffix, file_id, branch_name, term);
}

}  // namespace eloqstore
