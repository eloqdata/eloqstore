#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

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
    dst.reserve(mapping.size() * 4);
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

// ParseDataFileSuffix: parses suffix from data file name
// Input suffix formats:
//   "123_5" -> file_id=123, term=5 (term-aware, required)
// Returns true on success, false on parse error
inline bool ParseDataFileSuffix(std::string_view suffix,
                                FileId &file_id,
                                uint64_t &term)
{
    file_id = 0;
    term = 0;

    if (suffix.empty())
    {
        return false;
    }

    // Find separator for term
    size_t sep_pos = suffix.find(FileNameSeparator);
    if (sep_pos == std::string::npos)
    {
        // Legacy format "data_<id>" is no longer supported.
        return false;
    }

    // Term-aware format: file_id_term
    std::string_view file_id_str = suffix.substr(0, sep_pos);
    std::string_view term_str = suffix.substr(sep_pos + 1);

    uint64_t parsed_id = 0;
    uint64_t parsed_term = 0;
    if (ParseUint64(file_id_str, parsed_id) &&
        ParseUint64(term_str, parsed_term))
    {
        file_id = static_cast<FileId>(parsed_id);
        term = parsed_term;
        return true;
    }

    return false;
}

// ParseManifestFileSuffix: parses suffix from manifest file name
// Input suffix formats:
//   "5" -> term=5, timestamp=nullopt (term-aware "manifest_5", required)
//   "5_123456789" -> term=5, timestamp=123456789 (term-aware archive)
// Note: Legacy "manifest_<ts>" format is NOT supported (removed)
// Returns true on success, false on parse error
inline bool ParseManifestFileSuffix(std::string_view suffix,
                                    uint64_t &term,
                                    std::optional<uint64_t> &timestamp)
{
    term = 0;
    timestamp.reset();

    if (suffix.empty())
    {
        // Legacy format "manifest" is no longer supported.
        return false;
    }

    // Find separator for timestamp
    size_t sep_pos = suffix.find(FileNameSeparator);
    if (sep_pos == std::string::npos)
    {
        // Term-only format: "manifest_<term>"
        uint64_t parsed_term = 0;
        if (ParseUint64(suffix, parsed_term))
        {
            term = parsed_term;
            return true;
        }
        return false;
    }

    // Term-aware archive format: "manifest_<term>_<timestamp>"
    std::string_view term_str = suffix.substr(0, sep_pos);
    std::string_view timestamp_str = suffix.substr(sep_pos + 1);

    uint64_t parsed_term = 0;
    uint64_t parsed_ts = 0;
    if (ParseUint64(term_str, parsed_term) &&
        ParseUint64(timestamp_str, parsed_ts))
    {
        term = parsed_term;
        timestamp = parsed_ts;
        return true;
    }

    return false;
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

    uint64_t term = 0;
    std::optional<uint64_t> ts;
    if (!ParseManifestFileSuffix(suffix, term, ts))
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
    uint64_t term = 0;
    std::optional<uint64_t> ts;
    if (!ParseManifestFileSuffix(suffix, term, ts))
    {
        return false;
    }
    return ts.has_value();
}

}  // namespace eloqstore
