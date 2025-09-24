#include <unordered_map>

#include "types.h"

namespace eloqstore
{
constexpr uint32_t num_reserved_fd = 100;

inline std::pair<std::string_view, std::string_view> ParseFileName(
    std::string_view name)
{
    size_t pos = name.find(FileNameSeparator);
    std::string_view file_type;
    std::string_view file_id;

    if (pos == std::string::npos)
    {
        file_type = name;
    }
    else
    {
        file_type = name.substr(0, pos);
        file_id = name.substr(pos + 1);
    }

    return {file_type, file_id};
}

inline std::string DataFileName(FileId file_id)
{
    std::string name;
    name.reserve(std::size(FileNameData) + 11);
    name.append(FileNameData);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    return name;
}

inline std::string ArchiveName(uint64_t ts)
{
    std::string name;
    name.reserve(std::size(FileNameManifest) + 20);
    name.append(FileNameManifest);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(ts));
    return name;
}
}  // namespace eloqstore