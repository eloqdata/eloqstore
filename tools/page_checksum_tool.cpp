#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "../kv_options.h"
#include "../page.h"

// usage: ./page_checksum_tool data_file_path offset
namespace
{
using namespace eloqstore;

std::pair<bool, uint64_t> ParseUint64(const std::string &input)
{
    try
    {
        size_t idx = 0;
        uint64_t value = std::stoull(input, &idx, 0);
        if (idx != input.size())
        {
            return {false, 0};
        }
        return {true, value};
    }
    catch (const std::exception &)
    {
        return {false, 0};
    }
}

void PrintUsage(const char *prog)
{
    std::cerr
        << "Usage: " << prog
        << " <file_path> <offset_bytes> [page_size_bytes]\n"
        << "Offset and page size accept decimal or 0x-prefixed hex values.\n";
}

}  // namespace

int main(int argc, char **argv)
{
    using namespace eloqstore;
    if (argc < 3 || argc > 4)
    {
        PrintUsage(argv[0]);
        return 1;
    }

    const char *path = argv[1];
    auto [offset_ok, offset] = ParseUint64(argv[2]);
    if (!offset_ok)
    {
        std::cerr << "Invalid offset: " << argv[2] << "\n";
        return 1;
    }

    uint64_t page_size = KvOptions{}.data_page_size;
    if (argc == 4)
    {
        auto [size_ok, parsed_size] = ParseUint64(argv[3]);
        if (!size_ok || parsed_size == 0)
        {
            std::cerr << "Invalid page size: " << argv[3] << "\n";
            return 1;
        }
        page_size = parsed_size;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open " << path << ": " << std::strerror(errno)
                  << "\n";
        return 1;
    }

    file.seekg(0, std::ios::end);
    const auto file_size = static_cast<uint64_t>(file.tellg());
    if (offset + page_size > file_size)
    {
        std::cerr << "Requested range [" << offset << ", "
                  << (offset + page_size) << ") exceeds file size " << file_size
                  << "\n";
        return 1;
    }

    std::vector<char> buffer(page_size);
    file.seekg(offset, std::ios::beg);
    file.read(buffer.data(), static_cast<std::streamsize>(page_size));
    if (file.gcount() != static_cast<std::streamsize>(page_size))
    {
        std::cerr << "Unable to read " << page_size << " bytes at offset "
                  << offset << "\n";
        return 1;
    }

    const bool valid =
        ValidateChecksum(std::string_view(buffer.data(), page_size));
    std::cout << (valid ? "Checksum OK" : "Checksum FAILED")
              << " for page at offset " << offset << "\n";

    std::cout << "Page bytes (offset:value)" << '\n';
    constexpr size_t bytes_per_row = 16;
    for (size_t i = 0; i < page_size; i += bytes_per_row)
    {
        std::cout << std::hex << std::setw(6) << std::setfill('0') << i << ": ";
        for (size_t j = 0; j < bytes_per_row && i + j < page_size; ++j)
        {
            uint8_t value = static_cast<uint8_t>(buffer[i + j]);
            std::cout << std::setw(2) << static_cast<int>(value) << ' ';
        }
        std::cout << std::dec << '\n';
    }

    return valid ? 0 : 2;
}
