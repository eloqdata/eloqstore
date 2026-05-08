#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "coding.h"
#include "common.h"
#include "storage/page.h"
#include "storage/root_meta.h"
#include "types.h"

namespace
{

void PrintUsage(const char *prog)
{
    std::cerr << "Usage: " << prog << " <manifest_file_path>\n";
}

std::string BytesToHex(std::string_view payload)
{
    static constexpr char kHexDigits[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(payload.size() * 2);
    for (unsigned char byte : payload)
    {
        hex.push_back(kHexDigits[(byte >> 4) & 0xF]);
        hex.push_back(kHexDigits[byte & 0xF]);
    }
    return hex;
}

bool SkipPadding(std::ifstream &file, size_t padding)
{
    if (padding == 0)
    {
        return true;
    }
    file.seekg(static_cast<std::streamoff>(padding), std::ios::cur);
    return static_cast<bool>(file);
}

void PrintBranchMetadata(std::string_view data)
{
    using namespace eloqstore;
    BranchManifestMetadata metadata;
    if (!DeserializeBranchManifestMetadata(data, metadata))
    {
        std::cout << "  branch_metadata: PARSE FAILED\n";
        return;
    }
    std::cout << "  branch_name: \"" << metadata.branch_name << "\"\n";
    std::cout << "  branch_term: " << metadata.term << "\n";
    std::cout << "  file_ranges: " << metadata.file_ranges.size() << "\n";
    for (size_t i = 0; i < metadata.file_ranges.size(); ++i)
    {
        const auto &r = metadata.file_ranges[i];
        std::cout << "    [" << i << "] branch=\"" << r.branch_name_
                  << "\" term=" << r.term_ << " max_file_id=" << r.max_file_id_
                  << "\n";
    }
}

void PrintSnapshotPayload(std::string_view payload)
{
    using namespace eloqstore;

    uint64_t max_fp_id = 0;
    if (!GetVarint64(&payload, &max_fp_id))
    {
        std::cout << "  payload: PARSE FAILED (max_fp_id)\n";
        return;
    }
    std::cout << "  max_fp_id: " << max_fp_id << "\n";

    uint32_t dict_len = 0;
    if (!GetVarint32(&payload, &dict_len))
    {
        std::cout << "  payload: PARSE FAILED (dict_len)\n";
        return;
    }
    std::cout << "  dict_bytes: " << dict_len << "\n";
    if (payload.size() < dict_len)
    {
        std::cout << "  payload: TRUNCATED (dict)\n";
        return;
    }
    payload = payload.substr(dict_len);

    if (payload.size() < 4)
    {
        std::cout << "  payload: TRUNCATED (mapping_len)\n";
        return;
    }
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    std::cout << "  mapping_bytes: " << mapping_len << "\n";
    payload = payload.substr(4);

    if (payload.size() < mapping_len)
    {
        std::cout << "  payload: TRUNCATED (mapping)\n";
        return;
    }

    std::string_view mapping_view = payload.substr(0, mapping_len);
    uint32_t mapping_count = 0;
    while (!mapping_view.empty())
    {
        uint64_t value;
        if (!GetVarint64(&mapping_view, &value))
        {
            break;
        }
        ++mapping_count;
    }
    std::cout << "  mapping_entries: " << mapping_count << "\n";

    std::string_view branch_view = payload.substr(mapping_len);
    PrintBranchMetadata(branch_view);
}

void PrintLogPayload(std::string_view payload)
{
    using namespace eloqstore;

    if (payload.size() < 4)
    {
        std::cout << "  payload: TRUNCATED (mapping_len)\n";
        return;
    }
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    std::cout << "  mapping_bytes: " << mapping_len << "\n";
    payload = payload.substr(4);

    if (payload.size() < mapping_len)
    {
        std::cout << "  payload: TRUNCATED (mapping)\n";
        return;
    }

    std::string_view mapping_view = payload.substr(0, mapping_len);
    uint32_t mapping_count = 0;
    while (!mapping_view.empty())
    {
        uint32_t page_id;
        uint64_t value;
        if (!GetVarint32(&mapping_view, &page_id))
            break;
        if (!GetVarint64(&mapping_view, &value))
            break;
        ++mapping_count;
    }
    std::cout << "  mapping_entries: " << mapping_count << "\n";

    std::string_view branch_view = payload.substr(mapping_len);
    PrintBranchMetadata(branch_view);
}

}  // namespace

int main(int argc, char **argv)
{
    using namespace eloqstore;

    if (argc != 2)
    {
        PrintUsage(argv[0]);
        return 1;
    }

    const char *path = argv[1];
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open " << path << ": " << std::strerror(errno)
                  << "\n";
        return 1;
    }

    const size_t header_size = ManifestBuilder::header_bytes;
    std::vector<char> record(header_size);
    uint64_t offset = 0;
    uint32_t log_index = 0;
    bool checksum_failed = false;

    while (true)
    {
        const uint64_t record_offset = offset;
        file.read(record.data(), static_cast<std::streamsize>(header_size));
        const std::streamsize header_read = file.gcount();
        if (header_read == 0)
        {
            break;  // EOF
        }
        if (header_read != static_cast<std::streamsize>(header_size))
        {
            std::cerr << "Manifest truncated while reading header at offset "
                      << record_offset << "\n";
            return 1;
        }
        offset += header_size;

        const uint32_t payload_len =
            DecodeFixed32(record.data() + ManifestBuilder::offset_len);
        record.resize(header_size + payload_len);
        file.read(record.data() + header_size, payload_len);
        const std::streamsize payload_read = file.gcount();
        if (payload_read != static_cast<std::streamsize>(payload_len))
        {
            std::cerr << "Manifest truncated while reading payload at offset "
                      << offset << "\n";
            return 1;
        }
        offset += payload_len;

        const std::string_view record_view(record.data(), record.size());
        const bool checksum_ok = ManifestBuilder::ValidateChecksum(record_view);
        const PageId root =
            DecodeFixed32(record.data() + ManifestBuilder::offset_root);
        const PageId ttl_root =
            DecodeFixed32(record.data() + ManifestBuilder::offset_ttl_root);

        std::cout << "Log #" << log_index << " at offset " << record_offset
                  << "\n";
        std::cout << "  record size: " << record.size() << "\n";
        std::cout << "  root: " << root << "\n";
        std::cout << "  ttl_root: " << ttl_root << "\n";
        std::cout << "  payload_bytes: " << payload_len << "\n";
        std::cout << "  checksum: " << (checksum_ok ? "OK" : "FAILED") << "\n";

        if (payload_len > 0)
        {
            const std::string_view payload_view(record.data() + header_size,
                                                payload_len);
            if (log_index == 0)
            {
                PrintSnapshotPayload(payload_view);
            }
            else
            {
                PrintLogPayload(payload_view);
            }
        }

        if (!checksum_ok)
        {
            const std::string payload_hex = BytesToHex(
                std::string_view(record.data() + header_size, payload_len));
            std::cout << "  payload_hex: " << payload_hex << "\n";
        }

        checksum_failed = checksum_failed || !checksum_ok;

        const size_t record_bytes = header_size + payload_len;
        const size_t alignment = page_align;
        const size_t remainder = record_bytes & (alignment - 1);
        const size_t padding = remainder == 0 ? 0 : alignment - remainder;
        if (!SkipPadding(file, padding))
        {
            std::cerr << "Failed to skip padding after log #" << log_index
                      << " at offset " << offset << "\n";
            return 1;
        }
        offset += padding;
        ++log_index;
    }

    if (log_index == 0)
    {
        std::cerr << "No manifest logs found in " << path << "\n";
        return 1;
    }

    std::cout << "Parsed " << log_index << " logs from " << path << "\n";
    return checksum_failed ? 2 : 0;
}
