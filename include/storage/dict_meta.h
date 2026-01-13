#pragma once

#include <cstdint>

#include "compression.h"
#include "external/xxhash.h"

namespace eloqstore
{
struct DictMeta
{
    uint32_t dict_len{0};
    uint64_t dict_offset{0};
    uint64_t dict_checksum{0};

    bool HasDictionary() const
    {
        return dict_len > 0;
    }

    static DictMeta FromCompression(
        const compression::DictCompression &compression)
    {
        DictMeta meta;
        if (compression.HasDictionary())
        {
            const std::string &dict_bytes = compression.DictionaryBytes();
            meta.dict_len =
                static_cast<uint32_t>(dict_bytes.size());
            meta.dict_checksum = XXH3_64bits(dict_bytes.data(),
                                             dict_bytes.size());
        }
        return meta;
    }
};
}  // namespace eloqstore
