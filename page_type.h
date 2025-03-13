#pragma once

#include <cstdint>

#include "coding.h"
#include "crc32.h"

namespace kvstore
{
static uint16_t const page_crc_offset = 0;
static uint16_t const page_type_offset = page_crc_offset + sizeof(uint32_t);

enum struct PageType : uint8_t
{
    NonLeafIndex = 0,
    LeafIndex,
    Data,
    Deleted = 255
};

inline static PageType TypeOfPage(const char *p)
{
    return static_cast<PageType>(p[page_type_offset]);
}

inline static void SetPageType(char *p, PageType t)
{
    p[page_type_offset] = static_cast<char>(t);
}

inline static uint32_t Crc32OfPage(const char *p)
{
    return crc32::Unmask(DecodeFixed32(p + page_crc_offset));
}

inline static void SetPageCrc32(char *p, uint16_t pgsz)
{
    uint32_t crc = crc32::Value(p + page_type_offset, pgsz - page_type_offset);
    EncodeFixed32(p + page_crc_offset, crc32::Mask(crc));
}

inline static bool ValidatePageCrc32(char *p, uint16_t pgsz)
{
    uint32_t crc = crc32::Value(p + page_type_offset, pgsz - page_type_offset);
    return crc == Crc32OfPage(p);
}
}  // namespace kvstore