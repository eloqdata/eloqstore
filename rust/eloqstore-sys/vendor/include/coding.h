#pragma once

#include <bit>
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>

namespace eloqstore
{
// Standard Put... routines append to a string
void PutFixed16(std::string *dst, uint16_t value);
void PutFixed32(std::string *dst, uint32_t value);
void PutFixed64(std::string *dst, uint64_t value);
void PutVarint32(std::string *dst, uint32_t value);
void PutVarint64(std::string *dst, uint64_t value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool GetVarint32(std::string_view *input, uint32_t *value);
bool GetVarint64(std::string_view *input, uint64_t *value);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *v);
const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *v);

// Returns the length of the varint32 or varint64 encoding of "v"
int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char *EncodeVarint32(char *dst, uint32_t value);
size_t Varint32Size(uint32_t value);
char *EncodeVarint64(char *dst, uint64_t value);
size_t Varint64Size(uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written

inline void EncodeFixed16(char *dst, uint16_t value)
{
    uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

    // Recent clang and gcc optimize this to a single mov / str instruction.
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
}

inline void EncodeFixed32(char *dst, uint32_t value)
{
    uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

    // Recent clang and gcc optimize this to a single mov / str instruction.
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
}

inline void EncodeFixed64(char *dst, uint64_t value)
{
    uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

    // Recent clang and gcc optimize this to a single mov / str instruction.
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
    buffer[4] = static_cast<uint8_t>(value >> 32);
    buffer[5] = static_cast<uint8_t>(value >> 40);
    buffer[6] = static_cast<uint8_t>(value >> 48);
    buffer[7] = static_cast<uint8_t>(value >> 56);
}

inline uint64_t ToBigEndian(uint64_t value)
{
    return std::endian::native == std::endian::little ? __builtin_bswap64(value)
                                                      : value;
}

inline uint32_t ToBigEndian(uint32_t value)
{
    return std::endian::native == std::endian::little ? __builtin_bswap32(value)
                                                      : value;
}

inline uint64_t BigEndianToNative(uint64_t value)
{
    return std::endian::native == std::endian::little ? __builtin_bswap64(value)
                                                      : value;
}

inline uint32_t BigEndianToNative(uint32_t value)
{
    return std::endian::native == std::endian::little ? __builtin_bswap32(value)
                                                      : value;
}

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint16_t DecodeFixed16(const char *ptr)
{
    const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<uint32_t>(buffer[0])) |
           (static_cast<uint32_t>(buffer[1]) << 8);
}

inline uint32_t DecodeFixed32(const char *ptr)
{
    const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<uint32_t>(buffer[0])) |
           (static_cast<uint32_t>(buffer[1]) << 8) |
           (static_cast<uint32_t>(buffer[2]) << 16) |
           (static_cast<uint32_t>(buffer[3]) << 24);
}

inline uint64_t DecodeFixed64(const char *ptr)
{
    const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<uint64_t>(buffer[0])) |
           (static_cast<uint64_t>(buffer[1]) << 8) |
           (static_cast<uint64_t>(buffer[2]) << 16) |
           (static_cast<uint64_t>(buffer[3]) << 24) |
           (static_cast<uint64_t>(buffer[4]) << 32) |
           (static_cast<uint64_t>(buffer[5]) << 40) |
           (static_cast<uint64_t>(buffer[6]) << 48) |
           (static_cast<uint64_t>(buffer[7]) << 56);
}

// Internal routine for use by fallback path of GetVarint32Ptr
const char *GetVarint32PtrFallback(const char *p,
                                   const char *limit,
                                   uint32_t *value);
inline const char *GetVarint32Ptr(const char *p,
                                  const char *limit,
                                  uint32_t *value)
{
    if (p < limit)
    {
        uint32_t result = *(reinterpret_cast<const uint8_t *>(p));
        if ((result & 128) == 0)
        {
            *value = result;
            return p + 1;
        }
    }
    return GetVarint32PtrFallback(p, limit, value);
}

inline uint32_t EncodeInt32Delta(int32_t delta)
{
    if (delta >= 0)
    {
        return delta << 1;
    }
    else
    {
        delta = -delta;
        assert(delta >= 0);
        return ((delta << 1) | 1);
    }
}

inline int32_t DecodeInt32Delta(uint32_t val)
{
    int32_t signed_delta = 0;

    // The lowest bit encodes the sign.
    if ((val & 1) == 1)
    {
        signed_delta = -(val >> 1);
        assert(signed_delta < 0);
    }
    else
    {
        signed_delta = val >> 1;
        assert(signed_delta >= 0);
    }

    return signed_delta;
}

inline uint64_t EncodeInt64Delta(int64_t delta)
{
    if (delta >= 0)
    {
        return delta << 1;
    }
    else
    {
        delta = -delta;
        assert(delta >= 0);
        return ((delta << 1) | 1);
    }
}

inline int64_t DecodeInt64Delta(uint64_t val)
{
    int64_t signed_delta = 0;

    // The lowest bit encodes the sign.
    if ((val & 1) == 1)
    {
        signed_delta = -(val >> 1);
        assert(signed_delta < 0);
    }
    else
    {
        signed_delta = val >> 1;
        assert(signed_delta >= 0);
    }

    return signed_delta;
}
}  // namespace eloqstore