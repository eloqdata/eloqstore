#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "external/xxhash.h"

namespace eloqstore
{
constexpr uint8_t checksum_bytes = 8;
static uint16_t const page_type_offset = checksum_bytes;

enum struct PageType : uint8_t
{
    NonLeafIndex = 0,
    LeafIndex,
    Data,
    Overflow,
    Deleted = 255
};

PageType TypeOfPage(const char *p);
void SetPageType(char *p, PageType t);
void SetChecksum(std::string_view blob);
bool ValidateChecksum(std::string_view blob);

inline static size_t page_align = sysconf(_SC_PAGESIZE);
inline constexpr uintptr_t unregistered_ptr_mask =
    uintptr_t(1) << (sizeof(uintptr_t) * 8 - 1);

class Page
{
public:
    explicit Page(bool alloc);
    Page(Page &&other) noexcept;
    Page &operator=(Page &&other) noexcept;
    Page(const Page &) = delete;
    Page &operator=(const Page &) = delete;
    ~Page();
    friend void swap(Page &lhs, Page &rhs)
    {
        std::swap(lhs.ptr_, rhs.ptr_);
    }
    void Free();
    char *Ptr() const;
    bool IsRegistered() const;

private:
    uintptr_t ptr_{};
};

struct KvOptions;

class PagesPool
{
public:
    using UPtr = std::unique_ptr<char, decltype(&std::free)>;
    PagesPool(const KvOptions *options);
    char *Allocate();
    void Free(char *ptr);
    bool IsRegistered(const char *ptr) const;
    size_t RegisteredPages() const;
    size_t RegisteredBytes() const;
    char *RegisteredBase() const;
    std::optional<uint32_t> RegisteredIndex(const char *ptr) const;

private:
    void Extend(size_t pages, bool registered);

    struct FreePage
    {
        FreePage *next_;
    };

    struct MemChunk
    {
        UPtr uptr_;
        size_t size_;
    };

    const KvOptions *options_;
    std::vector<MemChunk> chunks_;
    FreePage *free_head_;
    size_t free_cnt_;
    char *registered_base_;
    size_t registered_bytes_;
    size_t registered_pages_;
    uint8_t page_shift_;
};

}  // namespace eloqstore
