#pragma once

#include <cstdint>
#include <deque>
#include <memory>
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

class Page
{
public:
    static constexpr uintptr_t kRegisteredMask = uintptr_t(1)
                                                 << (sizeof(uintptr_t) * 8 - 1);

    Page(bool alloc);
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
    uintptr_t ptr_{0};
};

struct KvOptions;

class PagesPool
{
public:
    using UPtr = std::unique_ptr<char, decltype(&std::free)>;
    PagesPool(const KvOptions *options);
    // Takes ownership of registered_buffer; it must be free()-compatible.
    // Do not free manually; PagesPool will free upon destruction.
    void Init(void *registered_buffer = nullptr, size_t buffer_size = 0);
    char *Allocate();
    void Free(char *ptr);

private:
    void Extend(size_t pages);
    void AddChunk(UPtr chunk, size_t size, bool registered);

    struct FreePage
    {
        FreePage *next_;
        bool registered_;
    };

    struct MemChunk
    {
        UPtr uptr_;
        size_t size_;
    };

    const KvOptions *options_;
    std::deque<MemChunk> chunks_;
    FreePage *free_head_{nullptr};
    size_t free_cnt_{0};
    bool initialized_{false};
};

}  // namespace eloqstore
