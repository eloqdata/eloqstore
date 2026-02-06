#include "storage/page.h"

#include <cstdlib>
#include <cstring>
#include <utility>

#include "storage/shard.h"

namespace eloqstore
{

PageType TypeOfPage(const char *p)
{
    return static_cast<PageType>(p[page_type_offset]);
}

void SetPageType(char *p, PageType t)
{
    p[page_type_offset] = static_cast<char>(t);
}

void SetChecksum(std::string_view blob)
{
    uint64_t checksum =
        XXH3_64bits(blob.data() + checksum_bytes, blob.size() - checksum_bytes);
    EncodeFixed64(const_cast<char *>(blob.data()), checksum);
}

bool ValidateChecksum(std::string_view blob)
{
    uint64_t checksum_stored = DecodeFixed64(blob.data());
    uint64_t checksum =
        XXH3_64bits(blob.data() + checksum_bytes, blob.size() - checksum_bytes);
    return checksum == checksum_stored;
}

Page::Page(bool alloc)
{
    if (alloc)
    {
        ptr_ = reinterpret_cast<uintptr_t>(shard->PagePool()->Allocate());
    }
    else
    {
        ptr_ = 0;
    }
}

Page::Page(Page &&other) noexcept : ptr_(other.ptr_)
{
    other.ptr_ = 0;
}

Page &Page::operator=(Page &&other) noexcept
{
    if (this != &other)
    {
        Free();
        ptr_ = other.ptr_;
        other.ptr_ = 0;
    }
    return *this;
}

Page::~Page()
{
    Free();
}

void Page::Free()
{
    if (ptr_ != 0 && shard != nullptr)
    {
        shard->PagePool()->Free(reinterpret_cast<char *>(ptr_));
        ptr_ = 0;
    }
}

char *Page::Ptr() const
{
    if (ptr_ == 0)
    {
        return nullptr;
    }
    return reinterpret_cast<char *>(ptr_ & ~kRegisteredMask);
}

bool Page::IsRegistered() const
{
    return (ptr_ & kRegisteredMask) != 0;
}

PagesPool::PagesPool(const KvOptions *options) : options_(options) {};

void PagesPool::Init(void *registered_buffer, size_t buffer_size)
{
    if (initialized_)
    {
        return;
    }
    if (registered_buffer != nullptr && buffer_size > 0)
    {
        [[maybe_unused]] uint16_t page_size = options_->data_page_size;
        assert((buffer_size % page_size) == 0);
        memset(registered_buffer, 0, buffer_size);
        AddChunk(UPtr(static_cast<char *>(registered_buffer), &std::free),
                 buffer_size,
                 true);
    }
    initialized_ = true;
}

void PagesPool::Extend(size_t pages)
{
    assert(pages > 0);
    uint16_t page_size = options_->data_page_size;
    const size_t chunk_size = pages * page_size;
    char *ptr = (char *) std::aligned_alloc(page_align, chunk_size);
    memset(ptr, 0, chunk_size);
    assert(ptr);
    AddChunk(UPtr(ptr, &std::free), chunk_size, false);
}

char *PagesPool::Allocate()
{
    if (free_head_ == nullptr)
    {
        Extend(1024);  // Extend the pool with 1024 pages if free list is empty.
        assert(free_head_ != nullptr);
    }
    FreePage *head = free_head_;
    free_head_ = head->next_;
    uintptr_t raw = reinterpret_cast<uintptr_t>(head);
    if (head->registered_)
    {
        raw |= Page::kRegisteredMask;
    }
    return reinterpret_cast<char *>(raw);
}

void PagesPool::Free(char *ptr)
{
    assert(ptr != nullptr);
    uintptr_t raw = reinterpret_cast<uintptr_t>(ptr);
    bool registered = (raw & Page::kRegisteredMask) != 0;
    ptr = reinterpret_cast<char *>(raw & ~Page::kRegisteredMask);
#ifndef NDEBUG
    // Fill with junk data for debugging purposes.
    memset(ptr, 123, options_->data_page_size);
#endif
    FreePage *free_page = new (ptr) FreePage;
    free_page->registered_ = registered;
    free_page->next_ = free_head_;
    free_head_ = free_page;
    free_cnt_++;
}

void PagesPool::AddChunk(UPtr chunk, size_t size, bool registered)
{
    assert(chunk);
    assert(size % options_->data_page_size == 0);
    char *ptr = chunk.get();
    chunks_.push_back(MemChunk{std::move(chunk), size});

    uint16_t page_size = options_->data_page_size;
    for (size_t offset = 0; offset < size; offset += page_size)
    {
        char *page_ptr = ptr + offset;
        FreePage *free_page = new (page_ptr) FreePage;
        free_page->registered_ = registered;
        free_page->next_ = free_head_;
        free_head_ = free_page;
        free_cnt_++;
    }
}
}  // namespace eloqstore
