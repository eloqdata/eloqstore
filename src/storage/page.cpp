#include "storage/page.h"

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
        ptr_ = shard->PagePool()->Allocate();
    }
    else
    {
        ptr_ = nullptr;
    }
}

Page::Page(char *ptr)
{
    ptr_ = ptr;
}

Page::Page(Page &&other) noexcept : ptr_(other.ptr_)
{
    other.ptr_ = nullptr;
}

Page &Page::operator=(Page &&other) noexcept
{
    if (this != &other)
    {
        Free();
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
    }
    return *this;
}

Page::~Page()
{
    Free();
}

void Page::Free()
{
    if (ptr_ != nullptr && shard != nullptr)
    {
        shard->PagePool()->Free(ptr_);
        ptr_ = nullptr;
    }
}

char *Page::Ptr() const
{
    return ptr_;
}

PagesPool::PagesPool(const KvOptions *options)
    : options_(options), free_head_(nullptr), free_cnt_(0)
{
    // Initially allocate only enough pages for the io_uring buffer ring and
    // grow lazily on demand when the pool runs out of free pages.
    size_t initial_pages =
        options->buf_ring_size > 0 ? options->buf_ring_size : 1;
    Extend(initial_pages);
}

void PagesPool::Extend(size_t pages)
{
    assert(pages > 0);
    uint16_t page_size = options_->data_page_size;
    const size_t chunk_size = pages * page_size;
    char *ptr = (char *) std::aligned_alloc(page_align, chunk_size);
    assert(ptr);
    chunks_.emplace_back(UPtr(ptr, &std::free), chunk_size);

    for (size_t i = 0; i < chunk_size; i += page_size)
    {
        Free(ptr + i);
    }
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
    return reinterpret_cast<char *>(head);
}

void PagesPool::Free(char *ptr)
{
    assert(ptr != nullptr);
#ifndef NDEBUG
    // Fill with junk data for debugging purposes.
    memset(ptr, 123, options_->data_page_size);
#endif
    FreePage *free_page = new (ptr) FreePage;
    free_page->next_ = free_head_;
    free_head_ = free_page;
    free_cnt_++;
}
}  // namespace eloqstore