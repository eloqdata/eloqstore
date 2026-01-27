#include "storage/page.h"

#include <glog/logging.h>

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
        char *ptr = shard->PagePool()->Allocate();
        bool registered = shard->PagePool()->IsRegistered(ptr);
        uintptr_t raw = reinterpret_cast<uintptr_t>(ptr);
        uintptr_t flag = static_cast<uintptr_t>(!registered);
        ptr_ = raw | (unregistered_ptr_mask & -flag);
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
        shard->PagePool()->Free(Ptr());
        ptr_ = 0;
    }
}

char *Page::Ptr() const
{
    if (ptr_ == 0)
    {
        return nullptr;
    }
    return reinterpret_cast<char *>(ptr_ & ~unregistered_ptr_mask);
}

bool Page::IsRegistered() const
{
    return ptr_ != 0 && ((ptr_ & unregistered_ptr_mask) == 0);
}

PagesPool::PagesPool(const KvOptions *options)
    : options_(options),
      free_head_(nullptr),
      free_cnt_(0),
      registered_base_(nullptr),
      registered_bytes_(0),
      registered_pages_(0),
      page_shift_(0)
{
    size_t page_size = options_->data_page_size;
    CHECK(page_size && ((page_size & (page_size - 1)) == 0))
        << "data_page_size must be power of two";
    page_shift_ = static_cast<uint8_t>(__builtin_ctzll(page_size));
    size_t initial_pages = options_->buffer_pool_size / page_size;
    if (initial_pages == 0)
    {
        initial_pages = 1;
    }
    registered_pages_ = initial_pages;
    registered_bytes_ = initial_pages * page_size;
    Extend(initial_pages, true);
}

void PagesPool::Extend(size_t pages, bool registered)
{
    assert(pages > 0);
    uint16_t page_size = options_->data_page_size;
    const size_t chunk_size = pages * page_size;
    auto a = butil::cpuwide_time_ns();
    char *ptr = (char *) std::aligned_alloc(page_align, chunk_size);
    auto t1 = butil::cpuwide_time_ns() - a;
    a = butil::cpuwide_time_ns();
    assert(ptr);
    chunks_.emplace_back(UPtr(ptr, &std::free), chunk_size);
    if (registered)
    {
        CHECK(registered_base_ == nullptr)
            << "Registered chunk can only be allocated once";
        registered_base_ = ptr;
        registered_bytes_ = chunk_size;
        registered_pages_ = pages;
    }
    auto t2 = butil::cpuwide_time_ns() - a;
    a = butil::cpuwide_time_ns();

    for (size_t i = 0; i < chunk_size; i += page_size)
    {
        Free(ptr + i);
    }
    auto t3 = butil::cpuwide_time_ns() - a;
    if (t1 + t2 + t3 > 500000)
    {
        LOG(ERROR) << "Extent t1 = " << t1 << " t2 = " << t2 << " t3 = " << t3
                   << ", size=" << pages;
    }
}

char *PagesPool::Allocate()
{
    if (free_head_ == nullptr)
    {
        auto t = butil::cpuwide_time_ns();
        Extend(16, false);
        t = butil::cpuwide_time_ns() - t;
        if (t > 500000)
        {
            LOG(INFO) << "Extent cost " << t;
        }
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

bool PagesPool::IsRegistered(const char *ptr) const
{
    if (registered_base_ == nullptr || ptr == nullptr)
    {
        return false;
    }
    uintptr_t base = reinterpret_cast<uintptr_t>(registered_base_);
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    return addr >= base && addr < base + registered_bytes_;
}

size_t PagesPool::RegisteredPages() const
{
    return registered_pages_;
}

size_t PagesPool::RegisteredBytes() const
{
    return registered_bytes_;
}

char *PagesPool::RegisteredBase() const
{
    return registered_base_;
}

std::optional<uint32_t> PagesPool::RegisteredIndex(const char *ptr) const
{
    if (!IsRegistered(ptr))
    {
        return std::nullopt;
    }
    uintptr_t base = reinterpret_cast<uintptr_t>(registered_base_);
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    uint32_t idx = static_cast<uint32_t>((addr - base) >> page_shift_);
    return idx;
}
}  // namespace eloqstore
