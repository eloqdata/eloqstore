#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "comparator.h"
#include "kv_options.h"
#include "page_type.h"

namespace kvstore
{
class PagePool
{
public:
    PagePool(uint16_t page_size);
    std::unique_ptr<char[]> Allocate();
    void Free(std::unique_ptr<char[]> page);

private:
    const uint16_t page_size_;
    std::vector<std::unique_ptr<char[]>> pages_;
};

inline thread_local PagePool *page_pool;

class DataPage
{
public:
    DataPage() = default;
    DataPage(uint32_t page_id, uint32_t page_size = 0);
    DataPage(const DataPage &) = delete;
    DataPage(DataPage &&rhs);
    DataPage &operator=(DataPage &&);
    ~DataPage();

    static uint16_t const page_size_offset = page_type_offset + sizeof(uint8_t);
    static uint16_t const prev_page_offset =
        page_size_offset + sizeof(uint16_t);
    static uint16_t const next_page_offset =
        prev_page_offset + sizeof(uint32_t);
    static uint16_t const content_offset = next_page_offset + sizeof(uint32_t);

    bool IsEmpty() const;
    uint16_t ContentLength() const;
    uint16_t RestartNum() const;
    uint32_t PrevPageId() const;
    uint32_t NextPageId() const;
    void SetPrevPageId(uint32_t page_id);
    void SetNextPageId(uint32_t page_id);
    void SetPageId(uint32_t page_id);
    uint32_t PageId() const;
    char *PagePtr() const;
    std::unique_ptr<char[]> GetPtr();
    void SetPtr(std::unique_ptr<char[]> ptr);
    void Clear();

private:
    uint32_t page_id_{UINT32_MAX};
    std::unique_ptr<char[]> page_{nullptr};
};

std::ostream &operator<<(std::ostream &out, DataPage const &page);

class DataPageIter
{
public:
    DataPageIter() = delete;
    DataPageIter(const DataPage *data_page, const KvOptions *options);

    void Reset(const DataPage *data_page, uint32_t size);
    void Reset();
    std::string_view Key() const;
    std::string_view Value() const;
    uint64_t Timestamp() const;

    bool HasNext() const;
    bool Next();
    void Seek(std::string_view search_key);

private:
    uint16_t RestartOffset(uint16_t restart_idx) const;
    void SeekToRestart(uint16_t restart_idx);
    bool ParseNextKey();
    void Invalidate();
    static const char *DecodeEntry(const char *p,
                                   const char *limit,
                                   uint32_t *shared,
                                   uint32_t *non_shared,
                                   uint32_t *value_length);

    const Comparator *const cmp_;
    std::string_view page_;
    uint16_t restart_num_;
    uint16_t restart_offset_;

    uint16_t curr_offset_{DataPage::content_offset};
    uint16_t curr_restart_idx_{0};

    std::string key_;
    std::string_view value_;
    uint64_t timestamp_;
};

}  // namespace kvstore