#pragma once

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "coding.h"
#include "comparator.h"
#include "kv_options.h"
#include "page.h"
#include "task.h"
#include "types.h"

namespace eloqstore
{
class IndexPageManager;
class MappingSnapshot;
struct TableIdent;

class MemIndexPage
{
public:
    static size_t const max_page_size = 1 << 16;
    static uint16_t const page_size_offset = page_type_offset + sizeof(uint8_t);
    static uint16_t const leftmost_ptr_offset =
        page_size_offset + sizeof(uint16_t);

    explicit MemIndexPage(bool alloc = true) : page_(alloc) {};
    uint16_t ContentLength() const;
    uint16_t RestartNum() const;

    char *PagePtr() const
    {
        return page_.Ptr();
    }

    void Deque();
    MemIndexPage *DequeNext();
    void EnqueNext(MemIndexPage *new_page);

    bool IsPointingToLeaf() const;

    void Pin()
    {
        ++ref_cnt_;
    }

    void Unpin()
    {
        CHECK_GT(ref_cnt_, 0);
        --ref_cnt_;
    }

    bool IsPinned() const
    {
        return ref_cnt_ > 0;
    }

    bool IsDetached() const
    {
        return prev_ == nullptr && next_ == nullptr;
    }

    PageId GetPageId() const
    {
        return page_id_;
    }

    FilePageId GetFilePageId() const
    {
        return file_page_id_;
    }

    void SetPageId(PageId page_id)
    {
        page_id_ = page_id;
    }

    void SetFilePageId(FilePageId file_page_id)
    {
        file_page_id_ = file_page_id;
    }

    bool IsPageIdValid() const
    {
        return page_id_ < MaxPageId;
    }

    /**
     * @brief Encode the page to a human readable string , used for debugging.
     */
    std::string String(const KvOptions *opts) const;

private:
    /**
     * @brief The page ID is 0, if the page is newly created in memory and has
     * not been flushed to storage.
     *
     */
    PageId page_id_{MaxPageId};
    /**
     * @brief Number of concurrent tasks that have pinned the page. A page is
     * pinned when a read/write task is traversing down the tree. A pinned page
     * cannot be evicted.
     *
     */
    uint32_t ref_cnt_{0};
    FilePageId file_page_id_{MaxFilePageId};
    Page page_;

    WaitingZone waiting_;

    /**
     * @brief A doubly-linked list of in-memory pages for cache replacement. An
     * in-memory page is either in the active list or in the free list.
     *
     */
    MemIndexPage *next_{nullptr};
    MemIndexPage *prev_{nullptr};
    const TableIdent *tbl_ident_{nullptr};
    friend class IndexPageManager;
};

class IndexPageIter
{
public:
    using uptr = std::unique_ptr<IndexPageIter>;

    IndexPageIter() = delete;
    IndexPageIter(const MemIndexPage *index_page, const KvOptions *opts);
    IndexPageIter(std::string_view page_view, const KvOptions *opts);

    bool HasNext() const
    {
        return curr_offset_ < restart_offset_;
    }

    bool Next()
    {
        return ParseNextKey();
    }

    void Seek(std::string_view key);

    std::string PeekNextKey() const;

    std::string_view Key() const
    {
        return {key_.data(), key_.size()};
    }

    PageId GetPageId() const
    {
        return page_id_;
    }

    void SeekToRestart(uint16_t restart_idx)
    {
        curr_restart_idx_ = restart_idx;
        curr_offset_ = RestartOffset(restart_idx);
        key_.clear();
        page_id_ = MaxPageId;
    }

private:
    uint16_t RestartOffset(uint16_t restart_idx) const
    {
        assert(restart_idx < restart_num_);
        return DecodeFixed16(page_.data() + restart_offset_ +
                             restart_idx * sizeof(uint16_t));
    }

    bool ParseNextKey();
    void Invalidate();

    static const char *DecodeEntry(const char *ptr,
                                   const char *limit,
                                   uint32_t *shared,
                                   uint32_t *non_shared);

    const Comparator *const comparator_;
    std::string_view const page_;

public:
    uint16_t const restart_num_;

private:
    uint16_t const restart_offset_;

    uint16_t curr_offset_{MemIndexPage::leftmost_ptr_offset};
    uint16_t curr_restart_idx_{0};

    std::string key_;
    PageId page_id_{MaxPageId};
};
}  // namespace eloqstore