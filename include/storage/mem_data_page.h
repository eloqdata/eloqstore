#pragma once

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "kv_options.h"
#include "storage/page.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class DataPageManager;

class MemDataPage
{
public:
    class Handle
    {
    public:
        Handle() = default;
        explicit Handle(MemDataPage *page)
        {
            Reset(page);
        }

        Handle(const Handle &) = delete;
        Handle &operator=(const Handle &) = delete;

        Handle(Handle &&other) noexcept : page_(other.page_)
        {
            other.page_ = nullptr;
        }
        Handle &operator=(Handle &&other) noexcept
        {
            if (this != &other)
            {
                Reset();
                page_ = other.page_;
                other.page_ = nullptr;
            }
            return *this;
        }

        ~Handle()
        {
            Reset();
        }

        void Reset(MemDataPage *page = nullptr)
        {
            if (page_ != nullptr)
            {
                page_->Unpin();
            }
            page_ = page;
            if (page_ != nullptr)
            {
                page_->Pin();
            }
        }

        MemDataPage *Release()
        {
            MemDataPage *p = page_;
            page_ = nullptr;
            return p;
        }

        MemDataPage *Get() const
        {
            return page_;
        }

        MemDataPage *operator->() const
        {
            return page_;
        }

        explicit operator bool() const
        {
            return page_ != nullptr;
        }

    private:
        MemDataPage *page_{nullptr};
    };

    explicit MemDataPage(bool alloc = true) : page_(alloc) {};

    char *PagePtr() const
    {
        return page_.Ptr();
    }

    void Deque();
    MemDataPage *DequeNext();
    void EnqueNext(MemDataPage *new_page);

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

    bool InFreeList() const
    {
        return in_free_list_;
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

    bool IsRegistered() const
    {
        return page_.IsRegistered();
    }

    void SetError(KvError err)
    {
        err_ = err;
    }

    KvError Error() const
    {
        return err_;
    }

    Page page_;

private:
    PageId page_id_{MaxPageId};
    uint32_t ref_cnt_{0};
    FilePageId file_page_id_{MaxFilePageId};
    KvError err_{KvError::NoError};
    WaitingZone waiting_;

    MemDataPage *next_{nullptr};
    MemDataPage *prev_{nullptr};
    const TableIdent *tbl_ident_{nullptr};
    bool in_free_list_{false};
    friend class IndexPageManager;
};
}  // namespace eloqstore
