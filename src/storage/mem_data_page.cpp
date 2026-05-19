#include "storage/mem_data_page.h"

namespace eloqstore
{
void MemDataPage::Deque()
{
    MemDataPage *prev = prev_;
    MemDataPage *next = next_;

    if (prev != nullptr)
    {
        prev->next_ = next;
    }
    if (next != nullptr)
    {
        next->prev_ = prev;
    }
    prev_ = nullptr;
    next_ = nullptr;
}

MemDataPage *MemDataPage::DequeNext()
{
    MemDataPage *target = next_;
    if (target != nullptr)
    {
        next_ = target->next_;
        if (next_ != nullptr)
        {
            next_->prev_ = this;
        }

        target->prev_ = nullptr;
        target->next_ = nullptr;
    }

    return target;
}

void MemDataPage::EnqueNext(MemDataPage *new_page)
{
    MemDataPage *old_next = next_;
    next_ = new_page;
    new_page->prev_ = this;

    new_page->next_ = old_next;
    if (old_next != nullptr)
    {
        old_next->prev_ = new_page;
    }
}
}  // namespace eloqstore
