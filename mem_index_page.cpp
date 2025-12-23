#include "mem_index_page.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "coding.h"
#include "kv_options.h"
#include "page_mapper.h"

namespace eloqstore
{
uint16_t MemIndexPage::ContentLength() const
{
    return DecodeFixed16(page_.Ptr() + page_size_offset);
}

uint16_t MemIndexPage::RestartNum() const
{
    return DecodeFixed16(page_.Ptr() + ContentLength() - sizeof(uint16_t));
}

void MemIndexPage::Deque()
{
    MemIndexPage *prev = prev_;
    MemIndexPage *next = next_;

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

MemIndexPage *MemIndexPage::DequeNext()
{
    MemIndexPage *target = next_;
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

void MemIndexPage::EnqueNext(MemIndexPage *new_page)
{
    MemIndexPage *old_next = next_;
    next_ = new_page;
    new_page->prev_ = this;

    new_page->next_ = old_next;
    if (old_next != nullptr)
    {
        old_next->prev_ = new_page;
    }
}

bool MemIndexPage::IsPointingToLeaf() const
{
    return TypeOfPage(page_.Ptr()) == PageType::LeafIndex;
}

std::string MemIndexPage::String(const KvOptions *opts) const
{
    std::string str = "{";
    if (IsPointingToLeaf())
    {
        str.push_back('L');
    }
    else
    {
        str.push_back('I');
    }
    str.append(std::to_string(GetPageId()));
    str.push_back('|');
    IndexPageIter iter(this, opts);
    while (iter.HasNext())
    {
        iter.Next();
        str.push_back('[');
        str.append(iter.Key());
        str.push_back(':');
        str.append(std::to_string(iter.GetPageId()));
        str.push_back(']');
    }
    str.push_back('}');
    return str;
}

IndexPageIter::IndexPageIter(const MemIndexPage *index_page,
                             const KvOptions *opts)
    : comparator_(opts->comparator_),
      page_(index_page == nullptr ? std::string_view{}
                                  : std::string_view{index_page->PagePtr(),
                                                     opts->data_page_size}),
      restart_num_(index_page == nullptr ? 0 : index_page->RestartNum()),
      restart_offset_(index_page == nullptr
                          ? 0
                          : index_page->ContentLength() -
                                (1 + restart_num_) * sizeof(uint16_t)),
      curr_offset_(MemIndexPage::leftmost_ptr_offset)
{
}

IndexPageIter::IndexPageIter(std::string_view page_view, const KvOptions *opts)
    : comparator_(opts->comparator_),
      page_(page_view),
      restart_num_(DecodeFixed16(page_view.data() + page_view.size() -
                                 sizeof(uint16_t))),
      restart_offset_(page_view.size() - (1 + restart_num_) * sizeof(uint16_t)),
      curr_offset_(MemIndexPage::leftmost_ptr_offset)
{
    assert(DecodeFixed16(page_view.data() + MemIndexPage::page_size_offset) ==
           page_view.size());
}

bool IndexPageIter::ParseNextKey()
{
    const char *pt = page_.data() + curr_offset_;
    const char *limit = page_.data() + restart_offset_;

    if (pt >= limit)
    {
        Invalidate();
        return false;
    }
    else if (curr_offset_ <
             MemIndexPage::leftmost_ptr_offset + sizeof(uint32_t))
    {
        key_.clear();
        page_id_ =
            DecodeFixed32(page_.data() + MemIndexPage::leftmost_ptr_offset);
        curr_offset_ = MemIndexPage::leftmost_ptr_offset + sizeof(uint32_t);
        curr_restart_idx_ = 0;
        return true;
    }

    bool is_restart_pointer = curr_offset_ == RestartOffset(curr_restart_idx_);
    uint32_t shared = 0, non_shared = 0;
    pt = DecodeEntry(pt, limit, &shared, &non_shared);

    if (pt == nullptr || key_.size() < shared)
    {
        Invalidate();
        return false;
    }
    else
    {
        key_.resize(shared);
        key_.append(pt, non_shared);
        pt += non_shared;

        // Parses the page Id. The stored value is the real page Id if this is
        // the restarting point, or the numerical delta to the previous pointer.
        uint32_t ptr_val;
        if ((pt = GetVarint32Ptr(pt, limit, &ptr_val)) == nullptr)
        {
            Invalidate();
            return false;
        }
        int32_t delta = DecodeInt32Delta(ptr_val);
        page_id_ =
            is_restart_pointer ? delta : static_cast<int32_t>(page_id_) + delta;

        curr_offset_ = pt - page_.data();

        if (curr_restart_idx_ + 1 < restart_num_ &&
            curr_offset_ >= RestartOffset(curr_restart_idx_ + 1))
        {
            ++curr_restart_idx_;
        }

        return true;
    }
}

void IndexPageIter::Invalidate()
{
    curr_offset_ = restart_offset_;
    curr_restart_idx_ = restart_num_;
    key_.clear();
    page_id_ = MaxPageId;
}

const char *IndexPageIter::DecodeEntry(const char *ptr,
                                       const char *limit,
                                       uint32_t *shared,
                                       uint32_t *non_shared)
{
    if (limit - ptr < 2)
    {
        return nullptr;
    }

    *shared = reinterpret_cast<const uint8_t *>(ptr)[0];
    *non_shared = reinterpret_cast<const uint8_t *>(ptr)[1];
    if ((*shared | *non_shared) < 128)
    {
        // Fast path: all three values are encoded in one byte each
        ptr += 2;
    }
    else
    {
        if ((ptr = GetVarint32Ptr(ptr, limit, shared)) == nullptr)
        {
            return nullptr;
        }

        if ((ptr = GetVarint32Ptr(ptr, limit, non_shared)) == nullptr)
        {
            return nullptr;
        }
    }

    return ptr;
}

std::string IndexPageIter::PeekNextKey() const
{
    const char *pt = page_.data() + curr_offset_;
    const char *limit = page_.data() + restart_offset_;

    if (pt >= limit)
    {
        return std::string();
    }

    uint32_t shared = 0, non_shared = 0;
    pt = DecodeEntry(pt, limit, &shared, &non_shared);

    if (pt == nullptr || key_.size() < shared)
    {
        return std::string();
    }
    else
    {
        std::string peek_key{key_.data(), shared};
        peek_key.append(pt, non_shared);
        return peek_key;
    }
}

void IndexPageIter::Seek(std::string_view search_key)
{
    int32_t left = 0;
    int32_t right = restart_num_ - 1;
    int cmp_ret = 0;

    if (!key_.empty())
    {
        // If we're already scanning, use the current position as a starting
        // point. This is beneficial if the key we're seeking to is ahead of the
        // current position.
        cmp_ret = comparator_->Compare(key_, search_key);
        if (cmp_ret < 0)
        {
            // Current key is smaller than the search key
            left = curr_restart_idx_;
        }
        else if (cmp_ret > 0)
        {
            right = curr_restart_idx_;
        }
        else
        {
            // We're seeking to the key we're already at.
            return;
        }
    }

    // Binary searches the floor restarting point of the input key.
    size_t cnt = right - left + 1;
    while (cnt > 0)
    {
        size_t step = cnt >> 1;
        size_t mid = right - step;
        uint16_t region_offset = RestartOffset(mid);
        uint32_t shared, non_shared;
        const char *key_ptr = DecodeEntry(page_.data() + region_offset,
                                          page_.data() + restart_offset_,
                                          &shared,
                                          &non_shared);

        if (key_ptr == nullptr || shared != 0)
        {
            Invalidate();
            return;
        }

        std::string_view pivot{key_ptr, non_shared};

        if (comparator_->Compare(pivot, search_key) > 0)
        {
            right = mid - 1;
            cnt -= step + 1;
        }
        else
        {
            cnt = step;
        }
    }

    if (right < 0)
    {
        // The input key is smaller than the first restarting point. Positions
        // to the leftmost pointer.
        curr_offset_ = MemIndexPage::leftmost_ptr_offset;
        curr_restart_idx_ = 0;
        ParseNextKey();
    }
    else
    {
        // Linear searches the region to find the floor index entry of the
        // search key.
        SeekToRestart(right);
        if (!ParseNextKey())
        {
            Invalidate();
            return;
        }

        // The first index key in th region matches the search key.
        if (comparator_->Compare(Key(), search_key) == 0)
        {
            return;
        }

        uint16_t limit = right < restart_num_ - 1 ? RestartOffset(right + 1)
                                                  : restart_offset_;

        uint16_t prev_offset = curr_offset_;
        std::string prev_key = key_;
        uint32_t prev_page_id = page_id_;

        while (curr_offset_ < limit)
        {
            if (!ParseNextKey())
            {
                Invalidate();
                return;
            }

            std::string_view index_key = Key();
            cmp_ret = comparator_->Compare(index_key, search_key);
            if (cmp_ret > 0)
            {
                // Finds the first index key greater than the search key.
                break;
            }
            else if (cmp_ret == 0)
            {
                // The index key matches the search key.
                return;
            }
            else
            {
                prev_offset = curr_offset_;
                prev_key = key_;
                prev_page_id = page_id_;
            }
        }

        // Positions to the prior index entry.
        curr_offset_ = prev_offset;
        key_ = prev_key;
        page_id_ = prev_page_id;
    }
}
}  // namespace eloqstore