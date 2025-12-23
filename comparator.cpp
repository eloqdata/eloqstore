#include "comparator.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace eloqstore
{
class BytewiseComparatorImpl : public Comparator
{
public:
    BytewiseComparatorImpl() = default;

    ~BytewiseComparatorImpl()
    {
    }

    const char *Name() const override
    {
        return "leveldb.BytewiseComparator";
    }

    int Compare(std::string_view a, std::string_view b) const override
    {
        if (a.size() == 0)
        {
            return b.size() == 0 ? 0 : -1;
        }
        else if (b.size() == 0)
        {
            return 1;
        }

        return a.compare(b);
    }

    void FindShortestSeparator(std::string *start,
                               std::string_view limit) const override
    {
        // Find length of common prefix
        size_t min_length = std::min(start->size(), limit.size());
        size_t diff_index = 0;
        while ((diff_index < min_length) &&
               ((*start)[diff_index] == limit[diff_index]))
        {
            diff_index++;
        }

        if (diff_index >= min_length)
        {
            // Do not shorten if one string is a prefix of the other
        }
        else
        {
            uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
            if (diff_byte < static_cast<uint8_t>(0xff) &&
                diff_byte + 1 < static_cast<uint8_t>(limit[diff_index]))
            {
                (*start)[diff_index]++;
                start->resize(diff_index + 1);
                assert(Compare(*start, limit) < 0);
            }
        }
    }

    std::string_view FindShortestSeparator(
        std::string_view left, std::string_view right) const override
    {
        assert(Compare(left, right) < 0);

        size_t diff_idx = 0;
        while (diff_idx < left.size() && left[diff_idx] == right[diff_idx])
        {
            ++diff_idx;
            assert(diff_idx >= left.size() || diff_idx < right.size());
        }
        assert(diff_idx <= left.size());
        assert(diff_idx < right.size());
        assert(diff_idx == left.size() ||
               uint8_t(left[diff_idx]) < uint8_t(right[diff_idx]));

        std::string_view split{right.data(), diff_idx + 1};
        assert(Compare(split, left) > 0 && Compare(split, right) <= 0);

        return split;
    }

    void FindShortSuccessor(std::string *key) const override
    {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = 0; i < n; i++)
        {
            const uint8_t byte = (*key)[i];
            if (byte != static_cast<uint8_t>(0xff))
            {
                (*key)[i] = byte + 1;
                key->resize(i + 1);
                return;
            }
        }
        // *key is a run of 0xffs.  Leave it alone.
    }
};

const Comparator *Comparator::DefaultComparator()
{
    static BytewiseComparatorImpl byte_cmp;
    return &byte_cmp;
}
}  // namespace eloqstore