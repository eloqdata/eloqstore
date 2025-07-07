#include "db_stress/expected_value.h"

#include <atomic>

namespace StressTest
{
void ExpectedValue::Put(bool pending)
{
    if (pending)
    {
        SetPendingWrite();
    }
    else
    {
        SetValueBase(NextValueBase());
        ClearDeleted();
        ClearPendingWrite();
    }
}

bool ExpectedValue::Delete(bool pending)
{
    if (pending && !Exists())
    {
        return false;
    }
    if (pending)
    {
        SetPendingDel();
    }
    else
    {
        SetDelCounter(NextDelCounter());
        SetDeleted();
        ClearPendingDel();
    }
    return true;
}

void ExpectedValue::SyncPut(uint32_t value_base)
{
    assert(ExpectedValue::IsValueBaseValid(value_base));

    SetValueBase(value_base);
    ClearDeleted();
    ClearPendingWrite();

    // This is needed in case crash happens during a pending delete of the key
    // assocated with this expected value
    ClearPendingDel();
}

void ExpectedValue::SyncPendingPut()
{
    Put(true /* pending */);
}

void ExpectedValue::SyncDelete()
{
    Delete(false /* pending */);
    // This is needed in case crash happens during a pending write of the key
    // assocated with this expected value
    ClearPendingWrite();
}

uint32_t ExpectedValue::GetFinalValueBase() const
{
    return PendingWrite() ? NextValueBase() : GetValueBase();
}

uint32_t ExpectedValue::GetFinalDelCounter() const
{
    return PendingDelete() ? NextDelCounter() : GetDelCounter();
}

bool ExpectedValueHelper::MustHaveNotExisted(ExpectedValue pre,
                                             ExpectedValue post)
{
    bool pre_deleted = pre.IsDeleted();

    bool no_write_during_read =
        (pre.GetValueBase() == post.GetFinalValueBase());
    return pre_deleted && no_write_during_read;
}

bool ExpectedValueHelper::MustHaveExisted(ExpectedValue pre, ExpectedValue post)
{
    bool pre_not_deleted = !pre.IsDeleted();
    bool no_delete_during_read =
        (pre.GetDelCounter() == post.GetFinalDelCounter());
    return pre_not_deleted && no_delete_during_read;
}

bool ExpectedValueHelper::InExpectedValueBaseRange(uint32_t value_base,
                                                   ExpectedValue pre,
                                                   ExpectedValue post)
{
    uint32_t pre_base = pre.GetValueBase();
    uint32_t post_final_base = post.GetFinalValueBase();

    if (pre_base <= post_final_base)
    {
        return pre_base <= value_base && value_base <= post_final_base;
    }
    else
    {
        return (value_base <= post_final_base) ||
               (pre_base <= value_base &&
                value_base <= ExpectedValue::GetValueBaseMask());
    }
}
/* old namestyle
bool ExpectedValueHelper::MustHaveNotExisted(
    ExpectedValue pre_read_expected_value,
    ExpectedValue post_read_expected_value)
{
    const bool pre_read_expected_deleted = pre_read_expected_value.IsDeleted();

    const uint32_t pre_read_expected_value_base =
        pre_read_expected_value.GetValueBase();

    const uint32_t post_read_expected_final_value_base =
        post_read_expected_value.GetFinalValueBase();

    const bool during_read_no_write_happened =
        (pre_read_expected_value_base == post_read_expected_final_value_base);
    return pre_read_expected_deleted && during_read_no_write_happened;
}

bool ExpectedValueHelper::MustHaveExisted(
    ExpectedValue pre_read_expected_value,
    ExpectedValue post_read_expected_value)
{
    const bool pre_read_expected_not_deleted =
        !pre_read_expected_value.IsDeleted();

    const uint32_t pre_read_expected_del_counter =
        pre_read_expected_value.GetDelCounter();
    const uint32_t post_read_expected_final_del_counter =
        post_read_expected_value.GetFinalDelCounter();

    const bool during_read_no_delete_happened =
        (pre_read_expected_del_counter == post_read_expected_final_del_counter);

    return pre_read_expected_not_deleted && during_read_no_delete_happened;
}

bool ExpectedValueHelper::InExpectedValueBaseRange(
    uint32_t value_base,
    ExpectedValue pre_read_expected_value,
    ExpectedValue post_read_expected_value)
{
    assert(ExpectedValue::IsValueBaseValid(value_base));

    const uint32_t pre_read_expected_value_base =
        pre_read_expected_value.GetValueBase();
    const uint32_t post_read_expected_final_value_base =
        post_read_expected_value.GetFinalValueBase();

    if (pre_read_expected_value_base <= post_read_expected_final_value_base)
    {
        const uint32_t lower_bound = pre_read_expected_value_base;
        const uint32_t upper_bound = post_read_expected_final_value_base;
        return lower_bound <= value_base && value_base <= upper_bound;
    }
    else
    {
        const uint32_t upper_bound_1 = post_read_expected_final_value_base;
        const uint32_t lower_bound_2 = pre_read_expected_value_base;
        const uint32_t upper_bound_2 = ExpectedValue::GetValueBaseMask();
        return (value_base <= upper_bound_1) ||
               (lower_bound_2 <= value_base && value_base <= upper_bound_2);
    }
}
*/
}  // namespace StressTest