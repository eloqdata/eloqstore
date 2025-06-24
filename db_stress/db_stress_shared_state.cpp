#include "db_stress_shared_state.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <sstream>
#include <string>

#include "db_stress_common.h"
#include "error.h"
#include "expected_value.h"

namespace StressTest
{
uint64_t UnixTimestamp()
{
    auto dur = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();
}

std::string Key(int64_t k)
{
    constexpr int sz = 8;
    std::stringstream ss;
    ss << std::setw(sz) << std::setfill('0') << k;
    std::string kstr = ss.str();
    assert(kstr.size() == sz);

    uint8_t level = JudgeKeyLevel(k);
    assert(level < 4);
    std::string post_fix(level * 8, 'x');
    kstr += post_fix;
    return kstr;
}

void ThreadState::ResetValues()
{
    const uint32_t del_mask = ExpectedValue::GetDelMask();
    for (size_t i = 0; i < n_partitions_; ++i)
    {
        for (size_t j = 0; j < max_key_; ++j)
        {
            Value(static_cast<int>(i), j) = del_mask;
        }
    }
}

void ThreadState::Precommit(uint32_t partition_id,
                            int64_t key,
                            ExpectedValue &value)
{
    Value(partition_id, key) = value.Read();
    // To prevent low-level instruction reordering that results
    // in db write happens before setting pending state in expected value
    std::atomic_thread_fence(std::memory_order_release);
}

PendingExpectedValue ThreadState::PreparePut(uint32_t partition_id, int64_t key)
{
    ExpectedValue expected_value = Load(partition_id, key);

    // Calculate the original expected value
    ExpectedValue orig_expected_value = expected_value;

    // Calculate the pending expected value
    expected_value.Put(true /* pending */);
    ExpectedValue pending_expected_value = expected_value;

    // Calculate the final expected value
    expected_value.Put(false /* pending */);
    ExpectedValue final_expected_value = expected_value;

    // Precommit
    Precommit(partition_id, key, pending_expected_value);
    return PendingExpectedValue(
        &Value(partition_id, key), orig_expected_value, final_expected_value);
}

PendingExpectedValue ThreadState::PrepareDelete(uint32_t partition_id,
                                                int64_t key)
{
    ExpectedValue expected_value = Load(partition_id, key);

    // Calculate the original expected value
    const ExpectedValue orig_expected_value = expected_value;

    // Calculate the pending expected value
    bool res = expected_value.Delete(true /* pending */);
    if (!res)
    {
        PendingExpectedValue ret =
            PendingExpectedValue(&Value(partition_id, key),
                                 orig_expected_value,
                                 orig_expected_value);
        return ret;
    }
    ExpectedValue pending_expected_value = expected_value;

    // Calculate the final expected value
    expected_value.Delete(false /* pending */);
    ExpectedValue final_expected_value = expected_value;

    // Precommit
    Precommit(partition_id, key, pending_expected_value);
    return PendingExpectedValue(
        &Value(partition_id, key), orig_expected_value, final_expected_value);
}

uint32_t FileThreadState::GetSeqno()
{
    uint32_t seqno;
    if (!std::filesystem::exists(GetFileNameForSeqno()))
    {
        seqno = 0;
    }
    else
    {
        FileReader.open(GetFileNameForSeqno());
        assert(FileReader >> seqno);
        ++seqno;
        FileReader.close();
    }

    return seqno;
}

void FileThreadState::ValuesFromFileToMem()
{
    if (!HasHistory())
        return;
    uint32_t value;
    FileReader.open(GetFileNameForLatest());
    assert(FileReader.is_open());
    for (size_t i = 0; i < n_partitions_; ++i)
    {
        for (size_t j = 0; j < max_key_; ++j)
        {
            assert(FileReader >> value);
            Value(static_cast<int>(i), j) = value;
        }
    }
    FileReader.close();
}

void FileThreadState::SaveAtAndAfter()
{
    std::filesystem::path source = GetFileNameForLatest();
    std::filesystem::path destination = GetFileNameForState(seqno_);

    try
    {
        std::filesystem::copy_file(
            source,
            destination,
            std::filesystem::copy_options::overwrite_existing);
    }
    catch (const std::filesystem::filesystem_error &e)
    {
        LOG(FATAL) << "Error When Copy File: " << e.what();
    }

    FileWriter.open(GetFileNameForSeqno());
    assert(FileWriter.is_open());
    FileWriter << seqno_;
    FileWriter.close();

    if (seqno_ > 0)
    {
        DeleteFile(GetFileNameForState(seqno_ - 1));
        DeleteFile(GetFileNameForTrace(seqno_ - 1));
    }
}

void FileThreadState::StartTracer()
{
    CHECK(std::filesystem::exists(GetFileNameForState(seqno_)));
    Tracer.open(GetFileNameForTrace(seqno_));
    assert(Tracer.is_open());
}

void FileThreadState::TraceOneBatch(uint32_t partition_id,
                                    std::vector<int64_t> &keys,
                                    bool IsPut)
{
    CHECK(Tracer.is_open());
    int opt = (IsPut ? 1 : 0);
    Tracer << partition_id << " " << opt << " ";
    for (size_t i = 0; i < keys.size(); ++i)
    {
        Tracer << keys[i] << " ";
    }
    Tracer << "\n";
    Tracer.flush();
}

void FileThreadState::Replay()
{
    if (seqno_ == 0)
        return;
    FileReader.open(GetFileNameForTrace(seqno_ - 1));
    CHECK(FileReader.is_open());
    uint32_t partition_id;
    bool IsPut;
    std::string line;
    while (std::getline(FileReader, line))
    {
        std::istringstream iss(line);
        if (iss >> partition_id)
            assert(0 <= partition_id && partition_id < n_partitions_);
        else
            break;

        if (iss >> IsPut)
            assert(IsPut == 0 || IsPut == 1);
        else
            break;

        int64_t key;
        while (iss >> key)
        {
            assert(0 <= key && key < max_key_);

            ExpectedValue expected_value = (Load(partition_id, key));
            if (IsPut)
            {
                expected_value.Put(false);
            }
            else
            {
                expected_value.Delete(false);
            }
            Value(partition_id, key) = expected_value.Read();
        }
    }
}

void FileThreadState::ValuesFromMemToFile()
{
    FileWriter.open(GetFileNameForLatest());
    CHECK(FileWriter.is_open());
    for (size_t i = 0; i < n_partitions_; ++i)
    {
        for (size_t j = 0; j < max_key_; ++j)
        {
            FileWriter << Value(static_cast<int>(i), j) << " ";
        }
    }
    FileWriter.close();
}

bool FileThreadState::DeleteFile(std::string filename)
{
    if (std::filesystem::exists(filename))
    {
        std::filesystem::remove(filename);
        CHECK(!std::filesystem::exists(filename));
        return true;
    }
    return false;
}

void FileThreadState::Clear()
{
    for (size_t i = 0; i <= seqno_; ++i)
    {
        DeleteFile(GetFileNameForState(i));
        DeleteFile(GetFileNameForTrace(i));
    }
    DeleteFile(GetFileNameForSeqno());
    DeleteFile(GetFileNameForLatest());
}

ThreadState *CreateFileThreadState()
{
    return new FileThreadState();
}

}  // namespace StressTest