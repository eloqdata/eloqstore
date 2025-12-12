#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "eloq_store.h"

#ifndef __AFL_INIT
#define __AFL_INIT() ((void) 0)
#endif

#ifndef __AFL_LOOP
static inline int __AFL_LOOP(unsigned int) noexcept
{
    return 1;
}
#endif

namespace fs = std::filesystem;

namespace
{
constexpr size_t kMaxInputSize = 4096;
constexpr size_t kMaxKeySize = 24;
constexpr size_t kMaxValueSize = 96;
constexpr size_t kMaxBatch = 24;
constexpr size_t kMaxInflight = 64;

using InflightCounter = std::atomic<size_t>;
using RequestHolder =
    std::unique_ptr<eloqstore::KvRequest,
                    std::function<void(eloqstore::KvRequest *)>>;

bool IsAcceptableStatus(const eloqstore::KvRequest *req)
{
    const eloqstore::KvError err = req->Error();
    switch (req->Type())
    {
    case eloqstore::RequestType::BatchWrite:
    case eloqstore::RequestType::Truncate:
        return err == eloqstore::KvError::NoError;
    case eloqstore::RequestType::Read:
    case eloqstore::RequestType::Floor:
    case eloqstore::RequestType::Scan:
        return err == eloqstore::KvError::NoError ||
               err == eloqstore::KvError::NotFound;
    default:
        return true;
    }
}

void WaitForInflightBudget(InflightCounter &inflight)
{
    while (inflight.load(std::memory_order_relaxed) >= kMaxInflight)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

template <typename Request, typename F>
void SubmitAsync(eloqstore::EloqStore &store,
                 InflightCounter &inflight,
                 std::vector<RequestHolder> &owned_reqs,
                 F &&fill_request)
{
    WaitForInflightBudget(inflight);
    auto *req = new Request();
    RequestHolder holder(
        req, [](eloqstore::KvRequest *ptr)
        { delete static_cast<Request *>(ptr); });
    fill_request(*req);
    inflight.fetch_add(1, std::memory_order_relaxed);
    auto on_finish = [&inflight](eloqstore::KvRequest *done)
    {
        [[maybe_unused]] const bool ok = IsAcceptableStatus(done);
        inflight.fetch_sub(1, std::memory_order_relaxed);
    };
    if (!store.ExecAsyn(req, 0, on_finish))
    {
        inflight.fetch_sub(1, std::memory_order_relaxed);
        return;
    }
    owned_reqs.push_back(std::move(holder));
}

struct Cursor
{
    Cursor(const uint8_t *data, size_t len) : data_(data), len_(len)
    {
    }

    size_t Remaining() const
    {
        return len_ - pos_;
    }

    bool TakeBool()
    {
        return (TakeU8() & 1U) != 0;
    }

    uint8_t TakeU8()
    {
        if (pos_ >= len_)
        {
            return 0;
        }
        return data_[pos_++];
    }

    uint64_t TakeU64()
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
        {
            v = (v << 8U) | TakeU8();
        }
        return v;
    }

    std::string TakeString(size_t max_len)
    {
        if (Remaining() == 0)
        {
            return {};
        }
        size_t want = TakeU8() % (max_len + 1);
        want = std::min(want, Remaining());
        std::string out;
        out.reserve(want);
        for (size_t i = 0; i < want && pos_ < len_; ++i)
        {
            out.push_back(static_cast<char>('a' + (data_[pos_++] % 26)));
        }
        return out;
    }

    size_t TakeSize(size_t max_val)
    {
        return static_cast<size_t>(TakeU8()) % (max_val + 1);
    }

private:
    const uint8_t *data_;
    size_t len_;
    size_t pos_{0};
};

void DoWrite(eloqstore::EloqStore &store,
             const eloqstore::TableIdent &tbl,
             Cursor &cursor,
             eloqstore::WriteOp op,
             InflightCounter &inflight,
             std::vector<RequestHolder> &owned_reqs)
{
    std::string key = cursor.TakeString(kMaxKeySize);
    if (key.empty())
    {
        key = "k";
    }
    std::string val;
    if (op == eloqstore::WriteOp::Upsert)
    {
        val = cursor.TakeString(kMaxValueSize);
    }
    uint64_t ts = cursor.TakeU64();
    uint64_t expire_ts =
        op == eloqstore::WriteOp::Delete
            ? 0
            : ((cursor.TakeU8() & 1U) != 0 ? cursor.TakeU64() : 0);

    std::vector<eloqstore::WriteDataEntry> batch;
    batch.emplace_back(std::move(key), std::move(val), ts, op, expire_ts);
    SubmitAsync<eloqstore::BatchWriteRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, batch = std::move(batch)](
            eloqstore::BatchWriteRequest &pending) mutable
        { pending.SetArgs(tbl, std::move(batch)); });
}

void DoRead(eloqstore::EloqStore &store,
            const eloqstore::TableIdent &tbl,
            Cursor &cursor,
            InflightCounter &inflight,
            std::vector<RequestHolder> &owned_reqs)
{
    std::string key = cursor.TakeString(kMaxKeySize);
    if (key.empty())
    {
        return;
    }
    SubmitAsync<eloqstore::ReadRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, key = std::move(key)](eloqstore::ReadRequest &pending)
        { pending.SetArgs(tbl, std::move(key)); });
}

void DoScan(eloqstore::EloqStore &store,
            const eloqstore::TableIdent &tbl,
            Cursor &cursor,
            InflightCounter &inflight,
            std::vector<RequestHolder> &owned_reqs)
{
    std::string begin = cursor.TakeString(kMaxKeySize);
    std::string end = cursor.TakeString(kMaxKeySize);
    if (begin.empty() && end.empty())
    {
        return;
    }
    if (end.empty())
    {
        end = begin;
        end.push_back('z');
    }
    if (begin >= end)
    {
        std::swap(begin, end);
    }

    const size_t prefetch = 1 + (cursor.TakeU8() % 4);
    SubmitAsync<eloqstore::ScanRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, begin = std::move(begin), end = std::move(end), prefetch](
            eloqstore::ScanRequest &pending)
        {
            pending.SetArgs(tbl, begin, end);
            pending.SetPagination(8, 4 * 1024);
            pending.SetPrefetchPageNum(prefetch);
        });
}

void DoFloor(eloqstore::EloqStore &store,
             const eloqstore::TableIdent &tbl,
             Cursor &cursor,
             InflightCounter &inflight,
             std::vector<RequestHolder> &owned_reqs)
{
    std::string key = cursor.TakeString(kMaxKeySize);
    if (key.empty())
    {
        return;
    }
    SubmitAsync<eloqstore::FloorRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, key = std::move(key)](eloqstore::FloorRequest &pending)
        { pending.SetArgs(tbl, std::move(key)); });
}

void DoTruncate(eloqstore::EloqStore &store,
                const eloqstore::TableIdent &tbl,
                Cursor &cursor,
                InflightCounter &inflight,
                std::vector<RequestHolder> &owned_reqs,
                std::vector<std::shared_ptr<std::string>> &truncate_payloads)
{
    std::string position = cursor.TakeString(kMaxKeySize);
    if (position.empty())
    {
        position = "0";
    }
    auto position_holder = std::make_shared<std::string>(std::move(position));
    truncate_payloads.push_back(position_holder);
    SubmitAsync<eloqstore::TruncateRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, position_holder](
            eloqstore::TruncateRequest &pending)
        { pending.SetArgs(tbl, *position_holder); });
}

void DoBatchWrite(eloqstore::EloqStore &store,
                  const eloqstore::TableIdent &tbl,
                  Cursor &cursor,
                  InflightCounter &inflight,
                  std::vector<RequestHolder> &owned_reqs)
{
    const size_t count = 1 + cursor.TakeSize(kMaxBatch);
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.reserve(count);

    // 第一步：读取游标数据到 entries 中（原有逻辑）
    for (size_t i = 0; i < count && cursor.Remaining() > 0; ++i)
    {
        std::string key = cursor.TakeString(kMaxKeySize);
        if (key.empty())
        {
            key = "k" + std::to_string(i);
        }
        const bool is_upsert = cursor.TakeBool();
        std::string val;
        if (is_upsert)
        {
            val = cursor.TakeString(kMaxValueSize);
        }
        uint64_t ts = cursor.TakeU64();
        uint64_t expire_ts =
            is_upsert ? (cursor.TakeBool() ? cursor.TakeU64() : 0) : 0;

        entries.emplace_back(
            std::move(key),
            std::move(val),
            ts,
            is_upsert ? eloqstore::WriteOp::Upsert : eloqstore::WriteOp::Delete,
            expire_ts);
    }

    std::sort(entries.begin(), entries.end());

    auto duplicate_predicate = [](const eloqstore::WriteDataEntry &a,
                                  const eloqstore::WriteDataEntry &b)
    { return a.key_ == b.key_; };
    auto dedup_end =
        std::unique(entries.begin(), entries.end(), duplicate_predicate);

    entries.erase(dedup_end, entries.end());

    SubmitAsync<eloqstore::BatchWriteRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, entries = std::move(entries)](
            eloqstore::BatchWriteRequest &pending) mutable
        { pending.SetArgs(tbl, std::move(entries)); });
}

void DoPaginatedScan(eloqstore::EloqStore &store,
                     const eloqstore::TableIdent &tbl,
                     Cursor &cursor,
                     InflightCounter &inflight,
                     std::vector<RequestHolder> &owned_reqs)
{
    std::string begin = cursor.TakeString(kMaxKeySize);
    std::string end = cursor.TakeString(kMaxKeySize);
    if (begin.empty() && end.empty())
    {
        return;
    }
    if (end.empty())
    {
        end = begin;
        end.push_back('z');
    }
    if (begin >= end)
    {
        std::swap(begin, end);
    }

    const size_t page_entries = 1 + cursor.TakeSize(64);
    const size_t page_size = 256 + (cursor.TakeSize(15) * 256);
    const size_t prefetch_pages = 1 + cursor.TakeSize(6);

    SubmitAsync<eloqstore::ScanRequest>(
        store,
        inflight,
        owned_reqs,
        [tbl, begin = std::move(begin), end = std::move(end), page_entries, page_size, prefetch_pages](
            eloqstore::ScanRequest &pending)
        {
            pending.SetArgs(tbl, begin, end);
            pending.SetPagination(page_entries, page_size);
            pending.SetPrefetchPageNum(prefetch_pages);
        });
}

void MaybeSleep(Cursor &cursor)
{
    const uint8_t pause_ms = cursor.TakeU8() % 5;
    if (pause_ms != 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
    }
}

eloqstore::KvOptions BuildOptions(const fs::path &workdir, Cursor &cursor)
{
    eloqstore::KvOptions opts;
    opts.store_path = {workdir.string()};
    opts.num_threads = 1 + (cursor.TakeU8() % 4);
    opts.fd_limit = 64 + cursor.TakeU8();
    opts.io_queue_size = 64 + (cursor.TakeU8() % 8) * 64;
    opts.max_write_batch_pages = 8 + cursor.TakeU8();
    opts.pages_per_file_shift = 10 + (cursor.TakeU8() % 6);
    opts.data_append_mode = true;
    opts.enable_compression = cursor.TakeBool();
    opts.skip_verify_checksum = cursor.TakeBool();
    opts.overflow_pointers = static_cast<uint8_t>(
        4 + (cursor.TakeU8() % (eloqstore::max_overflow_pointers - 3)));
    opts.data_page_size =
        static_cast<uint16_t>(4 * 1024 << (cursor.TakeU8() % 2));
    return opts;
}

void RunOneInput(const uint8_t *data, size_t len)
{
    if (data == nullptr || len == 0)
    {
        return;
    }

    Cursor cursor(data, len);
    uint64_t salt = cursor.TakeU64();

    fs::path base = fs::temp_directory_path() / "eloq_afl";
    fs::path workdir = base / std::to_string(salt);
    std::error_code ec;
    fs::create_directories(workdir, ec);

    eloqstore::KvOptions opts = BuildOptions(workdir, cursor);
    // fs::remove_all(workdir, ec);
    if (!eloqstore::EloqStore::ValidateOptions(opts))
    {
        fs::remove_all(workdir, ec);
        return;
    }

    eloqstore::EloqStore store(opts);
    if (store.Start() != eloqstore::KvError::NoError)
    {
        fs::remove_all(workdir, ec);
        return;
    }

    std::vector<eloqstore::TableIdent> tables;
    const size_t tbl_count = 1 + (cursor.TakeU8() % 3);
    tables.reserve(tbl_count);
    for (size_t i = 0; i < tbl_count; ++i)
    {
        std::string name = "afl_tbl_" + std::to_string(i);
        name.push_back(static_cast<char>('a' + (cursor.TakeU8() % 26)));
        tables.emplace_back(std::move(name),
                            static_cast<uint32_t>(cursor.TakeU8()));
    }

    const size_t op_budget = 1 + (cursor.TakeU8() % 64);
    InflightCounter inflight{0};
    std::vector<RequestHolder> owned_reqs;
    owned_reqs.reserve(op_budget + 4);
    std::vector<std::shared_ptr<std::string>> truncate_payloads;
    truncate_payloads.reserve(op_budget / 2 + 1);
    for (size_t i = 0; i < op_budget && cursor.Remaining() > 0; ++i)
    {
        const eloqstore::TableIdent &tbl =
            tables[cursor.TakeU8() % tables.size()];
        uint8_t op = cursor.TakeU8();
        switch (op % 7)
        {
        case 0:
            DoBatchWrite(store, tbl, cursor, inflight, owned_reqs);
            break;
        case 1:
            DoWrite(store,
                    tbl,
                    cursor,
                    eloqstore::WriteOp::Upsert,
                    inflight,
                    owned_reqs);
            break;
        case 2:
            DoWrite(store,
                    tbl,
                    cursor,
                    eloqstore::WriteOp::Delete,
                    inflight,
                    owned_reqs);
            break;
        case 3:
            DoRead(store, tbl, cursor, inflight, owned_reqs);
            break;
        case 4:
            DoScan(store, tbl, cursor, inflight, owned_reqs);
            break;
        case 5:
            DoPaginatedScan(store, tbl, cursor, inflight, owned_reqs);
            break;
        case 6:
            DoFloor(store, tbl, cursor, inflight, owned_reqs);
            break;
        }
        if ((op & 3U) == 0 && cursor.TakeBool())
        {
            DoTruncate(
                store, tbl, cursor, inflight, owned_reqs, truncate_payloads);
        }
        if ((op & 1U) != 0)
        {
            MaybeSleep(cursor);
        }
    }

    while (inflight.load(std::memory_order_acquire) != 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    owned_reqs.clear();
    truncate_payloads.clear();

    store.Stop();
    fs::remove_all(workdir, ec);
}
}  // namespace

int main()
{
    __AFL_INIT();
    std::vector<uint8_t> buf(kMaxInputSize);
    while (__AFL_LOOP(1000))
    {
        ssize_t len = read(STDIN_FILENO, buf.data(), buf.size());
        if (len <= 0)
        {
            break;
        }
        RunOneInput(buf.data(), static_cast<size_t>(len));
    }
    return 0;
}
