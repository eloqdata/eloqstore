#include "test_utils.h"

#include <sys/types.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>

#include "error.h"
#include "replayer.h"
#include "scan_task.h"
#include "types.h"
#include "utils.h"

namespace test_util
{
std::string Key(uint64_t key, uint16_t len)
{
    const uint16_t digits = std::min(len, static_cast<uint16_t>(20));
    std::stringstream ss;
    ss << std::setw(digits) << std::setfill('0') << key;
    std::string kstr = ss.str();
    if (kstr.size() < len)
    {
        kstr.resize(len, '#');
    }
    return kstr;
}

std::string Value(uint64_t val, uint32_t len)
{
    std::string s = std::to_string(val);
    if (s.size() < len)
    {
        thread_local std::mt19937 rng{std::random_device{}()};
        static const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);

        s.reserve(len);
        while (s.size() < len)
        {
            s.push_back(charset[dist(rng)]);
        }
    }
    return s;
}

void EncodeKey(char *dst, uint32_t key)
{
    eloqstore::EncodeFixed32(dst, eloqstore::ToBigEndian(key));
}

void EncodeKey(std::string *dst, uint32_t key)
{
    eloqstore::PutFixed32(dst, eloqstore::ToBigEndian(key));
}

uint32_t DecodeKey(const std::string &key)
{
    return eloqstore::BigEndianToNative(eloqstore::DecodeFixed32(key.data()));
}

void EncodeValue(std::string *dst, uint32_t val)
{
    eloqstore::PutFixed32(dst, val);
    assert(dst->size() == sizeof(uint32_t));
    if (val > dst->size())
    {
        dst->resize(val, '*');
    }
}

uint32_t DecodeValue(const std::string &val)
{
    uint32_t v = eloqstore::DecodeFixed32(val.data());
    CHECK(v == val.size() || v < sizeof(uint32_t));
    return v;
}

std::string FormatEntries(tcb::span<eloqstore::KvEntry> entries)
{
    std::string kvs_str;
    for (auto &[k, v, ts, exp] : entries)
    {
        uint32_t key = DecodeKey(k);
        uint32_t val = DecodeValue(v);
        kvs_str.push_back('{');
        kvs_str.append(std::to_string(key));
        kvs_str.push_back(':');
        kvs_str.append(std::to_string(val));
        kvs_str.push_back('}');
    }
    return kvs_str;
}

std::pair<std::string, eloqstore::KvError> Scan(
    eloqstore::EloqStore *store,
    const eloqstore::TableIdent &tbl_id,
    uint32_t begin,
    uint32_t end)
{
    char begin_buf[sizeof(uint32_t)];
    char end_buf[sizeof(uint32_t)];
    EncodeKey(begin_buf, begin);
    EncodeKey(end_buf, end);
    std::string_view begin_key(begin_buf, sizeof(uint32_t));
    std::string_view end_key(end_buf, sizeof(uint32_t));
    eloqstore::ScanRequest req;
    req.SetArgs(tbl_id, begin_key, end_key);
    store->ExecSync(&req);
    if (req.Error() != eloqstore::KvError::NoError)
    {
        return {{}, req.Error()};
    }
    auto result = req.Entries();
    std::span entries(result.data(), result.size());
    return {test_util::FormatEntries(entries), eloqstore::KvError::NoError};
}

MapVerifier::MapVerifier(eloqstore::TableIdent tid,
                         eloqstore::EloqStore *store,
                         bool validate,
                         uint16_t key_len)
    : tid_(std::move(tid)),
      eloq_store_(store),
      auto_validate_(validate),
      key_len_(key_len)
{
}

MapVerifier::~MapVerifier()
{
    if (!answer_.empty())
    {
        Clean();
    }
}

void MapVerifier::Upsert(uint64_t key)
{
    Upsert(key, key + 1);
}

void MapVerifier::Upsert(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Upsert(" << begin << ',' << end << ')';

    const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
    const eloqstore::WriteOp upsert = eloqstore::WriteOp::Upsert;
    std::vector<eloqstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx, key_len_);
        std::string val = Value(ts_ + idx, val_size_);
        uint64_t expire_ts = max_ttl_ == 0 ? 0 : now_ts + max_ttl_;
        entries.emplace_back(key, val, ts_, upsert, expire_ts);
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Delete(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Delete(" << begin << ',' << end << ')';

    std::vector<eloqstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx, key_len_);
        entries.emplace_back(key, "", ts_, eloqstore::WriteOp::Delete);
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Truncate(uint64_t position, bool delete_all)
{
    LOG(INFO) << "Truncate(" << position << ", delete_all=" << delete_all
              << ')';

    eloqstore::TruncateRequest req;
    if (delete_all)
    {
        // Empty position means delete all data
        req.SetTableId(tid_);
    }
    else
    {
        std::string key = Key(position, key_len_);
        req.SetArgs(tid_, key);
    }
    ExecWrite(&req);
}

void MapVerifier::WriteRnd(uint64_t begin,
                           uint64_t end,
                           uint8_t del,
                           uint8_t density)
{
    constexpr uint8_t max = 100;
    del = del > max ? max : del;
    density = density > max ? max : density;
    LOG(INFO) << "WriteRnd(" << begin << ',' << end << ',' << int(del) << ','
              << int(density) << ')';

    const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
    std::vector<eloqstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        if ((std::rand() % max) >= density)
        {
            continue;
        }

        std::string key = Key(idx, key_len_);
        uint64_t ts = ts_;
        if ((std::rand() % max) < del)
        {
            entries.emplace_back(
                std::move(key), std::string(), ts, eloqstore::WriteOp::Delete);
        }
        else
        {
            uint32_t len = (std::rand() % val_size_) + 1;
            std::string val = Value(ts + idx, len);
            uint64_t expire_ts = 0;
            if (max_ttl_ > 0 && (std::rand() & 1))
            {
                expire_ts = now_ts + std::rand() % max_ttl_;
            }
            entries.emplace_back(std::move(key),
                                 std::move(val),
                                 ts,
                                 eloqstore::WriteOp::Upsert,
                                 expire_ts);
        }
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Clean()
{
    LOG(INFO) << "Clean()";

    eloqstore::TruncateRequest req;
    req.SetArgs(tid_, {});
    ExecWrite(&req);
}

void MapVerifier::Read(uint64_t key)
{
    Read(Key(key, key_len_));
}

void MapVerifier::Read(std::string_view key)
{
    DLOG(INFO) << "Read(" << key << ')';

    eloqstore::ReadRequest req;
    req.SetArgs(tid_, key);
    eloq_store_->ExecSync(&req);
    std::string str_key(key);
    if (req.Error() == eloqstore::KvError::NoError)
    {
        eloqstore::KvEntry ret(str_key, req.value_, req.ts_, req.expire_ts_);
        CHECK(answer_.at(str_key) == ret);
    }
    else
    {
        CHECK(req.Error() == eloqstore::KvError::NotFound);
        auto it = answer_.find(str_key);
        if (it != answer_.end())
        {
            const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
            CHECK(it->second.expire_ts_ < now_ts);
            answer_.erase(it);
        }
    }
}

void MapVerifier::Floor(uint64_t key)
{
    Floor(Key(key, key_len_));
}

void MapVerifier::Floor(std::string_view key)
{
    LOG(INFO) << "Floor(" << key << ')';

    eloqstore::FloorRequest req;
    req.SetArgs(tid_, key);
    eloq_store_->ExecSync(&req);
    auto it_lb = answer_.upper_bound(std::string(key));
    if (it_lb != answer_.begin())
    {
        it_lb--;
        if (req.Error() == eloqstore::KvError::NotFound)
        {
            const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
            CHECK(it_lb->second.expire_ts_ < now_ts);
            answer_.erase(it_lb);
            return;
        }
        eloqstore::KvEntry ret(
            req.floor_key_, req.value_, req.ts_, req.expire_ts_);
        CHECK(it_lb->second == ret);
    }
    else
    {
        CHECK(req.Error() == eloqstore::KvError::NotFound);
    }
}

void MapVerifier::Scan(uint64_t begin,
                       uint64_t end,
                       size_t page_entries,
                       size_t page_size)
{
    Scan(Key(begin, key_len_), Key(end, key_len_), page_entries, page_size);
}

void MapVerifier::Scan(std::string_view begin,
                       std::string_view end,
                       size_t page_entries,
                       size_t page_size)
{
    DLOG(INFO) << "Scan(" << begin << ',' << end << ')';

    eloqstore::ScanRequest req;
    req.SetPagination(page_entries, page_size);

    std::string begin_key(begin);
    const std::string end_key(end);

    auto it = answer_.lower_bound(begin_key);
    const auto it_end = answer_.lower_bound(end_key);

    auto clean_expired = [&](std::string_view next_key)
    {
        const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
        while (it != it_end && it->first < next_key)
        {
            CHECK(it->second.expire_ts_ != 0);
            CHECK(it->second.expire_ts_ <= now_ts);
            answer_.erase(it++);
        }
    };

    req.SetArgs(tid_, begin_key, end_key);
    while (true)
    {
        eloq_store_->ExecSync(&req);
        if (req.Error() != eloqstore::KvError::NoError)
        {
            CHECK(req.Error() == eloqstore::KvError::NotFound);
            CHECK(answer_.empty());
            break;
        }

        // Verify scan result
        auto [n_entries, n_bytes] = req.ResultSize();
        CHECK(n_entries <= page_entries);
        CHECK(n_bytes <= page_size || n_entries == 1);
        for (auto &entry : req.Entries())
        {
            clean_expired(entry.key_);
            CHECK(entry == it->second);
            it++;
        }

        if (!req.HasRemaining())
        {
            break;
        }
        // Continue scan the next page.
        CHECK(!req.Entries().empty());
        begin_key = req.Entries().back().key_;
        req.SetArgs(tid_, begin_key, end_key, false);
    }
    clean_expired(end_key);
}

void MapVerifier::Validate()
{
    Scan({}, Key(UINT64_MAX, 20), 1000);
}

void MapVerifier::ExecWrite(eloqstore::KvRequest *req)
{
    switch (req->Type())
    {
    case eloqstore::RequestType::BatchWrite:
    {
        const auto wreq = static_cast<eloqstore::BatchWriteRequest *>(req);
        for (const eloqstore::WriteDataEntry &ent : wreq->batch_)
        {
            auto it = answer_.find(ent.key_);
            if (it == answer_.end())
            {
                if (ent.op_ == eloqstore::WriteOp::Delete)
                {
                    continue;
                }
                auto ret = answer_.try_emplace(ent.key_);
                assert(ret.second);
                it = ret.first;
            }
            else
            {
                if (ent.timestamp_ <= it->second.timestamp_)
                {
                    continue;
                }
            }
            assert(it != answer_.end());

            if (ent.op_ == eloqstore::WriteOp::Upsert)
            {
                it->second = eloqstore::KvEntry(
                    ent.key_, ent.val_, ent.timestamp_, ent.expire_ts_);
            }
            else if (ent.op_ == eloqstore::WriteOp::Delete)
            {
                answer_.erase(it);
            }
            else
            {
                assert(false);
            }
        }
        break;
    }
    case eloqstore::RequestType::Truncate:
    {
        const auto treq = static_cast<eloqstore::TruncateRequest *>(req);
        if (treq->position_.empty())
        {
            // Delete all data when position is empty
            answer_.clear();
        }
        else
        {
            auto it = answer_.lower_bound(std::string(treq->position_));
            answer_.erase(it, answer_.end());
        }
        break;
    }
    default:
        assert(false);
    }

    eloq_store_->ExecSync(req);
    CHECK(req->Error() == eloqstore::KvError::NoError);

    if (auto_validate_)
    {
        Validate();
    }
    ts_++;
}

void MapVerifier::SetAutoValidate(bool v)
{
    auto_validate_ = v;
}

void MapVerifier::SetValueSize(uint32_t val_size)
{
    val_size_ = val_size;
}

void MapVerifier::SetStore(eloqstore::EloqStore *store)
{
    eloq_store_ = store;
}

void MapVerifier::SetTimestamp(uint64_t ts)
{
    ts_ = ts;
}

void MapVerifier::SetMaxTTL(uint32_t max_ttl)
{
    max_ttl_ = max_ttl;
}

const std::map<std::string, eloqstore::KvEntry> &MapVerifier::DataSet() const
{
    return answer_;
}

void MapVerifier::SwitchDataSet(
    const std::map<std::string, eloqstore::KvEntry> &new_dataset)
{
    answer_ = new_dataset;
}

bool ConcurrencyTester::Partition::IsWriting() const
{
    return ticks_ & 1;
}

uint32_t ConcurrencyTester::Partition::FinishedRounds() const
{
    return ticks_ >> 1;
}

void ConcurrencyTester::Partition::FinishWrite()
{
    CHECK(req_.Error() == eloqstore::KvError::NoError);
    verify_cnt_ = 0;
    ticks_++;
}

ConcurrencyTester::ConcurrencyTester(eloqstore::EloqStore *store,
                                     std::string tbl_name,
                                     uint32_t n_partitions,
                                     uint16_t seg_count,
                                     uint8_t seg_size,
                                     uint32_t val_size)
    : val_size_(val_size),
      seg_size_(seg_size),
      seg_count_(seg_count),
      seg_sum_(seg_size * val_size),
      tbl_name_(std::move(tbl_name)),
      partitions_(n_partitions),
      finished_reqs_(n_partitions),
      store_(store)
{
    CHECK(val_size >= sizeof(uint32_t));
    for (uint32_t i = 0; i < n_partitions; i++)
    {
        partitions_[i].id_ = i;
    }
}

void ConcurrencyTester::Wake(eloqstore::KvRequest *req)
{
    bool ok = finished_reqs_.enqueue(req->UserData());
    CHECK(ok);
}

void ConcurrencyTester::ExecRead(Reader *reader)
{
    reader->partition_id_ = (reader->partition_id_ + 1) % partitions_.size();
    const Partition &partition = partitions_[reader->partition_id_];
    reader->start_tick_ = partition.ticks_;
    reader->begin_ = (std::rand() % seg_count_) * seg_size_;
    reader->end_ = reader->begin_ + seg_size_;
    EncodeKey(reader->begin_key_, reader->begin_);
    EncodeKey(reader->end_key_, reader->end_);
    std::string_view begin_key(reader->begin_key_, sizeof(uint32_t));
    std::string_view end_key(reader->end_key_, sizeof(uint32_t));
    reader->req_.SetArgs({tbl_name_, partition.id_}, begin_key, end_key);
    uint64_t user_data = reader->id_;
    bool ok =
        store_->ExecAsyn(&reader->req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}

void ConcurrencyTester::VerifyRead(Reader *reader, uint32_t write_pause)
{
    if (reader->req_.RetryableErr())
    {
        LOG(WARNING) << "read error " << reader->req_.ErrMessage();
        return;
    }
    CHECK(reader->req_.Error() == eloqstore::KvError::NoError);
    const uint32_t key_begin = reader->begin_;
    const uint32_t key_end = reader->end_;
    const uint16_t seg_id = key_begin / seg_size_;
    const uint32_t partition_id = reader->partition_id_;
    Partition &partition = partitions_[partition_id];
    auto result = reader->req_.Entries();
    std::span entries(result.data(), result.size());

    uint64_t sum_val = 0;
    for (auto &ent : entries)
    {
        uint32_t val = DecodeValue(ent.value_);
        sum_val += val;
    }
    if (seg_sum_ != sum_val)
    {
        LOG(FATAL) << "sum of value mismatch " << sum_val << " != " << seg_sum_
                   << '\n'
                   << DebugSegment(partition_id, seg_id, &entries);
    }
    verify_sum_++;

    if (!partition.IsWriting() && partition.ticks_ == reader->start_tick_)
    {
        uint32_t key_ans = key_begin;
        for (auto &[k, v, ts, exp] : entries)
        {
            while (partition.kvs_[key_ans] == 0)
            {
                key_ans++;
            }

            uint32_t key_res = DecodeKey(k);
            uint32_t val_res = DecodeValue(v);
            CHECK(key_res < key_end);
            if (key_ans != key_res || partition.kvs_[key_ans] != val_res)
            {
                LOG(FATAL) << "segment kvs mismatch " << '\n'
                           << DebugSegment(partition_id, seg_id, &entries);
            }

            key_ans++;
        }
        verify_kv_++;
    }
    reader->verify_cnt_++;
    partition.verify_cnt_++;

    if (!partition.IsWriting() && partition.verify_cnt_ >= write_pause)
    {
        // Trigger next write.
        ExecWrite(partition);
    }
}

// Tester: {100:5}{102:9}{103:2}
// Store:  {100:5}{102:9}{103:2}
std::string ConcurrencyTester::DebugSegment(
    uint32_t partition_id,
    uint16_t seg_id,
    std::span<eloqstore::KvEntry> *resp) const
{
    const Partition &partition = partitions_[partition_id];
    const uint32_t begin = seg_id * seg_size_;
    const uint32_t end = begin + seg_size_;

    std::string kvs_str =
        "table " + tbl_name_ + " partition " + std::to_string(partition_id) +
        " segment " + std::to_string(seg_id) + " [" + std::to_string(begin) +
        ',' + std::to_string(end) + ')';

    kvs_str.append("\nTester: ");
    for (uint32_t k = begin; k < end; k++)
    {
        uint32_t v = partition.kvs_[k];
        if (v > 0)
        {
            kvs_str.push_back('{');
            kvs_str.append(std::to_string(k));
            kvs_str.push_back(':');
            kvs_str.append(std::to_string(v));
            kvs_str.push_back('}');
        }
    }

    kvs_str.append("\nStore:  ");
    if (resp != nullptr)
    {
        kvs_str.append(FormatEntries(*resp));
        return kvs_str;
    }

    auto ret = Scan(store_, {tbl_name_, partition_id}, begin, end);
    if (ret.second != eloqstore::KvError::NoError)
    {
        kvs_str.append(eloqstore::ErrorString(ret.second));
    }
    else
    {
        kvs_str.append(ret.first);
    }
    return kvs_str;
}

void ConcurrencyTester::ExecWrite(Partition &partition)
{
    assert(!partition.IsWriting());
    partition.ticks_++;
    uint64_t ts = CurrentTimestamp();
    std::vector<eloqstore::WriteDataEntry> entries;
    const size_t total_size = partition.kvs_.size();
    uint32_t left = seg_sum_;
    uint32_t i = (std::rand() % seg_count_) * seg_size_;
    for (; i < total_size; i++)
    {
        uint32_t new_val = 0;
        const bool last = (i + 1) % seg_size_ == 0;
        if (last)
        {
            new_val = left;
            left = seg_sum_;
        }
        else if (std::rand() % 3 != 0)
        {
            new_val = std::rand() % (val_size_ * 3);
            new_val = std::min(new_val, left);
            left -= new_val;
        }

        if (new_val == 0)
        {
            if (partition.kvs_[i] != 0)
            {
                eloqstore::WriteDataEntry &ent = entries.emplace_back();
                EncodeKey(&ent.key_, i);
                ent.timestamp_ = ts;
                ent.op_ = eloqstore::WriteOp::Delete;
            }
        }
        else
        {
            eloqstore::WriteDataEntry &ent = entries.emplace_back();
            EncodeKey(&ent.key_, i);
            EncodeValue(&ent.val_, new_val);
            ent.timestamp_ = ts;
            ent.op_ = eloqstore::WriteOp::Upsert;
        }
        partition.kvs_[i] = new_val;

        if (last)
        {
            if ((std::rand() & 1) == 0)
            {
                i += seg_size_;
            }
        }
    }
    partition.req_.SetArgs({tbl_name_, partition.id_}, std::move(entries));
    SendWrite(partition);
}

void ConcurrencyTester::SendWrite(Partition &partition)
{
    uint64_t user_data = (partition.id_ | (uint64_t(1) << 63));
    bool ok =
        store_->ExecAsyn(&partition.req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}

bool ConcurrencyTester::HasWriting() const
{
    for (const Partition &partition : partitions_)
    {
        if (partition.IsWriting())
        {
            return true;
        }
    }
    return false;
}

void ConcurrencyTester::Init()
{
    uint64_t ts = CurrentTimestamp();
    const uint32_t kvs_num = seg_size_ * seg_count_;
    for (Partition &partition : partitions_)
    {
        eloqstore::TableIdent tbl_id(tbl_name_, partition.id_);

        // Try to load partition KVs from EloqStore
        eloqstore::ScanRequest scan_req;
        scan_req.SetArgs(tbl_id, {}, {});
        store_->ExecSync(&scan_req);
        CHECK(scan_req.Error() == eloqstore::KvError::NoError ||
              scan_req.Error() == eloqstore::KvError::NotFound);
        if (!scan_req.Entries().empty())
        {
            partition.kvs_.resize(kvs_num, 0);
            CHECK(scan_req.Entries().size() <= partition.kvs_.size());
            for (auto &[k, v, ts, exp] : scan_req.Entries())
            {
                uint32_t key_res = DecodeKey(k);
                uint32_t val_res = DecodeValue(v);
                CHECK(key_res < partition.kvs_.size());
                partition.kvs_[key_res] = val_res;
            }
            // verify partition KVs
            for (uint16_t seg = 0; seg < seg_count_; seg++)
            {
                uint64_t sum = 0;
                uint32_t idx = seg * seg_size_;
                for (uint8_t i = 0; i < seg_size_; i++)
                {
                    sum += partition.kvs_[idx++];
                }
                if (sum != seg_sum_)
                {
                    LOG(FATAL) << "segment sum is wrong " << '\n'
                               << DebugSegment(partition.id_, seg, nullptr);
                }
            }
            continue;
        }

        // Initialize partition KVs
        partition.kvs_.resize(kvs_num, val_size_);
        std::vector<eloqstore::WriteDataEntry> entries;
        for (uint32_t i = 0; i < kvs_num; i++)
        {
            eloqstore::WriteDataEntry &ent = entries.emplace_back();
            EncodeKey(&ent.key_, i);
            EncodeValue(&ent.val_, val_size_);
            ent.timestamp_ = ts;
            ent.op_ = eloqstore::WriteOp::Upsert;
        }
        partition.req_.SetArgs(tbl_id, std::move(entries));
        store_->ExecSync(&partition.req_);
        CHECK(partition.req_.Error() == eloqstore::KvError::NoError);
    }
}

void ConcurrencyTester::Run(uint16_t n_readers,
                            uint32_t ops,
                            uint32_t write_pause)
{
    uint16_t running_readers = 0;
    // Start readers
    std::vector<Reader> readers(n_readers);
    for (Reader &reader : readers)
    {
        reader.id_ = running_readers++;
        reader.partition_id_ = reader.id_ % partitions_.size();
        ExecRead(&reader);
    }

    while (running_readers > 0 || HasWriting())
    {
        uint64_t user_data;
        finished_reqs_.wait_dequeue(user_data);
        bool is_write = (user_data & (uint64_t(1) << 63));
        uint32_t id = (user_data & ((uint64_t(1) << 63) - 1));

        if (is_write)
        {
            Partition &partition = partitions_[id];
            if (partition.req_.RetryableErr())
            {
                LOG(WARNING) << "write error " << partition.req_.ErrMessage();
                SendWrite(partition);
                continue;
            }
            partition.FinishWrite();
            if (write_pause == 0 && partition.FinishedRounds() < ops)
            {
                ExecWrite(partition);
            }
        }
        else
        {
            Reader &reader = readers[id];
            VerifyRead(&reader, write_pause);
            if (reader.verify_cnt_ < ops)
            {
                ExecRead(&reader);
            }
            else
            {
                running_readers--;
            }
        }
    }

    LOG(INFO) << "concurrency test statistic: verify kvs " << verify_kv_
              << ", verify sum " << verify_sum_;
}

void ConcurrencyTester::Clear()
{
    for (Partition &part : partitions_)
    {
        eloqstore::TruncateRequest req;
        req.SetArgs({tbl_name_, part.id_}, {});
        store_->ExecSync(&req);
        CHECK(req.Error() == eloqstore::KvError::NoError);
    }
}

uint64_t ConcurrencyTester::CurrentTimestamp()
{
    return utils::UnixTs<std::chrono::nanoseconds>();
}

ManifestVerifier::ManifestVerifier(eloqstore::KvOptions opts)
    : options_(opts),
      io_mgr_(&options_),
      idx_mgr_(&io_mgr_),
      answer_(&idx_mgr_, &tbl_id_)
{
    if (!options_.data_append_mode)
    {
        answer_file_pages_ = static_cast<eloqstore::PooledFilePages *>(
            answer_.FilePgAllocator());
    }
}

std::pair<eloqstore::PageId, eloqstore::FilePageId>
ManifestVerifier::RandChoose()
{
    CHECK(!helper_.empty());
    auto it = std::next(helper_.begin(), std::rand() % helper_.size());
    return *it;
}

uint32_t ManifestVerifier::Size() const
{
    return helper_.size();
}

void ManifestVerifier::NewMapping()
{
    eloqstore::PageId page_id = answer_.GetPage();
    eloqstore::FilePageId file_page_id = answer_.FilePgAllocator()->Allocate();
    answer_.UpdateMapping(page_id, file_page_id);
    builder_.UpdateMapping(page_id, file_page_id);
    helper_[page_id] = file_page_id;
}

void ManifestVerifier::UpdateMapping()
{
    auto [page_id, old_fp_id] = RandChoose();
    root_id_ = page_id;

    eloqstore::FilePageId new_fp_id = answer_.FilePgAllocator()->Allocate();
    answer_.UpdateMapping(page_id, new_fp_id);
    builder_.UpdateMapping(page_id, new_fp_id);
    if (answer_file_pages_)
    {
        answer_file_pages_->Free({old_fp_id});
    }
    helper_[page_id] = new_fp_id;
}

void ManifestVerifier::FreeMapping()
{
    auto [page_id, file_page_id] = RandChoose();
    helper_.erase(page_id);
    if (page_id == root_id_)
    {
        root_id_ = Size() == 0 ? eloqstore::MaxPageId : RandChoose().first;
    }

    answer_.FreePage(page_id);
    builder_.DeleteMapping(page_id);
    if (answer_file_pages_)
    {
        answer_file_pages_->Free({file_page_id});
    }
}

void ManifestVerifier::Finish()
{
    if (!builder_.Empty())
    {
        if (file_.empty())
        {
            Snapshot();
        }
        else
        {
            std::string_view sv =
                builder_.Finalize(root_id_, eloqstore::MaxPageId);
            file_.append(sv);
            builder_.Reset();
        }
    }
}

void ManifestVerifier::Snapshot()
{
    eloqstore::FilePageId max_fp_id =
        answer_.FilePgAllocator()->MaxFilePageId();
    std::string_view sv = builder_.Snapshot(root_id_,
                                            eloqstore::MaxPageId,
                                            answer_.GetMapping(),
                                            max_fp_id,
                                            std::string_view{});
    file_ = sv;
    builder_.Reset();
}

void ManifestVerifier::Verify()
{
    eloqstore::MemStoreMgr::Manifest file(file_);
    eloqstore::Replayer replayer(&options_);
    eloqstore::KvError err = replayer.Replay(&file);
    CHECK(err == eloqstore::KvError::NoError);
    CHECK(replayer.root_ == root_id_);
    auto mapper = replayer.GetMapper(&idx_mgr_, &tbl_id_);

    const auto &answer_map = answer_.GetMapping()->mapping_tbl_;
    const auto &recovered_map = mapper->GetMapping()->mapping_tbl_;
    auto it_answer = answer_map.begin();
    auto it_recovered = recovered_map.begin();

    while (it_answer != answer_map.end() && it_recovered != recovered_map.end())
    {
        if (*it_answer != *it_recovered)
        {
            CHECK(!eloqstore::MappingSnapshot::IsFilePageId(*it_answer));
            CHECK(!eloqstore::MappingSnapshot::IsFilePageId(*it_recovered));
        }
        it_answer++;
        it_recovered++;
    }
    while (it_answer != answer_map.end())
    {
        CHECK(!eloqstore::MappingSnapshot::IsFilePageId(*it_answer));
        it_answer++;
    }
    while (it_recovered != recovered_map.end())
    {
        CHECK(!eloqstore::MappingSnapshot::IsFilePageId(*it_recovered));
        it_recovered++;
    }
}
}  // namespace test_util