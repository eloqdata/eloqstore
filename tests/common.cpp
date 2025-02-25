#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

#include "error.h"
#include "scan_task.h"
#include "table_ident.h"
#include "write_task.h"

void InitMemStore()
{
    if (memstore)
    {
        return;
    }

    kvstore::KvOptions opts;
    memstore = std::make_unique<kvstore::EloqStore>(opts);
    memstore->Start();
}

MapVerifier::MapVerifier(kvstore::TableIdent tid, kvstore::EloqStore *store)
    : tid_(std::move(tid)), eloq_store_(store)
{
}

MapVerifier::~MapVerifier()
{
    if (!answer_.empty())
    {
        Clean();
    }
}

void MapVerifier::Upsert(uint64_t begin, uint64_t end)
{
    std::cout << "Upsert(" << begin << ',' << end << ')' << std::endl;
    ts_++;

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        entries.emplace_back(
            Key(idx), std::to_string(idx), ts_, kvstore::WriteOp::Upsert);
        answer_.insert_or_assign(
            Key(idx), kvstore::KvEntry(Key(idx), std::to_string(idx), ts_));
    }
    req_.SetWrite(tid_, std::move(entries));
    DoWriteReq();
}

void MapVerifier::Delete(uint64_t begin, uint64_t end)
{
    std::cout << "Delete(" << begin << ',' << end << ')' << std::endl;
    ts_++;

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        entries.emplace_back(Key(idx), "", ts_, kvstore::WriteOp::Delete);
        answer_.erase(Key(idx));
    }
    req_.SetWrite(tid_, std::move(entries));
    DoWriteReq();
}

void MapVerifier::WriteRnd(uint64_t begin,
                           uint64_t end,
                           uint8_t del,
                           uint8_t density)
{
    constexpr uint8_t max = 100;
    del = del > max ? max : del;
    density = density > max ? max : density;
    std::cout << "WriteRnd(" << begin << ',' << end << ',' << int(del) << ','
              << int(density) << ')' << std::endl;
    ts_++;
    srand(ts_);

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        if ((rand() % max) >= density)
        {
            continue;
        }
        if ((rand() % max) < del)
        {
            entries.emplace_back(Key(idx), "", ts_, kvstore::WriteOp::Delete);
            answer_.erase(Key(idx));
        }
        else
        {
            entries.emplace_back(
                Key(idx), std::to_string(idx), ts_, kvstore::WriteOp::Upsert);
            answer_.insert_or_assign(
                Key(idx), kvstore::KvEntry(Key(idx), std::to_string(idx), ts_));
        }
    }
    req_.SetWrite(tid_, std::move(entries));
    DoWriteReq();
}

void MapVerifier::Clean()
{
    std::cout << "Clean()" << std::endl;
    ts_++;

    std::vector<kvstore::WriteDataEntry> entries;
    for (auto [k, _] : answer_)
    {
        entries.emplace_back(k, "", ts_, kvstore::WriteOp::Delete);
    }
    req_.SetWrite(tid_, std::move(entries));

    answer_.clear();

    DoWriteReq();
}

void MapVerifier::Read(uint64_t k)
{
    std::cout << "Read(" << k << ')' << std::endl;

    std::string key = Key(k);
    kvstore::KvRequest::Read &args = req_.SetRead(tid_, key);
    kvstore::KvError err = req_.ExecSync(eloq_store_);
    if (err == kvstore::KvError::NoError)
    {
        kvstore::KvEntry ret(key, args.val, args.ts);
        REQUIRE(answer_.at(key) == ret);
    }
    else
    {
        REQUIRE(err == kvstore::KvError::NotFound);
        REQUIRE(answer_.find(key) == answer_.end());
    }
}

void MapVerifier::Scan(uint64_t begin, uint64_t end)
{
    std::cout << "Scan(" << begin << ',' << end << ')' << std::endl;

    std::string begin_key = Key(begin);
    std::string end_key = Key(end);
    kvstore::KvRequest::Scan &args = req_.SetScan(tid_, begin_key, end_key);
    kvstore::KvError err = req_.ExecSync(eloq_store_);
    if (err == kvstore::KvError::NoError)
    {
        auto it = answer_.lower_bound(begin_key);
        for (auto &t : args.results)
        {
            REQUIRE(t == it->second);
            it++;
        }
    }
    else
    {
        assert(err == kvstore::KvError::NotFound);
        REQUIRE(answer_.empty());
    }
}

void MapVerifier::Validate()
{
    kvstore::KvRequest::Scan &args = req_.SetScan(tid_, "", "");
    kvstore::KvError err = req_.ExecSync(eloq_store_);
    if (err == kvstore::KvError::NotFound)
    {
        REQUIRE(answer_.empty());
        REQUIRE(args.results.empty());
        return;
    }
    REQUIRE(err == kvstore::KvError::NoError);
    REQUIRE(answer_.size() == args.results.size());
    auto it = answer_.begin();
    for (auto &t : args.results)
    {
        REQUIRE(t == it->second);
        it++;
    }
    REQUIRE(it == answer_.end());
}

std::string Key(uint64_t k)
{
    constexpr int sz = 16;
    std::stringstream ss;
    ss << std::setw(sz) << std::setfill('0') << k;
    std::string kstr = ss.str();
    REQUIRE(kstr.size() == sz);
    return kstr;
}

void MapVerifier::DoWriteReq()
{
    kvstore::KvError err = req_.ExecSync(eloq_store_);
    REQUIRE(err == kvstore::KvError::NoError);
    if (auto_validate_)
    {
        Validate();
    }
}

void MapVerifier::SetAutoValidate(bool v)
{
    auto_validate_ = v;
}

void MapVerifier::SetStore(kvstore::EloqStore *store)
{
    eloq_store_ = store;
}

bool ConcurrentTester::Writer::Running() const
{
    return ts_ & 1;
}

uint32_t ConcurrentTester::Writer::Round() const
{
    return ts_ >> 1;
}

ConcurrentTester::ConcurrentTester(kvstore::EloqStore *store,
                                   kvstore::TableIdent tbl_id,
                                   uint8_t seg_size,
                                   uint16_t seg_count,
                                   uint16_t n_readers)
    : seg_size_(seg_size),
      seg_count_(seg_count),
      seg_sum_(seg_size * average_v),
      tbl_id_(std::move(tbl_id)),
      readers_(n_readers),
      ready_(n_readers + 1),
      store_(store)
{
}

ConcurrentTester::~ConcurrentTester()
{
    writer_.ts_++;
    std::vector<kvstore::WriteDataEntry> entries;
    for (uint32_t i = 0; i < kvs_.size(); i++)
    {
        if (kvs_[i])
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            ent.timestamp_ = writer_.ts_;
            ent.op_ = kvstore::WriteOp::Delete;
        }
    }
    writer_.SetWrite(tbl_id_, std::move(entries));
    kvstore::KvError err = writer_.ExecSync(store_);
    REQUIRE(err == kvstore::KvError::NoError);
}

void ConcurrentTester::Wake(kvstore::KvRequest *req)
{
    bool ok = ready_.TryEnqueue(req->user_data_);
    assert(ok);
}

void ConcurrentTester::ExecRead(Reader *reader)
{
    reader->begin_ts_ = writer_.ts_;
    uint32_t begin = (rand() % seg_count_) * seg_size_;
    kvstore::EncodeFixed32(reader->begin_key_, kvstore::ToBigEndian(begin));
    kvstore::EncodeFixed32(reader->end_key_,
                           kvstore::ToBigEndian(begin + seg_size_));
    std::string_view begin_key(reader->begin_key_, sizeof(uint32_t));
    std::string_view end_key(reader->end_key_, sizeof(uint32_t));
    reader->SetScan(tbl_id_, begin_key, end_key);
    reader->user_data_ = uint64_t(reader);
    reader->wake_ = [this](kvstore::KvRequest *req) { Wake(req); };
    store_->SendRequest(reader);
}

uint32_t decode_key(const char *ptr)
{
    return __builtin_bswap32(kvstore::DecodeFixed32(ptr));
}

void ConcurrentTester::VerifyRead(Reader *reader)
{
    REQUIRE(reader->Error() == kvstore::KvError::NoError);
    auto arg = reader->Args<kvstore::KvRequest::Scan>();
    uint32_t key_ans = decode_key(reader->begin_key_);
    uint32_t key_end = decode_key(reader->end_key_);
    if (!writer_.Running() && writer_.ts_ == reader->begin_ts_)
    {
        for (auto &[k, v, _] : arg.results)
        {
            while (kvs_[key_ans] == 0)
            {
                key_ans++;
            }

            uint32_t key_res = decode_key(k.data());
            uint32_t val_res = kvstore::DecodeFixed32(v.data());
            REQUIRE(key_res < key_end);
            REQUIRE(key_ans == key_res);
            REQUIRE(kvs_[key_ans] == val_res);

            key_ans++;
        }

        verify_val_++;
    }
    else
    {
        uint64_t sum_res = 0;
        for (auto &ent : arg.results)
        {
            uint32_t val = kvstore::DecodeFixed32(std::get<1>(ent).data());
            sum_res += val;
        }
        REQUIRE(seg_sum_ == sum_res);

        verify_sum_++;
    }
}

bool ConcurrentTester::AllReadersFinished() const
{
    for (const Reader &reader : readers_)
    {
        if (!reader.IsDone())
        {
            return false;
        }
    }
    return true;
}

void ConcurrentTester::ExecWrite()
{
    writer_.ts_++;

    std::vector<kvstore::WriteDataEntry> entries;
    uint32_t left = seg_sum_;
    for (uint32_t i = 0; i < kvs_.size(); i++)
    {
        uint32_t new_val = 0;
        if ((i + 1) % seg_size_ == 0)
        {
            new_val = left;
            left = seg_sum_;
        }
        else if (rand() % 3 != 0)
        {
            new_val = rand() % (average_v * 3);
            new_val = std::min(new_val, left);
            left -= new_val;
        }

        if (new_val == 0)
        {
            if (kvs_[i] != 0)
            {
                kvstore::WriteDataEntry &ent = entries.emplace_back();
                kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
                ent.timestamp_ = writer_.ts_;
                ent.op_ = kvstore::WriteOp::Delete;
            }
        }
        else
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            kvstore::PutFixed32(&ent.val_, new_val);
            ent.timestamp_ = writer_.ts_;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        kvs_[i] = new_val;
    }

    writer_.SetWrite(tbl_id_, std::move(entries));
    writer_.wake_ = [this](kvstore::KvRequest *req) { Wake(req); };
    store_->SendRequest(&writer_);
}

void ConcurrentTester::Init()
{
    uint32_t kvs_num = seg_size_ * seg_count_;
    std::vector<kvstore::WriteDataEntry> entries;
    for (uint32_t i = 0; i < kvs_num; i++)
    {
        kvstore::WriteDataEntry &ent = entries.emplace_back();
        kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
        kvstore::PutFixed32(&ent.val_, average_v);
        ent.timestamp_ = 0;
        ent.op_ = kvstore::WriteOp::Upsert;
    }
    writer_.SetWrite(tbl_id_, std::move(entries));
    kvstore::KvError err = writer_.ExecSync(store_);
    REQUIRE(err == kvstore::KvError::NoError);
    kvs_.resize(kvs_num, average_v);
}

void ConcurrentTester::Run(uint32_t rounds)
{
    // Start readers
    for (Reader &reader : readers_)
    {
        ExecRead(&reader);
    }

    while (true)
    {
        const uint16_t bufsize = readers_.size() + 1;
        uint64_t buf[bufsize];
        size_t n = ready_.TryDequeueBulk(buf, bufsize);
        for (uint32_t i = 0; i < n; ++i)
        {
            Reader *reader = reinterpret_cast<Reader *>(buf[i]);
            if (reader)
            {
                read_ops_++;
                VerifyRead(reader);
                if (rounds > 0)
                {
                    ExecRead(reader);
                }
            }
            else
            {
                REQUIRE(writer_.Error() == kvstore::KvError::NoError);
                read_ops_ = 0;
                writer_.ts_++;
                rounds--;
            }
        }

        if (!writer_.Running())
        {
            if (rounds > 0)
            {
                // Pause between each round of write
                if (read_ops_ >= seg_count_)
                {
                    ExecWrite();
                }
            }
            else if (AllReadersFinished())
            {
                break;
            }
        }
    }

    LOG(INFO) << "concurrency test finished, verified stat " << verify_val_
              << ", sum " << verify_sum_;
}