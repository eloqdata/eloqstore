#include "test_utils.h"

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>

#include "error.h"
#include "replayer.h"
#include "scan_task.h"
#include "table_ident.h"
#include "write_task.h"

std::string Key(uint64_t k)
{
    constexpr int sz = 12;
    std::stringstream ss;
    ss << std::setw(sz) << std::setfill('0') << k;
    std::string kstr = ss.str();
    CHECK(kstr.size() == sz);
    return kstr;
}

std::string Value(uint64_t val, uint16_t len)
{
    std::string s = std::to_string(val);
    if (s.size() < len)
    {
        s.resize(len, '#');
    }
    return s;
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
    LOG(INFO) << "Upsert(" << begin << ',' << end << ')';

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx);
        std::string val = Value(ts_ + idx, val_len_);
        entries.emplace_back(key, val, ts_, kvstore::WriteOp::Upsert);
        answer_.insert_or_assign(key, kvstore::KvEntry(key, val, ts_));
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Delete(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Delete(" << begin << ',' << end << ')';

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx);
        entries.emplace_back(key, "", ts_, kvstore::WriteOp::Delete);
        answer_.erase(key);
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Truncate(uint64_t position)
{
    LOG(INFO) << "Truncate(" << position << ')';

    kvstore::TruncateRequest req;
    std::string key = Key(position);
    if (answer_.empty())
    {
        req.SetArgs(tid_, key);
        eloq_store_->ExecSync(&req);
        CHECK(req.Error() == kvstore::KvError::NotFound);
        return;
    }

    auto it = answer_.lower_bound(key);
    answer_.erase(it, answer_.end());
    req.SetArgs(tid_, key);
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
    srand(ts_);

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        if ((rand() % max) >= density)
        {
            continue;
        }
        std::string key = Key(idx);
        if ((rand() % max) < del)
        {
            entries.emplace_back(key, "", ts_, kvstore::WriteOp::Delete);
            answer_.erase(key);
        }
        else
        {
            std::string val = Value(ts_ + idx, val_len_);
            entries.emplace_back(key, val, ts_, kvstore::WriteOp::Upsert);
            answer_.insert_or_assign(key, kvstore::KvEntry(key, val, ts_));
        }
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Clean()
{
    LOG(INFO) << "Clean()";

    std::vector<kvstore::WriteDataEntry> entries;
    for (auto [k, _] : answer_)
    {
        entries.emplace_back(k, "", ts_, kvstore::WriteOp::Delete);
    }
    answer_.clear();

    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Read(uint64_t k)
{
    LOG(INFO) << "Read(" << k << ')';

    std::string key = Key(k);
    kvstore::ReadRequest req;
    req.SetArgs(tid_, key);
    eloq_store_->ExecSync(&req);
    if (req.Error() == kvstore::KvError::NoError)
    {
        kvstore::KvEntry ret(key, req.value_, req.ts_);
        CHECK(answer_.at(key) == ret);
    }
    else
    {
        CHECK(req.Error() == kvstore::KvError::NotFound);
        CHECK(answer_.find(key) == answer_.end());
    }
}

void MapVerifier::Scan(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Scan(" << begin << ',' << end << ')';

    std::string begin_key = Key(begin);
    std::string end_key = Key(end);
    kvstore::ScanRequest req;
    req.SetArgs(tid_, begin_key, end_key);
    eloq_store_->ExecSync(&req);
    if (req.Error() == kvstore::KvError::NoError)
    {
        auto it = answer_.lower_bound(begin_key);
        for (auto &t : req.entries_)
        {
            CHECK(t == it->second);
            it++;
        }
    }
    else
    {
        CHECK(req.Error() == kvstore::KvError::NotFound);
        CHECK(answer_.empty());
    }
}

void MapVerifier::Validate()
{
    kvstore::ScanRequest req;
    req.SetArgs(tid_, "", "");
    eloq_store_->ExecSync(&req);
    if (req.Error() == kvstore::KvError::NotFound)
    {
        CHECK(answer_.empty());
        CHECK(req.entries_.empty());
        return;
    }
    CHECK(req.Error() == kvstore::KvError::NoError);
    CHECK(answer_.size() == req.entries_.size());
    auto it = answer_.begin();
    for (auto &t : req.entries_)
    {
        CHECK(t == it->second);
        it++;
    }
    CHECK(it == answer_.end());
}

void MapVerifier::ExecWrite(kvstore::KvRequest *req)
{
    eloq_store_->ExecSync(req);
    CHECK(req->Error() == kvstore::KvError::NoError);
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

void MapVerifier::SetValueLength(uint16_t val_len)
{
    val_len_ = val_len;
}

void MapVerifier::SetStore(kvstore::EloqStore *store)
{
    eloq_store_ = store;
}

bool ConcurrencyTester::Partition::IsWriting() const
{
    return ts_ & 1;
}

uint32_t ConcurrencyTester::Partition::Rounds() const
{
    return ts_ >> 1;
}

ConcurrencyTester::ConcurrencyTester(kvstore::EloqStore *store,
                                     std::string tbl_name,
                                     uint32_t n_partitions,
                                     uint8_t seg_size,
                                     uint16_t seg_count,
                                     uint16_t n_readers)
    : seg_size_(seg_size),
      seg_count_(seg_count),
      seg_sum_(seg_size * average_v),
      tbl_name_(std::move(tbl_name)),
      partitions_(n_partitions),
      readers_(n_readers),
      ready_(n_readers + 1),
      store_(store)
{
    for (int i = 0; i < n_partitions; i++)
    {
        partitions_[i].id_ = i;
    }
}

ConcurrencyTester::~ConcurrencyTester()
{
    for (Partition &part : partitions_)
    {
        part.ts_++;
        std::vector<kvstore::WriteDataEntry> entries;
        for (uint32_t i = 0; i < part.kvs_.size(); i++)
        {
            if (part.kvs_[i])
            {
                kvstore::WriteDataEntry &ent = entries.emplace_back();
                kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
                ent.timestamp_ = part.ts_;
                ent.op_ = kvstore::WriteOp::Delete;
            }
        }
        part.writer_->SetArgs({tbl_name_, part.id_}, std::move(entries));
        store_->ExecSync(part.writer_.get());
        CHECK(part.writer_->Error() == kvstore::KvError::NoError);
    }
}

void ConcurrencyTester::Wake(kvstore::KvRequest *req)
{
    bool ok = ready_.TryEnqueue(req->UserData());
    CHECK(ok);
}

void ConcurrencyTester::ExecRead(Reader *reader)
{
    Partition &partition = partitions_[rand() % partitions_.size()];
    reader->begin_ts_ = partition.ts_;
    uint32_t begin = (rand() % seg_count_) * seg_size_;
    kvstore::EncodeFixed32(reader->begin_key_, kvstore::ToBigEndian(begin));
    kvstore::EncodeFixed32(reader->end_key_,
                           kvstore::ToBigEndian(begin + seg_size_));
    std::string_view begin_key(reader->begin_key_, sizeof(uint32_t));
    std::string_view end_key(reader->end_key_, sizeof(uint32_t));
    reader->SetArgs({tbl_name_, partition.id_}, begin_key, end_key);
    store_->ExecAsyn(reader,
                     uint64_t(reader),
                     [this](kvstore::KvRequest *req) { Wake(req); });
}

uint32_t decode_key(const char *ptr)
{
    return __builtin_bswap32(kvstore::DecodeFixed32(ptr));
}

void ConcurrencyTester::VerifyRead(Reader *reader)
{
    CHECK(reader->Error() == kvstore::KvError::NoError);
    uint32_t key_ans = decode_key(reader->begin_key_);
    uint32_t key_end = decode_key(reader->end_key_);
    Partition &partition = partitions_[reader->TableId().partition_id_];
    if (!partition.IsWriting() && partition.ts_ == reader->begin_ts_)
    {
        for (auto &[k, v, _] : reader->entries_)
        {
            while (partition.kvs_[key_ans] == 0)
            {
                key_ans++;
            }

            uint32_t key_res = decode_key(k.data());
            uint32_t val_res = kvstore::DecodeFixed32(v.data());
            CHECK(key_res < key_end);
            CHECK(key_ans == key_res);
            CHECK(partition.kvs_[key_ans] == val_res);

            key_ans++;
        }

        verify_val_++;
    }
    else
    {
        uint64_t sum_res = 0;
        for (auto &ent : reader->entries_)
        {
            uint32_t val = kvstore::DecodeFixed32(std::get<1>(ent).data());
            sum_res += val;
        }
        CHECK(seg_sum_ == sum_res);

        verify_sum_++;
    }
}

bool ConcurrencyTester::AllTasksDone(uint32_t rounds) const
{
    for (const Partition &partition : partitions_)
    {
        if (partition.Rounds() < rounds)
        {
            return false;
        }
    }

    for (const Reader &reader : readers_)
    {
        if (!reader.IsDone())
        {
            return false;
        }
    }
    return true;
}

void ConcurrencyTester::ExecWrite(ConcurrencyTester::Partition &partition)
{
    partition.ts_++;

    std::vector<kvstore::WriteDataEntry> entries;
    uint32_t left = seg_sum_;
    for (uint32_t i = 0; i < partition.kvs_.size(); i++)
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
            if (partition.kvs_[i] != 0)
            {
                kvstore::WriteDataEntry &ent = entries.emplace_back();
                kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
                ent.timestamp_ = partition.ts_;
                ent.op_ = kvstore::WriteOp::Delete;
            }
        }
        else
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            kvstore::PutFixed32(&ent.val_, new_val);
            ent.timestamp_ = partition.ts_;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        partition.kvs_[i] = new_val;
    }

    partition.writer_->SetArgs({tbl_name_, partition.id_}, std::move(entries));
    store_->ExecAsyn(partition.writer_.get(),
                     (uint64_t) partition.writer_.get(),
                     [this](kvstore::KvRequest *req) { Wake(req); });
}

void ConcurrencyTester::Init()
{
    const uint32_t kvs_num = seg_size_ * seg_count_;
    for (Partition &partition : partitions_)
    {
        partition.writer_ = std::make_unique<kvstore::WriteRequest>();

        std::vector<kvstore::WriteDataEntry> entries;
        for (uint32_t i = 0; i < kvs_num; i++)
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            kvstore::PutFixed32(&ent.val_, average_v);
            ent.timestamp_ = 0;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        partition.writer_->SetArgs({tbl_name_, partition.id_},
                                   std::move(entries));
        store_->ExecSync(partition.writer_.get());
        CHECK(partition.writer_->Error() == kvstore::KvError::NoError);
        partition.kvs_.resize(kvs_num, average_v);
    }
}

void ConcurrencyTester::Run(uint32_t rounds)
{
    // Start readers
    for (Reader &reader : readers_)
    {
        ExecRead(&reader);
    }

    do
    {
        uint64_t buf[128];
        size_t n = ready_.TryDequeueBulk(buf, 128);
        for (uint32_t i = 0; i < n; ++i)
        {
            auto req = reinterpret_cast<kvstore::KvRequest *>(buf[i]);
            Partition &partition = partitions_[req->TableId().partition_id_];
            if (req->Type() == kvstore::RequestType::Write)
            {
                CHECK(partition.writer_->Error() == kvstore::KvError::NoError);
                partition.write_interval_ = 0;
                partition.ts_++;
            }
            else
            {
                Reader *reader = static_cast<Reader *>(req);
                VerifyRead(reader);
                if (partition.Rounds() < rounds)
                {
                    ExecRead(reader);

                    // Pause between each round of write
                    partition.write_interval_++;
                    if (partition.write_interval_ > seg_count_ &&
                        !partition.IsWriting())
                    {
                        ExecWrite(partition);
                    }
                }
            }
        }
    } while (!AllTasksDone(rounds));

    LOG(INFO) << "concurrency test finished, verified stat " << verify_val_
              << ", sum " << verify_sum_;
}

ManifestVerifier::ManifestVerifier(kvstore::KvOptions opts) : options_(opts)
{
    answer_.InitPages(opts.init_page_count);
}

std::pair<uint32_t, uint32_t> ManifestVerifier::RandChoose()
{
    CHECK(!helper_.empty());
    auto it = std::next(helper_.begin(), rand() % helper_.size());
    return *it;
}

uint32_t ManifestVerifier::Size() const
{
    return helper_.size();
}

void ManifestVerifier::NewMapping()
{
    uint32_t page_id = answer_.GetPage();
    uint32_t file_page_id = answer_.GetFilePage();
    answer_.UpdateMapping(page_id, file_page_id);

    builder_.UpdateMapping(page_id, file_page_id);
    helper_[page_id] = file_page_id;
}

void ManifestVerifier::UpdateMapping()
{
    auto [page_id, old_fp_id] = RandChoose();
    root_id_ = page_id;

    uint32_t new_fp_id = answer_.GetFilePage();
    answer_.UpdateMapping(page_id, new_fp_id);
    answer_.FreeFilePage(old_fp_id);

    builder_.UpdateMapping(page_id, new_fp_id);

    helper_[page_id] = new_fp_id;
}

void ManifestVerifier::FreeMapping()
{
    auto [page_id, file_page_id] = RandChoose();
    helper_.erase(page_id);
    if (page_id == root_id_)
    {
        root_id_ = Size() == 0 ? UINT32_MAX : RandChoose().first;
    }

    answer_.FreePage(page_id);
    answer_.FreeFilePage(file_page_id);

    builder_.UpdateMapping(page_id, UINT32_MAX);
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
            std::string_view sv = builder_.Finalize(root_id_);
            file_.append(sv);
            builder_.Reset();
        }
    }
}

void ManifestVerifier::Snapshot()
{
    std::string_view sv = builder_.Snapshot(root_id_, answer_);
    file_ = sv;
    builder_.Reset();
}

void ManifestVerifier::Verify() const
{
    auto file = std::make_unique<kvstore::MemStoreMgr::Manifest>(file_);

    kvstore::Replayer replayer;
    kvstore::KvError err = replayer.Replay(std::move(file), &options_);
    CHECK(err == kvstore::KvError::NoError);
    auto mapper = replayer.Mapper(nullptr, nullptr);

    CHECK(replayer.root_ == root_id_);
    CHECK(mapper->EqualTo(answer_));
}