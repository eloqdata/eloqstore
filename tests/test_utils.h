#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "eloq_queue.h"
#include "eloq_store.h"
#include "table_ident.h"

std::string Key(uint64_t k);
std::string Value(uint64_t val, uint16_t len = 0);

class MapVerifier
{
public:
    MapVerifier(kvstore::TableIdent tid, kvstore::EloqStore *store);
    ~MapVerifier();
    void Upsert(uint64_t begin, uint64_t end);
    void Delete(uint64_t begin, uint64_t end);
    void Truncate(uint64_t position);
    void WriteRnd(uint64_t begin,
                  uint64_t end,
                  uint8_t del = 20,
                  uint8_t density = 25);
    void Clean();

    void Read(uint64_t k);
    void Scan(uint64_t begin, uint64_t end);

    void Validate();
    void SetAutoValidate(bool v);
    void SetValueLength(uint16_t val_size);
    void SetStore(kvstore::EloqStore *store);

private:
    void ExecWrite(kvstore::KvRequest *req);

    const kvstore::TableIdent tid_;
    uint64_t ts_{0};
    std::map<std::string, kvstore::KvEntry> answer_;
    bool auto_validate_{true};
    uint16_t val_len_{12};

    kvstore::EloqStore *eloq_store_;
};

class ConcurrencyTester
{
public:
    ConcurrencyTester(kvstore::EloqStore *store,
                      std::string tbl_name,
                      uint32_t n_partitions,
                      uint8_t seg_size,
                      uint16_t seg_count,
                      uint16_t n_readers);
    ~ConcurrencyTester();
    void Init();
    void Run(uint32_t rounds);

    static constexpr uint32_t average_v = 10;

private:
    struct Reader : kvstore::ScanRequest
    {
        uint32_t begin_ts_;
        char begin_key_[4];
        char end_key_[4];
    };

    struct Partition
    {
        bool IsWriting() const;
        uint32_t Rounds() const;
        uint32_t id_;
        std::vector<uint32_t> kvs_;
        uint32_t ts_{0};
        std::unique_ptr<kvstore::WriteRequest> writer_;
        uint32_t write_interval_{0};
    };

    void Wake(kvstore::KvRequest *req);
    void ExecRead(Reader *reader);
    void VerifyRead(Reader *reader);
    bool AllTasksDone(uint32_t rounds) const;
    void ExecWrite(Partition &partition);

    const uint8_t seg_size_;
    const uint16_t seg_count_;
    const uint32_t seg_sum_;
    const std::string tbl_name_;

    std::vector<Partition> partitions_;
    std::vector<Reader> readers_;
    eloq::SpscQueue<uint64_t> ready_;
    uint32_t verify_sum_{0};
    uint32_t verify_val_{0};
    kvstore::EloqStore *const store_;
};

class ManifestVerifier
{
public:
    ManifestVerifier(kvstore::KvOptions opts);
    void NewMapping();
    void UpdateMapping();
    void FreeMapping();
    void Finish();
    void Snapshot();

    void Verify() const;
    uint32_t Size() const;

private:
    std::pair<uint32_t, uint32_t> RandChoose();

    kvstore::KvOptions options_;
    uint32_t root_id_;
    kvstore::PageMapper answer_;
    std::unordered_map<uint32_t, uint32_t> helper_;

    kvstore::ManifestBuilder builder_;
    std::string file_;
};