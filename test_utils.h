#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "eloq_store.h"
#include "index_page_manager.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace test_util
{
std::string Key(uint64_t key, uint16_t len = 12);
std::string Value(uint64_t val, uint32_t len = 0);
void CheckKvEntry(const kvstore::KvEntry &left, const kvstore::KvEntry &right);

void EncodeKey(char *dst, uint32_t key);
void EncodeKey(std::string *dst, uint32_t key);
uint32_t DecodeKey(const std::string &key);
void EncodeValue(std::string *dst, uint32_t val);
uint32_t DecodeValue(const std::string &val);

std::string FormatEntries(const std::vector<kvstore::KvEntry> &entries);

std::pair<std::string, kvstore::KvError> Scan(kvstore::EloqStore *store,
                                              const kvstore::TableIdent &tbl_id,
                                              uint32_t begin,
                                              uint32_t end);

class MapVerifier
{
public:
    MapVerifier(kvstore::TableIdent tid,
                kvstore::EloqStore *store,
                bool validate = true,
                uint16_t key_len = 12);
    ~MapVerifier();
    void Upsert(uint64_t key);
    void Upsert(uint64_t begin, uint64_t end);
    void Delete(uint64_t begin, uint64_t end);
    void Truncate(uint64_t position);
    void WriteRnd(uint64_t begin,
                  uint64_t end,
                  uint8_t del = 20,
                  uint8_t density = 25);
    void Clean();
    void ExecWrite(kvstore::KvRequest *req);

    void Read(uint64_t key);
    void Read(std::string_view key);
    void Floor(std::string_view key);
    void Scan(uint64_t begin,
              uint64_t end,
              size_t page_entries = 0,
              size_t page_size = 0);
    void Scan(std::string_view begin,
              std::string_view end,
              size_t page_entries = 0,
              size_t page_size = 0);

    void Validate();
    void SetAutoValidate(bool v);
    void SetValueSize(uint32_t val_size);
    void SetStore(kvstore::EloqStore *store);
    void SetTimestamp(uint64_t ts);

private:
    const kvstore::TableIdent tid_;
    uint64_t ts_{0};
    std::map<std::string, kvstore::KvEntry> answer_;
    bool auto_validate_{true};
    const uint16_t key_len_;
    uint32_t val_size_{12};

    kvstore::EloqStore *eloq_store_;
};

class ConcurrencyTester
{
public:
    ConcurrencyTester(kvstore::EloqStore *store,
                      std::string tbl_name,
                      uint32_t n_partitions,
                      uint16_t seg_count,
                      uint8_t seg_size = 16,
                      uint32_t val_size = 10);
    void Init();
    void Run(uint16_t n_readers, uint32_t read_ops, uint32_t write_pause);
    void Clear();

    static uint64_t CurrentTimestamp();

private:
    struct Reader
    {
        Reader() = default;
        uint16_t id_;
        uint32_t start_tick_;
        uint32_t partition_id_;
        uint32_t begin_;
        uint32_t end_;
        char begin_key_[4];
        char end_key_[4];
        kvstore::ScanRequest req_;
        uint32_t verify_cnt_{0};
    };

    struct Partition
    {
        bool IsWriting() const;
        void FinishWrite();
        uint32_t FinishedRounds() const;

        uint32_t id_;
        std::vector<uint32_t> kvs_;
        uint32_t ticks_{0};
        kvstore::BatchWriteRequest req_;
        uint32_t verify_cnt_{0};
    };

    void Wake(kvstore::KvRequest *req);
    void ExecRead(Reader *reader);
    void VerifyRead(Reader *reader, uint32_t write_pause);
    void ExecWrite(Partition &partition);

    std::string DebugSegment(uint32_t partition_id,
                             uint16_t seg_id,
                             const std::vector<kvstore::KvEntry> *resp) const;

    const uint32_t val_size_;
    const uint8_t seg_size_;
    const uint16_t seg_count_;
    const uint32_t seg_sum_;
    const std::string tbl_name_;

    std::vector<Partition> partitions_;
    moodycamel::BlockingConcurrentQueue<uint64_t> finished_reqs_;
    uint32_t verify_sum_{0};
    uint32_t verify_kv_{0};
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

    void Verify();
    uint32_t Size() const;

private:
    std::pair<kvstore::PageId, kvstore::FilePageId> RandChoose();

    kvstore::KvOptions options_;
    kvstore::MemStoreMgr io_mgr_;
    kvstore::IndexPageManager idx_mgr_;
    kvstore::TableIdent tbl_id_;

    uint32_t root_id_;
    kvstore::PageMapper answer_;
    kvstore::PooledFilePages *answer_file_pages_{nullptr};
    std::unordered_map<kvstore::PageId, kvstore::FilePageId> helper_;

    kvstore::ManifestBuilder builder_;
    std::string file_;
};
}  // namespace test_util