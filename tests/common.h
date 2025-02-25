#pragma once

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <string_view>

#include "coding.h"
#include "eloq_queue.h"
#include "eloq_store.h"
#include "scan_task.h"

static constexpr kvstore::TableIdent test_tbl_id = {"t1", 1};
inline std::unique_ptr<kvstore::EloqStore> memstore = nullptr;

void InitMemStore();

inline std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = kvstore::ToBigEndian(key);
    kvstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

inline uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = kvstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}

std::string Key(uint64_t k);

class MapVerifier
{
public:
    MapVerifier(kvstore::TableIdent tid, kvstore::EloqStore *store);
    ~MapVerifier();
    void Upsert(uint64_t begin, uint64_t end);
    void Delete(uint64_t begin, uint64_t end);
    void WriteRnd(uint64_t begin,
                  uint64_t end,
                  uint8_t del = 20,
                  uint8_t density = 25);
    void Clean();

    void Read(uint64_t k);
    void Scan(uint64_t begin, uint64_t end);

    void Validate();
    void SetAutoValidate(bool v);
    void SetStore(kvstore::EloqStore *store);

private:
    void DoWriteReq();

    const kvstore::TableIdent tid_;
    uint64_t ts_{0};
    std::map<std::string, kvstore::KvEntry> answer_;
    bool auto_validate_{true};

    kvstore::KvRequest req_;
    kvstore::EloqStore *eloq_store_;
};

class ConcurrentTester
{
public:
    ConcurrentTester(kvstore::EloqStore *store,
                     kvstore::TableIdent tbl_id,
                     uint8_t seg_size,
                     uint16_t seg_count,
                     uint16_t n_readers);
    ~ConcurrentTester();
    void Init();
    void Run(uint32_t rounds);

    static constexpr uint32_t average_v = 10;

private:
    struct Writer : kvstore::KvRequest
    {
        bool Running() const;
        uint32_t Round() const;
        uint32_t ts_{0};
    };

    struct Reader : kvstore::KvRequest
    {
        uint32_t begin_ts_;
        char begin_key_[4];
        char end_key_[4];
    };
    void Wake(kvstore::KvRequest *req);
    void ExecRead(Reader *reader);
    void VerifyRead(Reader *reader);
    bool AllReadersFinished() const;
    void ExecWrite();

    const uint8_t seg_size_;
    const uint16_t seg_count_;
    const uint32_t seg_sum_;
    kvstore::TableIdent tbl_id_;
    std::vector<uint32_t> kvs_;

    Writer writer_;
    std::vector<Reader> readers_;
    eloq::SpscQueue<uint64_t> ready_;
    uint32_t read_ops_{0};
    uint32_t verify_sum_{0};
    uint32_t verify_val_{0};
    kvstore::EloqStore *store_;
};