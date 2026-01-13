#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "async_io_manager.h"
#include "coding.h"
#include "compression.h"
#include "external/xxhash.h"
#include "storage/compression_manager.h"
#include "storage/index_page_manager.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"

namespace
{
class FakeManifest : public eloqstore::ManifestFile
{
public:
    explicit FakeManifest(std::string_view content) : content_(content) {}

    eloqstore::KvError Read(char *dst, size_t n) override
    {
        if (content_.size() < n)
        {
            return eloqstore::KvError::EndOfFile;
        }
        memcpy(dst, content_.data(), n);
        content_.remove_prefix(n);
        return eloqstore::KvError::NoError;
    }

    eloqstore::KvError SkipPadding(size_t n) override
    {
        if (content_.size() < n)
        {
            return eloqstore::KvError::EndOfFile;
        }
        content_.remove_prefix(n);
        return eloqstore::KvError::NoError;
    }

private:
    std::string_view content_;
};

class FakeIoManager : public eloqstore::AsyncIoManager
{
public:
    explicit FakeIoManager(const eloqstore::KvOptions *opts)
        : eloqstore::AsyncIoManager(opts)
    {
    }

    void AddManifest(const eloqstore::TableIdent &tbl_id, std::string manifest)
    {
        manifests_[tbl_id] = std::move(manifest);
    }

    eloqstore::KvError Init(eloqstore::Shard * /*shard*/) override
    {
        return eloqstore::KvError::NoError;
    }

    void Submit() override {}
    void PollComplete() override {}

    std::pair<eloqstore::Page, eloqstore::KvError> ReadPage(
        const eloqstore::TableIdent & /*tbl_id*/,
        eloqstore::FilePageId /*file_page_id*/,
        eloqstore::Page page) override
    {
        return {std::move(page), eloqstore::KvError::NotFound};
    }

    eloqstore::KvError ReadPages(const eloqstore::TableIdent & /*tbl_id*/,
                                 std::span<eloqstore::FilePageId> /*page_ids*/,
                                 std::vector<eloqstore::Page> & /*pages*/)
        override
    {
        return eloqstore::KvError::NotFound;
    }

    eloqstore::KvError WritePage(const eloqstore::TableIdent & /*tbl_id*/,
                                 eloqstore::VarPage /*page*/,
                                 eloqstore::FilePageId /*file_page_id*/)
        override
    {
        return eloqstore::KvError::NotFound;
    }

    eloqstore::KvError WritePages(
        const eloqstore::TableIdent & /*tbl_id*/,
        std::span<eloqstore::VarPage> /*pages*/,
        eloqstore::FilePageId /*first_fp_id*/) override
    {
        return eloqstore::KvError::NotFound;
    }

    eloqstore::KvError SyncData(
        const eloqstore::TableIdent & /*tbl_id*/) override
    {
        return eloqstore::KvError::NoError;
    }

    eloqstore::KvError AbortWrite(
        const eloqstore::TableIdent & /*tbl_id*/) override
    {
        return eloqstore::KvError::NoError;
    }

    eloqstore::KvError AppendManifest(
        const eloqstore::TableIdent & /*tbl_id*/,
        std::string_view /*log*/,
        uint64_t /*offset*/) override
    {
        return eloqstore::KvError::NotFound;
    }

    eloqstore::KvError SwitchManifest(
        const eloqstore::TableIdent & /*tbl_id*/,
        std::string_view /*snapshot*/) override
    {
        return eloqstore::KvError::NotFound;
    }

    eloqstore::KvError CreateArchive(
        const eloqstore::TableIdent & /*tbl_id*/,
        std::string_view /*snapshot*/,
        uint64_t /*ts*/) override
    {
        return eloqstore::KvError::NotFound;
    }

    std::pair<eloqstore::ManifestFilePtr, eloqstore::KvError> GetManifest(
        const eloqstore::TableIdent &tbl_id) override
    {
        auto it = manifests_.find(tbl_id);
        if (it == manifests_.end())
        {
            return {nullptr, eloqstore::KvError::NotFound};
        }
        return {std::make_unique<FakeManifest>(it->second),
                eloqstore::KvError::NoError};
    }

    void CleanManifest(const eloqstore::TableIdent & /*tbl_id*/) override {}

private:
    std::unordered_map<eloqstore::TableIdent, std::string> manifests_;
};

uint64_t DictOffset(eloqstore::FilePageId max_fp_id,
                    const eloqstore::DictMeta &meta)
{
    if (!meta.HasDictionary())
    {
        return 0;
    }
    return eloqstore::ManifestBuilder::header_bytes +
           eloqstore::Varint64Size(max_fp_id) +
           eloqstore::Varint32Size(meta.dict_len) +
           eloqstore::Varint64Size(meta.dict_checksum) +
           eloqstore::Varint64Size(meta.dict_epoch);
}

std::string BuildManifest(const eloqstore::KvOptions &opts,
                          FakeIoManager &io_mgr,
                          const eloqstore::TableIdent &tbl_id,
                          std::string_view dict_bytes,
                          eloqstore::DictMeta &dict_meta_out)
{
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::PageMapper mapper(&idx_mgr, &tbl_id);

    dict_meta_out.dict_len = static_cast<uint32_t>(dict_bytes.size());
    dict_meta_out.dict_checksum =
        XXH3_64bits(dict_bytes.data(), dict_bytes.size());
    dict_meta_out.dict_epoch = dict_meta_out.dict_checksum;
    dict_meta_out.dict_offset =
        DictOffset(mapper.FilePgAllocator()->MaxFilePageId(), dict_meta_out);

    eloqstore::ManifestBuilder builder;
    std::string_view snapshot = builder.Snapshot(
        eloqstore::MaxPageId,
        eloqstore::MaxPageId,
        mapper.GetMapping(),
        mapper.FilePgAllocator()->MaxFilePageId(),
        dict_meta_out,
        dict_bytes);
    return std::string(snapshot);
}
}  // namespace

TEST_CASE("CompressionManager lazy loads and evicts dictionaries")
{
    eloqstore::KvOptions opts{};
    opts.dict_cache_size = 64;
    FakeIoManager io_mgr(&opts);

    const eloqstore::TableIdent tbl1{"t1", 0};
    const eloqstore::TableIdent tbl2{"t2", 0};
    const std::string dict1(48, 'a');
    const std::string dict2(48, 'b');

    eloqstore::DictMeta meta1;
    eloqstore::DictMeta meta2;
    io_mgr.AddManifest(tbl1, BuildManifest(opts, io_mgr, tbl1, dict1, meta1));
    io_mgr.AddManifest(tbl2, BuildManifest(opts, io_mgr, tbl2, dict2, meta2));

    eloqstore::CompressionManager mgr(&io_mgr, &opts, opts.dict_cache_size);

    auto [handle1, err1] = mgr.GetOrLoad(tbl1, meta1);
    REQUIRE(err1 == eloqstore::KvError::NoError);
    REQUIRE(handle1.Get() != nullptr);
    REQUIRE(handle1.Get()->HasDictionary());
    REQUIRE(handle1.Get()->DictionaryBytes() == dict1);

    std::weak_ptr<eloqstore::compression::DictCompression> weak_dict1 =
        handle1.Shared();
    handle1 = {};

    auto [handle2, err2] = mgr.GetOrLoad(tbl2, meta2);
    REQUIRE(err2 == eloqstore::KvError::NoError);
    REQUIRE(handle2.Get() != nullptr);
    REQUIRE(handle2.Get()->HasDictionary());
    REQUIRE(handle2.Get()->DictionaryBytes() == dict2);

    REQUIRE(weak_dict1.lock() == nullptr);
}

TEST_CASE("CompressionManager keeps pinned dictionary from eviction")
{
    eloqstore::KvOptions opts{};
    opts.dict_cache_size = 64;
    FakeIoManager io_mgr(&opts);

    const eloqstore::TableIdent tbl1{"t3", 0};
    const eloqstore::TableIdent tbl2{"t4", 0};
    const std::string dict1(48, 'c');
    const std::string dict2(48, 'd');

    eloqstore::DictMeta meta1;
    eloqstore::DictMeta meta2;
    io_mgr.AddManifest(tbl1, BuildManifest(opts, io_mgr, tbl1, dict1, meta1));
    io_mgr.AddManifest(tbl2, BuildManifest(opts, io_mgr, tbl2, dict2, meta2));

    eloqstore::CompressionManager mgr(&io_mgr, &opts, opts.dict_cache_size);

    auto [handle1, err1] = mgr.GetOrLoad(tbl1, meta1);
    REQUIRE(err1 == eloqstore::KvError::NoError);
    std::weak_ptr<eloqstore::compression::DictCompression> weak_dict1 =
        handle1.Shared();

    auto [handle2, err2] = mgr.GetOrLoad(tbl2, meta2);
    REQUIRE(err2 == eloqstore::KvError::NoError);
    REQUIRE(weak_dict1.lock() != nullptr);

    handle1 = {};
    handle2 = {};
}
