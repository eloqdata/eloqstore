#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "../include/common.h"
#include "../include/kv_options.h"
#include "../include/replayer.h"
#include "../include/storage/index_page_manager.h"
#include "../include/storage/root_meta.h"
#include "storage/page_mapper.h"
#include "types.h"

namespace
{
eloqstore::KvOptions MakeOpts(bool cloud_mode, uint8_t shift)
{
    eloqstore::KvOptions opts{};
    opts.data_append_mode = true;
    opts.pages_per_file_shift = shift;
    opts.init_page_count = 8;
    if (cloud_mode)
    {
        opts.cloud_store_path = "dummy_cloud";
    }
    return opts;
}
}  // namespace

TEST_CASE(
    "Replayer allocator bumping occurs when manifest_term != expect_term in "
    "cloud mode",
    "[replayer][term]")
{
    eloqstore::KvOptions opts =
        MakeOpts(true /*cloud_mode*/, 4 /*pages_per_file_shift*/);

    // Build an empty snapshot with max_fp_id not aligned to a file boundary.
    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot mapping(&idx_mgr, &tbl_id, {});
    // file_id=1, next boundary => 32 for shift=4
    const eloqstore::FilePageId max_fp_id = 17;
    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = eloqstore::MainBranchName;
    branch_meta.term = 1;
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 branch_meta);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // branch_metadata_.term == 1 (embedded in snapshot)
    // expect_term is equal to manifest_term => no bumping
    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 1);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);

    // expect_term differs => bump to next file boundary
    auto mapper2 = replayer.GetMapper(&idx_mgr, &tbl_id, 2);
    REQUIRE(mapper2 != nullptr);
    REQUIRE(mapper2->FilePgAllocator()->MaxFilePageId() == 32);
}

TEST_CASE("Replayer allocator bumping does not occur when terms match",
          "[replayer][term]")
{
    eloqstore::KvOptions opts = MakeOpts(true /*cloud_mode*/, 4);
    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot mapping(&idx_mgr, &tbl_id, {});
    const eloqstore::FilePageId max_fp_id = 17;
    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = eloqstore::MainBranchName;
    branch_meta.term = 7;
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 branch_meta);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // branch_metadata_.term == 7 (embedded in snapshot), expect_term matches
    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 7);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}

TEST_CASE("Replayer allocator bumping does not occur when expect_term==0",
          "[replayer][term]")
{
    eloqstore::KvOptions opts = MakeOpts(true /*cloud_mode*/, 4);
    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot mapping(&idx_mgr, &tbl_id, {});
    const eloqstore::FilePageId max_fp_id = 17;
    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = eloqstore::MainBranchName;
    branch_meta.term = 0;
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 branch_meta);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 0);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}

TEST_CASE("Replayer allocator bumping does not occur in local mode",
          "[replayer][term]")
{
    eloqstore::KvOptions opts = MakeOpts(false /*cloud_mode*/, 4);
    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot mapping(&idx_mgr, &tbl_id, {});
    const eloqstore::FilePageId max_fp_id = 17;
    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = eloqstore::MainBranchName;
    branch_meta.term = 0;
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 branch_meta);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 2);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}

TEST_CASE("Replayer replay with multi appended mapping table log",
          "[replayer][term]")
{
    eloqstore::KvOptions opts = MakeOpts(false /*cloud_mode*/, 4);
    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;

    std::unordered_map<eloqstore::PageId, eloqstore::FilePageId> all_page_map;

    // init mapping table
    mapping_tbl.Set(1, eloqstore::MappingSnapshot::EncodeFilePageId(2));
    mapping_tbl.Set(2, eloqstore::MappingSnapshot::EncodeFilePageId(3));
    mapping_tbl.Set(3, eloqstore::MappingSnapshot::EncodeFilePageId(4));
    mapping_tbl.Set(5, eloqstore::MappingSnapshot::EncodeFilePageId(5));
    mapping_tbl.Set(8, eloqstore::MappingSnapshot::EncodeFilePageId(9));
    mapping_tbl.Set(10, eloqstore::MappingSnapshot::EncodeFilePageId(10));
    all_page_map[1] = 2;
    all_page_map[2] = 3;
    all_page_map[3] = 4;
    all_page_map[5] = 5;
    all_page_map[8] = 9;
    all_page_map[10] = 10;
    eloqstore::MappingSnapshot mapping(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));
    const eloqstore::FilePageId max_fp_id = 17;

    // Snapshot with branch term = 10
    eloqstore::BranchManifestMetadata meta10;
    meta10.branch_name = eloqstore::MainBranchName;
    meta10.term = 10;
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 meta10);

    std::string manifest_buf;
    manifest_buf.append(snapshot);

    // append mapping table log1
    eloqstore::ManifestBuilder builder1;
    builder1.UpdateMapping(1, 11);
    builder1.UpdateMapping(5, 15);
    builder1.DeleteMapping(2);
    builder1.UpdateMapping(13, 13);
    builder1.UpdateMapping(25, 25);
    all_page_map[1] = 11;
    all_page_map[5] = 15;
    all_page_map[2] = 0;
    all_page_map[13] = 13;
    all_page_map[25] = 25;

    // Log1 carries branch term = 20
    eloqstore::BranchManifestMetadata meta20;
    meta20.branch_name = eloqstore::MainBranchName;
    meta20.term = 20;
    std::string meta20_str =
        eloqstore::SerializeBranchManifestMetadata(meta20);
    builder1.AppendBranchManifestMetadata(meta20_str);
    std::string_view append_log1 = builder1.Finalize(10, 10);

    manifest_buf.append(append_log1);

    // append mapping table log2
    eloqstore::ManifestBuilder builder2;
    builder2.UpdateMapping(20, 20);
    builder2.UpdateMapping(21, 21);
    all_page_map[20] = 20;
    all_page_map[21] = 21;

    // Log2 carries branch term = 30
    eloqstore::BranchManifestMetadata meta30;
    meta30.branch_name = eloqstore::MainBranchName;
    meta30.term = 30;
    std::string meta30_str =
        eloqstore::SerializeBranchManifestMetadata(meta30);
    builder2.AppendBranchManifestMetadata(meta30_str);
    std::string_view append_log2 = builder2.Finalize(30, 30);

    manifest_buf.append(append_log2);

    // check replayer result
    eloqstore::MemStoreMgr::Manifest file(manifest_buf);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 30);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 26);

    // check mapping table
    const auto &mapping_1 = mapper->GetMapping()->mapping_tbl_;
    REQUIRE(mapping_1.size() == 26);
    for (auto &[page_id, file_page_id] : all_page_map)
    {
        REQUIRE(eloqstore::MappingSnapshot::DecodeId(mapping_1.Get(page_id)) ==
                file_page_id);
    }

    // After replaying snapshot (term=10) + log1 (term=20) + log2 (term=30),
    // the final branch term should be 30.
    REQUIRE(replayer.branch_metadata_.term == 30);
}
