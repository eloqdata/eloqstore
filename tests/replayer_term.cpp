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
    eloqstore::FileIdTermMapping empty_mapping;
    std::string term_buf;
    eloqstore::SerializeFileIdTermMapping(empty_mapping, term_buf);
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 term_buf);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    replayer.file_id_term_mapping_->insert_or_assign(
        eloqstore::IouringMgr::LruFD::kManifest, 1);
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
    eloqstore::FileIdTermMapping empty_mapping;
    std::string term_buf;
    eloqstore::SerializeFileIdTermMapping(empty_mapping, term_buf);
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 term_buf);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // Set manifest_term to match expect_term (no bumping)
    replayer.file_id_term_mapping_->insert_or_assign(
        eloqstore::IouringMgr::LruFD::kManifest, 7);

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
    eloqstore::FileIdTermMapping empty_mapping;
    std::string term_buf;
    eloqstore::SerializeFileIdTermMapping(empty_mapping, term_buf);
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 term_buf);

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
    eloqstore::FileIdTermMapping empty_mapping;
    std::string term_buf;
    eloqstore::SerializeFileIdTermMapping(empty_mapping, term_buf);
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 term_buf);

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
    std::unordered_map<eloqstore::FileId, uint64_t> all_term_map;

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
    eloqstore::FileIdTermMapping term_mapping;
    term_mapping.insert_or_assign(eloqstore::IouringMgr::LruFD::kManifest, 10);
    term_mapping.insert_or_assign(1, 10);
    term_mapping.insert_or_assign(5, 10);
    term_mapping.insert_or_assign(10, 10);
    all_term_map[eloqstore::IouringMgr::LruFD::kManifest] = 10;
    all_term_map[1] = 10;
    all_term_map[5] = 10;
    all_term_map[10] = 10;
    std::string term_buf;
    eloqstore::SerializeFileIdTermMapping(term_mapping, term_buf);
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 term_buf);

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

    term_mapping.insert_or_assign(eloqstore::IouringMgr::LruFD::kManifest, 20);
    term_mapping.insert_or_assign(10, 20);
    term_mapping.insert_or_assign(13, 20);
    term_mapping.insert_or_assign(25, 20);
    all_term_map[eloqstore::IouringMgr::LruFD::kManifest] = 20;
    all_term_map[10] = 20;
    all_term_map[13] = 20;
    all_term_map[25] = 20;
    std::string term_buf2;
    eloqstore::SerializeFileIdTermMapping(term_mapping, term_buf2);

    builder1.AppendFileIdTermMapping(term_buf2);
    std::string_view append_log1 = builder1.Finalize(10, 10);

    manifest_buf.append(append_log1);

    // append mapping table log2
    eloqstore::ManifestBuilder builder2;
    builder2.UpdateMapping(20, 20);
    builder2.UpdateMapping(21, 21);
    all_page_map[20] = 20;
    all_page_map[21] = 21;

    term_mapping.insert_or_assign(eloqstore::IouringMgr::LruFD::kManifest, 30);
    term_mapping.insert_or_assign(30, 30);
    term_mapping.insert_or_assign(31, 30);
    term_mapping.insert_or_assign(32, 30);
    all_term_map[eloqstore::IouringMgr::LruFD::kManifest] = 30;
    all_term_map[30] = 30;
    all_term_map[31] = 30;
    all_term_map[32] = 30;
    std::string term_buf3;
    eloqstore::SerializeFileIdTermMapping(term_mapping, term_buf3);

    builder2.AppendFileIdTermMapping(term_buf3);
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

    // check file_id_term_mapping
    REQUIRE(replayer.file_id_term_mapping_->size() == 9);
    for (auto &[file_id, term] : all_term_map)
    {
        REQUIRE(replayer.file_id_term_mapping_->at(file_id) == term);
    }
}
