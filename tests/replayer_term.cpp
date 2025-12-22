#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string_view>

#include "../include/kv_options.h"
#include "../include/replayer.h"
#include "../include/storage/index_page_manager.h"
#include "../include/storage/root_meta.h"

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
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 empty_mapping);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // Set manifest_term in file_id_term_mapping_ to trigger bumping
    replayer.file_id_term_mapping_->insert_or_assign(
        eloqstore::IouringMgr::LruFD::kManifest, 1);

    // expect_term differs => bump to next file boundary
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
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 empty_mapping);

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
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 empty_mapping);

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
    std::string_view snapshot = builder.Snapshot(eloqstore::MaxPageId,
                                                 eloqstore::MaxPageId,
                                                 &mapping,
                                                 max_fp_id,
                                                 {},
                                                 empty_mapping);

    eloqstore::MemStoreMgr::Manifest file(snapshot);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 2);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}
