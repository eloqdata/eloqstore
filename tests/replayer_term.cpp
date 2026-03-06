#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "../include/common.h"
#include "../include/eloq_store.h"
#include "../include/kv_options.h"
#include "../include/replayer.h"
#include "../include/storage/index_page_manager.h"
#include "../include/storage/root_meta.h"
#include "../include/tasks/task.h"
#include "storage/page_mapper.h"
#include "types.h"

namespace
{
class ScopedGlobalStore
{
public:
    explicit ScopedGlobalStore(const eloqstore::KvOptions &opts)
        : prev_(eloqstore::eloq_store), store_(opts)
    {
        eloqstore::eloq_store = &store_;
    }

    ~ScopedGlobalStore()
    {
        eloqstore::eloq_store = prev_;
    }

private:
    eloqstore::EloqStore *prev_{nullptr};
    eloqstore::EloqStore store_;
};

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

// Variant with explicit append_mode control (for non-append / pooled-pages
// tests)
eloqstore::KvOptions MakeOpts(bool cloud_mode, uint8_t shift, bool append_mode)
{
    eloqstore::KvOptions opts = MakeOpts(cloud_mode, shift);
    opts.data_append_mode = append_mode;
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
    ScopedGlobalStore scoped_store(opts);

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

    eloqstore::MemStoreMgr::Manifest file2(snapshot);
    eloqstore::Replayer replayer2(&opts);
    REQUIRE(replayer2.Replay(&file2) == eloqstore::KvError::NoError);
    // branch_metadata_.term is already 1 from the replayed snapshot.
    // expect_term differs => bump to next file boundary
    auto mapper2 = replayer2.GetMapper(&idx_mgr, &tbl_id, 2);
    REQUIRE(mapper2 != nullptr);
    REQUIRE(mapper2->FilePgAllocator()->MaxFilePageId() == 32);
}

TEST_CASE("Replayer allocator bumping does not occur when terms match",
          "[replayer][term]")
{
    eloqstore::KvOptions opts = MakeOpts(true /*cloud_mode*/, 4);
    ScopedGlobalStore scoped_store(opts);
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
    ScopedGlobalStore scoped_store(opts);
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
    ScopedGlobalStore scoped_store(opts);
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
    ScopedGlobalStore scoped_store(opts);
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
    std::string meta20_str = eloqstore::SerializeBranchManifestMetadata(meta20);
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
    std::string meta30_str = eloqstore::SerializeBranchManifestMetadata(meta30);
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

TEST_CASE(
    "Replayer GetMapper filters parent-branch pages correctly for 3-level "
    "chained fork",
    "[replayer][branch]")
{
    // Local (non-cloud) mode, non-append (pooled) mode, 16 pages per file
    // (shift=4). Simulates a 3-level fork chain: main -> feature1 -> sub1.
    //   file 0 (fp_ids 0-15)  belongs to "main"
    //   file 1 (fp_ids 16-31) belongs to "feature1"
    //   file 2 (fp_ids 32-47) belongs to "sub1"
    // After GetMapper for "sub1", the free list must contain only pages from
    // file 2 (pages not already in use), never from files 0 or 1.
    eloqstore::KvOptions opts = MakeOpts(false /*cloud_mode*/,
                                         4 /*pages_per_file_shift*/,
                                         false /*append_mode*/);

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    // Build mapping: one page in each of the three files.
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.Set(
        0, eloqstore::MappingSnapshot::EncodeFilePageId(0));  // file 0 (main)
    mapping_tbl.Set(
        1,
        eloqstore::MappingSnapshot::EncodeFilePageId(16));  // file 1 (feature1)
    mapping_tbl.Set(
        2, eloqstore::MappingSnapshot::EncodeFilePageId(32));  // file 2 (sub1)
    eloqstore::MappingSnapshot mapping(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    // 3 files x 16 pages each => max_fp_id = 48
    const eloqstore::FilePageId max_fp_id = 48;

    // Branch metadata for "sub1" with 3-level file_ranges
    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = "sub1";
    branch_meta.term = 0;
    branch_meta.file_ranges = {
        {"main", 0, 0},      // file 0 belongs to "main"
        {"feature1", 0, 1},  // file 1 belongs to "feature1"
        {"sub1", 0, 2},      // file 2 belongs to "sub1"
    };

    eloqstore::ManifestBuilder builder;
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

    // Allocator max should equal max_fp_id (local mode: no term bumping)
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 48);

    // branch_metadata_ must reflect "sub1" with all 3 ranges preserved
    REQUIRE(replayer.branch_metadata_.branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges.size() == 3);
    REQUIRE(replayer.branch_metadata_.file_ranges[0].branch_name == "main");
    REQUIRE(replayer.branch_metadata_.file_ranges[0].max_file_id == 0);
    REQUIRE(replayer.branch_metadata_.file_ranges[1].branch_name == "feature1");
    REQUIRE(replayer.branch_metadata_.file_ranges[1].max_file_id == 1);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges[2].max_file_id == 2);

    // In non-append (pooled) mode the free list contains only pages from file 2
    // (fp_ids 32-47) minus fp_id 32 which is in use.  Allocate() must return a
    // value in [33, 47] — never from files 0 or 1.
    eloqstore::FilePageId allocated = mapper->FilePgAllocator()->Allocate();
    REQUIRE(allocated >= 33);
    REQUIRE(allocated <= 47);
}

TEST_CASE(
    "Replayer preserves evolving BranchFileMapping for chained fork across "
    "multiple term appends",
    "[replayer][branch]")
{
    // Cloud mode, 16 pages per file (shift=4).
    // Snapshot at term=5: sub1 has written one page in file 2.
    // Append log at term=10: sub1 writes another page in file 3.
    // After replay, branch_metadata_ must reflect the LATEST (term=10) mapping
    // and MaxFilePageId must be 64 (terms match => no bumping).
    eloqstore::KvOptions opts =
        MakeOpts(true /*cloud_mode*/, 4 /*pages_per_file_shift*/);

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    // --- Snapshot at term=5 ---
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.Set(
        0, eloqstore::MappingSnapshot::EncodeFilePageId(0));  // main, file 0
    mapping_tbl.Set(
        1,
        eloqstore::MappingSnapshot::EncodeFilePageId(16));  // feature1, file 1
    mapping_tbl.Set(
        2, eloqstore::MappingSnapshot::EncodeFilePageId(32));  // sub1, file 2
    eloqstore::MappingSnapshot mapping(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));
    const eloqstore::FilePageId snap_max_fp_id = 48;  // 3 files

    eloqstore::BranchManifestMetadata meta5;
    meta5.branch_name = "sub1";
    meta5.term = 5;
    meta5.file_ranges = {
        {"main", 0, 0},
        {"feature1", 0, 1},
        {"sub1", 5, 2},
    };

    eloqstore::ManifestBuilder builder;
    std::string_view snapshot_sv = builder.Snapshot(eloqstore::MaxPageId,
                                                    eloqstore::MaxPageId,
                                                    &mapping,
                                                    snap_max_fp_id,
                                                    {},
                                                    meta5);

    std::string manifest_buf;
    manifest_buf.append(snapshot_sv);

    // --- Append log at term=10: sub1 allocates page in file 3 ---
    // fp_id 48 = file 3, page 0
    eloqstore::ManifestBuilder builder1;
    builder1.UpdateMapping(3, 48);  // page_id 3 -> fp_id 48 (file 3)

    eloqstore::BranchManifestMetadata meta10;
    meta10.branch_name = "sub1";
    meta10.term = 10;
    meta10.file_ranges = {
        {"main", 0, 0},
        {"feature1", 0, 1},
        {"sub1", 10, 3},  // sub1 now covers up to file 3 at term=10
    };
    std::string meta10_str = eloqstore::SerializeBranchManifestMetadata(meta10);
    builder1.AppendBranchManifestMetadata(meta10_str);

    // max_fp_id after writing to file 3 = 4 files * 16 = 64
    std::string_view append_log =
        builder1.Finalize(eloqstore::MaxPageId, eloqstore::MaxPageId);
    manifest_buf.append(append_log);

    // --- Replay ---
    eloqstore::MemStoreMgr::Manifest file(manifest_buf);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // expect_term=10 matches manifest term=10 => no bumping
    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 10);
    REQUIRE(mapper != nullptr);

    // branch_metadata_ must carry the LATEST term=10 metadata
    REQUIRE(replayer.branch_metadata_.term == 10);
    REQUIRE(replayer.branch_metadata_.branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges.size() == 3);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges[2].term == 10);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].max_file_id == 3);

    // Page mapping: page_id 3 -> fp_id 48 (added by append log) must be visible
    const auto &mtbl = mapper->GetMapping()->mapping_tbl_;
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(mtbl.Get(3)) == 48);
}
