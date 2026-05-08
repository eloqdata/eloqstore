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
#include "coding.h"
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

eloqstore::KvOptions MakeOpts(bool cloud_mode, uint8_t shift, bool append_mode)
{
    eloqstore::KvOptions opts = MakeOpts(cloud_mode, shift);
    opts.data_append_mode = append_mode;
    return opts;
}

// Build a segment-delta log body for AppendSegmentMapping: a 4-byte length
// prefix followed by (varint32 seg_page_id, varint64 encoded_value) pairs.
// Mirrors the Replayer::ReplayLog() segment-delta parser.
std::string EncodeSegmentDeltas(
    const std::vector<std::pair<eloqstore::PageId, eloqstore::FilePageId>>
        &deltas)
{
    std::string body;
    for (const auto &[seg_page_id, seg_fp] : deltas)
    {
        eloqstore::PutVarint32(&body, seg_page_id);
        eloqstore::PutVarint64(
            &body, eloqstore::MappingSnapshot::EncodeFilePageId(seg_fp));
    }
    std::string out;
    out.resize(4);
    eloqstore::EncodeFixed32(out.data(), static_cast<uint32_t>(body.size()));
    out.append(body);
    return out;
}

// Append BranchManifestMetadata + an empty segment-delta section to a log
// builder. Tests that don't exercise segment deltas still need to write the
// empty section so the replayer's segment-delta parser short-circuits cleanly.
void AppendLogTail(eloqstore::ManifestBuilder &builder,
                   const eloqstore::BranchManifestMetadata &meta)
{
    std::string meta_str = eloqstore::SerializeBranchManifestMetadata(meta);
    builder.AppendBranchManifestMetadata(meta_str);
    std::string empty_seg;
    empty_seg.resize(4);
    eloqstore::EncodeFixed32(empty_seg.data(), 0);
    builder.AppendSegmentMapping(empty_seg);
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

    eloqstore::ManifestBuilder builder;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot mapping(&idx_mgr, &tbl_id, {});
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

    // expect_term equal to manifest_term => no bumping.
    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 1);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);

    eloqstore::MemStoreMgr::Manifest file2(snapshot);
    eloqstore::Replayer replayer2(&opts);
    REQUIRE(replayer2.Replay(&file2) == eloqstore::KvError::NoError);
    // expect_term differs => bump to next file boundary.
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

    // Append log 1 (term=20).
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

    eloqstore::BranchManifestMetadata meta20;
    meta20.branch_name = eloqstore::MainBranchName;
    meta20.term = 20;
    AppendLogTail(builder1, meta20);
    std::string_view append_log1 = builder1.Finalize(10, 10);

    manifest_buf.append(append_log1);

    // Append log 2 (term=30).
    eloqstore::ManifestBuilder builder2;
    builder2.UpdateMapping(20, 20);
    builder2.UpdateMapping(21, 21);
    all_page_map[20] = 20;
    all_page_map[21] = 21;

    eloqstore::BranchManifestMetadata meta30;
    meta30.branch_name = eloqstore::MainBranchName;
    meta30.term = 30;
    AppendLogTail(builder2, meta30);
    std::string_view append_log2 = builder2.Finalize(30, 30);

    manifest_buf.append(append_log2);

    eloqstore::MemStoreMgr::Manifest file(manifest_buf);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 30);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 26);

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
    eloqstore::KvOptions opts = MakeOpts(false /*cloud_mode*/,
                                         4 /*pages_per_file_shift*/,
                                         false /*append_mode*/);

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.Set(0, eloqstore::MappingSnapshot::EncodeFilePageId(0));
    mapping_tbl.Set(1, eloqstore::MappingSnapshot::EncodeFilePageId(16));
    mapping_tbl.Set(2, eloqstore::MappingSnapshot::EncodeFilePageId(32));
    eloqstore::MappingSnapshot mapping(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    const eloqstore::FilePageId max_fp_id = 48;

    eloqstore::BranchManifestMetadata branch_meta;
    branch_meta.branch_name = "sub1";
    branch_meta.term = 0;
    branch_meta.file_ranges = {
        {"main", 0, 0},
        {"feature1", 0, 1},
        {"sub1", 0, 2},
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

    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 48);

    REQUIRE(replayer.branch_metadata_.branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges.size() == 3);
    REQUIRE(replayer.branch_metadata_.file_ranges[0].branch_name_ == "main");
    REQUIRE(replayer.branch_metadata_.file_ranges[0].max_file_id_ == 0);
    REQUIRE(replayer.branch_metadata_.file_ranges[1].branch_name_ ==
            "feature1");
    REQUIRE(replayer.branch_metadata_.file_ranges[1].max_file_id_ == 1);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].branch_name_ == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges[2].max_file_id_ == 2);

    eloqstore::FilePageId allocated = mapper->FilePgAllocator()->Allocate();
    REQUIRE(allocated >= 33);
    REQUIRE(allocated <= 47);
}

TEST_CASE(
    "Replayer preserves evolving BranchFileMapping for chained fork across "
    "multiple term appends",
    "[replayer][branch]")
{
    eloqstore::KvOptions opts =
        MakeOpts(true /*cloud_mode*/, 4 /*pages_per_file_shift*/);

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.Set(0, eloqstore::MappingSnapshot::EncodeFilePageId(0));
    mapping_tbl.Set(1, eloqstore::MappingSnapshot::EncodeFilePageId(16));
    mapping_tbl.Set(2, eloqstore::MappingSnapshot::EncodeFilePageId(32));
    eloqstore::MappingSnapshot mapping(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));
    const eloqstore::FilePageId snap_max_fp_id = 48;

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

    eloqstore::ManifestBuilder builder1;
    builder1.UpdateMapping(3, 48);

    eloqstore::BranchManifestMetadata meta10;
    meta10.branch_name = "sub1";
    meta10.term = 10;
    meta10.file_ranges = {
        {"main", 0, 0},
        {"feature1", 0, 1},
        {"sub1", 10, 3},
    };
    AppendLogTail(builder1, meta10);

    std::string_view append_log =
        builder1.Finalize(eloqstore::MaxPageId, eloqstore::MaxPageId);
    manifest_buf.append(append_log);

    eloqstore::MemStoreMgr::Manifest file(manifest_buf);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    auto mapper = replayer.GetMapper(&idx_mgr, &tbl_id, 10);
    REQUIRE(mapper != nullptr);

    REQUIRE(replayer.branch_metadata_.term == 10);
    REQUIRE(replayer.branch_metadata_.branch_name == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges.size() == 3);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].branch_name_ == "sub1");
    REQUIRE(replayer.branch_metadata_.file_ranges[2].term_ == 10);
    REQUIRE(replayer.branch_metadata_.file_ranges[2].max_file_id_ == 3);

    const auto &mtbl = mapper->GetMapping()->mapping_tbl_;
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(mtbl.Get(3)) == 48);
}

TEST_CASE(
    "Replayer merges non-empty segment mapping across snapshot and append logs",
    "[replayer][segment]")
{
    // Verifies the full segment-mapping replay path:
    //  1) Snapshot seeds segment_mapping_tbl_ and max_segment_fp_id_.
    //  2) Two append logs with (page_id, file_page_id) deltas are applied
    //     in order; later writes overwrite earlier ones.
    //  3) GetSegmentMapper() returns a mapper whose AppendAllocator is
    //     positioned strictly past the largest segment file-page id.
    eloqstore::KvOptions opts = MakeOpts(/*cloud_mode=*/false, /*shift=*/4);
    opts.segments_per_file_shift = 3;  // 8 segments per segment file.

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    eloqstore::MappingSnapshot data_mapping(
        &idx_mgr, &tbl_id, eloqstore::MappingSnapshot::MappingTbl{});

    eloqstore::MappingSnapshot::MappingTbl seg_tbl;
    seg_tbl.Set(0, eloqstore::MappingSnapshot::EncodeFilePageId(2));
    seg_tbl.Set(1, eloqstore::MappingSnapshot::EncodeFilePageId(3));
    seg_tbl.Set(2, eloqstore::MappingSnapshot::EncodeFilePageId(9));  // file 1
    eloqstore::MappingSnapshot seg_mapping(
        &idx_mgr, &tbl_id, std::move(seg_tbl));
    const eloqstore::FilePageId max_segment_fp_id = 10u;

    eloqstore::BranchManifestMetadata meta1;
    meta1.branch_name = eloqstore::MainBranchName;
    meta1.term = 1;

    eloqstore::ManifestBuilder builder;
    std::string_view snap = builder.Snapshot(eloqstore::MaxPageId,
                                             eloqstore::MaxPageId,
                                             &data_mapping,
                                             /*max_fp_id=*/0u,
                                             /*dict_bytes=*/{},
                                             meta1,
                                             &seg_mapping,
                                             max_segment_fp_id);
    std::string manifest_buf(snap);

    // Append log 1: add segment at page_id=3 (file 2), overwrite page_id=1.
    eloqstore::ManifestBuilder builder1;
    eloqstore::BranchManifestMetadata meta1_log = meta1;
    std::string meta1_log_str =
        eloqstore::SerializeBranchManifestMetadata(meta1_log);
    builder1.AppendBranchManifestMetadata(meta1_log_str);
    std::string seg_log1 = EncodeSegmentDeltas({{3u, 16u}, {1u, 4u}});
    builder1.AppendSegmentMapping(seg_log1);
    manifest_buf.append(
        builder1.Finalize(eloqstore::MaxPageId, eloqstore::MaxPageId));

    // Append log 2: add segment at page_id=4 (file 3), overwrite page_id=0.
    eloqstore::ManifestBuilder builder2;
    builder2.AppendBranchManifestMetadata(meta1_log_str);
    std::string seg_log2 = EncodeSegmentDeltas({{4u, 24u}, {0u, 5u}});
    builder2.AppendSegmentMapping(seg_log2);
    manifest_buf.append(
        builder2.Finalize(eloqstore::MaxPageId, eloqstore::MaxPageId));

    eloqstore::MemStoreMgr::Manifest file(manifest_buf);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    REQUIRE(replayer.segment_mapping_tbl_.size() == 5);
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(
                replayer.segment_mapping_tbl_.Get(0)) == 5u);
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(
                replayer.segment_mapping_tbl_.Get(1)) == 4u);
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(
                replayer.segment_mapping_tbl_.Get(2)) == 9u);
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(
                replayer.segment_mapping_tbl_.Get(3)) == 16u);
    REQUIRE(eloqstore::MappingSnapshot::DecodeId(
                replayer.segment_mapping_tbl_.Get(4)) == 24u);
    REQUIRE(replayer.max_segment_fp_id_ == 25u);

    auto seg_mapper = replayer.GetSegmentMapper(&idx_mgr, &tbl_id, 0);
    REQUIRE(seg_mapper != nullptr);
    REQUIRE(seg_mapper->FilePgAllocator()->MaxFilePageId() == 25u);
    REQUIRE(seg_mapper->FilePgAllocator()->PagesPerFile() == 8u);
    REQUIRE(seg_mapper->FilePgAllocator()->Allocate() == 25u);
}

TEST_CASE(
    "Replayer GetSegmentMapper bumps segment allocator on term mismatch in "
    "cloud mode",
    "[replayer][segment][term]")
{
    // When manifest_term != expect_term in cloud mode, GetSegmentMapper
    // bumps max_segment_fp_id_ to the next segment-file boundary, mirroring
    // the data-file bumping contract in GetMapper.
    eloqstore::KvOptions opts = MakeOpts(/*cloud_mode=*/true, /*shift=*/4);
    opts.segments_per_file_shift = 3;  // 8 segments per segment file.

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);

    eloqstore::MappingSnapshot data_mapping(
        &idx_mgr, &tbl_id, eloqstore::MappingSnapshot::MappingTbl{});

    // One segment occupying file_id=0; max_segment_fp_id=3 sits mid-file so
    // bumping advances to the next 8-aligned boundary (=8).
    eloqstore::MappingSnapshot::MappingTbl seg_tbl;
    seg_tbl.Set(0, eloqstore::MappingSnapshot::EncodeFilePageId(2));
    eloqstore::MappingSnapshot seg_mapping(
        &idx_mgr, &tbl_id, std::move(seg_tbl));
    const eloqstore::FilePageId max_segment_fp_id = 3u;

    eloqstore::BranchManifestMetadata meta;
    meta.branch_name = eloqstore::MainBranchName;
    meta.term = 10;

    eloqstore::ManifestBuilder builder;
    std::string_view snap = builder.Snapshot(eloqstore::MaxPageId,
                                             eloqstore::MaxPageId,
                                             &data_mapping,
                                             /*max_fp_id=*/0u,
                                             /*dict_bytes=*/{},
                                             meta,
                                             &seg_mapping,
                                             max_segment_fp_id);

    eloqstore::MemStoreMgr::Manifest file(snap);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);

    // expect_term != manifest_term(=10) in cloud mode → allocator bumped.
    auto seg_mapper =
        replayer.GetSegmentMapper(&idx_mgr, &tbl_id, /*expect_term=*/99);
    REQUIRE(seg_mapper != nullptr);
    REQUIRE(seg_mapper->FilePgAllocator()->MaxFilePageId() == 8u);
    REQUIRE(seg_mapper->FilePgAllocator()->Allocate() == 8u);
}

TEST_CASE(
    "Replayer GetSegmentMapper returns nullptr when no segment mapping was "
    "recorded",
    "[replayer][segment]")
{
    // Snapshot without segment_mapping (data-only) must result in
    // GetSegmentMapper returning nullptr — the segment-path is opt-in and
    // should not fabricate an empty mapper.
    eloqstore::KvOptions opts = MakeOpts(/*cloud_mode=*/false, /*shift=*/4);
    opts.segments_per_file_shift = 3;

    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::MappingSnapshot data_mapping(
        &idx_mgr, &tbl_id, eloqstore::MappingSnapshot::MappingTbl{});

    eloqstore::BranchManifestMetadata meta;
    meta.branch_name = eloqstore::MainBranchName;
    meta.term = 0;

    eloqstore::ManifestBuilder builder;
    std::string_view snap = builder.Snapshot(eloqstore::MaxPageId,
                                             eloqstore::MaxPageId,
                                             &data_mapping,
                                             /*max_fp_id=*/0u,
                                             /*dict_bytes=*/{},
                                             meta);

    eloqstore::MemStoreMgr::Manifest file(snap);
    eloqstore::Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == eloqstore::KvError::NoError);
    REQUIRE(replayer.max_segment_fp_id_ == 0u);
    REQUIRE(replayer.segment_mapping_tbl_.size() == 0);
    REQUIRE(replayer.GetSegmentMapper(&idx_mgr, &tbl_id, 0) == nullptr);
}
