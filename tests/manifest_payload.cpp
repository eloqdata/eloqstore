#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../include/common.h"
#include "../include/storage/index_page_manager.h"
#include "../include/storage/page_mapper.h"
#include "../include/storage/root_meta.h"
#include "../include/types.h"

uint64_t MockEncodeFilePageId(eloqstore::FilePageId file_page_id)
{
    return (file_page_id << eloqstore::MappingSnapshot::TypeBits) |
           static_cast<uint64_t>(
               eloqstore::MappingSnapshot::ValType::FilePageId);
}

// On-disk snapshot payload layout produced by ManifestBuilder::Snapshot:
//   [max_fp_id (varint64)]
//   [dict_len (varint32) | dict_bytes]
//   [mapping_len (Fixed32) | mapping_tbl (varint64...)]
//   [branch_meta_len (Fixed32) | BranchManifestMetadata bytes]
//   [optional: max_segment_fp_id (varint64) |
//              seg_mapping_len (Fixed32) | seg_mapping_tbl (varint64...)]
//
// The Fixed32 length prefix on BranchManifestMetadata lets the replayer skip
// past it and reach the segment section that may follow.

TEST_CASE(
    "ManifestBuilder snapshot serializes BranchManifestMetadata after mapping "
    "table (non-empty)",
    "[manifest-payload]")
{
    eloqstore::TableIdent tbl_id("test", 1);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(100));
    mapping_tbl.PushBack(MockEncodeFilePageId(200));
    mapping_tbl.PushBack(MockEncodeFilePageId(300));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    const std::string dict_bytes = "DICT_BYTES";
    const eloqstore::FilePageId max_fp_id = 123456;

    eloqstore::ManifestBuilder builder;
    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = eloqstore::MainBranchName;
    branch_metadata.term = 42;
    std::string_view manifest = builder.Snapshot(/*root_id=*/1,
                                                 /*ttl_root=*/2,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 branch_metadata);
    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);

    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    // 1) max_fp_id
    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    REQUIRE(parsed_max_fp == max_fp_id);

    // 2) dict length + dict bytes
    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    REQUIRE(parsed_dict_len == dict_bytes.size());
    REQUIRE(payload.size() >= parsed_dict_len);

    std::string_view parsed_dict(payload.data(), parsed_dict_len);
    REQUIRE(parsed_dict == dict_bytes);
    payload.remove_prefix(parsed_dict_len);

    // 3) mapping_len (Fixed32, 4 bytes)
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    std::string_view mapping_view = payload.substr(0, mapping_len);

    // 4) mapping table
    eloqstore::MappingSnapshot::MappingTbl parsed_tbl;
    while (!mapping_view.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&mapping_view, &val));
        parsed_tbl.PushBack(val);
    }
    REQUIRE(parsed_tbl == mapping_snapshot.mapping_tbl_);
    payload.remove_prefix(mapping_len);

    // 5) BranchManifestMetadata: Fixed32 length prefix + serialized bytes
    REQUIRE(payload.size() >= 4);
    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(payload.size() >= branch_meta_len);
    std::string_view branch_meta_view = payload.substr(0, branch_meta_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));
    REQUIRE(parsed_meta.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed_meta.term == 42);
    payload.remove_prefix(branch_meta_len);

    // No segment section was passed; nothing should remain.
    REQUIRE(payload.empty());

    mapping_snapshot.mapping_tbl_.clear();
}

TEST_CASE(
    "ManifestBuilder snapshot writes empty BranchManifestMetadata section when "
    "branch is main with term 0",
    "[manifest-payload]")
{
    eloqstore::TableIdent tbl_id("test", 2);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(42));
    mapping_tbl.PushBack(MockEncodeFilePageId(43));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    const std::string dict_bytes = "D";
    const eloqstore::FilePageId max_fp_id = 7;

    eloqstore::ManifestBuilder builder;
    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = eloqstore::MainBranchName;
    branch_metadata.term = 0;
    std::string_view manifest = builder.Snapshot(/*root_id=*/3,
                                                 /*ttl_root=*/4,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 branch_metadata);

    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);
    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    REQUIRE(parsed_max_fp == max_fp_id);

    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    REQUIRE(parsed_dict_len == dict_bytes.size());
    payload.remove_prefix(parsed_dict_len);

    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    std::string_view mapping_view = payload.substr(0, mapping_len);

    std::vector<uint64_t> parsed_tbl;
    while (!mapping_view.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&mapping_view, &val));
        parsed_tbl.push_back(val);
    }
    REQUIRE(parsed_tbl.size() == 2);
    REQUIRE(parsed_tbl[0] == MockEncodeFilePageId(42));
    REQUIRE(parsed_tbl[1] == MockEncodeFilePageId(43));
    payload.remove_prefix(mapping_len);

    REQUIRE(payload.size() >= 4);
    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(payload.size() >= branch_meta_len);
    std::string_view branch_meta_view = payload.substr(0, branch_meta_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));
    REQUIRE(parsed_meta.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed_meta.term == 0);
    payload.remove_prefix(branch_meta_len);
    REQUIRE(payload.empty());

    mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}

// ---------------------------------------------------------------------------
// Direct BranchManifestMetadata serialization / deserialization tests
// ---------------------------------------------------------------------------

TEST_CASE(
    "BranchManifestMetadata serialization roundtrip with non-empty file_ranges",
    "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = "feature-a3f7b2c1";
    original.term = 99;
    original.file_ranges.push_back({"main", 1, 50});
    original.file_ranges.push_back({"feature-a3f7b2c1", 3, 150});
    original.file_ranges.push_back({"hotfix", 2, 200});

    std::string serialized =
        eloqstore::SerializeBranchManifestMetadata(original);

    eloqstore::BranchManifestMetadata parsed;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(serialized, parsed));

    REQUIRE(parsed.branch_name == "feature-a3f7b2c1");
    REQUIRE(parsed.term == 99);
    REQUIRE(parsed.file_ranges.size() == 3);

    REQUIRE(parsed.file_ranges[0].branch_name_ == "main");
    REQUIRE(parsed.file_ranges[0].term_ == 1);
    REQUIRE(parsed.file_ranges[0].max_file_id_ == 50);

    REQUIRE(parsed.file_ranges[1].branch_name_ == "feature-a3f7b2c1");
    REQUIRE(parsed.file_ranges[1].term_ == 3);
    REQUIRE(parsed.file_ranges[1].max_file_id_ == 150);

    REQUIRE(parsed.file_ranges[2].branch_name_ == "hotfix");
    REQUIRE(parsed.file_ranges[2].term_ == 2);
    REQUIRE(parsed.file_ranges[2].max_file_id_ == 200);
}

TEST_CASE(
    "BranchManifestMetadata serialization roundtrip with empty file_ranges",
    "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = eloqstore::MainBranchName;
    original.term = 7;

    std::string serialized =
        eloqstore::SerializeBranchManifestMetadata(original);

    eloqstore::BranchManifestMetadata parsed;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(serialized, parsed));

    REQUIRE(parsed.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed.term == 7);
    REQUIRE(parsed.file_ranges.empty());
}

TEST_CASE("BranchManifestMetadata serialization roundtrip with zero term",
          "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = "new-branch";
    original.term = 0;
    original.file_ranges.push_back({"main", 5, 1000});

    std::string serialized =
        eloqstore::SerializeBranchManifestMetadata(original);

    eloqstore::BranchManifestMetadata parsed;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(serialized, parsed));

    REQUIRE(parsed.branch_name == "new-branch");
    REQUIRE(parsed.term == 0);
    REQUIRE(parsed.file_ranges.size() == 1);
    REQUIRE(parsed.file_ranges[0].branch_name_ == "main");
    REQUIRE(parsed.file_ranges[0].term_ == 5);
    REQUIRE(parsed.file_ranges[0].max_file_id_ == 1000);
}

TEST_CASE("BranchManifestMetadata serialization roundtrip with large values",
          "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = "main";
    original.term = UINT64_MAX;
    original.file_ranges.push_back(
        {"branch-with-max-fileid", UINT64_MAX, eloqstore::MaxFileId});

    std::string serialized =
        eloqstore::SerializeBranchManifestMetadata(original);

    eloqstore::BranchManifestMetadata parsed;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(serialized, parsed));

    REQUIRE(parsed.branch_name == "main");
    REQUIRE(parsed.term == UINT64_MAX);
    REQUIRE(parsed.file_ranges.size() == 1);
    REQUIRE(parsed.file_ranges[0].term_ == UINT64_MAX);
    REQUIRE(parsed.file_ranges[0].max_file_id_ == eloqstore::MaxFileId);
}

TEST_CASE(
    "BranchManifestMetadata deserialization returns empty on truncated input",
    "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = "main";
    original.term = 42;
    original.file_ranges.push_back({"main", 1, 100});

    std::string serialized =
        eloqstore::SerializeBranchManifestMetadata(original);

    {
        eloqstore::BranchManifestMetadata parsed;
        REQUIRE_FALSE(eloqstore::DeserializeBranchManifestMetadata(
            std::string_view(serialized.data(), 2), parsed));
        REQUIRE(parsed.branch_name.empty());
        REQUIRE(parsed.term == 0);
        REQUIRE(parsed.file_ranges.empty());
    }

    {
        eloqstore::BranchManifestMetadata parsed;
        REQUIRE_FALSE(eloqstore::DeserializeBranchManifestMetadata(
            std::string_view(serialized.data(), 10), parsed));
        REQUIRE(parsed.branch_name.empty());
        REQUIRE(parsed.file_ranges.empty());
    }

    {
        eloqstore::BranchManifestMetadata parsed;
        REQUIRE_FALSE(eloqstore::DeserializeBranchManifestMetadata(
            std::string_view(), parsed));
        REQUIRE(parsed.branch_name.empty());
        REQUIRE(parsed.term == 0);
        REQUIRE(parsed.file_ranges.empty());
    }
}

TEST_CASE(
    "ManifestBuilder snapshot with non-empty file_ranges in "
    "BranchManifestMetadata",
    "[manifest-payload]")
{
    eloqstore::TableIdent tbl_id("test", 3);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(500));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    const std::string dict_bytes = "DICT";
    const eloqstore::FilePageId max_fp_id = 999;

    eloqstore::ManifestBuilder builder;
    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = "feature-xyz";
    branch_metadata.term = 10;
    branch_metadata.file_ranges.push_back({"main", 1, 50});
    branch_metadata.file_ranges.push_back({"feature-xyz", 10, 200});

    std::string_view manifest = builder.Snapshot(/*root_id=*/5,
                                                 /*ttl_root=*/6,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 branch_metadata);
    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);

    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    REQUIRE(parsed_max_fp == max_fp_id);

    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    payload.remove_prefix(parsed_dict_len);

    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4 + mapping_len);

    REQUIRE(payload.size() >= 4);
    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(payload.size() >= branch_meta_len);
    std::string_view branch_meta_view = payload.substr(0, branch_meta_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));

    REQUIRE(parsed_meta.branch_name == "feature-xyz");
    REQUIRE(parsed_meta.term == 10);
    REQUIRE(parsed_meta.file_ranges.size() == 2);
    REQUIRE(parsed_meta.file_ranges[0].branch_name_ == "main");
    REQUIRE(parsed_meta.file_ranges[0].term_ == 1);
    REQUIRE(parsed_meta.file_ranges[0].max_file_id_ == 50);
    REQUIRE(parsed_meta.file_ranges[1].branch_name_ == "feature-xyz");
    REQUIRE(parsed_meta.file_ranges[1].term_ == 10);
    REQUIRE(parsed_meta.file_ranges[1].max_file_id_ == 200);

    mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}

TEST_CASE(
    "ManifestBuilder snapshot appends segment mapping section after "
    "BranchManifestMetadata in layout order",
    "[manifest-payload]")
{
    // Build a snapshot with a non-empty segment mapping and verify the wire
    // layout:
    //   [max_fp_id | dict | mapping_len | mapping_tbl |
    //    branch_meta_len | BranchManifestMetadata |
    //    max_segment_fp_id | seg_mapping_len | seg_mapping_tbl]
    eloqstore::TableIdent tbl_id("test", 11);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);

    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(10));
    mapping_tbl.PushBack(MockEncodeFilePageId(11));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    eloqstore::MappingSnapshot::MappingTbl seg_mapping_tbl;
    seg_mapping_tbl.PushBack(MockEncodeFilePageId(500));
    seg_mapping_tbl.PushBack(MockEncodeFilePageId(501));
    seg_mapping_tbl.PushBack(MockEncodeFilePageId(600));
    eloqstore::MappingSnapshot seg_mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(seg_mapping_tbl));

    const std::string dict_bytes = "DICT";
    const eloqstore::FilePageId max_fp_id = 17u;
    const eloqstore::FilePageId max_segment_fp_id = 601u;

    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = eloqstore::MainBranchName;
    branch_metadata.term = 5;

    eloqstore::ManifestBuilder builder;
    std::string_view manifest = builder.Snapshot(/*root_id=*/9,
                                                 /*ttl_root=*/9,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 branch_metadata,
                                                 &seg_mapping_snapshot,
                                                 max_segment_fp_id);

    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    // 1) max_fp_id
    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    REQUIRE(parsed_max_fp == max_fp_id);

    // 2) dict
    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    REQUIRE(parsed_dict_len == dict_bytes.size());
    REQUIRE(std::string_view(payload.data(), parsed_dict_len) == dict_bytes);
    payload.remove_prefix(parsed_dict_len);

    // 3) mapping_len + mapping_tbl
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    std::string_view mapping_view = payload.substr(0, mapping_len);
    eloqstore::MappingSnapshot::MappingTbl parsed_tbl;
    while (!mapping_view.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&mapping_view, &val));
        parsed_tbl.PushBack(val);
    }
    REQUIRE(parsed_tbl == mapping_snapshot.mapping_tbl_);
    payload.remove_prefix(mapping_len);

    // 4) branch_meta_len + BranchManifestMetadata
    REQUIRE(payload.size() >= 4);
    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(payload.size() >= branch_meta_len);
    std::string_view branch_meta_view = payload.substr(0, branch_meta_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));
    REQUIRE(parsed_meta.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed_meta.term == 5);
    payload.remove_prefix(branch_meta_len);

    // 5) max_segment_fp_id
    uint64_t parsed_max_seg = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_seg));
    REQUIRE(parsed_max_seg == max_segment_fp_id);

    // 6) seg_mapping_len + seg_mapping_tbl
    REQUIRE(payload.size() >= 4);
    const uint32_t seg_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(payload.size() >= seg_len);
    std::string_view seg_view = payload.substr(0, seg_len);
    eloqstore::MappingSnapshot::MappingTbl parsed_seg_tbl;
    while (!seg_view.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&seg_view, &val));
        parsed_seg_tbl.PushBack(val);
    }
    REQUIRE(parsed_seg_tbl == seg_mapping_snapshot.mapping_tbl_);
    payload.remove_prefix(seg_len);
    REQUIRE(payload.empty());

    mapping_snapshot.mapping_tbl_.clear();
    seg_mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}

TEST_CASE(
    "ManifestBuilder snapshot without segment mapping leaves no trailing "
    "bytes after BranchManifestMetadata",
    "[manifest-payload]")
{
    // When no segment mapping is supplied, the builder must not write a
    // segment section. The replayer's "segment section exists" guard
    // (>= 4 bytes remaining) must short-circuit cleanly.
    eloqstore::TableIdent tbl_id("test", 12);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);
    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(1));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = eloqstore::MainBranchName;
    branch_metadata.term = 0;

    eloqstore::ManifestBuilder builder;
    std::string_view manifest = builder.Snapshot(/*root_id=*/5,
                                                 /*ttl_root=*/6,
                                                 &mapping_snapshot,
                                                 /*max_fp_id=*/8u,
                                                 /*dict_bytes=*/{},
                                                 branch_metadata);

    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    REQUIRE(parsed_dict_len == 0u);
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4 + mapping_len);

    REQUIRE(payload.size() >= 4);
    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4 + branch_meta_len);

    // After BranchManifestMetadata there must be exactly zero remaining bytes.
    REQUIRE(payload.empty());

    mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}

TEST_CASE(
    "ManifestBuilder snapshot with empty segment MappingSnapshot still writes "
    "an explicit (zero-length) segment section",
    "[manifest-payload]")
{
    // Opting into segment bookkeeping by passing a non-null MappingSnapshot
    // (even an empty one) commits the writer to the segment-section layout.
    // The parser must see max_segment_fp_id=0 and a zero-length table — not
    // interpret an empty trailer as "no section".
    eloqstore::TableIdent tbl_id("test", 13);
    eloqstore::KvOptions opts;
    eloqstore::IouringMgr io_mgr(&opts, 1000);
    eloqstore::IndexPageManager idx_mgr(&io_mgr);

    eloqstore::MappingSnapshot::MappingTbl mapping_tbl;
    mapping_tbl.PushBack(MockEncodeFilePageId(3));
    eloqstore::MappingSnapshot mapping_snapshot(
        &idx_mgr, &tbl_id, std::move(mapping_tbl));

    eloqstore::MappingSnapshot seg_mapping_snapshot(
        &idx_mgr, &tbl_id, eloqstore::MappingSnapshot::MappingTbl{});

    eloqstore::BranchManifestMetadata branch_metadata;
    branch_metadata.branch_name = eloqstore::MainBranchName;
    branch_metadata.term = 0;

    eloqstore::ManifestBuilder builder;
    std::string_view manifest = builder.Snapshot(/*root_id=*/7,
                                                 /*ttl_root=*/7,
                                                 &mapping_snapshot,
                                                 /*max_fp_id=*/4u,
                                                 /*dict_bytes=*/{},
                                                 branch_metadata,
                                                 &seg_mapping_snapshot,
                                                 /*max_segment_fp_id=*/0u);

    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);
    uint64_t tmp64 = 0;
    uint32_t tmp32 = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &tmp64));
    REQUIRE(eloqstore::GetVarint32(&payload, &tmp32));
    payload.remove_prefix(tmp32);
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4 + mapping_len);

    const uint32_t branch_meta_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4 + branch_meta_len);

    // Segment section: max_segment_fp_id, seg_mapping_len=0, zero bytes.
    uint64_t parsed_max_seg = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_seg));
    REQUIRE(parsed_max_seg == 0u);
    REQUIRE(payload.size() >= 4);
    const uint32_t seg_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);
    REQUIRE(seg_len == 0u);
    REQUIRE(payload.empty());

    mapping_snapshot.mapping_tbl_.clear();
    seg_mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}
