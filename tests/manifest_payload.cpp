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

TEST_CASE(
    "ManifestBuilder snapshot serializes BranchManifestMetadata after mapping "
    "table (non-empty)",
    "[manifest-payload]")
{
    // Prepare a simple mapping table.
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

    // Dict bytes and max_fp_id to embed into snapshot payload.
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

    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][mapping_len(4B)][mapping_tbl_...]
    // [BranchManifestMetadata]
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

    // 5) BranchManifestMetadata after the mapping table
    std::string_view branch_meta_view = payload.substr(mapping_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));
    REQUIRE(parsed_meta.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed_meta.term == 42);

    mapping_snapshot.mapping_tbl_.clear();
}

TEST_CASE(
    "ManifestBuilder snapshot writes empty BranchManifestMetadata section when "
    "mapping is null",
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
    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][mapping_len(4B)][mapping_tbl_...]
    // [BranchManifestMetadata]
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

    // 5) BranchManifestMetadata after the mapping table
    std::string_view branch_meta_view = payload.substr(mapping_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));
    REQUIRE(parsed_meta.branch_name == eloqstore::MainBranchName);
    REQUIRE(parsed_meta.term == 0);

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

    REQUIRE(parsed.file_ranges[0].branch_name == "main");
    REQUIRE(parsed.file_ranges[0].term == 1);
    REQUIRE(parsed.file_ranges[0].max_file_id == 50);

    REQUIRE(parsed.file_ranges[1].branch_name == "feature-a3f7b2c1");
    REQUIRE(parsed.file_ranges[1].term == 3);
    REQUIRE(parsed.file_ranges[1].max_file_id == 150);

    REQUIRE(parsed.file_ranges[2].branch_name == "hotfix");
    REQUIRE(parsed.file_ranges[2].term == 2);
    REQUIRE(parsed.file_ranges[2].max_file_id == 200);
}

TEST_CASE(
    "BranchManifestMetadata serialization roundtrip with empty file_ranges",
    "[branch-metadata]")
{
    eloqstore::BranchManifestMetadata original;
    original.branch_name = eloqstore::MainBranchName;
    original.term = 7;
    // file_ranges left empty

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
    // Newly created branches use term=0 (see CreateBranch in
    // background_write.cpp)
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
    REQUIRE(parsed.file_ranges[0].branch_name == "main");
    REQUIRE(parsed.file_ranges[0].term == 5);
    REQUIRE(parsed.file_ranges[0].max_file_id == 1000);
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
    REQUIRE(parsed.file_ranges[0].term == UINT64_MAX);
    REQUIRE(parsed.file_ranges[0].max_file_id == eloqstore::MaxFileId);
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

    // Truncate to less than the branch_name_len field (< 4 bytes)
    {
        eloqstore::BranchManifestMetadata parsed;
        REQUIRE_FALSE(eloqstore::DeserializeBranchManifestMetadata(
            std::string_view(serialized.data(), 2), parsed));
        REQUIRE(parsed.branch_name.empty());
        REQUIRE(parsed.term == 0);
        REQUIRE(parsed.file_ranges.empty());
    }

    // Truncate after branch_name_len but before the full name+term
    // 4 (name_len) + 4 (branch_name "main") + 8 (term) = 16 bytes minimum
    // to pass the guard. Give only 10 so it fails the size check.
    {
        eloqstore::BranchManifestMetadata parsed;
        REQUIRE_FALSE(eloqstore::DeserializeBranchManifestMetadata(
            std::string_view(serialized.data(), 10), parsed));
        // Guard at line 707: data.size()(6) < name_len(4) + 8 = 12 → true
        // Returns metadata with branch_name already default-empty and term=0
        REQUIRE(parsed.branch_name.empty());
        REQUIRE(parsed.file_ranges.empty());
    }

    // Empty input
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

    // Strip manifest header
    const uint32_t payload_len = eloqstore::DecodeFixed32(
        manifest.data() + eloqstore::ManifestBuilder::offset_len);
    std::string_view payload =
        manifest.substr(eloqstore::ManifestBuilder::header_bytes, payload_len);

    // Skip max_fp_id
    uint64_t parsed_max_fp = 0;
    REQUIRE(eloqstore::GetVarint64(&payload, &parsed_max_fp));
    REQUIRE(parsed_max_fp == max_fp_id);

    // Skip dict
    uint32_t parsed_dict_len = 0;
    REQUIRE(eloqstore::GetVarint32(&payload, &parsed_dict_len));
    payload.remove_prefix(parsed_dict_len);

    // mapping_len + mapping table
    const uint32_t mapping_len = eloqstore::DecodeFixed32(payload.data());
    payload.remove_prefix(4);

    // Extract BranchManifestMetadata after mapping table
    std::string_view branch_meta_view = payload.substr(mapping_len);
    eloqstore::BranchManifestMetadata parsed_meta;
    REQUIRE(eloqstore::DeserializeBranchManifestMetadata(branch_meta_view,
                                                         parsed_meta));

    REQUIRE(parsed_meta.branch_name == "feature-xyz");
    REQUIRE(parsed_meta.term == 10);
    REQUIRE(parsed_meta.file_ranges.size() == 2);
    REQUIRE(parsed_meta.file_ranges[0].branch_name == "main");
    REQUIRE(parsed_meta.file_ranges[0].term == 1);
    REQUIRE(parsed_meta.file_ranges[0].max_file_id == 50);
    REQUIRE(parsed_meta.file_ranges[1].branch_name == "feature-xyz");
    REQUIRE(parsed_meta.file_ranges[1].term == 10);
    REQUIRE(parsed_meta.file_ranges[1].max_file_id == 200);

    mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}
