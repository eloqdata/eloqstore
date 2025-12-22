#include <catch2/catch_test_macros.hpp>
#include <string>
#include <utility>
#include <vector>

#include "../include/async_io_manager.h"
#include "../include/coding.h"
#include "../include/kv_options.h"
#include "../include/storage/index_page_manager.h"
#include "../include/storage/page_mapper.h"
#include "../include/storage/root_meta.h"

uint64_t MockEncodeFilePageId(eloqstore::FilePageId file_page_id)
{
    return (file_page_id << eloqstore::MappingSnapshot::TypeBits) |
           static_cast<uint64_t>(
               eloqstore::MappingSnapshot::ValType::FilePageId);
}

TEST_CASE(
    "ManifestBuilder snapshot serializes FileIdTermMapping before mapping "
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

    // Prepare FileIdTermMapping with a few entries.
    eloqstore::FileIdTermMapping file_id_term;
    file_id_term[1] = 10;
    file_id_term[5] = 20;

    // Dict bytes and max_fp_id to embed into snapshot payload.
    const std::string dict_bytes = "DICT_BYTES";
    const eloqstore::FilePageId max_fp_id = 123456;

    eloqstore::ManifestBuilder builder;
    std::string_view manifest = builder.Snapshot(/*root_id=*/1,
                                                 /*ttl_root=*/2,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 file_id_term);
    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);

    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][FileIdTermMapping][mapping_tbl_...]
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

    // 3) FileIdTermMapping section (count + pairs), then mapping table.
    eloqstore::FileIdTermMapping parsed_mapping;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(payload, parsed_mapping));
    REQUIRE(parsed_mapping.size() == file_id_term.size());
    for (const auto &[fid, term] : file_id_term)
    {
        REQUIRE(parsed_mapping.at(fid) == term);
    }

    // 4) Remaining payload should be serialized mapping_tbl_.
    std::vector<uint64_t> parsed_tbl;
    while (!payload.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&payload, &val));
        parsed_tbl.push_back(val);
    }
    REQUIRE(parsed_tbl == mapping_snapshot.mapping_tbl_.Base());

    mapping_snapshot.mapping_tbl_.clear();
}

TEST_CASE(
    "ManifestBuilder snapshot writes empty FileIdTermMapping section when "
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
    // Pass empty FileIdTermMapping: should still write a count=0.
    eloqstore::FileIdTermMapping empty_mapping;
    std::string_view manifest = builder.Snapshot(/*root_id=*/3,
                                                 /*ttl_root=*/4,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 empty_mapping);

    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);
    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][FileIdTermMapping][mapping_tbl_...]
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

    // 3) FileIdTermMapping section should be present and decode to empty map.
    eloqstore::FileIdTermMapping parsed_mapping;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(payload, parsed_mapping));
    REQUIRE(parsed_mapping.empty());

    // 4) Remaining payload is mapping table.
    std::vector<uint64_t> parsed_tbl;
    while (!payload.empty())
    {
        uint64_t val = 0;
        REQUIRE(eloqstore::GetVarint64(&payload, &val));
        parsed_tbl.push_back(val);
    }
    REQUIRE(parsed_tbl == mapping_snapshot.mapping_tbl_.Base());

    builder.Reset();
}
