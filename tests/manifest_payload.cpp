#include <catch2/catch_test_macros.hpp>
#include <string>
#include <string_view>
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
    "ManifestBuilder snapshot serializes FileIdTermMapping after mapping "
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

    std::string file_term_mapping_str;
    eloqstore::SerializeFileIdTermMapping(file_id_term, file_term_mapping_str);
    eloqstore::ManifestBuilder builder;
    std::string_view manifest = builder.Snapshot(/*root_id=*/1,
                                                 /*ttl_root=*/2,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 file_term_mapping_str);
    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);

    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][mapping_len(4B)][mapping_tbl_...]
    // [file_term_mapping_len(4B)][file_term_mapping...]
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
    REQUIRE(parsed_tbl == mapping_snapshot.mapping_tbl_.Base());

    // 5) file_term_mapping
    std::string_view file_term_mapping_view = payload.substr(mapping_len);
    eloqstore::FileIdTermMapping parsed_mapping;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(file_term_mapping_view,
                                                    parsed_mapping));
    REQUIRE(parsed_mapping.size() == file_id_term.size());
    for (const auto &[fid, term] : file_id_term)
    {
        REQUIRE(parsed_mapping.at(fid) == term);
    }

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
    std::string file_term_mapping_str;
    eloqstore::SerializeFileIdTermMapping(empty_mapping, file_term_mapping_str);
    std::string_view manifest = builder.Snapshot(/*root_id=*/3,
                                                 /*ttl_root=*/4,
                                                 &mapping_snapshot,
                                                 max_fp_id,
                                                 dict_bytes,
                                                 file_term_mapping_str);

    REQUIRE(manifest.size() > eloqstore::ManifestBuilder::header_bytes);
    // Strip manifest header; inspect the payload layout:
    // [checksum][root_id][ttl_root][payload_len]
    // [max_fp_id][dict_len][dict_bytes][mapping_len(4B)][mapping_tbl_...]
    // [file_term_mapping_len(4B)][file_term_mapping...]
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

    // 5) file_term_mapping
    std::string_view file_term_mapping_view = payload.substr(mapping_len);
    eloqstore::FileIdTermMapping parsed_mapping;
    REQUIRE(eloqstore::DeserializeFileIdTermMapping(file_term_mapping_view,
                                                    parsed_mapping));
    REQUIRE(parsed_mapping.empty());

    mapping_snapshot.mapping_tbl_.clear();
    builder.Reset();
}
