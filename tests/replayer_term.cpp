#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string_view>

#include "kv_options.h"
#include "replayer.h"
#include "root_meta.h"

using namespace eloqstore;

namespace
{
KvOptions MakeOpts(bool cloud_mode, uint8_t shift)
{
    KvOptions opts{};
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

TEST_CASE("Replayer allocator bumping occurs when manifest_term != expect_term in cloud mode",
          "[replayer][term]")
{
    KvOptions opts = MakeOpts(true /*cloud_mode*/, 4 /*pages_per_file_shift*/);

    // Build an empty snapshot with max_fp_id not aligned to a file boundary.
    ManifestBuilder builder;
    MappingSnapshot mapping(nullptr, nullptr, {});
    const FilePageId max_fp_id = 17;  // file_id=1, next boundary => 32 for shift=4
    FileIdTermMapping empty_mapping;
    std::string_view snapshot = builder.Snapshot(
        MaxPageId, MaxPageId, &mapping, max_fp_id, {}, empty_mapping);

    MemStoreMgr::Manifest file(snapshot);
    Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == KvError::NoError);

    // Set manifest_term in file_id_term_mapping_ to trigger bumping
    replayer.file_id_term_mapping_->insert_or_assign(
        IouringMgr::LruFD::kManifest, 1);

    // expect_term differs => bump to next file boundary
    auto mapper = replayer.GetMapper(nullptr, nullptr, 1);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);

    // expect_term differs => bump to next file boundary
    auto mapper2 = replayer.GetMapper(nullptr, nullptr, 2);
    REQUIRE(mapper2 != nullptr);
    REQUIRE(mapper2->FilePgAllocator()->MaxFilePageId() == 32);
}

TEST_CASE("Replayer allocator bumping does not occur when terms match", "[replayer][term]")
{
    KvOptions opts = MakeOpts(true /*cloud_mode*/, 4);
    ManifestBuilder builder;
    MappingSnapshot mapping(nullptr, nullptr, {});
    const FilePageId max_fp_id = 17;
    FileIdTermMapping empty_mapping;
    std::string_view snapshot = builder.Snapshot(
        MaxPageId, MaxPageId, &mapping, max_fp_id, {}, empty_mapping);

    MemStoreMgr::Manifest file(snapshot);
    Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == KvError::NoError);

    // Set manifest_term to match expect_term (no bumping)
    replayer.file_id_term_mapping_->insert_or_assign(
        IouringMgr::LruFD::kManifest, 7);

    auto mapper = replayer.GetMapper(nullptr, nullptr, 7);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}

TEST_CASE("Replayer allocator bumping does not occur when expect_term==0", "[replayer][term]")
{
    KvOptions opts = MakeOpts(true /*cloud_mode*/, 4);
    ManifestBuilder builder;
    MappingSnapshot mapping(nullptr, nullptr, {});
    const FilePageId max_fp_id = 17;
    FileIdTermMapping empty_mapping;
    std::string_view snapshot = builder.Snapshot(
        MaxPageId, MaxPageId, &mapping, max_fp_id, {}, empty_mapping);

    MemStoreMgr::Manifest file(snapshot);
    Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == KvError::NoError);

    auto mapper = replayer.GetMapper(nullptr, nullptr, 0);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}

TEST_CASE("Replayer allocator bumping does not occur in local mode", "[replayer][term]")
{
    KvOptions opts = MakeOpts(false /*cloud_mode*/, 4);
    ManifestBuilder builder;
    MappingSnapshot mapping(nullptr, nullptr, {});
    const FilePageId max_fp_id = 17;
    FileIdTermMapping empty_mapping;
    std::string_view snapshot = builder.Snapshot(
        MaxPageId, MaxPageId, &mapping, max_fp_id, {}, empty_mapping);

    MemStoreMgr::Manifest file(snapshot);
    Replayer replayer(&opts);
    REQUIRE(replayer.Replay(&file) == KvError::NoError);

    auto mapper = replayer.GetMapper(nullptr, nullptr, 2);
    REQUIRE(mapper != nullptr);
    REQUIRE(mapper->FilePgAllocator()->MaxFilePageId() == 17);
}


