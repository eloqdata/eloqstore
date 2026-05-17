// Fault injection tests for the large-value zero-copy path (Phase 9.6).
//
// Crash simulation strategy: instead of SIGTERM (which would kill the Catch2
// process), each test uses filesystem manipulation to reconstruct the on-disk
// state that a crash at a specific point would have left behind, then verifies
// that a subsequent restart is safe and correct.
//
// Four scenarios from the implementation plan:
//   1. Crash after WriteSegments but before manifest flush.
//   2. Crash before compaction's UpdateMeta (single-manifest guarantee).
//   3. Crash during the archive flush that publishes the archive snapshot.
//   4. Term mismatch on a segment file (simulated by renaming the file).

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../include/common.h"
#include "../include/eloq_store.h"
#include "../include/error.h"
#include "../include/global_registered_memory.h"
#include "../include/io_string_buffer.h"
#include "../include/kv_options.h"
#include "../include/types.h"
#include "common.h"

namespace fs = std::filesystem;

namespace
{
constexpr uint32_t kFiSegmentSize = 256 * 1024;
constexpr size_t kFiChunkSize = 64ULL * 1024 * 1024;
constexpr size_t kFiTotalSize = 256ULL * 1024 * 1024;

// ── Deterministic fill / verify
// ───────────────────────────────────────────────

void FillDeterministic(char *dst, size_t n, uint64_t seed)
{
    for (size_t i = 0; i < n; ++i)
        dst[i] = static_cast<char>((seed * 2654435761u + i) & 0xFF);
}

bool VerifyDeterministic(const char *src, size_t n, uint64_t seed)
{
    for (size_t i = 0; i < n; ++i)
    {
        char expect = static_cast<char>((seed * 2654435761u + i) & 0xFF);
        if (src[i] != expect)
            return false;
    }
    return true;
}

// ── Per-test harness (one shard, local mode)
// ──────────────────────────────────

class FiHarness
{
public:
    explicit FiHarness(uint32_t seg_size = kFiSegmentSize)
        : seg_size_(seg_size),
          mem_(std::make_unique<eloqstore::GlobalRegisteredMemory>(
              seg_size, kFiChunkSize, kFiTotalSize)),
          reg_base_(0)
    {
    }

    eloqstore::GlobalRegisteredMemory *Memory()
    {
        return mem_.get();
    }
    uint32_t SegSize() const
    {
        return seg_size_;
    }

    eloqstore::KvOptions MakeOpts() const
    {
        eloqstore::KvOptions opts = append_opts;
        opts.num_threads = 1;
        opts.segment_size = seg_size_;
        opts.segments_per_file_shift = 3;  // 8 segments per segment file
        opts.num_retained_archives = 1;
        opts.archive_interval_secs = 0;
        opts.buffer_pool_size = 16 * eloqstore::MB;
        opts.write_buffer_size = 0;
        opts.write_buffer_ratio = 0.0;
        opts.global_registered_memories = {mem_.get()};
        return opts;
    }

    void BindStore(eloqstore::EloqStore *store)
    {
        eloqstore::TableIdent warmup{"__fi_warmup__", 0};
        eloqstore::ReadRequest r;
        r.SetArgs(warmup, "__fi_key__");
        store->ExecSync(&r);
        reg_base_ = store->GlobalRegMemIndexBase(0);
    }

    eloqstore::IoStringBuffer MakeLargeValue(size_t size, uint64_t seed)
    {
        const size_t n = (size + seg_size_ - 1) / seg_size_;
        eloqstore::IoStringBuffer buf;
        size_t rem = size;
        for (size_t i = 0; i < n; ++i)
        {
            auto [ptr, chunk_idx] = mem_->GetSegment([]() {});
            size_t bytes = std::min<size_t>(seg_size_, rem);
            FillDeterministic(ptr, bytes, seed + i);
            if (bytes < seg_size_)
                std::memset(ptr + bytes, 0, seg_size_ - bytes);
            buf.Append({ptr, static_cast<uint16_t>(reg_base_ + chunk_idx)});
            rem -= bytes;
        }
        buf.SetSize(size);
        return buf;
    }

    bool VerifyLargeValue(const eloqstore::IoStringBuffer &buf,
                          size_t size,
                          uint64_t seed) const
    {
        if (buf.Size() != size)
            return false;
        const auto &frags = buf.Fragments();
        const size_t expected = (size + seg_size_ - 1) / seg_size_;
        if (frags.size() != expected)
            return false;
        size_t rem = size;
        for (size_t i = 0; i < frags.size(); ++i)
        {
            size_t bytes = std::min<size_t>(seg_size_, rem);
            if (!VerifyDeterministic(frags[i].data_, bytes, seed + i))
                return false;
            rem -= bytes;
        }
        return true;
    }

    void RecycleBatch(eloqstore::BatchWriteRequest &req)
    {
        for (auto &e : req.batch_)
            e.RecycleLargeValue(mem_.get(), reg_base_);
    }

    void RecycleRead(eloqstore::ReadRequest &req)
    {
        if (auto *iosb =
                std::get_if<eloqstore::IoStringBuffer>(&req.large_value_dest_);
            iosb != nullptr && !iosb->Fragments().empty())
            iosb->Recycle(mem_.get(), reg_base_);
    }

    void AssertPoolFull() const
    {
        REQUIRE(mem_->FreeSegments() == mem_->TotalSegments());
    }

private:
    uint32_t seg_size_;
    std::unique_ptr<eloqstore::GlobalRegisteredMemory> mem_;
    uint16_t reg_base_;
};

// ── Write helpers
// ─────────────────────────────────────────────────────────────

void WriteLarge(eloqstore::EloqStore *store,
                FiHarness &harness,
                const eloqstore::TableIdent &tbl,
                std::string key,
                size_t size,
                uint64_t seed,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(std::move(key),
                         harness.MakeLargeValue(size, seed),
                         ts,
                         eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    harness.RecycleBatch(req);
}

void DeleteEntry(eloqstore::EloqStore *store,
                 const eloqstore::TableIdent &tbl,
                 std::string key,
                 uint64_t ts = 99)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        std::move(key), std::string{}, ts, eloqstore::WriteOp::Delete);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void AssertLargeReadOk(eloqstore::EloqStore *store,
                       FiHarness &harness,
                       const eloqstore::TableIdent &tbl,
                       const std::string &key,
                       size_t size,
                       uint64_t seed)
{
    eloqstore::ReadRequest req;
    req.SetArgs(tbl, key);
    req.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
    auto &iosb = std::get<eloqstore::IoStringBuffer>(req.large_value_dest_);
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    REQUIRE(req.value_.empty());
    REQUIRE(iosb.Size() == size);
    REQUIRE(harness.VerifyLargeValue(iosb, size, seed));
    harness.RecycleRead(req);
}

void AssertNotFound(eloqstore::EloqStore *store,
                    const eloqstore::TableIdent &tbl,
                    const std::string &key)
{
    eloqstore::ReadRequest req;
    req.SetArgs(tbl, key);
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NotFound);
    REQUIRE(req.value_.empty());
}

// Run an archive for the given table and wait for completion.  Returns true on
// success.  Archives trigger TriggerFileGC at the end, so this is the easiest
// way to run local-mode GC in tests.
bool RunArchive(eloqstore::EloqStore *store, const eloqstore::TableIdent &tbl)
{
    eloqstore::ArchiveRequest ar;
    ar.SetTableId(tbl);
    store->ExecAsyn(&ar);
    ar.Wait();
    return ar.Error() == eloqstore::KvError::NoError;
}

// ── Filesystem helpers
// ────────────────────────────────────────────────────────

// Return the path to the partition directory for a given table.
fs::path PartitionDir(const eloqstore::KvOptions &opts,
                      const eloqstore::TableIdent &tbl)
{
    return fs::path(opts.store_path[0]) / tbl.ToString();
}

// Return the path of the manifest file for a partition.  In local mode
// ProcessTerm() == 0, so the manifest filename is the branch-aware
// "manifest_main_0".
fs::path ManifestPath(const eloqstore::KvOptions &opts,
                      const eloqstore::TableIdent &tbl)
{
    return PartitionDir(opts, tbl) /
           eloqstore::BranchManifestFileName(eloqstore::MainBranchName, 0);
}

// Truncate a file to exactly `new_size` bytes.  Requires the file was larger.
void TruncateFile(const fs::path &path, std::uintmax_t new_size)
{
    const std::uintmax_t old_size = fs::file_size(path);
    REQUIRE(old_size > new_size);
    std::error_code ec;
    fs::resize_file(path, new_size, ec);
    REQUIRE(!ec);
    REQUIRE(fs::file_size(path) == new_size);
}

// Return all files in a directory whose names start with the given prefix.
std::vector<fs::path> GlobFiles(const fs::path &dir, std::string_view prefix)
{
    std::vector<fs::path> out;
    if (!fs::exists(dir))
        return out;
    for (const auto &entry : fs::directory_iterator(dir))
    {
        if (!entry.is_regular_file())
            continue;
        std::string name = entry.path().filename().string();
        if (name.rfind(prefix, 0) == 0)
            out.push_back(entry.path());
    }
    return out;
}

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 1 — Crash after WriteSegments but before manifest flush
// ─────────────────────────────────────────────────────────────────────────────
//
// WriteSegments in IouringMgr::WriteSegments flushes segment data to stable
// storage before returning.  The manifest is updated in a subsequent
// AppendManifest call.  If the process crashes after WriteSegments but before
// AppendManifest, the segment files contain data that the manifest does not
// reference.  On restart the old manifest is replayed — the orphaned data is
// silently ignored, and the physical segment ID allocator advances past the
// orphaned positions without any special recovery step.
TEST_CASE("crash after WriteSegments leaves orphaned segments harmless",
          "[fault-injection][large-value]")
{
    FiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_crash1", 0};
    const size_t vsz = harness.SegSize() * 2 + 7;  // spans 3 segments
    constexpr uint64_t k1_seed = 0xA1;
    constexpr uint64_t k2_seed = 0xA2;
    constexpr uint64_t k3_seed = 0xA3;

    // Phase 1: write K1, archive so the manifest captures K1's segment mapping.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        WriteLarge(store.get(), harness, tbl, "k1", vsz, k1_seed, /*ts=*/1);
        REQUIRE(RunArchive(store.get(), tbl));

        // Record manifest size AFTER K1 is safely archived.
        const fs::path mpath = ManifestPath(opts, tbl);
        const std::uintmax_t manifest_size_after_k1 = fs::file_size(mpath);

        // Write K2: segment data is flushed to disk AND a log record is
        // appended to the manifest.  We will truncate the manifest back so
        // that K2's log record is gone, simulating a crash after the segment
        // write but before the manifest was durably updated.
        WriteLarge(store.get(), harness, tbl, "k2", vsz, k2_seed, /*ts=*/2);

        store->Stop();
        harness.AssertPoolFull();

        // Simulate crash: roll the manifest back to the post-K1 state.
        TruncateFile(mpath, manifest_size_after_k1);
    }

    // Phase 2: restart and verify crash-safety.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        // K1 was in the manifest before truncation — must still be readable.
        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);

        // K2's manifest entry was truncated away — must not be visible.
        AssertNotFound(store.get(), tbl, "k2");

        // Write K3: the segment allocator advances past K2's orphaned physical
        // segment IDs and the write must succeed without corruption.
        WriteLarge(store.get(), harness, tbl, "k3", vsz, k3_seed, /*ts=*/3);
        AssertLargeReadOk(store.get(), harness, tbl, "k3", vsz, k3_seed);

        // Re-read K1 to confirm it was not disturbed by the K3 write.
        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);

        store->Stop();
        harness.AssertPoolFull();
    }

    CleanupStore(opts);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 2 — Crash before compaction's UpdateMeta (single-manifest guarantee)
// ─────────────────────────────────────────────────────────────────────────────
//
// BackgroundWrite::Compact() runs DoCompactDataFile then DoCompactSegmentFile
// and then writes ONE manifest record via UpdateMeta.  A crash anywhere before
// that UpdateMeta leaves the store with new bytes written to disk but the
// manifest unchanged — "neither pass applied".  On restart the manifest points
// at the pre-crash segment mapping, the old segment files are still on disk
// (the GC floor from archive1 protects them), and all keys that were live in
// the pre-crash manifest remain readable.
//
// Setup: segments_per_file_shift = 1 → 2 segments per file.
//   K1 and K2 are written into file 0.  Archive1 is taken with just K1 and K2
//   live (SAF = 1.0, no compaction; GC floor set to 2, protecting file 0 and
//   any future file 1).
//   The crash-point manifest size is recorded immediately after archive1,
//   before any additional writes that could trigger background compaction.
//   K3 and K4 are then written (file 1) and deleted, causing the pending-
//   compact path to fire, background compacts to run, and archive2 to be
//   written.  All of this produces new manifest records above the checkpoint.
//
// Simulated crash: after stopping the store, the manifest is truncated back
// to the post-archive1 checkpoint.  The store is then restarted to verify
// the "neither pass applied" outcome: K1 and K2 are readable from file 0; K3
// and K4 were never committed to this manifest snapshot; a new write (K5)
// succeeds; the segment pool is leak-free.
TEST_CASE("crash before compaction UpdateMeta leaves store consistent",
          "[fault-injection][large-value]")
{
    FiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    // 2 segments per file → file 0 holds K1, K2; file 1 will hold K3, K4.
    opts.segments_per_file_shift = 1;
    // Compact when SAF > 1; deleting K3/K4 after archive1 will push SAF
    // above this threshold and trigger the background-compact path.
    opts.segment_file_amplify_factor = 1;
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_compact", 0};
    // Each value fills exactly one segment.
    const size_t vsz = harness.SegSize();
    constexpr uint64_t k1_seed = 0xD1;
    constexpr uint64_t k2_seed = 0xD2;
    constexpr uint64_t k3_seed = 0xD3;
    constexpr uint64_t k4_seed = 0xD4;
    constexpr uint64_t k5_seed = 0xD5;

    // Phase 1: write K1 and K2, archive, record the crash-point manifest
    // size, then create fragmentation (K3/K4 write + delete) and archive2
    // to produce manifest records above the checkpoint.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        // K1 → seg 0 (file 0, slot 0), K2 → seg 1 (file 0, slot 1).
        WriteLarge(store.get(), harness, tbl, "k1", vsz, k1_seed, /*ts=*/1);
        WriteLarge(store.get(), harness, tbl, "k2", vsz, k2_seed, /*ts=*/2);

        // Archive1: SAF = 2/2 = 1.0, NOT > 1 → no compaction.
        // Sets the GC floor at segment_file_id = 1 (max_fp_id=2, >>1 = 1,
        // so floor = 2).  File 0 (ID 0 < 2) and file 1 (ID 1 < 2) are both
        // protected by this floor for the rest of the test.
        REQUIRE(RunArchive(store.get(), tbl));

        // Record the manifest size right here — after archive1 and before
        // any writes that could trigger CompactIfNeeded.  This is the
        // "crash point": truncating the manifest here later puts the store in
        // the state it would have had if the process crashed before any
        // compaction's UpdateMeta wrote its record.
        const fs::path mpath = ManifestPath(opts, tbl);
        const std::uintmax_t manifest_size_checkpoint = fs::file_size(mpath);

        // Write K3 and K4 into file 1, then delete them.  This pushes the
        // overall SAF above 1.0 and triggers the pending-compact path.
        // Background compact(s) run, moving live segments around and writing
        // manifest records above the checkpoint.  The exact compaction
        // sequence is not important; what matters is that at least one
        // manifest record is appended above manifest_size_checkpoint.
        WriteLarge(store.get(), harness, tbl, "k3", vsz, k3_seed, /*ts=*/3);
        WriteLarge(store.get(), harness, tbl, "k4", vsz, k4_seed, /*ts=*/4);
        DeleteEntry(store.get(), tbl, "k3", /*ts=*/50);
        DeleteEntry(store.get(), tbl, "k4", /*ts=*/51);

        // Archive2: may trigger another round of compaction internally; the
        // archive file itself captures the fully-compacted state.
        REQUIRE(RunArchive(store.get(), tbl));

        store->Stop();
        harness.AssertPoolFull();

        // The manifest must have grown beyond the checkpoint (compaction and
        // deletion log records were appended above it).
        REQUIRE(fs::file_size(mpath) > manifest_size_checkpoint);

        // File 0 must still be on disk (archive1 GC floor protects it).
        const fs::path pdir = PartitionDir(opts, tbl);
        REQUIRE(fs::exists(pdir /
                           eloqstore::SegmentFileName(/*file_id=*/0,
                                                      eloqstore::MainBranchName,
                                                      /*term=*/0)));

        // Simulate crash: roll the manifest back to the post-archive1
        // checkpoint.  All compaction and deletion records are erased; the
        // manifest now shows K1 and K2 live at their original physical
        // segments in file 0.  K3 and K4 were never committed in this view.
        TruncateFile(mpath, manifest_size_checkpoint);
    }

    // Phase 2: restart and verify the "neither pass applied" outcome.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        // K1 and K2 are live in the pre-crash manifest; file 0 is still on
        // disk → reads must succeed.
        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        // K3 and K4 were written and deleted after the checkpoint → they
        // must not appear in the manifest view the store was restored to.
        AssertNotFound(store.get(), tbl, "k3");
        AssertNotFound(store.get(), tbl, "k4");

        // Write K5: the segment allocator is restored from the checkpoint
        // (tail = 2, immediately after K1 and K2's allocations).  K5 lands
        // at physical segment 2 (file 1, slot 0), overwriting any orphaned
        // data that earlier compaction wrote there.  The write must succeed
        // and K5 must read back correctly.
        WriteLarge(store.get(), harness, tbl, "k5", vsz, k5_seed, /*ts=*/5);
        AssertLargeReadOk(store.get(), harness, tbl, "k5", vsz, k5_seed);

        // K1 and K2 map to segments in file 0 and are unaffected by K5.
        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        store->Stop();
        harness.AssertPoolFull();
    }

    CleanupStore(opts);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 3 — Crash during archive flush (archive file not committed)
// ─────────────────────────────────────────────────────────────────────────────
//
// CreateArchive uses WriteSnapshot which writes to a .tmp file and then
// atomically renames it.  If the crash happens before the rename, the archive
// file is absent on restart.  The archive floor is reconstructed from the most
// recent surviving archive, which is the PREVIOUS archive.  Both K1 and K2
// must still be readable from the manifest (manifest updates are independent of
// the archive write).
TEST_CASE("crash during archive write: store remains correct on restart",
          "[fault-injection][large-value]")
{
    FiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_crash3", 0};
    const size_t vsz = harness.SegSize() * 2;
    constexpr uint64_t k1_seed = 0xB1;
    constexpr uint64_t k2_seed = 0xB2;

    // Phase 1: write K1 and archive, then write K2 and archive again.
    // This creates two archive files.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        WriteLarge(store.get(), harness, tbl, "k1", vsz, k1_seed, /*ts=*/1);
        REQUIRE(RunArchive(store.get(), tbl));

        WriteLarge(store.get(), harness, tbl, "k2", vsz, k2_seed, /*ts=*/2);
        REQUIRE(RunArchive(store.get(), tbl));

        store->Stop();
        harness.AssertPoolFull();
    }

    // Phase 2: simulate a crash during the second archive write by deleting
    // the most recently created archive file.  Archive files are named
    // manifest_main_0_{timestamp}; the latest timestamp is the newest archive.
    const fs::path pdir = PartitionDir(opts, tbl);
    {
        // Archive files have names like "manifest_main_0_<timestamp>".
        std::string prefix =
            eloqstore::BranchManifestFileName(eloqstore::MainBranchName, 0) +
            eloqstore::FileNameSeparator;
        std::vector<fs::path> archives = GlobFiles(pdir, prefix);
        REQUIRE(archives.size() == 2);
        std::sort(archives.begin(), archives.end());
        // Delete the newer archive (last after lexicographic sort).
        std::error_code ec;
        fs::remove(archives.back(), ec);
        REQUIRE(!ec);
    }

    // Phase 3: restart — store must come up successfully.  Both K1 and K2
    // are readable because the manifest (updated per write) is intact and
    // independent of the archive files.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        // A new archive can be created after the partial-write incident.
        REQUIRE(RunArchive(store.get(), tbl));
        AssertLargeReadOk(store.get(), harness, tbl, "k1", vsz, k1_seed);
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        store->Stop();
        harness.AssertPoolFull();
    }

    CleanupStore(opts);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 4 — Term mismatch on a segment file
// ─────────────────────────────────────────────────────────────────────────────
//
// A segment file whose on-disk name carries an unexpected term is "never
// mapped": the io_uring manager looks for the file under the expected name
// (derived from the term resolved via BranchFileMapping for the segment's
// file_id) and fails to open the misnamed file.  Any read that resolves to that
// file returns an error.  The store must not crash, and subsequent writes to
// other keys must succeed.
//
// Note on GC in local mode: the GC deletion floor is based on the archive's
// MaxFilePageId high-watermark, not on whether the on-disk file has the correct
// term.  In local mode (ProcessTerm==0) a file named segment_0_99 has file_id=0
// which is under the archive floor (floor >= 1 once file 0 was allocated), so
// the file is not deleted by GC — the store simply can't open it.  In cloud
// mode the ProcessTerm check in DeleteUnreferencedCloudSegmentFiles provides
// the additional term-based cleanup guarantee (files with term < ProcessTerm
// are treated as stale orphans).
TEST_CASE("segment file with wrong term blocks reads but does not crash store",
          "[fault-injection][large-value]")
{
    FiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_term", 0};
    // 3 segments fit entirely within file 0 (8 segments per file, shift=3).
    const size_t vsz = harness.SegSize() * 3;
    constexpr uint64_t k1_seed = 0xC1;
    constexpr uint64_t k2_seed = 0xC2;

    // Phase 1: write K1 and archive so the manifest is fully flushed.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        WriteLarge(store.get(), harness, tbl, "k1", vsz, k1_seed, /*ts=*/1);
        REQUIRE(RunArchive(store.get(), tbl));

        store->Stop();
        harness.AssertPoolFull();
    }

    const fs::path pdir = PartitionDir(opts, tbl);

    // Phase 2: rename segment_0_0 → segment_0_99 to simulate a term mismatch.
    // In local mode ProcessTerm() == 0, so the correct on-disk name is
    // segment_0_0.  Renaming it prevents the io manager from opening file 0
    // with term=0 on the read path.
    const std::string correct_name = eloqstore::SegmentFileName(
        /*file_id=*/0, eloqstore::MainBranchName, /*term=*/0);
    const std::string wrong_name = eloqstore::SegmentFileName(
        /*file_id=*/0, eloqstore::MainBranchName, /*term=*/99);
    {
        std::error_code ec;
        REQUIRE(fs::exists(pdir / correct_name));
        fs::rename(pdir / correct_name, pdir / wrong_name, ec);
        REQUIRE(!ec);
    }

    // Phase 3: restart and verify behavior.
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        // Reading K1 must fail: the segment file that carries its data
        // (segment_0_0) is missing; the misnamed segment_0_99 is never opened.
        // ENOENT maps to KvError::NotFound, matching the file-open failure.
        // Attach an IoStringBuffer so the read actually attempts to open
        // the segment file (otherwise the dispatch would short-circuit to
        // a metadata-only read that never touches segment files).
        eloqstore::ReadRequest r1;
        r1.SetArgs(tbl, "k1");
        r1.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        store->ExecSync(&r1);
        REQUIRE(r1.Error() != eloqstore::KvError::NoError);

        // The store must remain operational: a write to a different key that
        // allocates fresh segments (bypassing the missing file 0) must succeed.
        // K2 lands in the next available slot in file 0 (allocator restored
        // from manifest, advances past K1's orphaned positions).
        WriteLarge(store.get(), harness, tbl, "k2", vsz, k2_seed, /*ts=*/2);
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        // Delete K1 (frees its logical segment IDs without reading the
        // missing file).  This is safe: DelLargeValue reads the data-page
        // value encoding but does not access the segment files themselves.
        DeleteEntry(store.get(), tbl, "k1", /*ts=*/50);

        // K1 is now gone from the B+-tree.
        AssertNotFound(store.get(), tbl, "k1");

        // Run archive: confirms the store can still create a consistent
        // archive snapshot after the deletion, and triggers GC.
        REQUIRE(RunArchive(store.get(), tbl));

        // K2 must still be readable after the archive+GC cycle.
        AssertLargeReadOk(store.get(), harness, tbl, "k2", vsz, k2_seed);

        // In local mode the archive floor (derived from MaxFilePageId
        // high-watermark, which covers file 0) protects segment_0_99 from
        // GC deletion.  The file therefore still exists on disk.  This is
        // correct: older archive snapshots may still reference segments in
        // file 0.  In cloud mode the ProcessTerm check provides the
        // additional cleanup guarantee.
        REQUIRE(fs::exists(pdir / wrong_name));

        store->Stop();
        harness.AssertPoolFull();
    }

    CleanupStore(opts);
}

// ===========================================================================
// Pinned-mode fault-injection scenarios (Phases 4 / 7)
// ===========================================================================
namespace
{
// Harness for KV Cache pinned-memory mode. Owns one 4 KiB-aligned backing
// buffer; sub-ranges are handed out by `AllocateSegmentAligned` or, for the
// scratch-fallback path, `AllocateChunkEnd` (places the value flush against
// the chunk end so the rounded-up tail spills past the registered range).
class PinnedFiHarness
{
public:
    // 32 MiB is comfortably larger than any single fault-injection test's
    // working set; pinned chunks are registered as fixed io_uring buffers,
    // so locked memory stays well under the 2GB RLIMIT_MEMLOCK.
    static constexpr size_t kPinnedSize = 32ULL * 1024 * 1024;

    explicit PinnedFiHarness(uint32_t seg_size = kFiSegmentSize)
        : seg_size_(seg_size)
    {
        void *raw = nullptr;
        REQUIRE(posix_memalign(&raw, 4096, kPinnedSize) == 0);
        REQUIRE(raw != nullptr);
        std::memset(raw, 0, kPinnedSize);
        base_ = static_cast<char *>(raw);
    }

    ~PinnedFiHarness()
    {
        std::free(base_);
    }

    uint32_t SegSize() const
    {
        return seg_size_;
    }

    eloqstore::KvOptions MakeOpts() const
    {
        eloqstore::KvOptions opts = append_opts;
        opts.num_threads = 1;
        opts.segment_size = seg_size_;
        opts.segments_per_file_shift = 3;
        opts.num_retained_archives = 1;
        opts.archive_interval_secs = 0;
        opts.buffer_pool_size = 16 * eloqstore::MB;
        opts.write_buffer_size = 0;
        opts.write_buffer_ratio = 0.0;
        opts.pinned_memory_chunks = {{base_, kPinnedSize}};
        opts.gc_global_mem_size_per_shard = 32ULL * eloqstore::MB;
        opts.pinned_tail_scratch_slots = eloqstore::max_segments_batch;
        return opts;
    }

    // Allocate `size` bytes from the cursor; ceil to a segment boundary so
    // back-to-back allocations stay segment-aligned. Used for fast-path
    // writes (the rounded-up tail stays inside the chunk).
    std::pair<char *, size_t> AllocateSegmentAligned(size_t size)
    {
        const size_t k = (size + seg_size_ - 1) / seg_size_;
        const size_t aligned = k * seg_size_;
        REQUIRE(cursor_ + aligned <= kPinnedSize);
        char *p = base_ + cursor_;
        cursor_ += aligned;
        return {p, size};
    }

    // Place `size` bytes flush against the chunk end so that the rounded-up
    // [ptr, ptr + ceil(size/seg)*seg) extends past the chunk -- forces the
    // scratch fallback. Cursor is not advanced (this is a terminal alloc).
    std::pair<char *, size_t> AllocateChunkEnd(size_t size)
    {
        REQUIRE(size > 0);
        REQUIRE(size <= kPinnedSize);
        const size_t k = (size + seg_size_ - 1) / seg_size_;
        REQUIRE(k * seg_size_ > size);  // must round up to trigger fallback
        char *p = base_ + (kPinnedSize - size);
        return {p, size};
    }

    void ResetCursor()
    {
        cursor_ = 0;
    }

    char *Base() const
    {
        return base_;
    }

private:
    uint32_t seg_size_;
    char *base_{nullptr};
    size_t cursor_{0};
};

// One-shot pinned write of a single (key, value+metadata) pair. Fills the
// destination with deterministic bytes seeded by `seed` and submits a
// BatchWriteRequest.
void WritePinned(eloqstore::EloqStore *store,
                 const eloqstore::TableIdent &tbl,
                 const std::string &key,
                 std::pair<char *, size_t> dst,
                 uint64_t seed,
                 std::string metadata,
                 uint64_t ts)
{
    FillDeterministic(dst.first, dst.second, seed);
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        key,
        std::move(metadata),
        std::make_pair(static_cast<const char *>(dst.first), dst.second),
        ts,
        eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

// Read via overload B (metadata + pinned bytes) and verify both halves.
void AssertPinnedReadOk(eloqstore::EloqStore *store,
                        PinnedFiHarness &harness,
                        const eloqstore::TableIdent &tbl,
                        const std::string &key,
                        size_t size,
                        uint64_t seed,
                        const std::string &expected_metadata)
{
    auto rb = harness.AllocateSegmentAligned(size);
    std::memset(rb.first, 0, rb.second);
    eloqstore::ReadRequest req;
    req.SetArgs(tbl, key);
    req.large_value_dest_ = std::make_pair(rb.first, rb.second);
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    REQUIRE(req.value_ == expected_metadata);
    REQUIRE(VerifyDeterministic(rb.first, size, seed));
}

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 5 — Crash after pinned WriteSegments but before manifest flush
// ─────────────────────────────────────────────────────────────────────────────
//
// Same strategy as Scenario 1, but the writes route through the pinned
// (KV Cache) arm of WriteLargeValue with metadata blobs attached. The
// crash-recovery contract is identical: orphaned segment bytes on disk
// have no manifest reference, so on restart they are silently ignored and
// the segment allocator advances past them when serving fresh writes. The
// metadata trailer encoded into the data page is also nowhere referenced
// (the data page record was never flushed), so it cannot smuggle the
// orphan segments back into the live mapping.
TEST_CASE(
    "pinned-mode crash after WriteSegments leaves orphan segments harmless",
    "[fault-injection][large-value][pinned]")
{
    PinnedFiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_pinned_crash", 0};
    // Spans 3 segments; 4 KiB aligned so the pinned read contract
    // `dst_size % 4096 == 0` (Phase 6) is satisfied. Mid-tail value forces
    // a partial last-segment read on the way back.
    const size_t vsz = harness.SegSize() * 2 + 4096;
    const std::string meta_k1 = "meta-K1-blob";
    const std::string meta_k2 = "meta-K2-different";
    const std::string meta_k3 = "meta-K3-after-crash";
    constexpr uint64_t k1_seed = 0xE100;
    constexpr uint64_t k2_seed = 0xE200;
    constexpr uint64_t k3_seed = 0xE300;

    // Phase 1: write K1 + archive (manifest captures K1), then write K2 to
    // exercise the pinned-arm WriteSegments path. The cleanly-stopped
    // manifest is rolled back to the post-K1 state -- simulating a crash
    // after the K2 segments were flushed to disk but before the manifest
    // record was made durable.
    std::uintmax_t manifest_size_after_k1 = 0;
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);

        auto k1_buf = harness.AllocateSegmentAligned(vsz);
        WritePinned(store.get(), tbl, "k1", k1_buf, k1_seed, meta_k1, /*ts=*/1);
        REQUIRE(RunArchive(store.get(), tbl));

        manifest_size_after_k1 = fs::file_size(ManifestPath(opts, tbl));

        auto k2_buf = harness.AllocateSegmentAligned(vsz);
        WritePinned(store.get(), tbl, "k2", k2_buf, k2_seed, meta_k2, /*ts=*/2);

        store->Stop();
        TruncateFile(ManifestPath(opts, tbl), manifest_size_after_k1);
    }

    // Phase 2: restart and check the recovery invariants.
    {
        harness.ResetCursor();
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);

        // K1 was archived before the simulated crash -- bytes + metadata
        // both round-trip cleanly.
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k1", vsz, k1_seed, meta_k1);

        // K2's manifest record was truncated away -- it must not be visible
        // even though its segment bytes are still on disk as orphans.
        AssertNotFound(store.get(), tbl, "k2");

        // Fresh write: the segment allocator advances past K2's orphan
        // positions; bytes + metadata round-trip on restart.
        auto k3_buf = harness.AllocateSegmentAligned(vsz);
        WritePinned(store.get(), tbl, "k3", k3_buf, k3_seed, meta_k3, /*ts=*/3);
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k3", vsz, k3_seed, meta_k3);

        // K1 was not disturbed by the K3 write.
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k1", vsz, k1_seed, meta_k1);

        store->Stop();
    }

    CleanupStore(opts);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 6 — Crash during a pinned write that used the tail-scratch fallback
// ─────────────────────────────────────────────────────────────────────────────
//
// Phase 7 introduced a per-shard scratch pool for the cross-boundary pinned
// write case (the rounded-up tail of [ptr, ptr + size) lies past the
// caller's pinned chunk). Two crash-recovery claims need verifying:
//
//   (a) The scratch slot is process-local state; restart starts with a
//       freshly allocated pool, so a subsequent scratch-fallback write on
//       the recovered store must succeed.
//   (b) The on-disk artifact of a scratch-fallback write -- a segment of
//       `segment_size` bytes whose tail is the scratch buffer's zero-padded
//       region -- has the same "orphan if not manifest-referenced"
//       property as Scenario 5. The manifest truncation must leave the
//       store consistent.
//
// Setup mirrors Scenario 5 but K2 is written via `AllocateChunkEnd` so the
// scratch fallback fires; a positive `TailScratchAcquireCount` assertion
// confirms the path was actually exercised.
TEST_CASE("pinned-mode crash during scratch-fallback write recovers cleanly",
          "[fault-injection][large-value][pinned][tail-scratch]")
{
    PinnedFiHarness harness;
    eloqstore::KvOptions opts = harness.MakeOpts();
    CleanupStore(opts);

    const eloqstore::TableIdent tbl{"fi_scratch_crash", 0};
    // Mid-segment size: ceil(size / seg) = 2 but size != 2*seg, so the
    // scratch fallback is required when the value is flush against the
    // chunk end.
    const size_t vsz = harness.SegSize() + 4096;
    const std::string meta_k1 = "k1-aligned";
    const std::string meta_k2 = "k2-scratch-fallback";
    const std::string meta_k3 = "k3-after-restart-scratch";
    constexpr uint64_t k1_seed = 0xF100;
    constexpr uint64_t k2_seed = 0xF200;
    constexpr uint64_t k3_seed = 0xF300;

    // Phase 1: K1 (fast path) + archive + K2 (scratch fallback).
    std::uintmax_t manifest_size_after_k1 = 0;
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);

        // Fast-path write: scratch must not be touched yet.
        auto k1_buf = harness.AllocateSegmentAligned(vsz);
        WritePinned(store.get(), tbl, "k1", k1_buf, k1_seed, meta_k1, /*ts=*/1);
        REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) == 0);
        REQUIRE(RunArchive(store.get(), tbl));

        manifest_size_after_k1 = fs::file_size(ManifestPath(opts, tbl));

        // Scratch-fallback write: place K2 flush against the chunk end so
        // ceil(size/seg) * seg extends past the registered range.
        auto k2_buf = harness.AllocateChunkEnd(vsz);
        WritePinned(store.get(), tbl, "k2", k2_buf, k2_seed, meta_k2, /*ts=*/2);
        REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) >= 1);

        store->Stop();
        TruncateFile(ManifestPath(opts, tbl), manifest_size_after_k1);
    }

    // Phase 2: restart and verify (a) + (b).
    {
        harness.ResetCursor();
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);

        // Fresh process: scratch pool starts at zero acquires.
        REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) == 0);

        // K1's manifest record survived the truncation; bytes + metadata
        // round-trip.
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k1", vsz, k1_seed, meta_k1);

        // K2 was orphaned by the manifest truncation -- the scratch-
        // fallback bytes on disk are no different from any other orphan
        // segment, so the key is not visible.
        AssertNotFound(store.get(), tbl, "k2");

        // (a) Fresh scratch-fallback write must succeed -- the pool is
        // process-local and was reinitialized from scratch on Start().
        auto k3_buf = harness.AllocateChunkEnd(vsz);
        WritePinned(store.get(), tbl, "k3", k3_buf, k3_seed, meta_k3, /*ts=*/3);
        REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) >= 1);

        // K3 reads back; metadata preserved through the scratch path.
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k3", vsz, k3_seed, meta_k3);

        // K1 still readable after the post-restart write.
        AssertPinnedReadOk(
            store.get(), harness, tbl, "k1", vsz, k1_seed, meta_k1);

        store->Stop();
    }

    CleanupStore(opts);
}
