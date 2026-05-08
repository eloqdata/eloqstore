# Implementation Plan: Zero-Copy Read and Write Paths for Very Large Values

Companion to [zero_copy_read.md](zero_copy_read.md). This document tracks the
phased implementation of the design. All phases (1–9) are complete.

## Phase 1 — Global Registered Memory (done)

Introduce `GlobalRegisteredMemory` as a per-core segment allocator shared with
the networking layer. Segments are 4KB aligned. The free list is a lock-free
stack using a Harris mark-then-CAS algorithm. Successor pointers are stored in
a **separate `successors_` array** (`std::unique_ptr<std::atomic<uintptr_t>[]>`,
one slot per segment), not inside the segment data. This is essential: once a
segment is returned to a caller, the caller may write arbitrary I/O data into
it, which would corrupt an intrusive list and enable ABA-induced crashes. The
encoding used by both `head_` and `successors_` entries:

```
bits 63..16  — successor pointer shifted left 16 (x86-64 user-space addresses
               fit in 48 bits; the upper 16 bits are always zero)
bits 15..1   — chunk_idx of the successor segment
bit  0       — Harris mark bit (1 = this node is being popped)
```

`head_` uses the same layout with the mark bit always 0. Before swinging
`head_`, the popping thread marks bit 0 of the head segment's `successors_` slot
via a CAS, claiming exclusive ownership. `Recycle()` is a standard
Treiber-stack push. `GetSegment()` is templated on a yield function and
invokes `evict_func_()` when the free list is empty.

- Files: [include/global_registered_memory.h](../include/global_registered_memory.h),
  [src/global_registered_memory.cpp](../src/global_registered_memory.cpp).
- `IouringMgr::BootstrapRing()` takes an optional
  `GlobalRegisteredMemory *` and appends its chunks to the iovec array before
  `io_uring_register_buffers()`. The base iovec index is recorded in
  `global_reg_mem_index_base_` so the IO manager can translate
  `(segment, chunk_index)` into an `io_uring` fixed-buffer index.

## Phase 2 — IoBufferRef and IoStringBuffer (done)

`IoBufferRef` carries a `(char *data_, uint16_t buf_index_)` pair identifying a
segment in the global registered memory area. `IoStringBuffer` is a linked
collection of `IoBufferRef` fragments plus a size field, representing a very
large value end-to-end across networking → cache → EloqStore.

- Files: [include/io_buffer_ref.h](../include/io_buffer_ref.h),
  [include/io_string_buffer.h](../include/io_string_buffer.h),
  [src/io_string_buffer.cpp](../src/io_string_buffer.cpp).
- `IoStringBuffer::Recycle(global_mem, reg_mem_index_base)` returns all
  fragments to `GlobalRegisteredMemory`. EloqStore tasks recycle on failure and
  on completion handoff back to the caller, matching the "EloqStore is
  stateless" rule from the design doc.

## Phase 3 — Data Page Large-Value Encoding (done)

Extend the data page value-length field's lowest 4 bits so the compression
bits have a 4th state meaning "very large string". When set, the value content
is `[ uint32_t actual_length | K × uint32_t logical_segment_id ]`, bounded so
the whole value record fits within a 4KB data page.

- `DataPageIter::IsLargeValue()` exposes the bit to readers/writers.
- Helpers `EncodeLargeValueContent` / `DecodeLargeValueContent` sit alongside
  the overflow-value helpers.

## Phase 4 — Segment Files and Append Allocator (done)

New file class keyed by `SegmentFileKey(FileId)` / file name
`segment_<id>_<term>`. Segment files are append-only; IDs roll forward and
never reuse. The file-page allocator is reused with segment-specific sizes
taken from `KvOptions`.

- `PartitionFiles::fds_` is keyed by `TypedFileId`, encoding file type in the
  LSB. `DataFileKey` / `SegmentFileKey` helpers construct the key; sentinel FD
  slots (directory / manifest) use reserved high values.
- Prewarm (`src/tasks/prewarm_task.cpp`) classifies segment files via
  `FileNameSegment` and `ParseSegmentFileSuffix`.

## Phase 5 — Segment Mapping (done)

`RootMeta::segment_mapper_` (a `PageMapper`) maps logical segment IDs to
physical file segment IDs. Created lazily in `MakeCowRoot()` — only
materialized when the first very large value is written for a partition.
Snapshots and reference-counted destruction follow the existing page-mapper
pattern.

- `meta->segment_mapping_snapshots_` tracks live snapshots for recycling.
- Segment mapping is serialized after the data mapping and
  `FileIdTermMapping` in each manifest record:
  `[ data_mapping_bytes_len | data_mapping_bytes | file_id_term_bytes_len |
   file_id_term_bytes | segment_mapping_bytes_len | segment_mapping_bytes ]`.
  Backward-compatible: no trailing bytes ⇒ empty segment mapping.

## Phase 6 — Read and BatchWrite Paths (done)

Read path ([src/tasks/task.cpp:170-227](../src/tasks/task.cpp#L170-L227)):
`GetLargeValue` decodes the value content, resolves each logical segment ID
through the segment mapping snapshot, calls `GlobalRegisteredMemory::GetSegment`
K times, builds an `IoStringBuffer`, and issues `ReadSegments` batches via
fixed buffers. On error the whole `IoStringBuffer` is recycled.

Write path ([src/tasks/batch_write_task.cpp](../src/tasks/batch_write_task.cpp)):
`WriteLargeValue` allocates K logical segment IDs from the COW segment
mapper, allocates K physical file segment IDs from the segment allocator,
calls `WriteSegments` to flush the `IoStringBuffer` fragments, updates the
mapping, and encodes the K logical IDs into the data page value content.
`DelLargeValue` frees logical IDs when a large value is overwritten or
deleted. `BatchWriteTask` accepts values via `std::variant<std::string,
IoStringBuffer>`.

Replayer restores the segment mapping on load (`Replayer::DeserializeSnapshot`
/ `ReplayLog`) and `IndexPageManager::FindRoot` assigns it to
`meta->segment_mapper_`.

## Phase 7 — Segment-File Garbage Collection and Archive Safety (done)

1. `FileGarbageCollector` namespace split into data and segment paths:
   `DeleteUnreferencedLocalSegmentFiles`,
   `DeleteUnreferencedCloudSegmentFiles`, each gated on a live
   `retained_segment_files` set derived from the current segment mapping.
2. Per-file term round-trip through `FileIdTermMapping` for segment files
   (keyed by `SegmentFileKey`), matching the data-file term-storage mechanism
   used to guard cross-process / cross-term collisions.
3. Archive-safety floor: `ArchivedMaxFileIds { data_file_id, segment_file_id }`
   struct consolidates both floors into a single
   `std::unordered_map<TableIdent, ArchivedMaxFileIds>
    least_not_archived_file_ids_` in `IouringMgr`. `GetOrUpdateArchivedMaxFileId`
   caches results via `try_emplace` (absence guaranteed by an earlier
   early-return); empty-archive partitions are not cached (avoids memory cost
   for partitions that never archive). `CreateArchive` publishes both floors
   via `insert_or_assign`.
4. Cleanup: `IndexPageManager::FindRoot` splits the
   "publish replayed mapping + stamp current manifest term" step — the full
   `replayer.file_id_term_mapping_` is published via `SetFileIdTermMapping`,
   then `SetFileIdTerm(entry_tbl, ManifestFileId(), ProcessTerm())` stamps the
   current process term using the dedicated targeted API.

## Phase 8 — Segment File Compaction (done)

### Current state

- [src/tasks/write_task.cpp:524-528](../src/tasks/write_task.cpp#L524-L528)
  calls `CompactIfNeeded` for both `cow_meta_.mapper_` and
  `cow_meta_.segment_mapper_`, which can set `shard->AddPendingCompact`.
- [src/tasks/background_write.cpp:98 `CompactDataFile`](../src/tasks/background_write.cpp#L98)
  is the only consumer of the pending-compact signal. It walks `meta->mapper_`
  and uses `ReadPages` / `WritePage`. A pending compact triggered by segment
  overflow silently runs data compaction and never rewrites segment files.

### Goal

When a segment file's live-segment ratio falls below the configured
amplification threshold, rewrite its live segments to the tail of the
segment-file space, update the segment mapping, flush a single manifest, and
trigger file GC so now-empty segment files are reclaimed. Data and segment
compaction share a single background run and a single `UpdateMeta`.

### Plan

1. **Keep the pending-compact signal coarse.** `WriteTask::CompactIfNeeded`
   continues to mark either mapping's overflow via a single
   `shard->AddPendingCompact(tbl_ident_)`. The background handler decides
   which mapping(s) to compact based on the current allocator stats.

2. **New `BackgroundWrite::CompactSegmentFile()`** paralleling
   `CompactDataFile`. Differences:
   - Iterates `cow_meta_.segment_mapper_->GetMapping()` to build
     `(FilePageId segment_id, PageId logical_segment_id)` pairs.
   - Per-file sizing uses `segments_per_file_shift` /
     `AppendAllocator::PagesPerFile()` on the segment allocator.
   - No `MemIndexPage` swizzling path; segments are not cached in-memory.
     Inner loop is straightforward `ReadSegments` → `WriteSegments`.
   - Compaction shares `GlobalRegisteredMemory` with foreground large-value
     reads/writes. Foreground impact is made predictable by bounding the
     background's in-flight segment count (≤ `max_segments_batch` per batch,
     recycled before the next batch) rather than by introducing a dedicated
     pool. Hiding background impact completely is not a goal; predictable,
     tunable impact is.
   - Updates segment mapper via `segment_mapper_->UpdateMapping(logical_id,
     new_fp_id)` and advances the segment `AppendAllocator`'s tail.

3. **Orchestrate both compactions against one COW + one `UpdateMeta`.** The
   core rewrite loops are split into `DoCompactDataFile()` and
   `DoCompactSegmentFile()`, each operating on `cow_meta_` in place. Each
   handles its stat-only case (empty mapping with non-empty space) internally
   via `AppendAllocator::UpdateStat` + early return, so the orchestrator
   carries no per-mapping classification. A new unconditional
   `BackgroundWrite::Compact()` runs one `MakeCowRoot`, up to two rewrite
   passes, one `UpdateMeta`, one `TriggerFileGC`. This matches the manifest
   record layout (both mappings serialized in one record).
   - `Compact` invokes `UpdateMeta(trigger_compact=false)` to suppress the
     foreground re-signaling path, so a background compaction cannot fire
     another compaction against the state it just produced.
   - Error handling: on any non-`NoError` return from either pass, the run
     aborts before touching the manifest. `MovingCachedPages` is a RAII
     guard that restores the original `FilePageId` on abort and is only
     `Finish()`ed after a successful `UpdateMeta`.
   - `Compact` is unconditional; deciding whether compaction is warranted is
     the caller's responsibility (see (4)).

4. **Trigger site changes.**
   - The shard's pending-compact dispatcher invokes `Compact()` directly —
     the pending-compact bit set by `WriteTask::CompactIfNeeded` already
     encodes the caller-side decision that a mapping overflowed.
   - `BackgroundWrite::CreateArchive` pins the live root, runs
     `WriteTask::MapperExceedsAmplification` once per mapper (data gated on
     `data_append_mode`, segment when `segment_mapper_` is non-null), and
     only invokes `Compact` when at least one mapper warrants it. The handle
     is dropped across the `Compact` call so it can take its own COW root
     reference without contention.

5. **Skip rules (applied per-mapping, consolidated through
   `MapperExceedsAmplification`).**
   - `amplify_factor == 0` → predicate returns false → caller skips
     `Compact`. Zero disables compaction for that mapping independently.
   - Empty mapper with non-empty space → predicate returns true; the rewrite
     loop's early-return branch drops the empty files via `UpdateStat`.
   - Segment mapper absent (`cow_meta_.segment_mapper_ == nullptr`) →
     `Compact` skips the segment pass; data-only behavior is unchanged.
   - Re-entry into `Compact` from its own `UpdateMeta` is blocked by
     `trigger_compact=false`, replacing the prior `Type() != BatchWrite`
     guard in `CompactIfNeeded`.

6. **KvOptions knobs.**
   - `segment_file_amplify_factor`, defaulting to `file_amplify_factor`.
     Segments are much larger than pages and rewrites are expensive, so a
     separate tuning knob is worthwhile. A zero value disables segment
     compaction independently.
   - `segment_compact_yield_every`, controlling how many segments the
     compaction loop processes before `YieldToLowPQ()`. Each segment is
     128KB–512KB, roughly two orders of magnitude larger than a 4KB page, so
     reusing the page-compaction cadence would hold the coroutine far longer
     between yields and risk tail-latency regressions for foreground reads.
     Default to yielding every segment batch; tune via benchmarks.

7. **Archive-floor interaction.** Segment compaction allocates new tail
   segment file IDs. No archive-floor special-casing is needed: the new tail
   IDs are above any floor, and stale IDs referenced only by archives are
   already protected by the `least_not_archived_file_ids_` floor from phase
   7.

8. **Term integration.** Newly allocated segment files use `ProcessTerm()`;
   `SetFileIdTerm(tbl_id, SegmentFileKey(new_file_id), ProcessTerm())` is
   invoked at allocation time (mirroring the data-file term-stamping in
   [write_task.cpp:349,356](../src/tasks/write_task.cpp#L349)). This makes
   sure the manifest flush in step 3 serializes the new segment-file term
   entries and the term mapping round-trips correctly on recovery.

### Files to change

- [include/tasks/background_write.h](../include/tasks/background_write.h) —
  declare unconditional `Compact` orchestrator; private `DoCompactDataFile`
  / `DoCompactSegmentFile` rewrite helpers.
- [src/tasks/background_write.cpp](../src/tasks/background_write.cpp) —
  implementation of the above; `CreateArchive` runs the per-mapper
  amplification check inline and calls `Compact` only when warranted.
- [include/kv_options.h](../include/kv_options.h) — add
  `segment_file_amplify_factor` and `segment_compact_yield_every`.
- [include/tasks/write_task.h](../include/tasks/write_task.h) /
  [src/tasks/write_task.cpp](../src/tasks/write_task.cpp) — extract
  `WriteTask::MapperExceedsAmplification` as a shared predicate used by
  both `CompactIfNeeded` (signal side) and `CreateArchive` (archive side);
  add `bool trigger_compact = true` to `UpdateMeta` so background compaction
  can suppress the re-signaling side effect. The `Type() != BatchWrite`
  guard in `CompactIfNeeded` is removed — the `trigger_compact` override
  now encodes the same intent explicitly.
- [src/storage/shard.cpp](../src/storage/shard.cpp) — pending-compact
  dispatcher calls `task->Compact()`.

## Phase 9 — Testing (done)

Testing is organized in four layers: standalone unit tests (no shards, no
io_uring), manifest/replayer round-trips (shard-less but exercising
on-disk layout), end-to-end tests driving the public `EloqStore` API, and
fault-injection / stress / benchmark runs. Each layer has a clear
responsibility so failures are localized: a unit failure in
`GlobalRegisteredMemory` should never reach the shard-driven suites.

All Catch2 sources live under [tests/](../tests/) and register via
[tests/CMakeLists.txt](../tests/CMakeLists.txt). New files should follow
the naming scheme `<area>.cpp` (e.g. `global_registered_memory.cpp`,
`large_value_e2e.cpp`, `segment_compact.cpp`) and reuse
[tests/common.h](../tests/common.h) /
[include/test_utils.h](../include/test_utils.h) where possible. Each bullet
below names the target file; contents with ✅ already exist and are called
out so they can be audited (and extended if gaps surface) rather than
rewritten.

### 1. Unit tests (no EloqStore, no shards) — done

Goal: pin down the individual building blocks introduced in phases 1–3 so
every higher-level test can treat them as trusted primitives.

- **`tests/global_registered_memory.cpp`** (✅ done)
  - Construction invariants: `segment_size` is 4KB aligned (128KB, 256KB,
    512KB boundary cases), `chunk_size` is a multiple of `segment_size` and
    4KB aligned, `total_size` is a multiple of `chunk_size`. Construction
    with non-conforming values must fail (or assert) before any chunk is
    allocated.
  - `MemChunks()` reports exactly `total_size / chunk_size` chunks; every
    chunk's `base_` is 4KB aligned and `size_ == chunk_size`.
  - Free-list exhaustion: allocate `TotalSegments()` segments without
    `evict_func_`; the (segment pointer, chunk index) pairs are all
    distinct, 4KB aligned, and cover every chunk; `FreeSegments()` drops to
    0; the next `TryGetSegment()` returns `{nullptr, 0}`.
  - Recycle round-trip: recycling in LIFO and FIFO orders both restores
    `FreeSegments()` to the full count, and subsequent `GetSegment` calls
    only return segments that were recycled (no double-issue).
  - Cross-chunk reuse: when the first chunk is fully allocated and only
    segments from the second chunk are recycled, subsequent allocations
    return segments with `chunk_index == 1`.
  - `evict_func_` path: `SetEvictFunc` is called once when the pool is
    empty and the injected func recycles one segment; `GetSegment` then
    returns without the yield func firing. Second scenario: `evict_func_`
    is a no-op so the yield func fires N times before a test harness
    eventually recycles a segment — assert the exact interleaving.
  - Free-list encoding: after recycling a known segment, the first 12 bytes
    of that segment encode `(next pointer, next chunk index)` pointing at
    the previously free head. This is a white-box check so a future
    refactor that breaks the embedded-list invariant fails loudly.

- **`tests/io_string_buffer.cpp`** (✅ done)
  - `Append` / `Fragments` / `Size` accounting: after K appends of known
    `IoBufferRef` values, `Fragments()` returns the same K refs in order
    and `Size()` matches the explicit `SetSize` value.
  - Move semantics: `IoStringBuffer` is non-copyable; move leaves the
    source empty with `Size() == 0` and no fragments.
  - `Recycle(mem, base)` returns every fragment exactly once to the backing
    `GlobalRegisteredMemory` (use a test-local instance), the buffer is
    reset to empty, and the pool's `FreeSegments()` is back to its
    pre-test value. Crucially, verify that recycling respects the
    `buf_index_ - base` → chunk-index recovery across multiple chunks.
  - Empty-buffer recycle is a no-op.

- **`tests/data_page_large_value.cpp`** (✅ done)
  - `IsLargeValueEncoding` truth table: iterate the two compression bits
    across all four states (`0b00`, `0b01`, `0b10`, `0b11`) and assert
    only `0b11` returns true, regardless of the other value-length bits.
  - `EncodeLargeValueContent` / `DecodeLargeValueContent` round-trip for
    K = 1, 2, `max_segments_batch`, and the largest K that still fits in a
    4KB data page — decoded `actual_length` and segment IDs match input.
  - Malformed input: `DecodeLargeValueContent` returns 0 on a truncated
    prefix and on `encoded.size() < 4`.
  - `DataPageIter::IsLargeValue` on a constructed page: build a page
    containing a short value, an overflow value, and a large value via
    `DataPageBuilder`; iterate and assert the flag matches the intent for
    each entry.

- **`tests/segment_allocator.cpp`** (✅ done; reuses the existing
  `AppendAllocator`)
  - Monotonic allocation: successive calls return strictly increasing
    physical segment IDs. Never reuses an ID even after the mapping
    releases it.
  - File-boundary transition: at `segments_per_file_shift = 3` (8 segments
    per file), allocating 9 segments produces IDs whose high bits move from
    file 0 to file 1 at the expected point; `CurrentFileId()` advances
    accordingly.
  - `PagesPerFile()` reports `1 << segments_per_file_shift`; sized
    consistently with `KvOptions::SegmentFileSize`.
  - Allocator state survives `UpdateStat` when the underlying mapping is
    emptied and non-empty space must be reclaimed (the Phase 8 empty-mapper
    early-return path).

- **Extend `tests/fileid_term_mapping.cpp`** (✅ done)
  - ✅ Existing test already covers a `SegmentFileKey` round-trip alongside
    `DataFileKey` and `ManifestFileId`.
  - ✅ `DataFileKey(N)` and `SegmentFileKey(N)` produce `TypedFileId` values
    that differ by exactly the LSB; serialization preserves type
    discrimination across the entire `N` range used on disk.
  - ✅ A mapping containing only segment entries still round-trips.

### 2. Manifest / replayer round-trip (phase 5, 7 integration without a live store) — done

- **Extend `tests/manifest_payload.cpp`** (✅ done)
  - ✅ `Snapshot` serializes a non-empty segment mapping after the
    `FileIdTermMapping` section, parsed in the expected order
    `[ data_mapping | file_id_term | segment_mapping ]`.
  - ✅ Backward compatibility — a manifest record built without the segment
    section parses to an empty segment mapping; the parser does **not**
    misinterpret trailing padding.
  - ✅ Both `Snapshot` and `UpdateRecord` paths preserve the segment
    mapping. An update record that only touches the segment mapping
    (`AppendSegmentMapping` non-empty, `UpdateMapping`/`DeleteMapping`
    empty) applies cleanly.

- **Extend `tests/replayer_term.cpp`** (✅ done)
  - ✅ Snapshot + two append logs where each log appends segment-mapping
    updates; final replayed segment mapper contains the merged state and
    the segment `AppendAllocator` is advanced past the maximum physical
    segment ID.
  - ✅ Segment-file term entries keyed by `SegmentFileKey` round-trip
    through `file_id_term_mapping_` and survive allocator bumping when
    `expect_term != manifest_term` in cloud mode.

### 3. End-to-end tests through `EloqStore` (phases 1–7) — done

These exercise the whole stack: an externally-provided
`GlobalRegisteredMemory`, `BatchWriteTask` writing very large values,
`ReadTask` populating `ReadRequest::large_value_`, manifest flush,
restart, GC. Implemented in **`tests/large_value_e2e.cpp`** (✅ done),
following [tests/batch_write.cpp](../tests/batch_write.cpp) for the
request wiring and `MapVerifier` pattern.

A small harness helper, `LargeValueHarness`, should own a
`GlobalRegisteredMemory` per shard (wired into
`KvOptions::global_registered_memories`), a recycled-segment accounting
bookkeeper, and helpers `MakeLargeValue(size)` / `ReadLargeValue(key)` that
materialize bytes into / out of an `IoStringBuffer`. The harness asserts
after every test that `FreeSegments() == TotalSegments()` — this is the
primary guard against segment leaks.

Golden-path coverage:

- Single-partition batch write of N values sized {128KB, 256KB, 1MB, 4MB,
  10MB}; point-read every key and assert:
  - `ReadRequest::value_` is empty; `large_value_.Size()` matches input;
    concatenating fragments equals input bytes.
  - The number of fragments equals `ceil(size / segment_size)`.
  - `buf_index_` values are within the registered range reported by
    `IouringMgr::global_reg_mem_index_base_` + chunk count.
- Mixed batch: short values, overflow values, and very large values in
  the same `BatchWriteRequest`; verify each key reads back via the
  appropriate request output field (`value_` vs `large_value_`) and no
  large fragments leak into short-value reads.
- Overwrite semantics: (a) large → short, (b) short → large, (c) large
  → large with different segment count. After each overwrite, re-read to
  confirm content, then run a follow-up `BatchWriteRequest` to force a
  manifest flush and confirm `DelLargeValue` freed the old logical
  segments (inspect `segment_mapper_` free-list size via a test-only
  accessor, or drive GC and count deleted segment files).
- Delete: after `Delete` of a large value, `Read` returns `NotFound`; GC
  reclaims every physical segment previously occupied by that value.
- Segment exhaustion + evict: seed the pool smaller than the working set,
  install an `evict_func_` that recycles one segment at a time, drive
  enough concurrent reads to block on `GetSegment`, confirm (a) no reader
  deadlocks, (b) every value is eventually returned correctly, (c) the
  eviction hook was called at least once.
- Lazy segment-mapper creation: a partition that never writes a large
  value must not allocate a `segment_mapper_` (cheap assertion via a
  test-only getter on `RootMeta`). Writing the first large value
  materializes it; the subsequent manifest contains a non-empty segment
  section.

Restart recovery:

- Write large values, issue one archive (or just drain the shard), stop
  the store, restart with the same options, read every key — bytes match,
  segment-file terms round-trip, `segment_mapper_` is populated from the
  manifest. Confirm that a subsequent write allocates new physical
  segment IDs strictly greater than any pre-restart ID (allocator
  restored from the manifest).
- Write → hard-stop (skip flush) → restart: already-flushed values are
  readable; any values that were mid-flight when the stop fired are
  either wholly present or absent — never partially readable.

Segment-file GC and archive safety (phase 7):

- **`tests/large_value_gc.cpp`** (✅ done, patterned on
  [tests/gc.cpp](../tests/gc.cpp)).
  - Local mode: write → overwrite → drive enough writes to trigger GC;
    assert `DeleteUnreferencedLocalSegmentFiles` removes the orphaned
    `segment_<id>_<term>` files and leaves referenced files intact.
  - Cloud mode: same scenario, validated via `ListCloudFiles` /
    `GetCloudSize`; confirm `DeleteUnreferencedCloudSegmentFiles` removes
    only orphans.
  - Archive floor: create a retained archive that references segment file
    F; overwrite all keys pointing at F; GC must **not** delete F while
    the archive is retained. After the retained archive rotates out, GC
    reclaims F. This pins the `least_not_archived_file_ids_` /
    `ArchivedMaxFileIds { segment_file_id }` path added in phase 7.
  - Term collisions: after a simulated term bump (cloud mode, mismatched
    `expect_term`), any pre-existing segment file whose on-disk term does
    not match the current term is ignored by GC and cannot mask a live
    segment file.

### 4. Segment compaction (phase 8) — done

**`tests/segment_compact.cpp`** (✅ done). Compaction is the biggest surface
added after phase 7 — this file is the primary guard on that behavior.

- Skip rules (no compaction should run):
  - `segment_file_amplify_factor = 0` disables segment compaction
    independently; data compaction still runs if `file_amplify_factor` is
    non-zero.
  - `segment_mapper_ == nullptr` (partition never wrote a large value) —
    `Compact()` runs data pass and exits cleanly.
  - `trigger_compact=false` re-entry: a background `UpdateMeta` does not
    schedule another compaction against its own output. Assert by
    counting pending-compact signals before and after.
- Data-only, segment-only, and both-mapper compactions each produce
  exactly one `UpdateMeta` and one `TriggerFileGC`. White-box hook on the
  shard or a test-only counter.
- Empty-mapper + non-empty space path: drop every large value via
  `DelLargeValue`, trigger compaction, confirm the allocator's `UpdateStat`
  early return retires the now-empty segment files without a rewrite
  pass.
- Post-compaction correctness: populate until SAF exceeds the configured
  factor, trigger compaction, then for every key:
  - `Read` returns the same bytes as before.
  - The segment mapping points at a strictly greater tail physical
    segment ID than before (new tail allocation).
  - Old segment files are GC'd; new segment files carry `ProcessTerm()`
    in the manifest.
- Bounded foreground impact (per the bounded-background-concurrency
  guideline — predictable impact, not isolated pool): with
  `segment_compact_yield_every` set to 1 and a large compaction in
  flight, a concurrent foreground read latency stays within a
  documented p99 envelope. The envelope is informational but regressions
  larger than ~2× the no-compaction baseline should fail the test.
- `CreateArchive` gating: when neither mapper exceeds the amplification
  threshold, `CreateArchive` must not call `Compact`. Assert via the
  `Compact`-invocation counter.
- `MovingCachedPages` RAII: inject a failure into the second rewrite pass
  and verify the first pass's cached-page state is rolled back (original
  `FilePageId` restored), the manifest is untouched, and a subsequent
  retry succeeds.

### 5. Concurrency / stress — done

- **`tests/large_value_concurrency.cpp`** (✅ done; patterned on
  [tests/concurrency.cpp](../tests/concurrency.cpp)).
  - Multiple shards, multiple partitions, interleaved large-value reads
    and writes for several seconds; assert the `LargeValueHarness` pool
    accounting (`FreeSegments() == TotalSegments()`) holds after
    quiescence and no read ever surfaces partial bytes.
  - Reader holding an `IoStringBuffer` while a writer overwrites the same
    key; the reader's bytes remain the pre-overwrite value until it
    recycles. This is the "segment mapping is snapshot-per-task" guarantee
    from the design.
  - GC + compaction under load: run writes, overwrites, and archives
    concurrently; assert no reader ever observes a segment whose
    backing file has been deleted.

### 6. Fault injection — done

Build on the existing kill-point machinery in
[include/kill_point.h](../include/kill_point.h) (used by other crash
tests). Implemented in **`tests/large_value_fault_injection.cpp`** (✅ done).

- ✅ Crash after `WriteSegments` but before manifest flush: restart, confirm
  no reader observes the orphan segments and the data page never
  references them; a subsequent allocation reuses / overwrites the
  orphan range without corruption.
- ✅ Crash during compaction between the data-rewrite and segment-rewrite
  passes: because phase 8 guarantees a single `UpdateMeta`, the manifest
  on restart either shows both passes applied or neither; assert both
  outcomes are readable and match a pre-crash snapshot.
- ✅ Crash during the manifest flush that publishes the new
  `ArchivedMaxFileIds`: on restart, the floor is reconstructed
  conservatively (old floor retained) so a partially-written archive can
  still be cleaned on the next successful archive.
- ✅ Term mismatch on a segment file (simulated by rewriting the file name
  to an older term): on startup, the file is treated as orphan and GC'd,
  never mapped.

### 7. Benchmarks (informational, not gating) — done

Implemented in **`tests/large_value_benchmark.cpp`** (✅ done). Run with:
```
sudo prlimit --memlock=unlimited ./build/tests/large_value_benchmark '[benchmark]'
```

- ✅ p50 / p99 point-read latency for values at 128KB, 1MB, 4MB, 10MB:
  zero-copy path vs. the overflow-page path. Results on reference hardware
  confirm the zero-copy path is 5–11× faster at ≥ 1 MB and slightly slower
  at 128 KB (sub-segment), matching the design expectation.
- ✅ Batch-write throughput for 1MB–10MB values, `IoStringBuffer` vs.
  `std::string` input. `IoStringBuffer` measured 3–10× faster due to
  segment-file writes vs multi-level overflow page chaining.
- ✅ Compaction overhead: foreground read p99 during a forced segment
  compaction with `segment_compact_yield_every` = {1, 8, 32}. Results
  confirm the expected gradient: yield_every=1 → ~2× overhead,
  yield_every=32 → ~3.6× overhead, validating the knob-tuning guidance
  from phase 8 bullet 6.

### Test wiring checklist — done

- ✅ All unit-test sources added to [tests/CMakeLists.txt](../tests/CMakeLists.txt).
  Standard tests are in `STANDARD_TARGETS` (built with `Catch2::Catch2WithMain`).
  Tests that use `GlobalRegisteredMemory` (`large_value_e2e`,
  `segment_compact`, `large_value_concurrency`, `large_value_fault_injection`,
  `large_value_benchmark`) are in `LARGE_VALUE_TARGETS` and built with
  `Catch2::Catch2` plus [tests/large_value_main.cpp](../tests/large_value_main.cpp).
- ✅ **`RLIMIT_MEMLOCK` must be at least 2 GB** for tests in `LARGE_VALUE_TARGETS`.
  `io_uring_register_buffers` pins all registered pages in RAM; with 256 MB
  of `GlobalRegisteredMemory` plus the page-pool buffer the total exceeds the
  typical 64 MB kernel default and registration fails with `ENOMEM`.
  [tests/large_value_main.cpp](../tests/large_value_main.cpp) calls
  `setrlimit(RLIMIT_MEMLOCK, 2 GB)` at startup, but this only succeeds when
  the hard limit allows it. Raise the hard limit before running these tests:
  ```
  sudo prlimit --memlock=unlimited ./build/tests/<test_binary>
  ```
- ✅ All shard-driven suites reuse `InitStore` / `CleanupStore` and carry
  their own option structs at the top of the file (pattern from
  [tests/gc.cpp](../tests/gc.cpp)).
- ✅ E2E and compaction tests populate
  `KvOptions::global_registered_memories`; the `LargeValueHarness` owns
  those instances and tears them down before `CleanupStore`.
- ✅ Cloud-mode tests use the `S3TestClient` helper in
  [tests/common.h](../tests/common.h) against the same MinIO endpoint as
  the existing cloud suites.
