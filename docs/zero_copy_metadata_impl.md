# Implementation Plan: Metadata Support for Very Large Values

Companion to [zero_copy_metadata.md](zero_copy_metadata.md). Tracks the phased
implementation of metadata + pinned-memory support on top of the existing
zero-copy large-value path described in
[zero_copy_read.md](zero_copy_read.md) /
[zero_copy_impl.md](zero_copy_impl.md).

The whole feature ships on the `zero_copy_large_value` branch and is not yet
released, so format changes are allowed as long as a manifest produced by an
older `zero_copy_large_value` commit still parses — i.e. backward compatibility
is required against the **existing on-disk encoding without metadata**, not
against any pre-`zero_copy_large_value` format.

## Phase 1 — Data Page Large-Value Encoding (metadata trailer)

Extend the value-content encoding emitted into the data page to optionally
carry a metadata blob. The encoding piggy-backs on the existing
`actual_length` word so that records without metadata are byte-for-byte
identical to today's:

```
word0          : uint32_t
                  bit 31      = has_metadata flag (1 = trailer present)
                  bits 30..0  = actual_length (bytes; up to 2 GiB)
segment_ids    : K * uint32_t  (PageId per segment)
-- if has_metadata: --
metadata_length: uint16_t      (sits immediately after the segment array)
metadata_bytes : metadata_length bytes
```

K is not stored. The decoder derives it from `actual_length` and the
configured segment size: `K = ceil(actual_length / segment_size)`. With K
known, the segment array has a fixed size and the metadata trailer (length +
bytes) follows immediately after. When `has_metadata == 0` the trailer is
absent and the byte layout matches the pre-metadata encoding exactly.

This is why `DecodeLargeValueHeader` takes `segment_size` as a parameter; both
existing call sites already have it via `Options()->segment_size`.

`uint16_t` caps metadata at 64 KiB; revisit only if KV Cache needs more.

- Files: [include/storage/data_page.h:46-93](../include/storage/data_page.h#L46-L93).
- `EncodeLargeValueContent(actual_length, segment_ids, metadata, dst)` sets
  bit 31 of word0 iff `!metadata.empty()` and appends the trailer.
- Decoding is split into two helpers so callers that already know K can pass
  a correctly-sized buffer, and callers that only want one half (metadata or
  segment IDs) can skip the other:
  - `std::optional<LargeValueHeader> DecodeLargeValueHeader(encoded,
    segment_size)` parses word0 + the optional `metadata_length` and derives
    `num_segments = ceil(actual_length / segment_size)`. Returns `nullopt`
    on malformed input.
  - `bool DecodeLargeValueContent(encoded, header, segment_ids = {},
    metadata_out = nullptr)` writes the segment IDs (when the span is
    non-empty; size must match `header.num_segments`) and/or the metadata
    blob (when `metadata_out` is non-null). Both outputs are independently
    optional.
- Existing callers in [src/tasks/batch_write_task.cpp](../src/tasks/batch_write_task.cpp)
  and [src/tasks/task.cpp](../src/tasks/task.cpp) pass an empty metadata view
  on encode and an empty `metadata_out` / sized segment span on decode --
  their behavior is unchanged.

## Phase 2 \xe2\x80\x94 Pinned Memory Bootstrap

KV Cache owns its own pinned memory (PyTorch-registered). EloqStore must
register the same chunks in each shard's `io_uring` and still keep an internal
`GlobalRegisteredMemory` for GC / compaction, since those paths cannot use
pinned memory whose lifecycle EloqStore does not control.

- Change `IouringMgr::BootstrapRing` signature in
  [include/async_io_manager.h:924](../include/async_io_manager.h#L924) and
  [src/async_io_manager.cpp:212](../src/async_io_manager.cpp#L212) to accept
  ```
  std::variant<GlobalRegisteredMemory *,
               std::span<const std::pair<char *, size_t>>>
  ```
  Exactly one variant is used per `EloqStore` instance.
- When the pinned variant is selected:
  - Append every `(ptr, size)` to the iovec array before
    `io_uring_register_buffers`. Record:
    - `pinned_chunks_` \xe2\x80\x94 a per-`IouringMgr` copy of the chunk array (shared
      across shards, but each shard's ring registers them independently).
    - `pinned_index_base_` \xe2\x80\x94 the iovec index of the first pinned chunk
      (analogous to today's `global_reg_mem_index_base_`).
  - Construct a private `GlobalRegisteredMemory` for the shard, sized from a
    new `KvOptions::gc_global_mem_size_per_shard` knob (default 32 MiB), and
    register its chunks in the same iovec array. Wire this instance into the
    existing `global_reg_mem_` / `global_reg_mem_index_base_` slots so the GC
    and compaction paths in [src/tasks/background_write.cpp:318](../src/tasks/background_write.cpp#L318)
    work unchanged.
  - Sanity check at startup that
    `max_segments_batch * segment_size \xe2\x89\xa4 gc_global_mem_size_per_shard`,
    matching the bounded-background-concurrency guideline. No new throttle.
- New helper `IouringMgr::BufIndexForAddress(const char *ptr) const`:
  linear-searches `pinned_chunks_` and returns the iovec index (or asserts on
  not-found in debug builds). Used by the new read/write paths to translate a
  caller-supplied pinned pointer into an `io_uring` fixed-buffer index.
- The pinned variant is mutually exclusive with the externally-supplied
  `GlobalRegisteredMemory` variant. `KvOptions` exposes only one of them at
  a time; the variant captures that invariant at the API.

## Phase 3 -- Read Path

`ReadTask` exposes three `Read` overloads in
[include/tasks/read_task.h](../include/tasks/read_task.h). They share the
index-tree descent and data-page lookup; they differ only in what they do
with the located entry. The header's pre-existing typo (`vlau`) is fixed in
this phase.

**Overload A** -- `(... std::string &value, &ts, &expire_ts,
IoStringBuffer *large_value = nullptr)`:
- Non-large entry: `value` receives the decoded inline / overflow /
  compressed bytes.
- Large entry: `value` receives the metadata blob (empty when the entry has
  no metadata trailer). When `large_value != nullptr` the segments are
  additionally fetched into the `IoStringBuffer` via zero-copy reads (legacy
  behavior); when nullptr only metadata is extracted -- no segment reads.

**Overload B** -- `(... std::string &value, &ts, &expire_ts,
char *large_value, size_t large_value_size)` (KV Cache, metadata + pinned
value):
- Non-large entry: `value` receives the decoded bytes; the pinned buffer is
  unused.
- Large entry: `value` receives the metadata blob and, when `large_value !=
  nullptr`, the value bytes are read into `[large_value, large_value +
  large_value_size)` via `GetLargeValueContiguous`. Pass (nullptr, 0) to
  extract only metadata.

**Overload C** -- `(... &ts, &expire_ts, char *large_value,
size_t large_value_size)` (KV Cache, value-only):
- Caller asserts the entry is a large value (typically because it just read
  metadata via overload B in a prior point read). No metadata extraction,
  no `std::string` sink. Returns `KvError::InvalidArgs` if the resolved
  entry is not a large value or if `large_value == nullptr`.

**Shared helpers in [src/tasks/task.cpp](../src/tasks/task.cpp):**
- `ResolveValueOrMetadata(tbl, mapping, iter, value, compression)` -- writes
  `std::string &value` for any entry: inline / overflow / compressed bytes
  for non-large entries, metadata trailer for large entries. Never touches
  segment bytes; every parameter (`mapping`, `compression`) is used directly
  in the function body with no plumbing hidden in delegation. Replaces the
  pre-existing `ResolveValue` (and its short-lived companion
  `ResolveValueContiguous`).
- `GetLargeValue(tbl, seg_mapping, encoded, IoStringBuffer&, global_mem,
  reg_mem_index_base, yield)` -- existing zero-copy fetch into an
  `IoStringBuffer`.
- `GetLargeValueContiguous(tbl, seg_mapping, encoded, char *dst, dst_size,
  AsyncIoManager*)` -- new sibling that:
  - Resolves K logical -> physical segment IDs via the snapshot.
  - Calls `AsyncIoManager::BufIndexForAddress(dst)` once and reuses the
    `buf_index` for every segment. Debug-asserts
    `BufIndexForAddress(dst + dst_size - 1) == buf_index`.
  - For segment i, destination pointer is `dst + i * segment_size`.
  - No `IoStringBuffer`; no segments allocated from the private
    `GlobalRegisteredMemory`. The caller owns the buffer.
  - **Partial last-segment reads are deferred to Phase 6.** This phase
    requires `dst_size == K * segment_size`; the caller pre-allocates a
    segment-aligned buffer and uses `actual_length` from the decoded header
    to read the meaningful prefix. Honoring `size < K * segment_size`
    requires extending `AsyncIoManager::ReadSegments` (virtual, with
    subclasses `IouringMgr`, `CloudStoreMgr`, `StandbyStoreMgr`) with a
    tail-size override -- see Phase 6.

**Shared descent helper in [src/tasks/read_task.cpp](../src/tasks/read_task.cpp):**
- `LocateAndProcess<Handler>(tbl, key, &ts, &expire_ts, handler)` is an
  anonymous-namespace template that walks the index tree, loads the data
  page, positions a `DataPageIter` at the key, returns `NotFound` when the
  partition is empty or the key isn't present, invokes
  `handler(meta, mapping, seg_mapping, iter)` for the divergent step, and
  stamps `timestamp` / `expire_ts` on success. All three Read overloads
  reduce to a single `LocateAndProcess` call with a lambda; each lambda
  composes `ResolveValueOrMetadata` and/or the appropriate `GetLargeValue*`
  helper based on the overload's contract.
- `Floor` is left on its own path -- it uses `SeekFloor` with a prev-page
  fallback, which doesn't fit the simple-Seek shape the three Read overloads
  share. Factoring it would require a parallel helper and is out of scope.

**Polymorphic buf-index lookup:**
- `AsyncIoManager` gains a virtual `BufIndexForAddress(const char *) const`
  that default-returns `UINT16_MAX`. `IouringMgr` overrides it via the
  static `LookupBufIndex(chunks, base_index, ptr)` helper introduced in
  Phase 2. This keeps `GetLargeValueContiguous` polymorphic across IO-manager
  subclasses without a `static_cast`.

## Phase 4 \xe2\x80\x94 Write Path

- `WriteDataEntry` in [include/types.h:167](../include/types.h#L167):
  replace `IoStringBuffer large_val_` with
  ```
  std::variant<std::monostate,
               IoStringBuffer,
               std::pair<const char *, size_t>> large_val_;
  ```
  - Existing constructor `(key, val, ts, op, expire_ts)` keeps inline-value
    semantics; `large_val_` defaults to `monostate`.
  - Existing constructor `(key, IoStringBuffer, ts, op, expire_ts)` stays.
  - New constructor `(key, std::string val, std::pair<const char*, size_t>
    large, ts, op, expire_ts)` for the metadata + pinned case. `val_` is the
    metadata; `large_val_` holds the pinned pointer / size.
  - `HasLargeValue()` returns `!std::holds_alternative<std::monostate>(large_val_)`.
- `BatchWriteTask::WriteLargeValue` in
  [src/tasks/batch_write_task.cpp:1466](../src/tasks/batch_write_task.cpp#L1466):
  factor out the post-allocation flow into a private helper
  ```
  KvError WriteLargeValueSegments(uint32_t actual_length,
                                  std::span<const char *> ptrs,
                                  std::span<const uint16_t> buf_indices,
                                  std::string_view metadata);
  ```
  Both variant arms feed this helper:
  - `IoStringBuffer` arm: derives `ptrs` / `buf_indices` from
    `large_val.Fragments()` (today's logic).
  - `std::pair<const char *, size_t>` arm: splits the contiguous range into K
    segment-sized chunks, computes K = `ceil(size / segment_size)`, the
    shared `buf_index` from `IouringMgr::BufIndexForAddress(ptr)`, and an
    array of K pointers `ptr + i * segment_size`.
- The helper handles segment-ID allocation, batched `WriteSegments` calls,
  and the final `EncodeLargeValueContent(actual_length, segment_ids,
  metadata, large_value_content_)` -- metadata comes from `entry.val_` when
  the entry carries a large value.
- **Tail-segment write contract** (pinned arm): writes always flush K full
  segments. When `size` does not divide `segment_size`, the kernel reads
  `K*segment_size - size` bytes past the caller's logical value end and
  flushes them to disk as logical garbage. Phase 4 ships with the strict
  contract -- `[ptr, ptr + K*segment_size)` must be a single registered
  chunk -- enforced by the dispatcher's single-chunk check. Phase 7
  loosens this to "`[ptr, ptr + size)` is a single chunk" by adding a
  scratch fallback for the cross-boundary case; see Phase 7.
  The garbage bytes on disk are never surfaced on read: small payloads use
  the header's `actual_length`, and contiguous-pinned reads use Phase 6's
  `ReadSegments` `tail_size` to stop the final read at the meaningful
  boundary.

## Phase 5 \xe2\x80\x94 Touchpoint Sweep

After the variant change in `WriteDataEntry`, every direct access to
`entry.large_val_` needs to match the new shape. Known sites:

- [src/tasks/batch_write_task.cpp:639](../src/tasks/batch_write_task.cpp#L639),
  [src/tasks/batch_write_task.cpp:717](../src/tasks/batch_write_task.cpp#L717),
  [src/tasks/batch_write_task.cpp:1466](../src/tasks/batch_write_task.cpp#L1466).
- `HasLargeValue()` usage in any size or flag check.
- `IoStringBuffer::Recycle` paths on failure / handoff \xe2\x80\x94 the pinned variant
  never recycles (caller owns the memory); guarded by `std::visit`.

A short grep pass before final commit catches any new sites added since the
plan was written.

## Phase 6 -- Partial Last-Segment Reads (deferred from Phase 3)

Phase 3 requires `dst_size == K * segment_size` for `GetLargeValueContiguous`.
KV Cache callers can satisfy this by padding their pinned buffer to a
segment-aligned size, but two costs follow: (1) extra disk bandwidth on the
tail segment, and (2) the caller must allocate up to `segment_size - 1`
bytes of slack past the actual value. This phase closes that gap by teaching
the IO manager to honor a per-request read size on the final segment. It is
purely additive on the read path; the encoded format and the write path are
unchanged.

### API change

Extend `AsyncIoManager::ReadSegments` with an optional `tail_size` for the
**last** segment of the batch (all other segments still read at the full
`segment_size`):

```
virtual KvError ReadSegments(const TableIdent &tbl_id,
                             std::span<const FilePageId> segment_ids,
                             std::span<char *> dst_ptrs,
                             std::span<const uint16_t> buf_indices,
                             uint32_t tail_size = 0);
```

- `tail_size == 0` (default) means "all segments read full segment_size" --
  preserves the semantics today's three callers (`GetLargeValue`,
  `GetLargeValueContiguous`, `BackgroundWrite::CompactSegmentFile`) rely on,
  so they keep working unchanged.
- `tail_size > 0` overrides the read length of the **final** entry in the
  batch (`segment_ids.back()`). Must be 4 KiB aligned and `<= segment_size`.
  This matches the design contract that only the last segment of a value
  can be partial; non-tail segments are always full.

### IouringMgr changes

- `IouringMgr::ReadSegments` overrides the new signature. Internally:
  per-segment read size is `segment_size` for all entries except the last,
  whose size is `tail_size` (when non-zero). Encoding this only requires
  branching in the `send_req` lambda and the short-read retry threshold;
  the existing `SegReadReq` struct grows no new field (the size is computed
  from `is_tail ? tail_size : seg_size` at use sites).
- Validation: `tail_size > 0` requires `tail_size % 4096 == 0` and
  `tail_size <= segment_size`. Out-of-range values return
  `KvError::InvalidArgs`.

### CloudStoreMgr / StandbyStoreMgr

- Both inherit `IouringMgr` and pick up the new behavior automatically
  unless they override `ReadSegments`. Audit confirms today neither does;
  the new signature flows through without subclass edits beyond a rebuild.

### GetLargeValueContiguous

- Drop the strict `dst_size == K * segment_size` check. New contract:
  `(K - 1) * segment_size < dst_size <= K * segment_size` and
  `dst_size % 4096 == 0`.
- Compute `tail_bytes = dst_size - (K - 1) * segment_size`. The batching
  loop sends full-size batches for every batch *except the one containing
  the final segment*; that final batch is sent with `tail_size = tail_bytes`
  (or `tail_size = 0` when `tail_bytes == segment_size`, i.e. caller
  happened to allocate exactly K segments).
- The single `BufIndexForAddress` lookup at `dst` is unchanged. The trailing
  range `[dst + dst_size, dst + K * segment_size)` is no longer touched --
  the caller can keep adjacent data there if they want.

### Files to change

- [include/async_io_manager.h](../include/async_io_manager.h) /
  [src/async_io_manager.cpp](../src/async_io_manager.cpp) -- extend the
  `ReadSegments` virtual signature with `uint32_t tail_size = 0`; update
  `IouringMgr::ReadSegments` so the final entry's read length is
  `tail_size` when non-zero, and the short-read retry threshold matches.
- [include/tasks/task.h](../include/tasks/task.h) /
  [src/tasks/task.cpp](../src/tasks/task.cpp) -- relax
  `GetLargeValueContiguous`'s `dst_size` contract; in the batching loop,
  pass `tail_size = dst_size - (K - 1) * segment_size` on the batch that
  contains the final segment and `tail_size = 0` on every other batch.
- Tests for this phase land in Phase 8 (Tests), alongside the rest of the
  feature.

### Out of scope for Phase 6

- `WriteSegments` partial-write support. File segments are always allocated
  and written at `segment_size` granularity; the tail's garbage past
  `actual_length` is fine on disk and is precisely what the new partial
  read skips.
- Any change to `GetLargeValue` (IoStringBuffer path). That path allocates
  segment-sized fragments from `GlobalRegisteredMemory`, so a partial read
  saves bandwidth only -- not a goal here.

## Phase 7 -- Pinned-Write Tail Scratch (relaxed write contract)

Phase 4's pinned-arm write requires `[ptr, ptr + K*segment_size)` to be a
single registered pinned chunk -- the strict pad-up-to-segment-boundary
contract. The reason is that `io_uring_prep_write_fixed` bounds-checks
`[addr, addr + nbytes)` against the registered iovec and rejects anything
that crosses the boundary, so even though the bytes past `actual_length`
are "logical garbage", they must still be valid registered memory for the
final fixed write to succeed.

This phase loosens the contract to "`[ptr, ptr + size)` is a single
registered chunk" -- which is the natural shape of KV Cache's pinned
allocations -- by adding a small per-shard scratch pool for the
cross-boundary case. The on-disk invariant ("every segment write is full
`segment_size`") is preserved so compaction is unchanged.

### Allocation

`IouringMgr` gains a tail-scratch pool, allocated in `BootstrapRing`'s
pinned-mode branch and sized by a new `KvOptions::pinned_tail_scratch_slots`
knob (default = `max_segments_batch`). Each slot is `segment_size` bytes,
registered as a fixed buffer alongside the pinned chunks and the private
GC pool. The slots form a `WriteBufSlot`-style free list with a
`WaitingZone` for backpressure.

A 0-slot setting disables the fallback (the strict Phase 4 contract still
applies). `ValidateOptions` emits a `LOG(WARNING)` in that case.

### API

`AsyncIoManager` gains two virtuals (default no-op for non-pinned
managers); `IouringMgr` overrides them:

```
virtual char *AcquireTailScratch(uint16_t &buf_index);  // yields if empty
virtual void  ReleaseTailScratch(char *ptr, uint16_t buf_index);
```

### Pinned-arm dispatch in WriteLargeValue

```
base_buf_index = BufIndexForAddress(base);          // must exist
if (BufIndexForAddress(base + size - 1) != base_buf_index)
    return InvalidArgs;                              // meaningful range
                                                     // must live in one
                                                     // chunk -- same as
                                                     // before
full_tail = base + K*segment_size - 1;
needs_scratch = (BufIndexForAddress(full_tail) != base_buf_index);

ptrs[i]        = base + i*segment_size, for i in [0, K)
buf_indices[i] = base_buf_index

if (needs_scratch) {
    scratch = AcquireTailScratch(scratch_buf_index);
    if (!scratch) return InvalidArgs;                // 0-slot pool
    tail_bytes = size - (K-1)*segment_size;
    memcpy(scratch, base + (K-1)*segment_size, tail_bytes);
    memset(scratch + tail_bytes, 0, segment_size - tail_bytes);
    ptrs[K-1]        = scratch;
    buf_indices[K-1] = scratch_buf_index;
}

result = WriteLargeValueSegments(size, ptrs, buf_indices, metadata);
if (needs_scratch) ReleaseTailScratch(scratch, scratch_buf_index);
return result;
```

The first K-1 segments always come from the caller's pinned memory: by the
definition of K, `(K-1)*segment_size < size`, so every byte of those
segments is inside `[base, base + size)` and therefore inside the
registered chunk.

### Files to change

- [include/kv_options.h](../include/kv_options.h) -- new
  `pinned_tail_scratch_slots` knob.
- [include/async_io_manager.h](../include/async_io_manager.h) /
  [src/async_io_manager.cpp](../src/async_io_manager.cpp) --
  `AcquireTailScratch` / `ReleaseTailScratch` virtuals + `IouringMgr`
  overrides; pool allocation in `BootstrapRing`.
- [src/eloq_store.cpp](../src/eloq_store.cpp) -- 0-slot warning in
  `ValidateOptions`.
- [src/tasks/batch_write_task.cpp](../src/tasks/batch_write_task.cpp) --
  relax `WriteLargeValue` pinned arm; acquire/release scratch on the
  cross-boundary path.
- Tests land in Phase 8.

### Out of scope for Phase 7

- Tail scratch for the IoStringBuffer write path -- those fragments are
  already segment-aligned by construction, so the fallback never triggers.
- A separate write-side `tail_size` (mirror of Phase 6's read-side
  feature). With this scratch, writes always send full `segment_size` to
  disk, so compaction needs no awareness of value-level tails. The
  trade-off described in the earlier discussion (write_buf_ reuse vs.
  dedicated pool, partial writes vs. always-full + scratch) was resolved
  in favor of this dedicated pool.

## Phase 8 -- Tests

### Unit tests

**[tests/data_page_large_value.cpp](../tests/data_page_large_value.cpp)** -- encoding helpers:
- Round-trip with `metadata.empty()` and with a non-empty metadata blob.
- Round-trip at `actual_length` values that exercise both high-bit
  interpretations (e.g. 1 KiB, 10 MiB, near `INT32_MAX`).
- Decoder rejects a truncated trailer (`has_metadata == 1` but
  `encoded.size() < sizeof(uint32_t) + K*sizeof(PageId) + sizeof(uint16_t)`).
- All four combinations of `DecodeLargeValueContent`'s independently
  optional sinks: (segments only) / (metadata only) / (both) / (neither --
  structural validation). All return success on a well-formed buffer.
- Debug deathtest: encoder asserts `(actual_length &
  kLargeValueHasMetadataBit) == 0` and `metadata.size() <=
  UINT16_MAX`. Guarded `#ifndef NDEBUG`.

**[tests/iouring_pinned_lookup.cpp](../tests/iouring_pinned_lookup.cpp)** -- chunk lookup:
- `LookupBufIndex` over a 3-chunk array -- boundary addresses
  (chunk start, chunk end, mid-chunk) resolve to the right index;
  out-of-range returns `UINT16_MAX`.
- `LookupPinnedChunk` (Phase 7 sibling) over the same array -- returns
  the full `PinnedChunkInfo {base, size, buf_index}` on hit and
  `nullopt` on miss. Multi-chunk array with gaps: address in a gap
  returns `nullopt`.

**New `tests/iouring_tail_scratch.cpp`** -- intrusive free list:
- Acquire from a fresh pool returns N distinct slots, each
  `segment_size`-aligned within the backing buffer.
- LIFO order: the last released slot is the next acquired.
- Acquire from an empty pool blocks on the `WaitingZone`; a release on
  another coroutine wakes the waiter and hands it the released slot.
- N concurrent acquires followed by N releases restores `tail_scratch_free_`
  to its initial state (pool count invariant).
- Sized pool of zero slots: `AcquireTailScratch` returns `nullptr`
  immediately.

**Unit / integration around `IouringMgr::ReadSegments`** -- tail-size
validation (`tail_size > segment_size` or `tail_size % 4096 != 0` ->
`InvalidArgs`). Reuse the existing standalone test harness for the IO
manager if available, else fold into an e2e check.

**[tests/eloq_store_test.cpp](../tests/eloq_store_test.cpp)** -- `ValidateOptions`
rejection paths added by phases 2 and 7:
- `pinned_memory_chunks` non-empty **and** `global_registered_memories`
  non-empty -> reject (mutual exclusion).
- A pinned chunk with non-4-KiB-aligned base or size -> reject.
- `gc_global_mem_size_per_shard < max_segments_batch * segment_size` ->
  reject.
- `gc_global_mem_size_per_shard % segment_size != 0` -> reject.
- `pinned_tail_scratch_slots == 0` with pinned mode -> accept with a
  `LOG(WARNING)`; the strict Phase 4 contract applies at runtime.

### End-to-end tests (extend [tests/large_value_e2e.cpp](../tests/large_value_e2e.cpp))

**Metadata + pinned-memory features (Phases 1, 3, 4):**
- Write a large value via the pinned `(char*, size_t)` constructor with
  non-empty metadata; read back metadata-only via overload A, then read
  the full value via overload B, confirm metadata and value bytes both
  match. (The IoStringBuffer + metadata write pattern is not in scope.)
- Mixed-overload regression: a partition with mixed metadata /
  no-metadata large values reads back correctly through both overload A
  and overload B.

**Overload C (pinned-only read, Phase 3):**
- After a prior overload-B read returned the metadata, call overload C
  with a fresh pinned buffer and confirm the bytes match the original
  value.
- Overload C on a non-large entry returns `KvError::InvalidArgs`.
- Overload C with `large_value == nullptr` returns `KvError::InvalidArgs`.

**Partial last-segment reads (Phase 6):**
- `dst_size` lands mid-segment (e.g., `2 * segment_size + 4096`,
  `2 * segment_size + segment_size/2` rounded to 4 KiB). Bytes past
  `dst_size` are pre-filled with a sentinel pattern and must remain
  untouched after the read.
- `dst_size == K * segment_size` regression -- the Phase 3 path still
  works after the contract relaxation.
- `dst_size` of exactly 4 KiB on a single-segment value (smallest
  allowed partial).
- **Multi-batch partial read:** value with `K >= max_segments_batch + 1`
  segments and a partial tail. The first batch issues full-size reads
  with `tail_size = 0`; only the batch containing the final segment
  uses `tail_size = tail_bytes`. Verifies the `is_last_batch` branch
  in `GetLargeValueContiguous`.
- Invalid sizes (not 4 KiB aligned; > `K * segment_size`; <=
  `(K-1) * segment_size`) return `KvError::InvalidArgs`.

**Pinned-write tail scratch (Phase 7):**
- Pinned write where `(ptr, size)` is segment-aligned (`size %
  segment_size == 0`): fast path -- scratch pool is untouched.
- Pinned write where `size` is mid-segment **and** the chunk has slack:
  fast path again (still no scratch). Asserted via a test-only counter
  on `AcquireTailScratch`.
- Pinned write where `size` is mid-segment **and** `(ptr, size)` is
  flush against the chunk end (no slack): scratch fallback triggers,
  the value reads back correctly, and the scratch slot is released.
- Multiple concurrent cross-boundary writes saturating the pool: second
  writer yields on `tail_scratch_waiting_`, makes progress when the
  first writer releases. Pool accounting (free-list length) returns to
  its initial state after quiescence.
- `pinned_tail_scratch_slots = 0`: cross-boundary write returns
  `KvError::InvalidArgs`; segment-aligned writes still succeed.

**Restart recovery with metadata-bearing values:**
- Write a mix of metadata-bearing and no-metadata large values, drain
  the shard, stop and restart the store, read every key -- bytes and
  metadata both match. Segment-file terms round-trip through the
  manifest; `segment_mapper_` is restored.

### Compaction and GC

**[tests/segment_compact.cpp](../tests/segment_compact.cpp)** -- compaction in pinned mode:
- In KV Cache pinned mode, compaction reads/writes flow through the
  private `GlobalRegisteredMemory`, not the caller's pinned chunks.
  Asserted via a sentinel: the caller's pinned buffer is filled with a
  recognizable pattern before compaction; the pattern is unchanged
  after.
- Bound check: `max_segments_batch * segment_size <=
  gc_global_mem_size_per_shard` holds at runtime (assertion).
- Compaction preserves the metadata trailer: write large values with
  metadata, trigger compaction, read back -- metadata is identical.

**[tests/large_value_gc.cpp](../tests/large_value_gc.cpp)** -- GC of metadata-bearing
records:
- Overwrite metadata-bearing large values, drive GC. Old segment files
  are reclaimed; new files retain only the live values' segments.
- The encoded metadata trailer doesn't keep stale segment files alive
  (segments are referenced solely via the segment mapping, not the
  trailer).

### Fault injection ([tests/large_value_fault_injection.cpp](../tests/large_value_fault_injection.cpp))

- Crash after a pinned `WriteSegments` but before the manifest flush:
  on restart, the orphan segments are not referenced and the data page
  never points at them. A subsequent allocation reuses the orphan
  range without corruption.
- Crash during a scratch-fallback write (after `AcquireTailScratch`,
  during `WriteSegments`): scratch slot is process-local state, so on
  restart the pool is freshly allocated and all slots are free. The
  partial on-disk write is treated as an orphan segment per the above.

### Concurrency ([tests/large_value_concurrency.cpp](../tests/large_value_concurrency.cpp))

- Multi-shard pinned writes saturating each shard's tail-scratch pool
  independently: one shard's pool exhaustion must not block another
  shard. Per-shard `tail_scratch_free_` length is tracked through
  quiescence on each shard.

### Benchmarks (extend [tests/large_value_benchmark.cpp](../tests/large_value_benchmark.cpp))

The existing benchmarks compare the IoStringBuffer large-value path
against overflow-page and measure compaction interference. The new
features (Phases 3-7) deserve parallel coverage. Each new benchmark
prints a small table to stdout; assertions are minimal (success of
every operation) -- the value is the printed numbers.

**Pinned-mode write throughput (Phases 4, 7):**
- Batch-write throughput of the pinned-memory path (KV Cache) vs the
  legacy IoStringBuffer path at 1 MB / 4 MB / 10 MB. Pinned saves the
  per-segment copy from `std::string` / `IoStringBuffer` fragments
  into the io_uring registered buffer because the bytes already live
  in pinned memory; expected throughput >= IoStringBuffer.
- Tail-scratch overhead: same sizes, value placed flush against the
  pinned chunk end so the scratch fallback fires every write. Reports
  p50/p99 latency for the fast path and the scratch-fallback path
  side-by-side; the scratch overhead should be a small fraction of
  total write time.

**Read throughput on a dataset > OS page cache, with concurrent clients
(Phases 3, 6):**
All read-throughput benchmarks pre-populate N distinct keys per size
case (target ~256 MiB / size). Before timing each `(N_threads, K_inflight)`
config, every data/segment file in the partition is `fsync`'d and then
`posix_fadvise(POSIX_FADV_DONTNEED)`'d so the OS page cache is dropped
for the dataset. The read phase spawns `N_threads` worker threads, each
maintaining `K_inflight` outstanding requests via `ExecAsyn` / `Wait`
(FIFO) -- total in-flight = `N x K`. Per-request latency is measured
from each submit to its matching Wait return; aggregate throughput is
total bytes read divided by spawn->join wall time. Configs tested:
`{(1,1), (1,2), (2,1), (2,2), (4,1), (4,2)}`.

All three opt into the appropriate large-value destination
(`IoStringBuffer` or pinned) on every `ReadRequest`. Note: a
`ReadRequest` whose `large_value_dest_` is left as default
(`std::monostate`) falls into metadata-only mode and skips segment
reads entirely -- callers porting from the pre-variant API must
explicitly opt in.

- Bench 1 -- IoStringBuffer mode: random reads of `large-value` vs
  `overflow-page` paths across the NxK matrix.
- Bench 6 -- Pinned mode (overload B): random reads across the NxK
  matrix.
- Bench 7 -- Pinned mode windowed-locality (KV Cache pattern): each
  worker reads a window of `W = 8` consecutive keys (close in the
  index tree), then jumps to a different random window; `K_inflight`
  requests overlap inside the window. Comparing the same NxK row
  against Bench 6 isolates the locality benefit for a workload where
  each client request loads a contiguous batch of tokens' KV state
  before yielding to a different request.

## Files to change

- [include/storage/data_page.h](../include/storage/data_page.h) \xe2\x80\x94 encode /
  decode helpers gain a metadata parameter.
- [include/types.h](../include/types.h) /
  [src/types.cpp](../src/types.cpp) \xe2\x80\x94 `WriteDataEntry::large_val_` becomes a
  variant; new constructor for the pinned + metadata case.
- [include/tasks/read_task.h](../include/tasks/read_task.h) /
  [src/tasks/read_task.cpp](../src/tasks/read_task.cpp) -- fix typo; add
  the three Read overloads (IoStringBuffer / metadata + pinned / pinned-only)
  and route each through the shared `LocateAndProcess` template.
- [include/tasks/task.h](../include/tasks/task.h) /
  [src/tasks/task.cpp](../src/tasks/task.cpp) -- replace `ResolveValue` with
  the single-purpose `ResolveValueOrMetadata`; add `GetLargeValueContiguous`
  alongside `GetLargeValue`; drop the short-lived `ResolveValueContiguous`.
- [src/tasks/scan_task.cpp](../src/tasks/scan_task.cpp) -- update the
  existing `ResolveValue` call site to `ResolveValueOrMetadata` (same 5-arg
  shape; scan already short-circuits large entries).
- [include/tasks/batch_write_task.h](../include/tasks/batch_write_task.h) /
  [src/tasks/batch_write_task.cpp](../src/tasks/batch_write_task.cpp) \xe2\x80\x94
  variant handling for `large_val_`; shared `WriteLargeValueSegments` helper;
  metadata threading into `EncodeLargeValueContent`.
- [include/async_io_manager.h](../include/async_io_manager.h) /
  [src/async_io_manager.cpp](../src/async_io_manager.cpp) \xe2\x80\x94 `BootstrapRing`
  variant signature, `pinned_chunks_` / `pinned_index_base_`,
  `BufIndexForAddress`, private `GlobalRegisteredMemory` construction.
- [include/kv_options.h](../include/kv_options.h) \xe2\x80\x94
  `gc_global_mem_size_per_shard` (default 32 MiB) and the pinned-chunk
  option.
- [include/eloq_store.h](../include/eloq_store.h) /
  [src/eloq_store.cpp](../src/eloq_store.cpp) \xe2\x80\x94 plumb the pinned-chunk
  array through to per-shard `BootstrapRing` calls.
- Tests as listed in Phase 8.

## Open items (verify during implementation)

- Confirm that current GC / compaction inflight \xe2\x89\xa4
  `gc_global_mem_size_per_shard / segment_size` and add an assertion next to
  the existing `max_segments_batch` constant. If the assertion fires under
  load, revisit the throttle decision.
- Confirm no read path mutates `WriteDataEntry::large_val_` in a way that
  breaks the `std::pair` arm's "caller owns the memory" contract. The pair's
  pointer stays valid only for the duration of the write request; the
  shared helper must copy out anything it needs before yielding.
