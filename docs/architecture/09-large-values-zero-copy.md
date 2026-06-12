# Very Large Values and Zero-Copy I/O

Source: `include/global_registered_memory.h`, `src/global_registered_memory.cpp`,
`include/io_buffer_ref.h`, `include/io_string_buffer.h`,
`include/storage/data_page.h` (large-value encoding), `src/tasks/task.cpp`
(`GetLargeValue*`), `src/tasks/batch_write_task.cpp` (`WriteLargeValue*`),
`include/async_io_manager.h` (segments, pinned chunks, tail scratch).

Historical design docs `docs/zero_copy_read.md` / `docs/zero_copy_metadata.md`
motivated this feature; where they disagree with this doc, the code (and this
doc) win.

## Problem

KV-cache workloads read/write 1–10 MiB values. Routing those through 4 KiB
overflow pages and memcpy chains is wasteful. The design stores big values in
**segment files** and moves bytes with io_uring *fixed* (pre-registered
buffer) I/O end-to-end, so the engine never copies value bytes.

## Storage: segments

- A value larger than the overflow threshold is split into
  K = ceil(len / `segment_size`) segments (default 256 KiB, 4 KiB-aligned).
- Segments live in append-only **segment files**
  (`segment_<id>_<branch>_<term>`, `segments_per_file_shift` segments per
  file), allocated by a second `AppendAllocator` on
  `RootMeta::segment_mapper_` — a full parallel mapping
  (logical segment id → physical file segment), persisted in the manifest as
  snapshot + deltas, replayed like the page mapping (doc 06).
- The leaf entry's value field stores the encoding (compression bits `0b11`
  marks it):

```
word0(4B: bit31 = has-metadata, bits30..0 = actual_length)
| K × uint32 logical segment ids
| [uint16 metadata_len | metadata bytes]        (optional trailer)
```

  K is derived from `actual_length`, not stored. The optional **metadata
  trailer** (≤ 64 KiB) lets callers attach a small blob (e.g. KV-cache block
  descriptors) readable *without* touching the segments —
  metadata-only reads cost the same as a normal point read.
- GC and compaction treat segment files like data files, with their own
  amplification knob (`segment_file_amplify_factor`) and retained-set/guard
  rules (doc 08). Scans don't support large values.

## Memory: two mutually exclusive modes

Configured in `KvOptions`; both register memory as io_uring fixed buffers at
`BootstrapRing`:

### 1. `global_registered_memories` (legacy zero-copy mode)

One external `GlobalRegisteredMemory` per shard, owned by the embedding
system and shared with its networking/cache layers (registered in multiple
rings; per-ring `buf_index = chunk_idx + GlobalRegMemIndexBase()`).

`GlobalRegisteredMemory` hands out fixed-size segments from a **lock-free
ABA-tagged Treiber stack**: `head_` packs `(version32, seg_idx32)`; successor
indices live in a separate `successors_` array — never inside the segment
itself — both to avoid data races with in-flight DMA into popped segments and
to keep segment cache lines cold (the header comment documents this in
detail). `GetSegment(yield)` loops `TryGetSegment → evict_func_() → yield()`
under pressure; `Recycle(ptr, chunk_idx)` returns one segment.

Values travel as `IoStringBuffer` = vector of `IoBufferRef{data_, buf_index_}`
fragments + logical size:

- **Write**: caller builds an `IoStringBuffer` in `WriteDataEntry.large_val_`;
  `BatchWriteTask::WriteLargeValue` maps fragments → `WriteSegments` fixed
  writes.
- **Read**: `ReadRequest.large_value_dest_ = IoStringBuffer{}`; the read task
  allocates segments from the shard's pool (`GetLargeValue`), fixed-reads
  into them, and the caller drains fragments and must `Recycle` them
  (`EloqStore::GlobalRegMemIndexBase(shard_id)` provides the index base).

### 2. `pinned_memory_chunks` (KV-cache pinned mode)

The caller (e.g. a PyTorch-managed KV cache) owns big pinned chunks shared by
all shards; each shard registers them in its ring. Values move directly
between the caller's contiguous pinned buffer and segment files:

- **Write**: `WriteDataEntry.large_val_ = {ptr, size}` (plus `val_` as the
  metadata blob). The writer splits the range into K chunks,
  resolves the fixed-buffer index once (`BufIndexForAddress` /
  `PinnedChunkFor`), and issues `WriteSegments`.
  **Tail handling**: the last segment's fixed write covers
  `K*segment_size - size` bytes past the value end; if those bytes still lie
  inside the registered chunk this is free, otherwise the writer copies the
  tail into a per-shard registered **tail-scratch slot**
  (`AcquireTailScratch`, `pinned_tail_scratch_slots` × `segment_size` pool,
  zero-padded) and writes from there.
- **Read**: `ReadRequest.large_value_dest_ = {ptr, size}` reads segments
  straight into the caller's range (`GetLargeValueContiguous`; the range must
  sit inside one registered chunk; partial tail reads are 4 KiB-aligned).
  `large_value_only_ = true` skips metadata extraction for the
  "metadata first, bytes later" pattern.
- The `IoStringBuffer` read arm is **rejected** in pinned mode: fragments
  would come from the internal GC pool the caller can't recycle.
- Background GC/compaction can't use caller memory, so EloqStore allocates a
  private per-shard `GlobalRegisteredMemory`
  (`gc_global_mem_size_per_shard`, must hold `max_segments_batch` segments)
  for its own segment rewrites.

## Testing notes

Fixed-buffer registration pins pages against `RLIMIT_MEMLOCK`. The
`large_value_*` test binaries use a custom Catch2 main
(`tests/large_value_main.cpp`) that raises the limit to 2 GiB; CI runs with
`--ulimit memlock=-1:-1`.
