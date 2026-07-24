# Data Lifecycle: Reads, Writes, Compaction, GC

Source: `src/tasks/read_task.cpp`, `src/tasks/scan_task.cpp`,
`src/tasks/batch_write_task.cpp`, `src/tasks/write_task.cpp`,
`src/tasks/background_write.cpp`, `src/tasks/reopen_task.cpp`,
`src/tasks/archive_crond.cpp`, `src/tasks/prewarm_task.cpp`,
`include/file_gc.h`, `src/file_gc.cpp`.

## Point read (`ReadTask`)

1. `PageManager::FindRoot(tbl_id)` — pins the partition's `RootMeta` (cold
   partitions replay their manifest here, doc 06) and takes a
   `MappingSnapshot::Ref`.
2. `SeekIndex` descends the in-memory index (swizzled pointers; missing index
   pages are loaded once, with concurrent waiters parking on the page).
3. Load the one data page (through the optional data-page cache), binary
   search via `DataPageIter`.
4. Resolve the value: inline → copy out; overflow → `GetOverflowValue` chases
   overflow pages; compressed → decompress; very large →
   metadata-from-trailer and/or segment reads (doc 09).

`Floor` is the same descent with floor semantics (`SeekFloor`).

## Scan (`ScanTask` / `ScanIterator`)

Builds an explicit index-frame stack to the begin key, then iterates leaves
via their next-links, prefetching up to `prefetch_page_num` leaves per batch
(`ReadPages`). Results accumulate into the request until the
entry/byte pagination limit; `HasRemaining()` tells the caller to resubmit
from the last key. Holding the mapping snapshot for the whole scan page keeps
the view consistent; very large values abort with `LargeValueUnsupported`.

## Batch write (`BatchWriteTask`)

One write task per partition at a time (doc 04). `Apply()`:

1. `MakeCowRoot` — private `CowRootMeta` with cloned mappers.
2. Sort/dedup the batch; entries older (timestamp) than the stored version
   are skipped. Deletes drop entries; TTL changes are mirrored into a
   `ttl_batch_` applied to the TTL tree afterward.
3. Merge into leaves page-by-page (`ApplyOnePage`), CoW-allocating
   replacement pages, maintaining the leaf doubly-linked list via the
   `leaf_triple_`, writing overflow pages / large-value segments as needed.
4. Rebuild touched index path bottom-up.
5. `UpdateMeta` → `FlushManifest` (append a manifest log record, or rotate to
   a fresh snapshot past `manifest_limit`; in cloud mode wait for uploads) →
   `PageManager::UpdateRoot` publishes the new root → old snapshot's freed
   file pages recycle when its last reader releases.
6. In append mode, `UpdateMeta(trigger_compact=true)` checks the data/segment
   space amplification against their configured factors. It flags the shard's
   pending-compact set only when a threshold is reached; the resulting
   compaction runs file GC after it finishes. BatchWrite does not run
   compaction or file GC unconditionally. `TriggerTTL` is scheduled
   independently.

Failure at any point: `Abort()` — the CoW state is discarded, `AbortWrite`
cancels buffered file writes, the published tree is untouched.

`Truncate`, `Drop`, `CleanExpiredKeys` are alternate entry points on the same
task class (drop deletes the manifest first, making deletion durable, then
relies on GC for files).

## Background maintenance (`BackgroundWrite`)

All run through the same per-partition write queue, so they serialize with
user writes. Scheduled via shard pending-sets
(`AddPendingCompact/TTL/FileGc/LocalGc`) which enqueue the embedded singleton
requests in each `PendingWriteQueue`.

- **Compaction** (`Compact()`, append mode only): one `MakeCowRoot`; rewrite
  pass over under-utilized data files (live pages re-written to the tail,
  per-file utilization re-checked against `file_amplify_factor`), then over
  segment files (`segment_file_amplify_factor`, yielding every
  `segment_compact_yield_every` segments to the low-priority queue); one
  `UpdateMeta(false)`; one `RunFileGc`. The segment pass is also part of the
  append-mode compaction pipeline; non-append segment compaction is not a
  supported mode.
- **TTL cleanup** (`CleanExpiredKeys`): when `RootMeta::next_expire_ts_` has
  passed, scan the TTL tree range `[0, now]`, delete expired keys from both
  trees.
- **File GC** (append mode only) — `FileGcRequest` selects cloud or local
  collection from the store mode. It runs after amplification-triggered
  compaction and is queued after Drop only when `data_append_mode` is enabled.
  `LocalGcRequest` is a separate local-only operation used when reopen must
  discard cached state without deleting remote objects.
- **CreateArchive / CreateBranch / DeleteBranch** — manifest-level operations
  (doc 06) executed under the write lock for the partition.

`ArchiveCrond` (its own thread) periodically submits archive requests for all
owned partitions at `archive_interval_secs`, bounded by `max_archive_tasks`.

## File garbage collection (`include/file_gc.h`, `src/file_gc.cpp`)

Deletes data/segment files no longer referenced by any manifest, archive, or
in-flight write. Two drivers, same rules:

- `ExecuteLocalGC` (local/standby modes, plus explicitly local-only cleanup in
  cloud mode) — lists the partition directory.
- `ExecuteCloudGC` (cloud mode) — lists the partition's objects; deletions
  propagate to the local cache (`ScheduleLocalFileCleanup`, then directory
  cleanup when the partition empties).

Inputs assembled per run:

1. **Retained sets** — walk the live mapping tables (`GetRetainedFiles`) and
   every on-disk/cloud manifest *including archives*
   (`AugmentRetainedFilesFromBranchManifests`) collecting
   `RetainedFileKey{file_id, branch, term}` for data and segment files.
2. **BranchGuards** — per-branch in-flight write protection:
   `{newest_term, least_unflushed_file_id, least_unflushed_seg_file_id}`
   derived from `RootMeta::first_unflushed_*_fp_id_` (active branch) and
   manifest file ranges, maxed together. A file whose term ≥ the branch's
   newest term and whose id ≥ the watermark may belong to a write that hasn't
   flushed its manifest yet — never delete it. The `>=` (not `==`) term
   comparison protects against a concurrent primary that bumped its term but
   hasn't flushed a manifest. Callers pre-seed the guard for
   (active branch, process term) so the very first un-manifested file is safe.
3. Files not in the retained set and not guard-protected are deleted; old
   archives beyond `num_retained_archives` are pruned (`DeleteOldArchives`).

File identity is the (id, branch, term) triple — sibling branches can reuse
ids (doc 06).

## Prewarm (`src/tasks/prewarm_task.cpp`)

Cloud mode + `prewarm_cloud_cache`: at startup `PrewarmService` lists the
bucket (respecting `partition_filter`), feeds candidate files through each
shard's bounded `prewarm_queue_`, and `prewarm_task_count` `Prewarmer` tasks
per shard download them into the local cache until space or listing is
exhausted; foreground load always preempts (prewarm runs only when the shard
is otherwise idle — `NeedPrewarm`/`RunPrewarm` in the work loop).
