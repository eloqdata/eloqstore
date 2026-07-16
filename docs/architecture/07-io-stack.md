# I/O Stack

Source: `include/async_io_manager.h`, `src/async_io_manager.cpp`,
`include/storage/object_store.h`, `src/storage/object_store.cpp`,
`include/storage/cloud_backend.h`, `src/storage/cloud_backend.cpp`,
`include/cloud_storage_service.h`, `src/cloud_storage_service.cpp`,
`include/standby_service.h`, `src/standby_service.cpp`,
`include/direct_io_buffer.h`.

## Layering, not alternatives

`AsyncIoManager` is the shard-facing storage interface. One instance per
shard, chosen by `AsyncIoManager::Instance` from the store mode:

```text
AsyncIoManager (abstract: ReadPage/WritePage/ReadSegments/Manifest ops/...)
├── MemStoreMgr        store_path empty — pages and manifests in RAM (tests)
└── IouringMgr         local filesystem over io_uring — the substrate
    ├── CloudStoreMgr  + object storage sync; local files become a cache
    └── StandbyStoreMgr + rsync pull-through from a master
```

The two remote managers *extend* the local one: local manifest and page
semantics stay identical, which is what keeps recovery (doc 06) uniform
across modes. New backends should subclass within this contract and must
route completions back through the owning shard (doc 04).

## IouringMgr (local substrate)

Owns the shard's `io_uring` (`io_queue_size` entries, DEFER_TASKRUN; a forced
`io_uring_enter` every 10 empty polls works around a CQE-delivery window that
otherwise deadlocks at high read concurrency — see the comment at
`kForceSubmitEveryNoOps`).

Responsibilities:

- **Page I/O** — `ReadPage`/`ReadPages` (batched, into pool buffers, fixed
  reads when the buffer is registered), `WritePage`. `ConvFilePageId` splits a
  `FilePageId` into `(file_id, offset)` by `pages_per_file_shift`.
- **In-flight page-IO budgets** (`IoBudget`, see `docs/design/io_qos.md`
  M1/M2) — two per-shard counters in configured `data_page_size` units with
  independent caps:
  `read_budget_` (`max_inflight_read`; `ReadPage`/`ReadPages`, per-page
  acquisition so a batch larger than the cap cannot deadlock) and
  `write_budget_` (`max_inflight_write`; `WritePage` cost 1,
  `SubmitMergedWrite` cost `bytes / data_page_size`). Budget is acquired
  immediately before SQE prep and released per CQE in `PollComplete`, which
  distinguishes budgeted page reads from metadata ops via the
  `KvTaskPageRead`/`BaseReqPageRead` user-data types. A cap of 0 disables a
  budget; a single request costlier than the cap is admitted alone once the
  budget drains. Metadata, manifest, bulk file/snapshot paths (`ReadFile`,
  `ReadFilePrefix`, `WriteSnapshot`), `Fdatasync`, and segment IO are exempt.
  With `enable_data_page_cache`, `max_inflight_write` also bounds cached-page
  pins retained by write promotion until the corresponding IO completes.
  The read budget carries a **background sub-budget** (`bg_read_ratio`
  percent of `max_inflight_read`): budgeted page reads from `BatchWrite` and
  `BackgroundWrite` (compaction) tasks are additionally bounded by it, so they
  cannot crowd foreground point reads out of the device queue. `EvictFile` and
  `Prewarm` are background task types, but local-GC `ReadFile` and
  prewarm/download whole-file bulk IO remain exempt. Foreground may use the
  entire read budget while background has no pending demand. Once a background
  acquisition enters the wait path, its unused sub-budget stays reserved
  through admission, including the wake-to-admit gap. Each class waits on its
  own FIFO zone; release wakes background first and always wakes foreground.
  The write budget has no split — all page writes come from write tasks, i.e.
  background.
  `GetIoQosStats()` (also surfaced as `EloqStore::GetIoQosStats(shard_id)`)
  exposes in-flight/high-watermark/blocked counters (total read, bg-read slice,
  write) plus write-path fdatasync count and latency. The blocked fields are
  per admission class: `read_` counts foreground waits, `bg_read_` background
  waits, and `write_` all write waits.
- **FD cache** — `LruFD` per (partition, `TypedFileId`), doubly-linked LRU
  bounded by the shard's fd budget; `EvictFD` closes idle descriptors.
  Open/close exclusion per FD via a coroutine `Mutex`. Data files open with
  `O_DIRECT`. Directory FDs and the manifest FD are cached under sentinel
  TypedFileIds. `LruFD::Ref` is the RAII reference; eviction skips referenced
  FDs.
- **Manifest ops** — `AppendManifest`, `SwitchManifest` (write temp +
  rename), `GetManifest` (buffered reader), archive/branch-manifest
  create/delete, `DropManifest`.
- **Append-mode merged writes** — small page writes destined for the same
  file tail are aggregated in `write_buf_` slots (`WriteBufferAggregator`,
  `SubmitMergedWrite`) into ~`write_buffer_size` chunks, cutting SQE and
  device-write counts.
- **Branch bookkeeping** — the authoritative in-memory
  `branch_file_mapping_` per partition (Get/SetBranchFileIdTerm,
  SetBranchFileMapping) with interned branch-name storage.
- **Registered buffers** — page-pool registration (fixed I/O), plus the
  zero-copy bootstrap (`BootstrapRing` with `GlobalMemoryConfig`): registers
  caller `GlobalRegisteredMemory` chunks or KV-cache pinned chunks, the
  private GC pool, and the pinned-write tail-scratch pool (doc 09).
- `ReadSegments`/`WriteSegments` — batched fixed I/O on segment files
  (doc 09).

## CloudStoreMgr (cloud overlay)

Local disk = bounded LRU cache of cloud objects; object store = durable copy.

- **Upload path**: as append-mode writes flush, `OnFileRangeWritePrepared`
  captures the written ranges into the task's `UploadState`; sealing a file
  (`OnDataFileSealed`) or committing (manifest append) uploads the
  file/manifest via `ObjectStore` upload tasks. A write request completes
  only after its uploads are acknowledged (`WaitPendingUploads`), so a
  completed write is durable in the object store, not merely on local disk.
  Cloud mode skips `fdatasync` entirely (local files are re-downloadable).
- **Download path**: a read missing its local file (`OpenFile` local miss)
  triggers `DownloadFile` into the cache and retries; `GetManifest` can fetch
  the manifest object; `RefreshManifest(tag)` pulls a newer/archived manifest
  before replay (used by reopen and auto-reopen, doc 03).
- **Cache management**: every cached file is tracked (`closed_files_` LRU,
  `used_local_space_` vs per-shard slice of `local_space_limit`).
  `ReserveCacheSpace` evicts closed files (the background `FileCleaner` task)
  before creating/downloading; "evicting path" keys serialize concurrent
  open-vs-unlink on the same filename. Partition directory removal is
  coordinated with directory-FD busy counts (`RegisterDirBusy` /
  `pending_dir_cleanup_`).
- **Warm restart**: with `allow_reuse_local_caches`,
  `RestoreLocalCacheState()` walks partition dirs at Init, re-registers
  reusable files into the LRU (removing `*.tmp` strays and empty dirs) so a
  restart doesn't re-download the working set.
- **Concurrency limits**: cloud ops take a slot (`AcquireCloudSlot`,
  `max_cloud_concurrency`) and a pooled `DirectIoBuffer`
  (`AcquireCloudBuffer`); both park the coroutine when exhausted.
- **Terms**: `ReadTermFile`/bootstrap CAS on `CURRENT_TERM_*` (doc 06);
  `process_term_`/`partition_group_id_` stamp uploads and validate manifests.

## Cloud transport

Three layers, crossing the thread boundary exactly once each way:

1. **`ObjectStore`** (per shard) — task-facing API; wraps work items as
   `ObjectStore::{Download,Upload,List,Delete}Task` tagged with the owning
   `KvTask` and shard.
2. **`CloudStorageService`** (per store) — `cloud_request_threads` worker
   threads, each running a curl multi loop; jobs from a given shard always go
   to the same worker (curl handles are not thread-safe). Also runs the
   bootstrap term-file CAS at Start.
3. **`AsyncHttpManager`** (per shard, driven by the service worker) — builds
   signed HTTP requests via the pluggable `CloudBackend`
   (`include/storage/cloud_backend.h`; AWS S3 / GCS variants selected by
   `cloud_provider`), handles retry with backoff and error classification
   (`Timeout` vs `CloudErr` vs `OssInsufficientStorage`…), parses list
   responses.

Completion: the worker thread never touches task state; it enqueues the
finished `ObjectStore::Task` onto the owning shard's
`cloud_ready_tasks_` queue (`EnqueueCloudReadyTask`); the shard loop drains it
and resumes the waiting coroutine.

## StandbyStoreMgr + StandbyService (standby overlay)

`StandbyStoreMgr` fills local misses by *pulling from the master* instead of
an object store. `StandbyService` runs a single supervisor thread that spawns
bounded (`standby_max_concurrency`) rsync/ssh child processes (pidfd+epoll
supervision), with job types: rsync a partition's files, prepare/stage a
remote manifest (staged to temp + atomic rename before replay), list remote
partitions. Per-partition in-flight dedup; completions flow back through
per-shard ready queues (`ProcessReadyTasks`). Master address/store-paths are
hot-updatable (`UpdateStandbyMasterAddr/StorePaths`). A master in
`StandbyMaster` mode is just a local store whose directory layout replicas
may rsync.

## DirectIoBuffer

`include/direct_io_buffer.h`: page-aligned, size-reserved buffers used for
non-page I/O (uploads, downloads, manifest snapshots, GC file reads), pooled
per shard (`direct_io_buffer_pool_size`, `non_page_io_batch_size` batching).
