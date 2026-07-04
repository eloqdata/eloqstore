# Runtime and Lifecycle

Source: `include/eloq_store.h`, `src/eloq_store.cpp`, `include/kv_options.h`,
`src/kv_options.cpp`, `include/storage/shard.h` (lifecycle parts).

## Ownership graph

`EloqStore` is the root owner of every long-lived runtime object:

```
EloqStore
├── KvOptions options_            (validated + rewritten at Start)
├── root_fds_                     (one O_DIRECTORY fd per store_path)
├── CloudStorageService           (cloud mode only; HTTP worker threads)
├── StandbyService                (standby modes only; rsync supervisor thread)
├── Shard × num_threads
│   ├── worker thread + WorkLoop
│   ├── TaskManager               (pooled KvTask coroutines)
│   ├── AsyncIoManager            (mode-specific subclass, owns io_uring)
│   ├── PagesPool                 (aligned page buffers)
│   ├── PageManager               (index-page cache + RootMetaMgr)
│   └── GlobalRegisteredMemory*   (optional, zero-copy large values)
├── ArchiveCrond                  (optional periodic archiver thread)
├── PrewarmService                (optional cloud-cache warmup)
└── EloqStoreModule               (ELOQ_MODULE_ENABLED embedding bridge)
```

A comment in `eloq_store.h` records one non-obvious ordering constraint:
`cloud_service_` is declared before `shards_` because shard teardown triggers
async HTTP cleanup that calls back into `CloudStorageService`.

There is also a process-global `inline EloqStore *eloq_store` and a
thread-local `inline thread_local Shard *shard` (`include/tasks/task.h`).
Task code reaches its shard, options, and io manager through these — which is
why **one process hosts at most one running EloqStore** and why task code must
run on a shard thread.

## Status state machine

`EloqStore::Status`: `Stopped → Starting → Running → Stopping → Stopped`.
`Start()` CAS-transitions Stopped→Starting and rejects concurrent or repeated
starts. `SendRequest` accepts work only while `Running`.

## Start() sequence

`EloqStore::Start(branch, term, partition_group_id)` —
the order is load-bearing; later steps assume earlier ones:

1. `ValidateOptions(options_)` — sanity checks plus *rewrites* (e.g. in cloud
   mode `max_write_concurrency == 0` is rewritten to `max_cloud_concurrency`;
   store-path weight suffixes `path:w1,w2` are parsed out).
2. Derive `StoreMode`; construct `CloudStorageService` or `StandbyService`
   (and destroy whichever no longer applies).
3. `InitStoreSpace()` — create/open every `store_path` root directory, hold
   `root_fds_`, build `store_path_lut` (partition→path placement honoring disk
   capacity or explicit weights).
4. Record identity: `term_` (forced 0 in Local mode), `branch_`,
   `partition_group_id_` (cloud only).
5. Compute per-shard fd budget: `fd_limit` clamped to process
   `RLIMIT_NOFILE` headroom minus `num_reserved_fd` (100), divided by
   `num_threads`. Fails with `OpenFileLimit` if insufficient.
6. Construct + `Init()` every `Shard` (allocates queues; the heavy I/O
   bootstrap happens later on the shard's own thread).
7. Start remote service: `CloudStorageService::Start()` then **block** on its
   bootstrap (verifies bucket, CAS-upserts the `CURRENT_TERM_<branch>_<pg_id>`
   object; a stale term fails the whole `Start`). Or `StandbyService::Start()`.
8. `Shard::Start()` for each shard — spawns worker threads (normal build) or
   marks shards runnable (module build).
9. Module build only: register `EloqStoreModule` and notify workers.
10. Start `ArchiveCrond` if `data_append_mode && num_retained_archives > 0 &&
    archive_interval_secs > 0`.
11. Start `PrewarmService` if `prewarm_cloud_cache` and mode is Cloud.
12. Publish `Status::Running`.

Any failure routes through `fail_start` → `CleanupRuntime(started_shards)`, so
partial startups unwind in the same order as full shutdown.

## Stop() / CleanupRuntime sequence

Inverse dependency order — the goal is that no late completion touches freed
state:

1. `NotifyStoreStopping()` on every shard's io manager (stops ingest of new
   background work, lets `WorkLoop` observe shutdown).
2. Stop `PrewarmService`, then `ArchiveCrond` (background request *sources*
   stop before request *executors*).
3. Module build: flag shards `Stopping`, wait until each reports `Stopped`,
   unregister the module.
4. `Shard::Stop()` for each shard — joins the worker thread; the thread's exit
   path runs `TaskManager::Shutdown()` then `PageManager::Shutdown()` then
   `io_mgr_->Stop()` *on the shard thread* (these structures assume
   shard-thread affinity even during teardown).
5. Close `root_fds_`.
6. Stop `StandbyService` / `CloudStorageService` — after shards, so in-flight
   shard tasks waiting on remote completions can finish first.

## KvOptions

`include/kv_options.h` documents every field; highlights that shape behavior:

- **Persisted-after-first-use options** (the struct marks them explicitly):
  `store_path`, `cloud_store_path`, `data_page_size`, `pages_per_file_shift`,
  `overflow_pointers`, `data_append_mode`, `enable_compression`, etc. Changing
  them on an existing store is invalid.
- **Topology**: `num_threads` (shards), `fd_limit`, `io_queue_size`,
  `buffer_pool_size` (index-page cache per shard; data pages share it only if
  `enable_data_page_cache`), `root_meta_cache_size` (global RootMeta LRU).
- **Write shaping**: `max_write_concurrency`,
  `write_buffer_size`/`write_buffer_ratio` (append-mode aggregation),
  `manifest_limit` (snapshot-rotation threshold).
  (`max_write_batch_pages` is deprecated and ignored — superseded by the
  `max_inflight_write` IO budget, doc 07.)
- **IO QoS** (doc 07, `docs/design/io_qos.md`): `max_inflight_read`
  (device-calibrated read queue-depth cap), `bg_read_ratio` (background
  read sub-budget, the tail-predictability policy knob),
  `max_inflight_write` (write queue-depth cap; also sizes the write
  request pools).
- **Cloud**: `cloud_provider/endpoint/region/keys`, `local_space_limit` +
  `reserve_space_ratio` (cache budget), `max_cloud_concurrency`,
  `cloud_request_threads`, `allow_reuse_local_caches`, `prewarm_cloud_cache`.
- **Compaction/GC**: `file_amplify_factor`, `segment_file_amplify_factor`,
  `num_retained_archives`, `archive_interval_secs`.
- **Zero-copy / large values** (doc 09): `segment_size`,
  `global_registered_memories` XOR `pinned_memory_chunks`,
  `gc_global_mem_size_per_shard`, `pinned_tail_scratch_slots`,
  `segments_per_file_shift`.
- `partition_filter` — predicate telling this instance which partitions it
  owns (drives prewarm/global-op scoping in multi-instance deployments).
- `LoadFromIni(path)` parses the same fields from an `.ini` file (used by
  benchmarks/tools; see `benchmark/*.ini`).

## Metrics

With `ELOQSTORE_WITH_TXSERVICE` defined, per-shard `metrics::Meter` objects
record request latency/counters and round durations (`InitializeMetrics`).
Without it, only the data-page-cache hit/miss counters on `EloqStore` exist.
