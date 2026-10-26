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
run on a shard thread. One store is enough: it hosts every data store shard
the process serves (next sections).

## Status state machine

`EloqStore::Status`: `Stopped → Starting → Running → Stopping → Stopped`.
`StartInstance()` CAS-transitions Stopped→Starting and rejects concurrent or
repeated starts. `SendRequest` accepts work only while `Running`, and only
for an attached data store shard (next section).

## Data store shards

The embedder's unit of ownership is a *data store shard*: a set of partitions
hosted as one unit, with its own term (in EloqDB, node group g's data is data
store shard g). One store hosts several at once, each attached and detached
on its own while the others keep serving, so one process runs one store and
one budget (buffer pool, fd limit, local cache space) however many node groups
its node leads. Only cloud mode hosts more than one: data on a node's own
disk has one owner.

- **Table.** `ds_shards_[id]` holds each shard's state (`DsShardState`:
  Detached, Attaching, Attached, Expired, Detaching) and term, read without a
  lock on every request. `AttachShard`, `DetachShard` and `Stop` change it
  under `ds_shard_mux_`, which keeps their multi-field changes consistent when
  the embedder calls them concurrently (an attach's "no other shard is
  hosted" check and its claim; a late attach against `Stop`'s reset).
  `MarkShardExpired` flips one slot with a compare-and-swap. The mutex is
  never held across the term-file wait or the detach's wait for the worker
  shards: in module builds a second caller blocking on it would block a
  host worker pthread that drives a worker shard. Ids are below
  `kMaxDsShards` (1024).
- **Requests name their shard** (`KvRequest::SetDsShard`). `SendRequest`
  resolves a request naming none to the default shard (the one `Start`
  attached, else the first attached) and admits the request while the shard
  is Attached or Expired; any other state refuses it (`NotRunning`). An
  Expired shard's request goes on to its worker shard, which fails it with
  `ExpiredTerm`: completing it inside `SendRequest` would run its callback
  inside the submit call, which the fan-out sweeps do not expect. The worker
  shard checks again when it
  dequeues the request (`Shard::AdmitRequest`), so a detach that started in
  between still refuses it.
- **A request has no term; its task does.** The sender cannot tell when,
  during a request's life, the shard's term changes, so the request does not
  carry one. A request still on its way to its worker shard when the shard is
  detached (a submission not yet returned, or one not yet dequeued) is
  refused if the shard is still detached when the worker shard gets to it,
  and runs in the new term if the shard has been attached again by then. What
  holds is that a task runs entirely in one term: a detach waits for the
  shard's running tasks, and an attach needs the slot Detached, so the
  shard's term never changes under a running task.
- **Terms travel with the task.** When a task starts (`Shard::StartTask`) it
  copies the request's shard id and reads the shard's term from the table
  once (`KvTask::ds_shard_id_`, `KvTask::term_`). Every switch to a task
  (`Shard::SwitchToTask`) records both on the worker shard beside the running
  task (`Shard::running_ds_shard_id_`, `Shard::running_term_`, read through
  `ThdDsShard()` / `ThdTerm()` like `ThdTask()`), and
  `AsyncIoManager::ProcessTerm()` returns that term: no lookup by partition
  anywhere. The internal requests a task queues for
  its partition (compaction, file GC, local GC, TTL cleaning, the reopen
  driver behind parked waiters) are stamped with the task's shard, so they
  are gated and counted like external ones and run in the same term. The fd
  syncs that a file eviction runs from another shard's task name a manifest
  by the fd's own term, as data files always were.
- **`AttachShard(id, term)`** (cloud mode): raises the shard's term file
  `CURRENT_TERM_<branch>_<id>` with a compare-and-swap, fails with
  `ExpiredTerm` if the file holds a newer term, marks the slot Attached,
  starts the shard's archive schedule (below) and queues the shard's prewarm.
  The swap is an ordinary task: `AttachShard` sends one `AttachShardRequest`
  to worker shard `id % num_threads` (bypassing the admission gate, as the
  slot is still Attaching) and waits for it with a bthread-aware wait. The
  `AttachShardTask` takes the shard's term from the slot like any task
  (`StartTask`) and runs `CloudStoreMgr::UpsertTermFile` through that worker's
  object store: read the file and its ETag, then a conditional upload
  (`If-Match` on the ETag, or `If-None-Match: *` when it is missing), reading
  again when a concurrent writer wins (409/412, or 404 on an `If-Match`), up
  to 10 attempts. No thread blocks on HTTP.
  Each partition then takes over lazily on its first open: `GetManifest`
  promotes its manifest to the shard's term, and a manifest of a newer term
  fails that partition's request with `ExpiredTerm` and marks the whole shard
  Expired (`Shard::OnTaskFinished`), since the term file is per shard.
- **`DetachShard(id)`** marks the slot Detaching (the gate), ends the shard's
  archive schedule, then sends one
  `DetachShardRequest` to every worker shard and waits for all of them. On
  each worker the request runs as a `DetachShardTask`: it fails the requests
  queued for the shard's partitions (pending writes, parked reopen waiters,
  delayed reopen drivers), blocks until no task runs on them (the task that
  brings `active_tasks_by_shard_` to 0 resumes it), then `EvictDsShard` drops
  their root metas and cached pages, closes their files and drops their local
  manifests (cloud mode). It finds the partitions without a lookup table:
  of every partition with a root meta (`RootMetaMgr`) or an open file
  (`IouringMgr::PartitionFiles`) on the worker, it drops those
  `KvOptions::partition_filter` gives the detaching shard. The filter must
  still give them when the detach runs, so the embedder detaches a shard
  before reassigning its partitions; a leader change does not reassign
  them. Hosting more than one shard needs the filter; `AttachShard` refuses
  a second shard without it. The cached copies of data files stay:
  a later term starts a new file, so no file of an older term changes, and a
  later attach reuses them.
- **Archives are scheduled per shard.** With scheduled archiving on
  (`archive_interval_secs`), `ArchiveCrond` keeps one schedule per attached
  shard: `AttachShard` starts it, first due one interval later, and
  `DetachShard` ends it before draining, so a shard attached again at a newer
  term starts afresh. One thread waits for the earliest due shard, submits
  that shard's `GlobalArchiveRequest` (named with the shard's term, limited to
  its partitions by `partition_filter`) with a completion callback, and sets
  the shard's next due time one interval after the archive completes. The
  archives of different shards run independently; an Expired or Detaching
  shard's turn is put off one interval. File GC keeps
  `num_retained_archives` per partition, so retention is per shard too.
- **`Stop()`** drops everything wholesale and resets the table.
- **`Start(branch, term, id)`** is `StartInstance(branch)` followed by
  `AttachShard(id, term)`, for embedders that host one shard; a failed attach
  stops the store again. `Term()` and `PartitionGroupId()` report the default
  shard's term and id.

## StartInstance() sequence

`EloqStore::StartInstance(branch)` —
the order is load-bearing; later steps assume earlier ones:

1. `ValidateOptions(options_)` — sanity checks plus *rewrites* (e.g. in cloud
   mode `max_write_concurrency == 0` is rewritten to `max_cloud_concurrency`;
   store-path weight suffixes `path:w1,w2` are parsed out).
2. Derive `StoreMode`; construct `CloudStorageService` or `StandbyService`
   (and destroy whichever no longer applies).
3. `InitStoreSpace()` — create/open every `store_path` root directory, hold
   `root_fds_`, build `store_path_lut` (partition→path placement honoring disk
   capacity or explicit weights).
4. Record `branch_`, which every hosted data store shard shares. Terms are
   per shard (previous section).
5. Compute per-shard fd budget: `fd_limit` clamped to process
   `RLIMIT_NOFILE` headroom minus `num_reserved_fd` (100), divided by
   `num_threads`. Fails with `OpenFileLimit` if insufficient.
6. Construct + `Init()` every `Shard` (allocates queues; the heavy I/O
   bootstrap happens later on the shard's own thread).
7. Start remote service: `CloudStorageService::Start()` or
   `StandbyService::Start()`. No term file is touched here: each
   `AttachShard` raises its shard's.
8. `Shard::Start()` for each shard — spawns worker threads (normal build) or
   marks shards runnable (module build).
9. Module build only: register `EloqStoreModule` and notify workers.
10. Start `ArchiveCrond` if `data_append_mode && num_retained_archives > 0 &&
    archive_interval_secs > 0`; it archives each attached shard on that
    shard's own schedule (see "Data store shards").
11. Start `PrewarmService` if `prewarm_cloud_cache` and mode is Cloud; it
    warms a shard's partitions when the shard is attached.
12. Publish `Status::Running`. No request is admitted until a shard is
    attached.

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
- **Write shaping**: `max_write_batch_pages`, `max_write_concurrency`,
  `write_buffer_size`/`write_buffer_ratio` (append-mode aggregation),
  `manifest_limit` (snapshot-rotation threshold).
- **Cloud**: `cloud_provider/endpoint/region/keys`, `local_space_limit` +
  `reserve_space_ratio` (cache budget), `max_cloud_concurrency`,
  `cloud_request_threads`, `allow_reuse_local_caches`, `prewarm_cloud_cache`.
- **Compaction/GC**: `file_amplify_factor`, `segment_file_amplify_factor`,
  `num_retained_archives`, `archive_interval_secs`.
- **Zero-copy / large values** (doc 09): `segment_size`,
  `global_registered_memories` XOR `pinned_memory_chunks`,
  `gc_global_mem_size_per_shard`, `pinned_tail_scratch_slots`,
  `segments_per_file_shift`.
- `partition_filter(ds_shard_id, tbl_id)` — predicate telling which
  partitions belong to a hosted data store shard; scopes prewarm and the
  global archive, reopen and create-branch sweeps to the shard they run for.
- `LoadFromIni(path)` parses the same fields from an `.ini` file (used by
  benchmarks/tools; see `benchmark/*.ini`).

## Metrics

With `ELOQSTORE_WITH_TXSERVICE` defined, per-shard `metrics::Meter` objects
record request latency/counters and round durations (`InitializeMetrics`).
Without it, only the data-page-cache hit/miss counters on `EloqStore` exist.
