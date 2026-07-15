# Request API

Source: `include/eloq_store.h`, `src/eloq_store.cpp` (request handling +
fanout), `src/storage/shard.cpp` (`ProcessReq`), `include/error.h`.

## Submission and completion contract

Callers allocate a `KvRequest` subclass, fill its inputs, and submit:

- `ExecSync(req)` — blocks via `req->Wait()`. Only legal when no callback is
  set (`Wait()` CHECKs `callback_ == nullptr`). If the store is not `Running`,
  the request completes immediately with `KvError::NotRunning`.
- `ExecAsyn(req)` / `ExecAsyn(req, user_data, callback)` — returns `false` if
  the store rejected the request (not running); otherwise the callback fires
  exactly once on completion.

`KvRequest::SetDone(err)` is the **single completion gate**: it either invokes
the async callback or wakes the sync waiter — never both. Completion happens
on the owning shard's thread (or on the store thread for immediate
rejections), so callbacks must be cheap and must not block.

Input lifetime: `ReadRequest`, `FloorRequest`, `ScanRequest`,
`TruncateRequest` accept either borrowed `string_view`s or owned `std::string`
storage. Borrowed inputs must stay alive until the request completes.

The request object itself is caller-owned and must outlive completion; the
engine never frees a `KvRequest`.

## Request taxonomy

`RequestType` ordering is semantic: `KvRequest::ReadOnly()` is implemented as
`Type() < RequestType::BatchWrite`. **Inserting or reordering enum values
changes read/write classification** — keep all read-only types before
`BatchWrite`.

| Category | Requests | Notes |
|----------|----------|-------|
| Point/range reads | `ReadRequest`, `FloorRequest`, `ScanRequest` | `ReadRequest.large_value_dest_` variant selects metadata-only / zero-copy IoStringBuffer / pinned-memory destination (doc 09). `ScanRequest` is iterative: `HasRemaining()` ⇒ resubmit with begin = previous end; pagination via `SetPagination`; prefetch clamped to `max_read_pages_batch`. Scans reject very-large values with `LargeValueUnsupported`. |
| Remote listing | `ListObjectRequest`, `ListStandbyPartitionRequest` | Synchronous helpers bridging to cloud/standby services; `ListObjectRequest` paginates via continuation tokens. |
| Partition-scoped writes (`WriteRequest` subclasses) | `BatchWriteRequest`, `ReopenRequest`, `TruncateRequest`, `DropRequest`, `ArchiveRequest`, `CompactRequest`, `LocalGcRequest`, `CleanExpiredRequest`, `CreateBranchRequest`, `DeleteBranchRequest` | Serialized per partition through the shard's pending-write queue (doc 04). `WriteRequest::next_` is an intrusive link used both by that queue and by callers managing free lists. |
| Store-level fanout | `DropTableRequest`, `GlobalArchiveRequest`, `GlobalReopenRequest`, `GlobalListArchiveTagsRequest`, `GlobalCreateBranchRequest` | Handled in `EloqStore::SendRequest` *before* shard dispatch: they discover the affected partitions, materialize per-partition subrequests, fan them out (throttled by `max_global_request_batch` / `max_archive_tasks`), keep the first error, and complete when all subrequests do. |

Semantics worth knowing:

- **`BatchWriteRequest`** — atomic multi-key upsert/delete batch
  (`WriteDataEntry{key, val, large_val_, timestamp, op, expire_ts}`) against
  one partition. An entry whose timestamp is older than the stored version is
  silently skipped.
- **`ReopenRequest`** — discard the in-memory state of a partition and replay
  its manifest; in cloud/standby mode this is how a node adopts a newer
  remote snapshot. `SetTag()` reopens from an archive instead of the live
  manifest. `SetClean(true)` tolerates a missing remote snapshot by
  installing an empty one.
- **`TruncateRequest`** — delete all keys ≥ position (used for log-style
  tables).
- **`ArchiveRequest` / `GlobalArchiveRequest`** — create (or delete, via
  `Action`) a tagged manifest archive; `GlobalArchiveRequest::ResultArchive()`
  returns the generated archive name.
- **`GlobalCreateBranchRequest`** — forks every owned partition onto a new
  branch; the engine salts the user-supplied name (`SetSaltTimestamp` makes
  the salt deterministic) and returns it via `ResultBranch()`.

## Routing

`EloqStore::SendRequest`:

1. Reject unless `Running`.
2. Global fanout types are handled at store level (above).
3. Everything else: `shards_[req->TableId().ShardIndex(shards_.size())]
   ->AddKvRequest(req)` — a lock-free MPSC enqueue onto the shard's request
   queue. The shard converts requests to tasks in `Shard::ProcessReq` (doc 04).

## Error model and automatic retries

`KvError` (`include/error.h`) is a flat enum; `ErrorString()` and
`IsRetryableErr()` (OpenFileLimit/Busy/TryAgain) are defined alongside.

Two retry loops live *inside* the engine (`Shard::StartTask` epilogue +
`OnTaskFinished`):

- **Auto-reopen retry** — if a task fails with `KvError::ResourceMissing` in
  Cloud/StandbyReplica mode and the request type opts in
  (`AutoReopenRetry()`: Read/Floor/Scan), the shard parks the request in a
  per-table `PendingReopenState` and arms an internal `ReopenRequest` that
  executes after `auto_reopen_pending_time_us` (a delayed heap absorbs
  request storms into one reopen); parked requests are re-run after the
  reopen completes, up to `auto_reopen_retry_times` each. This is how
  readers transparently follow a primary that has advanced the partition's
  term. Only then is `SetDone` deferred; otherwise the error surfaces
  directly. The internal request's pending time is shard-private
  (`ReopenRequest::SetPendingTime` is not public API), and only the
  internal reopen request's own completion tears the state down unless an
  external `ReopenRequest` for the same table reaches the write slot first. In
  that case the external reopen replaces the internal driver in
  `PendingReopenState`; the stale delayed entry is skipped later, and the
  parked requests are completed from the external reopen result.
- **OOM retry** — a task aborted by `KvError::OutOfMem` (e.g. buffer pool
  exhaustion from pinned pages) is re-enqueued at the back of the shard queue
  up to `auto_oom_retry_times`, giving other tasks a chance to release pins.

`ExpiredTerm` is terminal by design: it means another process owns a newer
term and this writer must stop (doc 06).
