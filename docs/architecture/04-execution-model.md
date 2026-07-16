# Execution Model

Source: `include/storage/shard.h`, `src/storage/shard.cpp`,
`include/tasks/task.h`, `src/tasks/task.cpp`, `include/tasks/task_manager.h`,
`src/tasks/task_manager.cpp`.

## The one rule

**Shard-thread affinity replaces locking.** Each shard owns one worker thread;
all task state, page caches, FD tables, mappings, and pools belonging to that
shard are mutated only from that thread. Asynchronous completions produced on
other threads (cloud HTTP workers, standby supervisor) are *marshalled back*
to the shard (via concurrent queues drained in the shard loop) and only there
resume tasks. Any new async backend must follow this pattern; resuming a
coroutine from a foreign thread is a correctness bug even if it appears to
work.

The thread-local `shard` pointer (`include/tasks/task.h`) plus helpers
`ThdTask()`, `IoMgr()`, `Options()`, `Comp()` are how deep call stacks reach
shard context without parameter plumbing.

## Shard work loop

`Shard::WorkLoop()` (normal build) repeats:

1. `io_mgr_->Submit()` — flush prepared io_uring SQEs.
2. `io_mgr_->PollComplete()` — reap CQEs; each completion either finishes a
   blocked task's I/O (`FinishIo`) or processes a background write request.
   Cloud/standby ready-queues are drained here too.
3. `ExecuteReadyTasks()` — resume coroutines from `ready_tasks_`, then (when
   the normal queue is empty) `low_priority_ready_tasks_` (background
   compaction/GC yield here to protect foreground latency).
4. Dequeue up to 128 new `KvRequest`s from the MPSC `requests_` queue
   (blocking with 100 ms timeout only when fully idle) and feed each to
   `OnReceivedReq`.

Exit: when the store is stopping and the shard is idle. Teardown runs on the
shard thread: `TaskManager::Shutdown()` → `PageManager::Shutdown()` →
`io_mgr_->Stop()` (tasks may hold page pins, so task state dies first).

In the module build (`ELOQ_MODULE_ENABLED`, doc 10) the same logic is exposed
as `WorkOneRound()` and an external runtime drives it; `IsIdle()` tells the
runtime whether the shard needs another round.

## Tasks are pooled coroutines

`KvTask` (`include/tasks/task.h`) wraps a `boost::context::continuation` with
status (`Idle/Ongoing/Blocked/BlockedIO/Finished`), inflight-I/O counters, and
an intrusive `next_` pointer. Stack size is `KvOptions::coroutine_stack_size`
(32 KiB default; protected stacks in debug builds catch overflows).

Scheduling primitives:

- `Yield()` / `YieldToLowPQ()` — park the coroutine and re-enqueue it on the
  (low-priority) ready queue; used to cooperate inside long loops.
- `WaitIo()` / `FinishIo()` — park until `inflight_io_` completions arrive;
  the completion path enqueues the task back on the ready queue.
- `WaitIoResult()` — `WaitIo` + return the single completion's result code.
- `WaitingZone` / `WaitingSeat` / `Mutex` — intra-shard wait lists (no real
  locks; they park/wake coroutines). Used for FD open/close exclusion, pool
  exhaustion waits, upload completion, etc.
- IO-budget waits (`IoBudget::Acquire`, `async_io_manager.h`) — tasks park on
  a budget's `WaitingZone` when admitting their page IO would exceed the
  shard's in-flight read/write cap (`max_inflight_read` /
  `max_inflight_write`); `PollComplete` releases per CQE and wakes waiters,
  so release never depends on the blocked task being scheduled. Background
  tasks (`KvTask::IsBackground()`: BatchWrite, BackgroundWrite, EvictFile,
  Prewarm) are additionally confined to a read sub-budget (`bg_read_ratio`)
  and wait on a separate FIFO zone. From a background acquisition's first wait
  through admission (including the wake-to-admit gap), its unused sub-budget is
  reserved from new foreground admissions. Release wakes background first and
  always wakes foreground; both classes re-check admission, so neither can
  starve the other. See `docs/design/io_qos.md` (M1/M2); the acquire order is
  FD/mutex → pools/buffers → budget → SQE, with no voluntary yield after budget
  admission and an equal-cost release per CQE.

`TaskManager` keeps one free-list pool per task type (`BatchWriteTask`,
`BackgroundWrite`, `ReadTask`, `ScanTask`, `ListObjectTask`,
`ListStandbyPartitionTask`, `ReopenTask`). Pools grow on demand except the
write pools, which are bounded — `GetBatchWriteTask` returning `nullptr` is
the write-concurrency backpressure mechanism (`max_write_concurrency`).
`NumActive()` feeds the idle check; `AddExternalTask/FinishExternalTask`
account for background tasks (file cleaner, prewarmers) that live outside the
pools.

## Request → task dispatch

`Shard::OnReceivedReq(req)`:

- **Read-only requests** start immediately: `ProcessReq` grabs a pooled task
  and `StartTask` runs the coroutine body (first slice runs inline until it
  blocks on I/O).
- **Write requests** (`!ReadOnly()`) are appended to the partition's
  `PendingWriteQueue` and started by `TryStartPendingWrite` only if that
  partition has no running write. **At most one write task runs per partition
  at any time** — this is the engine's write-serialization point, sitting
  above the coroutine layer. If the task pool is exhausted, the request stays
  queued and `TryDispatchPendingWrites` retries when any write finishes.

Each `PendingWriteQueue` also embeds singleton internal requests
(`compact_req_`, `local_gc_req_`, `expire_req_`): background maintenance for a
partition is queued exactly like a user write and therefore serializes with
user writes (see `AddPendingCompact` / `AddPendingTTL` / `AddPendingLocalGc`).

`StartTask`'s epilogue (in `include/storage/shard.h`) centralizes task
completion: abort handling, OOM retry, auto-reopen retry, `SetDone`, and
metrics. `OnTaskFinished` releases the partition's write slot, frees the task
to its pool, and re-dispatches pending writes.

## Blocking I/O from a task's perspective

A task never calls a syscall directly. It calls `AsyncIoManager` methods
(`ReadPage`, `WritePage`, `AppendManifest`, …) which prepare SQEs tagged with
the task pointer, increment `inflight_io_`, and `WaitIo()`. The shard loop
submits, polls, and resumes the task with results in `io_res_`/`io_flags_`.
Cloud and standby operations look identical to the task: submit job → 
`WaitIo()` → resumed by the shard after the remote thread pushes a completion
onto the shard's ready queue.
