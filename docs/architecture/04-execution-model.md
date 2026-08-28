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

1. `io_mgr_->Submit()` — refill the device rate budget (`RefillAndWake`, its
   only wake source) and enter the kernel. The ring uses
   `IORING_SETUP_DEFER_TASKRUN`, so this entry is also what delivers CQEs.
2. `io_mgr_->PollComplete()` — reap CQEs (pure user space: `peek_cqe` +
   `for_each_cqe`); each completion either finishes a blocked task's I/O
   (`FinishIo`) or processes a background write request. Cloud/standby
   ready-queues are drained here too.
3. `PromoteReadyDelayedReopenRequests()`.
4. Dequeue up to 128 new `KvRequest`s from the MPSC `requests_` queue
   (blocking with 100 ms timeout only when fully idle) and feed each to
   `OnReceivedReq`.
5. `ExecuteReadyTasks()` — resume coroutines from `ready_tasks_`, then (when
   the normal queue is empty) `low_priority_ready_tasks_` (background
   compaction/GC yield here to protect foreground latency).
6. `io_mgr_->FlushSubmit()` — issue the SQEs this round prepared.

**The order is load-bearing.** Four producers feed `ready_tasks_` — rate-budget
grants (step 1), I/O completions (step 2), delayed reopens (step 3), and new
requests (step 4) — and all of them are placed before the single
`ExecuteReadyTasks`, so work admitted in a round runs in that same round.
`FlushSubmit` then closes the loop on the output side: without it, SQEs
prepared in step 5 would wait for the *next* round's `Submit`, which in module
mode is a full external scheduling quantum of dead device time on every I/O
hop. `FlushSubmit` deliberately does not touch `consecutive_skipped_submits_`
(the DEFER_TASKRUN forced-enter safety net stays owned by `Submit`) and is a
no-op when the round prepared nothing.

Exit: when the store is stopping and the shard is idle. Teardown runs on the
shard thread: `TaskManager::Shutdown()` → `PageManager::Shutdown()` →
`io_mgr_->Stop()` (tasks may hold page pins, so task state dies first).

In the module build (`ELOQ_MODULE_ENABLED`, doc 10) the same logic is exposed
as `WorkOneRound()` (driven by `EloqStoreModule::Process`) and an external
runtime drives it; `IsIdle()` tells the runtime whether the shard needs
another round. It runs the **same step order**, which matters most there: the
gap between rounds is an external scheduling decision rather than a few
microseconds, so both a delayed flush and mis-ordered admission cost a full
quantum. The only structural difference is that the request *dequeue* happens
first because the idle-round test depends on its count; admission
(`OnReceivedReq`) still runs at step 4, after `PollComplete`.

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
- Rate-budget waits (`RateBudget::Acquire`, `async_io_manager.h`) — tasks park
  on a per-class `WaitingZone` when admitting their page IO would drive the
  shard's device rate budget (`disk_rate_limit_iops`/`disk_rate_limit_mbps`,
  M4) non-positive. Wakes are **refill-driven**, not completion-driven: the
  once-per-loop `RefillAndWake` (peek-and-grant — it charges the FIFO head's
  recorded cost before waking it) is the only wake source, so waiter progress
  depends only on the shard loop running, never on another task completing.
  Background tasks (`KvTask::IsBackground()`: BatchWrite, BackgroundWrite,
  EvictFile, Prewarm) draw from the background class share (`rate_bg_ratio`)
  on a separate FIFO zone; foreground may borrow background's idle surplus but
  background never borrows foreground's, so foreground's share is a hard
  guarantee. The optional `max_inflight_io` window is the only occupancy cap
  that releases per CQE. See `docs/design/io_qos.md` (M4); the acquire order is
  FD/mutex → pools/buffers → rate budget → window → SQE, with no voluntary
  yield after admission. (The former `IoBudget` count budgets are retired.)

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
