# Refactor: Directory Deletion Coordination

## Problem

Partition-directory cleanup coordination is split across multiple
data structures and protocols, even though it is conceptually the
same shape as file management (a refcount plus a way to wait):

- `dir_busy_counts_` (CloudStoreMgr) — writer refcount per partition.
- `evicting_paths_` (CloudStoreMgr) — in-flight eviction map keyed
  by `FileKey`; the directory case uses a sentinel filename
  (`kDirectoryEvictingPath`) inside this otherwise-file-oriented map.
- `pending_dir_cleanup_` (CloudStoreMgr) — work queue of partitions
  ready to evict (consumed by FileCleaner).
- `closed_file_counts_` + `closed_files_` — local-cache file accounting,
  also used as a proxy for "directory still has files."
- `WriteTask::need_wait_dir_eviction_` and `WriteTask::dir_busy_registered_`
  — per-task flags so the first `OpenOrCreateFD` waits if a dir
  eviction was in flight at task start.

Three overlapping concepts ("busy" / "evicting" / "has files") live
in three data structures plus per-task flags. The pattern doesn't
mirror how files are managed, and the special-case handshake for
the directory inside `evicting_paths_` keeps surfacing in unrelated
code paths (e.g. `EvictingPathKey`'s sentinel branch).

## Proposed design

A single per-partition `DirState` value:

```cpp
struct DirState
{
    uint32_t    active_count_{0};   // shared count: active readers + writers
    bool        evicting_{false};   // unlinkat in flight
    WaitingZone openers_waiting_;   // openers wait while evicting_ is true
    WaitingZone evictor_waiting_;   // evictor waits while active_count_ > 0
};
absl::flat_hash_map<TableIdent, DirState> dir_states_;
```

This subsumes:

| Today                                           | Proposed                                                  |
|-------------------------------------------------|-----------------------------------------------------------|
| `dir_busy_counts_[tbl_id]`                       | `DirState::active_count_`                                 |
| Directory case in `evicting_paths_` + sentinel   | `DirState::evicting_` + `DirState::openers_waiting_`      |
| `WriteTask::need_wait_dir_eviction_` flag        | Implicit: openers wait inside `OpenOrCreateFD` when needed |
| `WriteTask::dir_busy_registered_` flag            | Implicit: open path bumps/decrements `active_count_`      |
| `RegisterDirBusy` / `UnregisterDirBusy` / `IsDirEvicting` / `HasDirBusy` | Inlined into the open / cleanup paths |

`pending_dir_cleanup_` stays. It is a scheduling mechanism, not
coordination state, and mirrors how `pending_gc_cleanup_` and the
LRU file list already feed the FileCleaner.

`evicting_paths_` stays for files only; the directory branch in
`EvictingPathKey` and its sentinel filename go away.

## Protocol

### `OpenOrCreateFD` (file)

1. `DirState &s = dir_states_[tbl_id]` (insert if absent).
2. While `s.evicting_`: `s.openers_waiting_.Wait()`.
3. `s.active_count_++`.
4. Cache-hit fast path / slow path against `tables_[tbl_id].fds_` —
   unchanged from today.
5. On return (or error): `s.active_count_--`. If it drops to zero
   *and* the partition is now cleanup-eligible (no entries in
   `fds_`, no entries in `closed_files_` for `tbl_id`), enqueue
   `tbl_id` into `pending_dir_cleanup_` (or wake `s.evictor_waiting_`
   if a cleaner is already waiting).

### `OpenOrCreateFD` (directory, i.e. `LruFD::kDirectory`)

Operates on `fds_` only — opens or creates the directory FD. Does
**not** touch `dir_states_`.

This matters because file open paths nest into the directory open
path (today's `OpenOrCreateFD(tbl_id, kDirectory, …)` at
[`async_io_manager.cpp:1531`](../src/async_io_manager.cpp#L1531)
on the create-on-ENOENT branch). The outer file open already holds
the count for `tbl_id`; the inner directory open is just an FD-cache
lookup against a different map.

### Cleanup (FileCleaner)

For each `tbl_id` popped from `pending_dir_cleanup_`:

1. Re-check eligibility: `s.active_count_ == 0` and no tracked
   local files for `tbl_id`. If ineligible: drop and return.
   This matches how the LRU file eviction loop pops a candidate
   and re-checks `IsEvictingKey` / `pending_gc_cleanup_.contains`
   before committing.
2. Set `s.evicting_ = true`.
3. Submit `unlinkat` (yields).
4. On completion: clear `s.evicting_`, call `s.openers_waiting_.WakeAll()`,
   and (optionally) erase `dir_states_[tbl_id]` if both wait queues
   are empty.

### `WriteTask`

The `Reset` / `Abort` / `ReleaseDirBusy` plumbing simplifies. The
two flags (`need_wait_dir_eviction_`, `dir_busy_registered_`) are
removed; the open path itself drives `active_count_` correctly.

## Pros

1. **Conceptual unification.** Directory state mirrors file state —
   refcount plus a wait primitive in both. New contributors learn
   one pattern.
2. **One source of truth per partition.** "is busy" / "is evicting"
   / "has waiters" all live on the same struct. No drift between
   `dir_busy_counts_` and `evicting_paths_`'s sentinel entry.
3. **`evicting_paths_` becomes file-only.** The
   `kDirectoryEvictingPath` sentinel and the directory branch in
   `EvictingPathKey` go away.
4. **Readers and writers handled uniformly.** Today only writers
   `RegisterDirBusy`; correctness relies indirectly on `fds_`
   non-empty implying "dir alive." Shared counting makes the
   coupling direct and explicit.
5. **No new primitive needed.** Counter + two `WaitingZone`s —
   both already in [`include/tasks/task.h`](../include/tasks/task.h).
   No `Mutex` reentrancy or `shared_mutex` to design.
6. **No reentrancy issues.** The nested
   `OpenOrCreateFD(file) → OpenOrCreateFD(kDirectory)` call composes
   trivially because the inner directory open targets `fds_`, not
   `dir_states_`. The outer call's count is held throughout.

## Cons

Remaining costs are implementation effort, not design risk:

1. **Migration footprint.** ~12 call sites across `OpenOrCreateFD`,
   `WriteTask::Reset` / `Abort` / `ReleaseDirBusy`,
   `TryCleanupLocalPartitionDir`,
   `RegisterDirBusy` / `UnregisterDirBusy` / `HasDirBusy`,
   `IsDirEvicting`, and the FileCleaner main loop.
2. **Test updates.** [`tests/cloud.cpp`](../tests/cloud.cpp) and
   [`tests/gc.cpp`](../tests/gc.cpp) exercise the partition-cleanup
   races; they stay as the safety net.

## Non-issues considered and dismissed

- **"Cleaner work queue still exists."** It does, and it should.
  `pending_dir_cleanup_` is a producer/consumer queue between "this
  partition just became cleanup-eligible" and the FileCleaner. That
  is structural, identical in shape to today's file queues
  (`pending_gc_cleanup_`, the LRU list). Pop-then-recheck handles
  state changes between enqueue and pop. Not a cost the proposal
  introduces.
- **"Per-open lock cost."** Cache-hit fast path in `OpenOrCreateFD`
  doesn't touch `dir_states_` (cache hits never need the directory).
  Slow path takes a count `++/--` pair — same order as today's
  `WaitForEvictingPath(file_id, ...)` call.
- **"Local-mode cost."** Operations are O(1) hash + counter
  ±1 — same magnitude as today's cloud-mode path. The cleanup
  trigger only fires on DropTable / partition-clear when files are
  already gone, so the lock is uncontended in normal local-mode
  operation.

## Difficulty

Medium. About two days of focused implementation, plus an audit of
FileCleaner / Prewarmer call paths to confirm no lock-cycle
hazards introduced by the new state. Existing partition-cleanup
tests in `tests/cloud.cpp` and `tests/gc.cpp` give coverage of the
race-prone paths.

## Scheduling

Not in scope for the current rebase / zero-copy work. Track as a
follow-up branch and pick up after the rebase merges to `main`.
