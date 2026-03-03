# Phase 4: Branch-Aware Read/Write Path - Summary

**Status**: ✅ COMPLETED  
**Date**: February 28 – March 2, 2026  
**Commits**: 877b67c, 90a57f9, cb2dfb6, 5222241

---

## Overview

Phase 4 wires the active-branch tracking established in Phase 3 into every step of the write pipeline. After this phase, all data files, manifest files, and `CURRENT_TERM` files written during normal store operation carry the correct branch name in their filenames (`data_<id>_<branch>_<term>`, `manifest_<branch>_<term>`). Per-branch file-ID allocation is also introduced so that branches do not reuse file IDs from their parent.

---

## Problem / Motivation

Phases 1–3 built the naming conventions and the branch-create/delete operations but the normal read/write path still used `MainBranchName` as a hard-coded fallback in several places. As a result, writes on a non-`main` branch would still produce files named `data_<id>_main_<term>`, defeating the whole purpose of branching. Phase 4 fixes every call site.

---

## Design Decisions

### Active Branch Flows Through the Write Pipeline
The write pipeline is: `EloqStore::ExecWrite` → `WriteTask::Reset` → `Shard::ProcessReq` → `BackgroundWrite::OnFileRangeWritePrepared` → `SyncFile/SyncFiles` → `SwitchManifest`.  
At each step the active branch is now read from `io_mgr_->GetActiveBranch()` instead of a hard-coded constant.

### `active_branch_` Lives on `IouringMgr`
After an initial placement on `CloudStoreMgr` only, the member was moved to `IouringMgr` (the common base for local I/O) so local-mode shards also track their branch correctly. `Shard::Init()` calls `io_mgr_->SetActiveBranch(branch)` unconditionally.

### Remove `manifest_branch_term_` Cache
An intermediate `manifest_branch_term_` cache (holding `(active_branch, process_term)`) was added during development but turned out to always equal `(GetActiveBranch(), ProcessTerm())`. It was removed to eliminate the redundancy; all call sites now call those two accessors directly.

### `branch_name` Parameter on `OnFileRangeWritePrepared`
Rather than re-deriving the branch inside `OnFileRangeWritePrepared` via `GetBranchNameAndTerm` (which requires a map lookup), the branch is passed as an explicit `std::string_view` parameter from the already-known call-site value.

### Remove `ToFilename`
The old `ToFilename` helper that produced legacy (branch-unaware) filenames was removed entirely once no callers remained, preventing accidental regression to old naming.

---

## Implementation Details

### Write Path Changes

**`include/eloq_store.h` / `src/eloq_store.cpp`**
- `WriteRequest` base class gains a `branch_name` field
- `BatchWriteRequest::SetArgs` overload accepts `branch_name`
- `EloqStore::ExecWrite` stamps `branch_name` from the active store branch onto outgoing requests

**`include/tasks/write_task.h` / `src/tasks/write_task.cpp`**
- `WriteTask::Reset` overload accepting `branch_name`
- Calls `io_mgr_->SetActiveBranch(branch_name)` at task start so subsequent I/O uses the right branch

**`include/async_io_manager.h` / `src/async_io_manager.cpp`**
- `OnFileRangeWritePrepared(tbl_id, branch_name, term, ...)` — `branch_name` parameter added
- `SyncFile` / `SyncFiles`: replace `ToFilename` with `BranchDataFileName(file_id, branch_name, term)` (branch comes from `fd.Get()->branch_name_`)
- `SwitchManifest`: `OpenFD` now passes `GetActiveBranch()` instead of `MainBranchName`
- `CreateArchive`: uses actual `active_br` in archive snapshot metadata instead of `MainBranchName`
- `DownloadFile`: unified cloud key and local path to single branch-aware filename

**`src/async_io_manager.cpp` — `LruFD`**
```cpp
struct LruFD {
    // ...
    std::string branch_name_;  // set in OpenOrCreateFD for data files
};
```
`branch_name_` is set during `OpenOrCreateFD` for every newly opened data-file FD, fixing a crash that occurred when evicting cloud-downloaded files (whose `branch_file_mapping_` entry was never populated).

**`src/async_io_manager.cpp` — `OnFileRangeWritePrepared` bug fix**
An `if (state.invalid)` guard that was accidentally dropped during Phase 3 refactoring — causing the error block to fire unconditionally — was restored.

### Per-Branch File-ID Allocation

`IouringMgr` now tracks per-branch next file ID:

```cpp
// Get/set the next file ID for a specific branch.
FileId GetNextFileIdForBranch(std::string_view branch);
void   InitFileIdForBranch(std::string_view branch, FileId start);
```

When a new branch is created (`BackgroundWrite::CreateBranch`), `InitFileIdForBranch` seeds the allocator at `parent_max_file_id + 1`, ensuring the new branch's data files never overlap with the parent's.

### Files Modified

| File | Change |
|------|--------|
| `include/async_io_manager.h` | `active_branch_` on `IouringMgr`; `branch_name` param on `OnFileRangeWritePrepared`; per-branch file-ID methods; `branch_name_` on `LruFD`; remove `ToFilename` |
| `src/async_io_manager.cpp` | `SyncFile`, `SyncFiles`, `SwitchManifest`, `CreateArchive`, `DownloadFile`, `OnFileRangeWritePrepared` all branch-aware; set `branch_name_` in `OpenOrCreateFD`; restore `if (state.invalid)` guard |
| `include/eloq_store.h` | `branch_name` field on `WriteRequest`; `SetArgs` overload |
| `src/eloq_store.cpp` | Stamp `branch_name` on write requests |
| `include/tasks/write_task.h` | `Reset` overload with `branch_name` |
| `src/tasks/write_task.cpp` | Call `SetActiveBranch` at task start |
| `include/tasks/batch_write_task.h` | `SetArgs` overload with `branch_name` |
| `src/tasks/batch_write_task.cpp` | Propagate `branch_name` through batch write |
| `src/storage/shard.cpp` | Call `SetActiveBranch` unconditionally in `Init`; remove CloudStoreMgr-only cast block |
| `src/storage/index_page_manager.cpp` | Remove stale `manifest_branch_term_` usage |
| `include/storage/page_mapper.h` / `src/storage/page_mapper.cpp` | Per-branch allocator tracking |
| `include/common.h` | Remove `ToFilename`; `GetBranchNameAndTerm` helpers |
| `src/tasks/background_write.cpp` | Use `GetActiveBranch()` / `ProcessTerm()` directly |

---

## Testing

Phase 4 did not add a new standalone test file. Correctness was validated by:
- All existing tests in `branch_operations`, `eloq_store_test`, `cloud`, `persist`, and `manifest` suites continuing to pass
- Manual verification that written files carry branch-aware names

---

## Commits

| Hash | Message |
|------|---------|
| `877b67c` | feat(Phase4): add branch support infrastructure for write path |
| `90a57f9` | Phase 4 update |
| `cb2dfb6` | refactor: remove manifest_branch_term_ cache, fix active_branch_ ownership, add branch_name to OnFileRangeWritePrepared |
| `5222241` | refactor: complete branch-aware file naming for all cloud I/O paths |

---

## Next Steps (Phase 5)

Phase 5 updates the archive path (`CreateArchive`) to embed the real active branch in archive filenames and fixes the existing test suite to use branch-aware filename helpers consistently, enabling the full test suite to pass with the new naming conventions.
