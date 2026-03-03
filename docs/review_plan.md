# Branch Feature Review Plan (Phases 0–6)

Top-down review: intent → contract → data model → I/O interface → operations → GC → tests.

---

## Level 1 — Design Intent

**Goal:** Confirm the implemented behavior matches the stated design.

| Step | What to read | What to check |
|------|-------------|---------------|
| 1.1 | `docs/branch_feature_design.md` | Re-read the full spec: naming rules, manifest metadata format, branch operations, GC algorithm, archive retention |
| 1.2 | `docs/implementation_plan_summary.md` | Verify all planned phases were completed; note any deviations called out |
| 1.3 | Phase summary docs (phases 0–6) | Cross-check each summary against the design doc — do the summaries claim anything the design doesn't specify, or vice versa? |

---

## Level 2 — Public API (what callers see)

**Goal:** Verify the caller-visible surface is complete, consistent, and safe.

**File:** `include/eloq_store.h`

Key questions:
- Are `CreateBranchRequest` / `DeleteBranchRequest` ergonomic and consistent with other request types?
- Does `EloqStore::Start(branch, term)` replace or extend the old `Start(term)` — is there any ambiguity for existing callers?
- Is `GlobalCreateBranchRequest` fully wired, or is it a stub?
- Are error codes (`KvError::InvalidArgs`, `KvError::Corrupted`, `KvError::NotFound`) consistently chosen for branch error paths?

---

## Level 3 — Core Data Types

**Goal:** Verify the data model is correct and unambiguous.

**File:** `include/types.h`

Key questions:
- `BranchFileRange` — does `{branch_name, max_file_id}` fully describe a range, or is a `min_file_id` also needed?
- `BranchFileMapping` (sorted vector of `BranchFileRange`) — is the sort order documented and enforced everywhere it is mutated?
- `BranchManifestMetadata` — fields `branch_name`, `term`, `file_ranges`: are all three always populated together, or can `branch_name` be empty in valid states?
- `FileNameSeparator = '_'` and `CurrentTermFileNameSeparator = '.'` — are these constraints enforced at the validation layer (i.e., does `IsValidBranchName` enforce no `_` and no `.`)?

---

## Level 4 — Naming Conventions & Utilities

**Goal:** Verify filename generation/parsing is correct, symmetric, and covers all cases.

**File:** `include/common.h` (~799 lines)

Key questions:
- `NormalizeBranchName` / `IsValidBranchName`: does validation reject `.` (dot), which would otherwise conflict with `CurrentTermFileNameSeparator`?
- `ParseDataFileSuffix` / `ParseManifestFileSuffix`: are the left-to-right parsing rules unambiguous for all legal inputs? What happens with a branch name that is a valid decimal number (e.g., `"42"`)?
- `ParseCurrentTermFilename`: does it handle `CURRENT_TERM.main` vs `CURRENT_TERM` (no dot) cleanly?
- `SerializeBranchManifestMetadata` / `DeserializeBranchManifestMetadata`: are these inverses of each other? Is there a test for the roundtrip?
- `GetBranchNameAndTerm`: called in several I/O hot paths — is it O(n) in the number of branches, and is that acceptable?
- Are the old non-branch helpers (`DataFileName`, `ManifestFileName`, `ArchiveName`) still present? If so, are there any remaining callers that should have been migrated?

---

## Level 5 — Storage I/O Interface

**Goal:** Verify the abstract I/O layer correctly exposes branch-aware operations and that all three implementations are consistent.

**File:** `include/async_io_manager.h` (~1196 lines)

Key questions:
- `GetActiveBranch()` / `SetActiveBranch()`: base class has a default implementation returning `MainBranchName`; does `CloudStoreMgr` override, or does it inherit from `IouringMgr`? Is the inheritance chain correct?
- `WriteBranchManifest`, `WriteBranchCurrentTerm`, `DeleteBranchFiles`: are these declared `virtual` on the base class and implemented in all three (`IouringMgr`, `CloudStoreMgr`, `MemStoreMgr`)?
- `OnFileRangeWritePrepared(tbl_id, branch_name, term, ...)`: is `branch_name` always the correct branch at the call site, or could it ever be stale?
- `LruFD::branch_name_`: set in `OpenOrCreateFD` for data files — is it also set for directory FDs? What value does it hold for non-data FDs?
- `CreateArchive(tbl_id, snapshot, ts, branch_name)`: all three implementations receive `branch_name` — do `CloudStoreMgr` and `MemStoreMgr` use it correctly, or does one of them still hard-code `MainBranchName`?

---

## Level 6 — Manifest Serialization

**Goal:** Verify branch metadata survives a write → read → replay cycle.

**Files:** `include/replayer.h`, `src/storage/root_meta.cpp` (for `ManifestBuilder::Snapshot`)

Key questions:
- `Replayer::branch_metadata_`: is it populated on every successful `Replay()` call, including for the `main` branch? Can it be left with `branch_name = ""` for legacy manifests?
- `ManifestBuilder::Snapshot(root, ttl_root, mapping, max_fp_id, dict_bytes, branch_metadata)`: is `branch_metadata` always passed in — could any call site accidentally use a default-constructed (empty-branch) metadata?
- `BranchManifestMetadata` serialization — does the format include a version or length prefix that would allow forward/backward detection in Phase 7?

---

## Level 7 — Branch Operations

**Goal:** Verify CreateBranch and DeleteBranch are correct and handle all edge cases.

**File:** `src/tasks/background_write.cpp` (lines 350–480)

Key questions:
- `CreateBranch`: what happens if the branch already exists? Does it overwrite the existing manifest (silent no-op), return an error, or is it undefined?
- File-ID seeding: `SetCurrentFileId(parent_max_file_id + 1)` mutates the allocator on the currently active shard — could a concurrent write race with this during `CreateBranch`?
- `DeleteBranch` is idempotent (returns `NoError` for non-existent branches) — is this the right behavior, or should callers be informed the branch didn't exist?
- `DeleteBranch` calls `DeleteBranchFiles(tbl_ident_, normalized_branch, 0)` with `term=0` — what if the branch was written at a higher term? Does `DeleteBranchFiles` scan and delete all terms, or only `manifest_<branch>_0`?

---

## Level 8 — Write Pipeline

**Goal:** Verify branch name correctly flows through the entire write path.

**Files:** `src/async_io_manager.cpp` (focus on `SyncFile`, `SyncFiles`, `SwitchManifest`, `OnFileRangeWritePrepared`), `src/tasks/write_task.cpp`

Key questions:
- `SyncFile` / `SyncFiles`: use `fd.Get()->branch_name_` — for files opened before Phase 5 patched this, could `branch_name_` be empty? Is there a fallback?
- `SwitchManifest`: is the manifest written atomically (temp file + rename), or is there a window where a partial manifest is visible?
- Does the write path correctly handle the case where `active_branch_` is changed (via `SetActiveBranch`) mid-flush? Is there any locking?
- Are there any remaining call sites to the removed `ToFilename` helper? (`git grep ToFilename`)

---

## Level 9 — Garbage Collection

**Goal:** Verify GC never deletes files that any live branch still references.

**Files:** `include/file_gc.h`, `src/file_gc.cpp`

Key questions:
- `AugmentRetainedFilesFromBranchManifests`: iterates `manifest_branch_names` from `ClassifyFiles`. Does `ClassifyFiles` include the *active* branch's manifest in that list, meaning the active branch's files are counted twice (once from `retained_files`, once from augmentation)? That's fine for correctness but worth confirming.
- What if a branch manifest file is being written concurrently with GC (e.g., `CreateBranch` in progress)? Could GC miss the new manifest and delete its files?
- `DeleteOldArchives`: groups archives by branch and retains `num_retained_archives` per branch. Is `num_retained_archives` documented in `KvOptions`? What is its default value?
- Cloud GC: `AugmentRetainedFilesFromBranchManifests` calls `DownloadArchiveFile` for each branch manifest. Is the cost acceptable (N network round-trips per GC run, one per branch)?

---

## Level 10 — Tests

**Goal:** Verify test coverage matches the design, and identify any gaps.

| Test file | Phase covered | What to check |
|-----------|--------------|---------------|
| `tests/branch_filename_parsing.cpp` | 1 | All roundtrip tests present? Underscore explicitly rejected? Purely-numeric branch names rejected? |
| `tests/branch_operations.cpp` | 3 | CreateBranch idempotency (create same branch twice)? Delete-then-recreate? Invalid term? |
| `tests/branch_gc.cpp` | 6 | Is there a test for cloud GC? Is there a test for archive retention across branches? |
| `tests/gc.cpp` | pre-existing | Do existing GC tests still pass? Do they test with `main` branch only? |
| `tests/manifest.cpp`, `tests/replayer_term.cpp`, `tests/manifest_payload.cpp` | 2, 5 | Roundtrip for `BranchManifestMetadata`? Multi-branch manifest? |

**Notable gaps to flag:**
- No test for CreateBranch when branch already exists (idempotency / overwrite behavior)
- No test verifying that `DeleteBranch` with `term > 0` correctly removes the right manifest
- No cloud-mode branch GC test
- No test for `AugmentRetainedFilesFromBranchManifests` when a manifest file is corrupt (warn-and-skip path)

---

## Cross-Cutting Concerns (check throughout)

1. **Thread safety** — `active_branch_` is a `std::string` on `IouringMgr`. Is it ever mutated from a different thread than the one reading it during I/O?
2. **Old helpers still present** — `DataFileName`, `ManifestFileName`, `ArchiveName` in `common.h` are the non-branch versions. Any remaining caller would silently use `main` naming. Worth a `git grep` pass.
3. **`main` branch special-casing** — Several places check `normalized_branch == MainBranchName`. Is this list exhaustive? Could a future code path accidentally allow deleting `main`?
4. **Phase 7 readiness** — The manifest header currently has no version field. Review which parts of the serialization would need to change in Phase 7, so Phase 6 code doesn't inadvertently make that harder.

---

## Suggested Review Order

| Step | File(s) | Est. time |
|------|---------|-----------|
| 1. Design docs | `branch_feature_design.md`, phase summaries | 30 min |
| 2. Types + naming | `types.h`, `common.h` | 45 min |
| 3. Public API | `eloq_store.h` (branch sections only) | 20 min |
| 4. I/O interface | `async_io_manager.h` (branch methods) | 30 min |
| 5. Operations | `background_write.cpp` lines 350–480 | 20 min |
| 6. Write pipeline | `write_task.cpp`, `async_io_manager.cpp` (sync/switch/archive) | 40 min |
| 7. GC | `file_gc.h`, `file_gc.cpp` | 30 min |
| 8. Tests | `branch_*.cpp`, `gc.cpp`, `manifest.cpp` | 30 min |
