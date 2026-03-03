# Phase 6: GC with Cross-Branch References - Summary

**Status**: ✅ COMPLETED  
**Date**: March 3, 2026  
**Commits**: 4b0e91d

---

## Overview

Phase 6 makes Garbage Collection (GC) branch-aware. Before this phase, GC only consulted the retained-file set from the currently active manifest. If a data file was referenced by a non-active branch manifest but not by the active branch, GC would delete it — silently corrupting that branch. Phase 6 fixes this by reading every on-disk branch manifest, replaying each one, and unioning all referenced file IDs into the retained set before any deletion occurs.

---

## Problem / Motivation

After Phases 3–5, multiple branch manifests can exist on disk simultaneously (e.g., `manifest_main_5`, `manifest_feature1_0`, `manifest_feature2_0`). GC was called with a `retained_files` set derived only from the active branch's manifest. A scenario such as:

1. Write data on `main` → data files `data_1_main_5`, `data_2_main_5`
2. Create branch `feature` → branch manifest references the same data files
3. Delete all data from `main` → GC runs with `retained_files = {}`
4. **Bug**: GC deletes `data_1_main_5` and `data_2_main_5`, even though `feature` still needs them

Phase 6 adds step 3a: augment `retained_files` from all other branch manifests before deletion.

---

## Design Decisions

### `AugmentRetainedFilesFromBranchManifests` as a Standalone Function
The augmentation logic is factored into a free function (not a method on `IouringMgr` or `BackgroundWrite`) to keep `file_gc.cpp` self-contained and to make the logic easy to unit-test independently.

### Input: `manifest_branch_names` + `manifest_terms` from `ClassifyFiles`
Rather than re-scanning the directory, the function reuses the lists already produced by `ClassifyFiles` during the GC flow. This avoids an extra directory scan and ensures consistency between classification and augmentation.

### Per-Manifest Error Handling: Warn and Skip
If reading or replaying a manifest fails (e.g., corruption, concurrent delete), the function logs a `LOG(WARNING)` and continues to the next manifest. It does not abort GC. This is intentional: a warning is better than refusing to GC at all, and a truly corrupt manifest would also cause problems at read time.

### Unified Local and Cloud Mode
```
if (!io_mgr->options_->cloud_store_path.empty())
    DownloadArchiveFile(...)   // cloud: download from object store
else
    io_mgr->ReadFile(...)      // local: read from disk
```
A single function handles both modes by branching on the presence of `cloud_store_path`. In cloud mode it casts the `IouringMgr*` to `CloudStoreMgr*` (safe because cloud GC always runs on a `CloudStoreMgr`).

### Augmented Set is a Copy
```cpp
auto all_retained = retained_files;  // copy
AugmentRetainedFilesFromBranchManifests(..., all_retained, ...);
// then pass all_retained to DeleteUnreferencedLocalFiles / DeleteUnreferencedCloudFiles
```
The original `retained_files` is not modified, keeping the GC call sites clean and the augmentation clearly scoped.

---

## Implementation Details

### New Function Declaration (`include/file_gc.h`)

```cpp
/// Reads every manifest listed in manifest_branch_names / manifest_terms,
/// replays each, and unions the referenced file IDs into retained_files.
/// Per-manifest errors are logged as warnings and skipped.
KvError AugmentRetainedFilesFromBranchManifests(
    const TableIdent &tbl_id,
    const std::vector<std::string> &manifest_branch_names,
    const std::vector<uint64_t> &manifest_terms,
    absl::flat_hash_set<FileId> &retained_files,
    uint8_t pages_per_file_shift,
    IouringMgr *io_mgr);
```

### Implementation (`src/file_gc.cpp:337`)

```cpp
KvError AugmentRetainedFilesFromBranchManifests(...)
{
    for (size_t i = 0; i < manifest_branch_names.size(); ++i) {
        const std::string &branch = manifest_branch_names[i];
        uint64_t term = manifest_terms[i];
        std::string filename = BranchManifestFileName(branch, term);

        DirectIoBuffer buf;
        KvError err = KvError::NoError;

        if (!io_mgr->options_->cloud_store_path.empty()) {
            CloudStoreMgr *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr);
            err = DownloadArchiveFile(tbl_id, filename, buf, cloud_mgr, cloud_mgr->options_);
        } else {
            err = io_mgr->ReadFile(tbl_id, filename, buf);
        }

        if (err != KvError::NoError) {
            LOG(WARNING) << "AugmentRetainedFilesFromBranchManifests: failed to read "
                         << filename << " ...; skipping";
            continue;
        }

        MemStoreMgr::Manifest manifest(buf.view());
        Replayer replayer(Options());
        replayer.branch_metadata_.term = term;

        KvError replay_err = replayer.Replay(&manifest);
        if (replay_err != KvError::NoError) {
            LOG(WARNING) << "AugmentRetainedFilesFromBranchManifests: failed to replay "
                         << filename << " ...; skipping";
            continue;
        }

        GetRetainedFiles(retained_files, replayer.mapping_tbl_, pages_per_file_shift);
    }
    return KvError::NoError;
}
```

### Integration in `ExecuteLocalGC` (`src/file_gc.cpp:96`)

```cpp
// 2a. augment retained_files from all other branch manifests on disk.
auto all_retained = retained_files;
AugmentRetainedFilesFromBranchManifests(tbl_id,
                                        manifest_branch_names,
                                        manifest_terms,
                                        all_retained,
                                        io_mgr->options_->pages_per_file_shift,
                                        io_mgr);
// ...
// 5. delete unreferenced data files.
err = DeleteUnreferencedLocalFiles(
    tbl_id, data_files, all_retained, least_not_archived_file_id, io_mgr);
```

### Integration in `ExecuteCloudGC` (`src/file_gc.cpp:913`)

```cpp
// 3a. augment retained_files from all other branch manifests in cloud.
auto all_retained = retained_files;
AugmentRetainedFilesFromBranchManifests(
    tbl_id, manifest_branch_names, manifest_terms, all_retained,
    cloud_mgr->options_->pages_per_file_shift,
    static_cast<IouringMgr *>(cloud_mgr));
// ...
// 5. delete unreferenced data files.
err = DeleteUnreferencedCloudFiles(tbl_id, data_files, manifest_terms,
                                   manifest_branch_names, all_retained,
                                   least_not_archived_file_id, cloud_mgr);
```

### Files Modified

| File | Change |
|------|--------|
| `include/file_gc.h` | Declare `AugmentRetainedFilesFromBranchManifests` |
| `src/file_gc.cpp` | Implement `AugmentRetainedFilesFromBranchManifests`; call from both `ExecuteLocalGC` and `ExecuteCloudGC` |
| `tests/branch_gc.cpp` | NEW: 5 integration test cases |
| `tests/CMakeLists.txt` | Add `branch_gc` test target |

---

## Testing

### Test File: `tests/branch_gc.cpp`

**Test Statistics:**
- **Total test cases:** 5
- **Total assertions:** 22
- **Pass rate:** 100%

**Test Cases:**

| Tag | Test Case | Description |
|-----|-----------|-------------|
| `[regression]` | `gc baseline: no branch, delete all triggers data file cleanup` | Baseline: no branch created → GC deletes all data files after `Delete(0, 50)` |
| `[branch-gc]` | `gc branch protection: active branch keeps data files alive` | Create `feature` branch → delete all from `main` → GC does NOT delete data files (branch still references them) |
| `[branch-gc]` | `gc branch protection: deleted branch allows data file cleanup` | Create then delete `feature` → delete all from `main` → GC deletes all data files |
| `[branch-gc]` | `gc branch protection: multiple active branches keep data files` | Create `feature1` and `feature2` → delete all from `main` → GC does NOT delete data files |
| `[branch-gc]` | `gc branch protection: one deleted branch, one live still protects` | Create `feature1` and `feature2`, delete `feature1` → delete all from `main` → `feature2` still protects; GC does NOT delete data files |

**Test Setup:**
- Dedicated `KvOptions` with `store_path = "/tmp/test-branch-gc"`, `pages_per_file_shift = 8` (1 MB files), `data_append_mode = true`
- `CountDataFiles()` helper: scans the table directory and counts files where `ParseFileName` returns `FileNameData`
- `WaitForGC(seconds)`: adds a short sleep after deletes to allow any async GC book-keeping to settle

---

## Commit

| Hash | Message |
|------|---------|
| `4b0e91d` | phase 6: GC with cross-branch references |

Full commit message:
> Read all branch manifests on disk before deleting data files.
> AugmentRetainedFilesFromBranchManifests unions file IDs from every
> on-disk manifest into retained_files before DeleteUnreferencedLocalFiles
> / DeleteUnreferencedCloudFiles runs, preventing GC from removing data
> files that are still live in another branch.
>
> Add branch_gc.cpp with 5 test cases covering:
> - baseline (no branch → GC deletes files)
> - active branch protects data files
> - deleted branch allows GC to collect
> - multiple active branches protect data files
> - one of two branches deleted, remaining branch still protects

---

## Next Steps (Phase 7)

Phase 7 will add version headers to all file formats:
- 4-byte version field in manifest and data file headers
- New manifest header layout: `[Checksum(8B) | Version(4B) | Root(4B) | TTL Root(4B) | Payload Len(4B)]`
- Version validation on read to reject incompatible formats
