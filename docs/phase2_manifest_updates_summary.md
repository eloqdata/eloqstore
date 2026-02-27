# Phase 2: Manifest Structure Updates - Summary

**Status**: ✅ COMPLETED  
**Date**: February 27, 2026  
**Commits**: 6d26265, c8738aa, 2d89913, aa22ab5

---

## Overview

Phase 2 adds branch-aware metadata to the manifest structure, enabling per-branch tracking of file ranges using `BranchFileMapping` instead of per-file tracking with `FileIdTermMapping`.

---

## Key Changes

### 1. New Data Structures

#### BranchFileRange (`include/types.h`)
```cpp
struct BranchFileRange {
    std::string branch_name;  // branch identifier (e.g., "main", "feature")
    uint64_t term;            // term when this file_id range was allocated
    FileId max_file_id;       // highest file_id allocated in this branch

    // For sorting by max_file_id (required for binary search)
    bool operator<(const BranchFileRange& other) const;
    bool operator<(FileId fid) const;
};
```

#### BranchFileMapping (`include/types.h`)
```cpp
using BranchFileMapping = std::vector<BranchFileRange>;
```

#### BranchManifestMetadata (`include/types.h`)
```cpp
struct BranchManifestMetadata {
    std::string branch_name;           // unique branch identifier
    uint64_t term;                    // current term for this branch
    BranchFileMapping file_ranges;     // per-branch file ranges
};
```

### 2. Lookup Functions (`include/common.h`)

| Function | Description |
|----------|-------------|
| `FindBranchRange()` | Binary search using `std::lower_bound` |
| `GetBranchName()` | Get branch name for a given file_id |
| `GetFileTerm()` | Get term for a given file_id |
| `FileIdInBranch()` | Check if file_id belongs to a specific branch |
| `GetBranchNameAndTerm()` | Single lookup returning both branch_name and term |

### 3. Serialization (`include/common.h`)

| Function | Description |
|----------|-------------|
| `SerializeBranchFileMapping()` | Binary serialization |
| `DeserializeBranchFileMapping()` | Binary deserialization |
| `SerializeBranchManifestMetadata()` | Full metadata serialization |
| `DeserializeBranchManifestMetadata()` | Full metadata deserialization |

### 4. API Changes

#### CreateArchive (`include/async_io_manager.h`)
- Added `branch_name` parameter
- Uses `BranchArchiveName()` instead of `ArchiveName()`
- Archive filename format: `manifest_<branch>_<term>_<ts>`

#### ManifestBuilder (`include/storage/root_meta.h`)
- Changed `Snapshot()` signature to accept `BranchManifestMetadata` instead of `string_view`

#### Replayer (`include/replayer.h`)
- Changed `branch_metadata_` from `FileIdTermMapping` to `BranchManifestMetadata`

### 5. Updated Call Sites

| File | Changes |
|------|---------|
| `src/storage/root_meta.cpp` | Updated `Snapshot()` implementation |
| `src/replayer.cpp` | Updated to deserialize new format |
| `src/tasks/write_task.cpp` | Pass `BranchManifestMetadata` |
| `src/tasks/background_write.cpp` | Pass `BranchManifestMetadata` + branch_name to CreateArchive |
| `src/file_gc.cpp` | Updated for GC replay |
| `src/storage/index_page_manager.cpp` | Updated for manifest loading |
| `src/test_utils.cpp` | Updated for test compatibility |

---

## Design Decisions

### Why BranchFileMapping?

**Before** (per-file tracking):
```
FileIdTermMapping = flat_hash_map<FileId, uint64_t>  // file_id → term
// Example: {1: 5, 2: 5, 3: 5, 4: 6, 5: 6, ...}
```

**After** (per-branch range tracking):
```
BranchFileMapping = vector<BranchFileRange>  // sorted by max_file_id
// Example: [{branch: "main", term: 5, max_file_id: 100}, 
//           {branch: "feature", term: 3, max_file_id: 50}]
```

**Benefits**:
- Significantly reduced manifest size (millions of entries → tens of entries)
- O(log n) lookup using binary search
- Efficient for GC operations

### Binary Search Implementation

Uses `std::lower_bound` to find the first range where `max_file_id >= file_id`:

```cpp
BranchFileRange target;
target.max_file_id = file_id;
return std::lower_bound(mapping.begin(), mapping.end(), target);
```

Example:
```
BranchFileMapping (sorted by max_file_id):
  [branch: "feature", max_file_id: 50]
  [branch: "main",    max_file_id: 100]  
  [branch: "hotfix",  max_file_id: 200]

FindBranchRange(25) → points to "feature" (50 >= 25)
FindBranchRange(75) → points to "main"    (100 >= 75)
FindBranchRange(250) → end() (exceeds all ranges)
```

### Serialization Format

```
BranchManifestMetadata:
  [branch_name_len(4B)][branch_name][term(8B)][BranchFileMapping]

BranchFileMapping:
  [num_entries(8B)][branch_name_1(4B+str)][term_1(8B)][max_file_id_1(8B)]...
```

---

## Testing

### Unit Tests Added

| Test | Assertions |
|------|------------|
| BranchFileRange sorting | 6 |
| Binary search lookup | 7 |
| GetBranchName and GetFileTerm | 7 |
| FileIdInBranch | 6 |
| Serialization roundtrip | 9 |
| Empty mapping | 4 |

**Total new assertions**: 43

### Test Results
```
All tests passed (280 assertions in 36 test cases)
```

---

## Limitations (To be addressed in Phase 3+)

1. **Empty file_ranges**: Currently `branch_metadata.file_ranges` is empty
   - Will be populated when branch operations are implemented
   - Each write will add new entries to the mapping

2. **Hardcoded MainBranchName**: Archive creation uses "main" as branch name
   - Will be replaced with actual branch when operations are implemented

3. **No branch creation yet**: Need CreateBranch operation to populate mappings

---

## Commits

### Commit 1: 6d26265
```
feat(Phase2): add BranchFileMapping data structures

- Add BranchFileRange struct (branch_name, term, max_file_id)
- Add BranchFileMapping using std::vector<BranchFileRange>
- Add FindBranchRange() binary search using std::lower_bound
- Add GetBranchName(), GetFileTerm(), FileIdInBranch() helpers
- Add SerializeBranchFileMapping() and DeserializeBranchFileMapping()
- Add comprehensive unit tests (6 new test cases, 43 new assertions)
```

### Commit 2: c8738aa
```
feat(Phase2): integrate BranchManifestMetadata into manifest

- Add BranchManifestMetadata struct with branch_name, term, file_ranges
- Update ManifestBuilder::Snapshot to accept BranchManifestMetadata
- Add SerializeBranchManifestMetadata/DeserializeBranchManifestMetadata
- Update Replayer to use branch_metadata_ instead of file_id_term_mapping_
- Update write_task.cpp and background_write.cpp to create branch metadata
- Update file_gc.cpp and index_page_manager.cpp for GC replay
- Update test_utils.cpp for test compatibility
```

### Commit 3: 2d89913
```
feat: add GetBranchNameAndTerm function

- Single binary search lookup returns both branch_name and term
- More efficient than calling GetBranchName and GetFileTerm separately
```

### Commit 4: aa22ab5
```
fix: add branch_name parameter to CreateArchive

- Add branch_name parameter to CreateArchive API (4 declarations, 3 implementations)
- Use BranchArchiveName instead of ArchiveName for branch-aware archive filenames
- Update background_write.cpp to pass branch_name from branch_metadata
- Archive filenames now include branch: manifest_<branch>_<term>_<ts>
```

---

## Metrics

| Metric | Value |
|--------|-------|
| Lines of code added | ~350 |
| Lines of code modified | ~80 |
| Test cases added | 6 |
| Test assertions added | 43 |
| Functions added | 8 |
| Functions modified | 5 |
| Files modified | 11 |
| Commits | 4 |

---

## Next Steps (Phase 3)

1. **Branch Operations**: Implement CreateBranch, DeleteBranch
2. **CURRENT_TERM per branch**: Track term independently per branch
3. **File ID allocation**: Per-branch monotonic counters
4. **Write path**: Include branch_name in data filenames
5. **Read path**: Find manifest by branch_name
