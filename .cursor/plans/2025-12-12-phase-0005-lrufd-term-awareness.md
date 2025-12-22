---
name: LruFD Term Awareness
overview: Add term member to LruFD and implement reopen-on-mismatch logic in OpenFD/OpenOrCreateFD to handle term changes. Update ToFilename to use term-aware helpers.
todos:
  - id: lrufd-term-member
    content: Add term_ member to LruFD class
    status: pending
  - id: openfd-term-check
    content: Implement term mismatch check and reopen logic in OpenFD/OpenOrCreateFD
    status: pending
    dependencies:
      - lrufd-term-member
  - id: tofilename-term-aware
    content: Update ToFilename to use term-aware filename helpers
    status: pending
    dependencies:
      - lrufd-term-member
---

# Phase 5: LruFD Term Awareness

## Overview

Add term awareness to LruFD (file descriptor cache entry) to detect when cached FD has wrong term. When term mismatch detected, close and reopen with correct term filename. Update OpenFD/OpenOrCreateFD and ToFilename to accept term as parameter instead of getting it internally. Callers are responsible for providing the correct term value.

## Implementation Steps

### 5.1 Add term to LruFD

**Goal**: Add term_ member to LruFD class to track which term the FD corresponds to.

**Steps**:
1. Add `uint64_t term_` member to `LruFD` class in `async_io_manager.h`:
   ```cpp
   class LruFD {
       // ... existing members ...
       uint64_t term_{0};  // Term of the file this FD represents
   };
   ```
2. Initialize to 0 in constructor (legacy default):
   - Existing constructor: `LruFD(PartitionFiles *tbl, FileId file_id)`
   - Add optional term parameter or set via setter
3. Update constructor to accept optional term:
   - Add `uint64_t term = 0` parameter
   - Or add setter method `SetTerm(uint64_t term)`
4. Ensure backward compatibility:
   - Default term=0 works for legacy files
   - Existing code continues to work

**Files**: `async_io_manager.h`

**Success Criteria**:
- [x] term_ member added to LruFD
- [x] Constructor accepts term parameter
- [x] Default term=0 for backward compatibility
- [ ] Unit tests verify term storage

### 5.2 Update OpenFD/OpenOrCreateFD

**Goal**: Check for term mismatch and reopen FD if term changed. Accept term as parameter instead of getting it internally.

**Steps**:
1. Update function signatures in `async_io_manager.h`:
   - `OpenFD(const TableIdent &tbl_id, FileId file_id, bool direct = false, uint64_t term = 0)`
   - `OpenOrCreateFD(const TableIdent &tbl_id, FileId file_id, bool direct = false, bool create = true, uint64_t term = 0)`
2. Update implementations in `async_io_manager.cpp`:
   - Remove logic that gets term from IO manager or FileIdTermMapping
   - Accept `term` parameter from caller
   - Before using cached FD, check term mismatch:
     - Compare passed `term` with `LruFD::term_`
     - If mismatch and `term != 0` (cloud mode):
       - Close existing FD
       - Remove from cache (or mark for removal)
       - Reopen with correct term filename
3. Set term_ on newly opened FDs:
   - After opening file, set `LruFD::term_ = term` (from parameter)
   - Use term-aware filename from ToFilename (passing term parameter)
4. Handle edge cases:
   - If `term == 0` (local mode or legacy), skip mismatch check
   - If FD doesn't exist, open with passed term
   - Ensure thread safety (mu_ lock already present)
5. Update all call sites:
   - Find all places calling `OpenFD` and `OpenOrCreateFD`
   - Pass appropriate term value (0 for local mode, actual term for cloud mode)
   - Callers should get term from appropriate source (e.g., process term, FileIdTermMapping)

**Files**: 
- `async_io_manager.h` (function signatures)
- `async_io_manager.cpp` (OpenFD, OpenOrCreateFD implementations and call sites)

**Success Criteria**:
- [x] Function signatures accept term parameter
- [x] Term mismatch detected correctly using passed term
- [x] FD closed and reopened on mismatch
- [x] New FDs have correct term set from parameter
- [x] All call sites updated to pass term
- [x] Thread safety maintained
- [ ] Unit tests verify reopen logic
- [ ] Integration tests with term changes

### 5.3 Update ToFilename in CloudStoreMgr

**Goal**: Make ToFilename use term-aware helpers to generate correct filenames. Accept term as parameter instead of getting it internally.

**Steps**:
1. Update function signature in `async_io_manager.h`:
   - `ToFilename(const TableIdent &tbl_id, FileId file_id, uint64_t term = 0)`
   - Add term parameter with default 0 for backward compatibility
2. Update implementation in `async_io_manager.cpp`:
   - Remove logic that gets term from FileIdTermMapping or IO manager
   - Accept `term` parameter from caller
   - Use term-aware helpers with passed term:
     - For data files: `DataFileName(file_id, term)`
     - For manifest: `ManifestFileName(term)`
3. Handle special file_ids:
   - `LruFD::kManifest` → `ManifestFileName(term)`
   - `LruFD::kDirectory` → directory name (no term, term ignored)
   - Regular file_id → `DataFileName(file_id, term)`
4. Update all call sites:
   - Find all places calling `ToFilename`
   - Pass appropriate term value (0 for local mode, actual term for cloud mode)
   - Callers should get term from appropriate source (e.g., process term, FileIdTermMapping, or from file context)
   - Common call sites include:
     - `OpenFile` - should pass term from OpenFD/OpenOrCreateFD parameter
     - `CreateFile` - should pass term from OpenFD/OpenOrCreateFD parameter
     - `ReadFiles` - should get term from FileIdTermMapping or process term
     - `UploadFiles` - should get term from FileIdTermMapping or process term
     - Other file operations

**Files**: 
- `async_io_manager.h` (CloudStoreMgr::ToFilename signature)
- `async_io_manager.cpp` (CloudStoreMgr::ToFilename implementation and all call sites)

**Success Criteria**:
- [x] ToFilename signature accepts term parameter
- [x] ToFilename uses term-aware helpers with passed term
- [x] All file types handled correctly
- [x] All call sites updated to pass term
- [x] Term-aware filenames generated correctly
- [ ] Unit tests verify filename generation with different terms
- [ ] Integration tests verify file operations

## Dependencies

- Phase 1: Filename Helpers & Parsing (for DataFileName, ManifestFileName)

## Related Phases

- Phase 6: Cloud Download & Manifest Selection (uses ToFilename)
- Phase 9: Filename Usage Updates (uses ToFilename)

## Design Notes

- FD cache keyed by file_id only, but term_ mismatch triggers reopen
- FileKey already includes full filename, so term is implicitly part of key
- Reopening on mismatch ensures correct file is opened
- Term=0 (local mode) doesn't need term checking
- **Term is passed as parameter**: OpenFD/OpenOrCreateFD and ToFilename accept term from callers rather than fetching it internally. This gives callers control over which term to use and simplifies the function interfaces.
- **Caller responsibility**: Callers must determine the appropriate term value (e.g., from process term, FileIdTermMapping, or file context) and pass it to these functions.

## Testing

Create unit tests:
- Test term mismatch detection
- Test FD reopen on mismatch
- Test ToFilename with different terms
- Test special file_ids (manifest, directory)
- Integration tests with term changes during runtime

