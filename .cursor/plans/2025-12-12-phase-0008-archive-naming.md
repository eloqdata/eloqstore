---
name: Archive Naming
overview: Update CreateArchive to use term-aware archive names for both local and cloud modes, and update archive reading to only support term-aware format (remove legacy format support).
todos:
  - id: createarchive-term-aware
    content: Update CreateArchive to use term-aware archive names
    status: completed
  - id: archive-reading-term-support
    content: Update archive reading to only support term-aware format
    status: completed
    dependencies:
      - createarchive-term-aware
---

# Phase 8: Archive Naming

## Overview

Update archive creation to use term-aware archive names (`manifest_<term>_<ts>`) for both local and cloud modes. Update archive reading to only support term-aware format (remove legacy `manifest_<ts>` support).

## Implementation Steps

### 8.1 Update CreateArchive

**Goal**: Use term-aware archive names for both local and cloud modes.

**Steps**:
1. Locate `CreateArchive` method in `async_io_manager.cpp`:
   - `IouringMgr::CreateArchive` (local mode)
   - `CloudStoreMgr::CreateArchive` (cloud mode)
2. Add `process_term_` member to `IouringMgr` (similar to `CloudStoreMgr`):
   - Add `uint64_t process_term_{0};` member
   - Add `SetProcessTerm(uint64_t term)` and `ProcessTerm()` methods
3. Update both `CreateArchive` implementations:
   - Get process term from IO manager (`process_term_` member)
   - Always use `ArchiveName(process_term_, ts)` (even when term == 0)
   - Remove legacy `ArchiveName(ts)` usage
4. Ensure archive filenames are term-aware:
   - Generate filename using `ArchiveName(process_term_, ts)`
   - Use term-aware filename for archive creation
   - Upload to cloud with term-aware filename (if cloud mode)

**Files**: `async_io_manager.h`, `async_io_manager.cpp` (CreateArchive methods)

**Success Criteria**:
- [x] `IouringMgr` has `process_term_` member and accessors
- [x] Term-aware archive names generated for both local and cloud modes
- [x] Archive creation works correctly
- [x] Cloud upload uses term-aware name
- [ ] Unit tests verify archive naming
- [ ] Integration tests with archive creation

### 8.2 Update archive reading

**Goal**: Only support term-aware archive format (remove legacy format support).

**Steps**:
1. Locate archive reading code in `file_gc.cpp`:
   - `ClassifyFiles` function
   - Archive parsing logic
2. Update to only support term-aware format:
   - `manifest_<term>_<ts>` (term-aware, required)
   - Remove support for `manifest_<ts>` (legacy format)
3. Parse term from archive filename:
   - Use `ParseFileName` and `ParseManifestFileSuffix` helpers from Phase 1
   - Extract term and timestamp from filename
   - Require both term and timestamp to be present
4. Handle archive processing:
   - Only recognize term-aware format
   - Extract term and timestamp correctly
   - Skip files that don't match term-aware format
5. Update archive classification:
   - Update `ClassifyFiles` to only handle term-aware names
   - Ensure legacy format files are ignored (not classified as archives)

**Files**: `file_gc.cpp` (ClassifyFiles function)

**Success Criteria**:
- [x] Only term-aware archive format recognized
- [x] Term extracted correctly from term-aware archives
- [x] Legacy format files are ignored
- [x] Archive classification works correctly
- [ ] Unit tests verify archive parsing
- [ ] Integration tests with term-aware format

## Dependencies

- Phase 1: Filename Helpers & Parsing (for ArchiveName, ParseFileName)

## Related Phases

- Phase 9: Filename Usage Updates (uses archive naming)

## Design Notes

- Term-aware archives: `manifest_<term>_<ts>` (required format)
- Both local and cloud modes use term-aware archive names
- Readers only support term-aware format (legacy format removed)
- Writers always emit term-aware format (even when term == 0)

## Testing

Create unit tests:
- Test CreateArchive with term != 0 (term-aware name)
- Test CreateArchive with term == 0 (term-aware name with term=0)
- Test archive parsing with term-aware format
- Test ClassifyFiles ignores legacy format files
- Test ClassifyFiles only recognizes term-aware archives
- Integration tests with archive operations

