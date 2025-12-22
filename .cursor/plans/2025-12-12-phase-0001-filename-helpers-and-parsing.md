---
name: Filename Helpers & Parsing
overview: Implement term-aware filename helpers and enhanced parser in common.h to support both legacy and term-aware filename formats.
todos:
  - id: parse-filename-extend
    content: Extend ParseFileName to return {type, id_or_ts, term_opt} tuple
    status: completed
  - id: filename-generators
    content: Add term-aware filename generators (DataFileName, ManifestFileName, ArchiveName)
    status: completed
---

# Phase 1: Filename Helpers & Parsing

## Overview

Implement term-aware filename parsing and generation helpers in `common.h` to support both legacy and term-aware filename formats. This provides the foundation for all term-aware file operations.

## Implementation Steps

### 1.1 Extend filename parsing

**Goal**: Update `ParseFileName` to return `{type, suffix}` and add specialized parsers for data and manifest files.

**Steps**:
1. Update `ParseFileName` signature to return `{type, suffix}` pair:
   - Return type: `std::pair<std::string_view, std::string_view>`
   - `type` is the file type prefix (e.g., "data", "manifest")
   - `suffix` is everything after the first separator (or empty if no separator)
   - Examples:
     - `data_123` → `{type="data", suffix="123"}`
     - `data_123_5` → `{type="data", suffix="123_5"}`
     - `manifest` → `{type="manifest", suffix=""}`
     - `manifest_5` → `{type="manifest", suffix="5"}`
     - `manifest_5_123456789` → `{type="manifest", suffix="5_123456789"}`
2. Add `ParseDataFileSuffix` function:
   - Signature: `bool ParseDataFileSuffix(std::string_view suffix, FileId &file_id, uint64_t &term)`
   - Parses `file_id` and `term` (term is set to 0 when absent/legacy)
   - Returns `true` on success, `false` on parse error
   - Supports formats:
     - `123` → `file_id=123, term=0` (legacy)
     - `123_5` → `file_id=123, term=5` (term-aware)
3. Add `ParseManifestFileSuffix` function:
   - Signature: `bool ParseManifestFileSuffix(std::string_view suffix, uint64_t &term, std::optional<uint64_t> &timestamp)`
   - Parses `term` (set to 0 when absent/legacy) and optional `timestamp`
   - Returns `true` on success, `false` on parse error
   - Supports formats:
     - `""` (empty) → `term=0, timestamp=nullopt` (legacy "manifest")
     - `5` → `term=5, timestamp=nullopt` (term-aware "manifest_5")
     - `5_123456789` → `term=5, timestamp=123456789` (term-aware archive)
   - **Note**: Does NOT support legacy `manifest_<ts>` format (removed)
4. Handle edge cases:
   - Empty strings
   - Malformed numbers
   - Invalid formats

**Files**: `common.h`

**Success Criteria**:
- [x] ParseFileName returns {type, suffix} correctly
- [x] ParseDataFileSuffix parses file_id and term correctly (term=0 when absent)
- [x] ParseManifestFileSuffix parses term (defaults to 0) and timestamp correctly
- [x] Legacy manifest_<ts> format is NOT supported (removed)
- [x] Edge cases handled (empty strings, malformed names)
- [x] Unit tests pass for all formats

### 1.2 Add term-aware filename generators

**Goal**: Create helper functions to generate term-aware filenames with backward compatibility.

**Steps**:
1. Implement `DataFileName(FileId file_id, uint64_t term)`:
   - Returns `data_<id>_<term>` when term != 0
   - Returns `data_<id>` when term == 0
   - Keep legacy `DataFileName(FileId)` overload for backward compatibility
2. Implement `ManifestFileName(uint64_t term)`:
   - Returns `manifest_<term>` when term != 0
   - Returns `manifest` when term == 0
3. Implement `ArchiveName(uint64_t term, uint64_t ts)`:
   - Returns `manifest_<term>_<ts>` (term-aware format only)
   - **Note**: Legacy `ArchiveName(uint64_t ts)` overload is NOT supported
   - All call sites must provide term parameter (use 0 if term not available yet)
4. Add inline implementations in `common.h` for performance

**Files**: `common.h`

**Success Criteria**:
- [x] All generators produce correct filenames for term=0 and term>0
- [x] Legacy overloads work correctly (except ArchiveName - no legacy overload)
- [x] ArchiveName requires term parameter (no legacy ArchiveName(ts) support)
- [x] Generated filenames can be parsed back correctly
- [x] Unit tests verify roundtrip (generate → parse → verify)

## Dependencies

- None (foundation phase)

## Related Phases

- Phase 2: FileIdTermMapping Structure (uses filename helpers)
- Phase 5: LruFD Term Awareness (uses filename helpers)
- Phase 6: Cloud Download (uses filename helpers)
- Phase 8: Archive Naming (uses filename helpers)

## Testing

Create unit tests in `tests/common_test.cpp`:
- Test `ParseFileName` with all legacy formats
- Test `ParseFileName` with all term-aware formats
- Test `DataFileName` with term=0 and term>0
- Test `ManifestFileName` with term=0 and term>0
- Test `ArchiveName` with term=0 and term>0
- Test roundtrip: generate → parse → verify

