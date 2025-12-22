---
name: FileIdTermMapping Structure
overview: Define FileIdTermMapping structure with serialize/deserialize methods and integrate into IouringMgr using shared_ptr for easy passing between classes.
todos:
  - id: define-fileid-term-mapping
    content: Define FileIdTermMapping type and serialize/deserialize methods
    status: completed
  - id: integrate-partitionfiles
    content: Integrate FileIdTermMapping into PartitionFiles using shared_ptr
    status: completed
---

# Phase 2: FileIdTermMapping Structure

## Overview

Define `FileIdTermMapping` structure to track which term each file_id was created with. Store it in `IouringMgr` using `shared_ptr` per table (keyed by TableIdent) for easy sharing between classes (Replayer, PageMapper, WriteTask, etc.).

## Implementation Steps

### 2.1 Define FileIdTermMapping

**Goal**: Create the FileIdTermMapping type with serialization support.

**Steps**:
1. Define `FileIdTermMapping` type:
   ```cpp
   using FileIdTermMapping = std::unordered_map<FileId, uint64_t>;
   ```
2. Add serialize method:
   - Write varint64 count of entries
   - For each `(file_id, term)` pair:
     - Write varint64(file_id)
     - Write varint64(term)
   - If mapping is empty, write count=0 (backward compatible)
3. Add deserialize method:
   - Read varint64 count
   - Read count pairs of (file_id, term)
   - Return empty map if absent/malformed
   - Handle errors gracefully (return empty map)
4. Place in appropriate header (e.g., `common.h` or new `file_id_term_mapping.h`)

**Files**: `common.h` or new `file_id_term_mapping.h`

**Success Criteria**:
- [ ] Serialize produces correct varint format
- [ ] Deserialize correctly reads serialized data
- [ ] Empty mapping serializes to count=0
- [ ] Malformed data handled gracefully
- [ ] Unit tests verify roundtrip serialization

### 2.2 Integrate into IouringMgr

**Goal**: Store FileIdTermMapping in IouringMgr using shared_ptr for easy sharing.

**Steps**:
1. Add `std::unordered_map<TableIdent, std::shared_ptr<FileIdTermMapping>> file_terms_` member to `IouringMgr` class:
   ```cpp
   class IouringMgr {
   public:
       std::unordered_map<TableIdent, std::shared_ptr<FileIdTermMapping>> file_terms_;
   };
   ```
2. Initialize in constructor:
   - Create empty shared_ptr or allocate empty mapping
   - Default to `std::make_shared<FileIdTermMapping>()`
3. Provide getter/setter methods on IouringMgr:
   - `GetOrCreateFileIdTermMapping(const TableIdent &)` - returns shared_ptr (creates if null)
   - `SetFileIdTermMapping(const TableIdent &, std::shared_ptr<FileIdTermMapping>)` - sets mapping
   - `GetFileIdTerm(const TableIdent &, FileId)` / `SetFileIdTerm(const TableIdent &, FileId, uint64_t)` - read/write single entry via `file_terms_`
4. Update Replayer to accept/use shared_ptr:
   - Replayer can receive mapping from `IouringMgr::file_terms_`
   - Replayer can restore mapping to `IouringMgr::file_terms_` via `SetFileIdTermMapping`
5. Update other classes that need access:
   - PageMapper can access via IouringMgr helpers
   - WriteTask can access via IouringMgr helpers
   - All share the same shared_ptr instance

**Files**: `async_io_manager.h`/`.cpp` (IouringMgr), `replayer.h`, `replayer.cpp`

**Success Criteria**:
- [ ] IouringMgr stores `file_terms_` correctly
- [ ] Replayer can receive and restore mapping
- [ ] Multiple classes can access same mapping instance via IouringMgr
- [ ] Memory management correct (shared_ptr lifecycle)
- [ ] Unit tests verify sharing between classes

## Dependencies

- Phase 1: Filename Helpers & Parsing (completed)

## Related Phases

- Phase 3: Manifest Payload Serialization (serializes FileIdTermMapping)
- Phase 4: Replayer Term Validation (uses FileIdTermMapping)
- Phase 7: File Allocation & Term Recording (records to FileIdTermMapping)

## Design Notes

- Using `shared_ptr` allows the mapping to be easily passed between:
  - IouringMgr (owner, via `file_terms_`)
  - Replayer (reads/restores)
  - PageMapper (reads term for file_id)
  - WriteTask (reads for manifest serialization)
- The mapping is per-partition (stored in IouringMgr per TableIdent)
- Empty mapping (count=0) is backward compatible with old manifests

