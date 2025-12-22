---
name: File Allocation & Term Recording
overview: Update file allocation paths to record term in FileIdTermMapping when new file_id is first used. Ensure term is recorded when files are created.
todos:
  - id: appendallocator-term-recording
    content: Update AppendAllocator to record term when new file_id is first allocated
    status: pending
  - id: file-creation-term-recording
    content: Update file creation paths to record term in FileIdTermMapping
    status: pending
    dependencies:
      - appendallocator-term-recording
---

# Phase 7: File Allocation & Term Recording

## Overview

Update file allocation paths (AppendAllocator) to record term in FileIdTermMapping when a new file_id is first used. This ensures each file_id is associated with the term it was created in, preventing cross-term file reuse.

## Implementation Steps

### 7.1 Update AppendAllocator

**Goal**: Record term in FileIdTermMapping when new file_id is first allocated.

**Steps**:
1. Locate `AppendAllocator::Allocate` in `page_mapper.cpp`
2. Detect first allocation for a new file_id:
   - When `file_id` changes (crosses file boundary)
   - Check if this file_id is new (not in FileIdTermMapping)
3. Record term in FileIdTermMapping:
   - Access FileIdTermMapping via shard_ptr in PartitionFiles
   - Get PartitionFiles from PageMapper context
   - Or pass term through allocation path
4. Access the mapping via shard_ptr:
   - Get shared_ptr from PartitionFiles
   - GetOrCreate if null
   - Insert `(file_id, term)` pair
5. Ensure term is recorded when file_id is first used:
   - Check if file_id exists in mapping
   - If not, insert with current process_term
   - If exists, verify term matches (optional check)

**Files**: `page_mapper.cpp`, `page_mapper.h`

**Success Criteria**:
- [ ] Term recorded when new file_id allocated
- [ ] FileIdTermMapping updated correctly
- [ ] Access via shared_ptr works
- [ ] No duplicate recording
- [ ] Unit tests verify term recording
- [ ] Integration tests with file allocation

### 7.2 Update file creation paths

**Goal**: Ensure term is recorded when new data files are created.

**Steps**:
1. Locate file creation methods in `async_io_manager.cpp`:
   - `CreateFile` or similar methods
   - File creation during write operations
2. When creating new data files:
   - Get file_id for new file
   - Access FileIdTermMapping from PartitionFiles
   - Record term in mapping
3. Update `CreateFile` and related methods:
   - Accept term parameter or get from IO manager
   - Record term in FileIdTermMapping
   - Ensure mapping is updated before file is used
4. Handle edge cases:
   - File creation during recovery
   - File creation during normal writes
   - Ensure term is available in all contexts

**Files**: `async_io_manager.cpp` (file creation methods)

**Success Criteria**:
- [ ] Term recorded when files created
- [ ] All file creation paths updated
- [ ] Mapping updated before file use
- [ ] Edge cases handled
- [ ] Unit tests verify file creation
- [ ] Integration tests with file operations

## Dependencies

- Phase 2: FileIdTermMapping Structure (for mapping access)

## Related Phases

- Phase 3: Manifest Payload Serialization (serializes recorded terms)
- Phase 10: Manifest Writing (detects mapping changes)

## Design Notes

- Term recorded on first allocation, not on every allocation
- FileIdTermMapping tracks which term each file_id belongs to
- Term lookup used in Phase 6 for downloads
- Mapping changes trigger full snapshot (Phase 10)

## Testing

Create unit tests:
- Test term recording on new file_id allocation
- Test no duplicate recording for same file_id
- Test term recording during file creation
- Test mapping access via shared_ptr
- Integration tests with file allocation and creation

