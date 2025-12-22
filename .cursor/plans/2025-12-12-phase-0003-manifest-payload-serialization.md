---
name: Manifest Payload Serialization
overview: Update ManifestBuilder::Snapshot to serialize FileIdTermMapping and Replayer::DeserializeSnapshot to deserialize it from manifest payload.
todos:
  - id: manifest-snapshot-serialize
    content: Update ManifestBuilder::Snapshot to serialize FileIdTermMapping after mapping table
    status: completed
  - id: replayer-deserialize-snapshot
    content: Update Replayer::DeserializeSnapshot to deserialize FileIdTermMapping
    status: completed
    dependencies:
      - manifest-snapshot-serialize
---

# Phase 3: Manifest Payload Serialization

## Overview

Update manifest snapshot serialization to include `FileIdTermMapping` in the payload (written before the mapping table). Update Replayer to deserialize this mapping and store it in shared_ptr for use by other components.

## Implementation Steps

### 3.1 Update ManifestBuilder::Snapshot

**Goal**: Append FileIdTermMapping to manifest snapshot payload (always present).

**Steps**:
1. Locate `ManifestBuilder::Snapshot` in `root_meta.cpp`
2. Before `mapping->Serialize(buff_)`, append FileIdTermMapping:
   - Get FileIdTermMapping from PartitionFiles (via shared_ptr)
   - Write count via PutVarint64 (count can be 0 if file_id_mapping is empty)
   - For each `(file_id, term)` pair:
     - PutVarint64(file_id)
     - PutVarint64(term)
3. Update function signature if needed to accept FileIdTermMapping:
   - Pass shared_ptr from PartitionFiles
   - Or access via PageMapper/WriteTask context

**Files**: `root_meta.cpp`, `root_meta.h`

**Success Criteria**:
- [ ] FileIdTermMapping serialized in the snapshot payload before the mapping table
- [ ] Empty mapping writes count=0 (still written)
- [ ] Serialized format matches deserialization expectations
- [ ] Unit tests verify serialization

### 3.2 Update Replayer::DeserializeSnapshot

**Goal**: Deserialize FileIdTermMapping from manifest snapshot and store in shared_ptr (mapping section always present).

**Steps**:
1. Locate `Replayer::DeserializeSnapshot` in `replayer.cpp`
2. After reading mapping table, expect mapping data to follow:
   - Read count via GetVarint64
   - If count == 0, mapping is empty
   - Otherwise, read count pairs:
     - GetVarint64(file_id)
     - GetVarint64(term)
     - Insert into mapping
3. Handle errors gracefully:
   - If deserialization fails, clear mapping and treat as empty (but still return success to avoid crash); log warning.
4. Store in shared_ptr:
   - Create new shared_ptr with deserialized mapping
   - Store in Replayer member for later use in GetMapper
   - Will be restored to PartitionFiles in Phase 4

**Files**: `replayer.cpp`, `replayer.h`

**Success Criteria**:
- [ ] FileIdTermMapping deserialized correctly from the dedicated mapping section
- [ ] Empty mapping (count=0) handled correctly
- [ ] Malformed data handled gracefully (clear mapping, warn)
- [ ] Mapping stored in shared_ptr
- [ ] Unit tests verify deserialization
- [ ] Roundtrip tests (serialize → deserialize → verify)

## Dependencies

- Phase 2: FileIdTermMapping Structure (completed)

## Related Phases

- Phase 4: Replayer Term Validation (uses deserialized mapping)
- Phase 10: Manifest Writing (serializes mapping)

## Design Notes

- FileIdTermMapping is only in full snapshots, not WAL records
- Snapshot payload always contains the FileIdTermMapping section:
  - A varint count field is always written (count may be 0 for empty file_id_mapping)
  - Followed by that many `(file_id, term)` varint64 pairs
- Mapping changes force full snapshot (handled in Phase 10)
- Term=0 is the default for files not present in the mapping

## Testing

Create unit tests:
- Test serialization of empty file_id_mapping (count=0)
- Test serialization of non-empty mapping
- Test deserialization from valid data (including empty file_id_mapping)
- Test deserialization from malformed data (error handling, mapping cleared, warning logged)
- Test roundtrip: serialize → deserialize → verify

