---
name: Term Injection at Startup
overview: Add term parameter to EloqStore::Start() and wire term through shards to IO managers. Term is injected at startup, not stored in KvOptions.
todos:
  - id: eloqstore-term-member
    content: Add term_ member to EloqStore and Start(term) method
    status: completed
  - id: shard-term-wiring
    content: Wire term from EloqStore through Shard to AsyncIoManager
    status: completed
    dependencies:
      - eloqstore-term-member
  - id: iomanager-term-storage
    content: Store term in IouringMgr and CloudStoreMgr
    status: completed
    dependencies:
      - shard-term-wiring
---

# Phase 10: Term Injection at Startup

## Overview

Add term parameter to `EloqStore::Start(term)` and wire term through shards to IO managers. Term is injected at startup (not in KvOptions) to allow per-process/instance term configuration without persisting in options.

## Implementation Steps

### 10.1 Add term to EloqStore

**Goal**: Add term_ member to EloqStore and Start(term) method.

**Steps**:
1. Add `uint64_t term_` member to `EloqStore` class:
   ```cpp
   class EloqStore {
       // ... existing members ...
       uint64_t term_{0};  // Process term, injected at startup
   };
   ```
2. Update `Start()` method signature:
   - Change to `KvError Start(uint64_t term = 0)`
   - Store term in member: `term_ = term`
   - Default to 0 for backward compatibility
3. Add getter method:
   - `uint64_t Term() const { return term_; }`
   - Expose term to shards and other components
4. Ensure backward compatibility:
   - Default term=0 maintains legacy behavior
   - Existing code without term parameter works

**Files**: `eloq_store.h`, `eloq_store.cpp`

**Success Criteria**:
- [x] term_ member added to EloqStore
- [x] Start(term) method accepts term parameter
- [x] Term stored correctly
- [x] Getter method available
- [x] Backward compatible (default term=0)
- [ ] Unit tests verify term storage

### 10.2 Wire term through Shard

**Goal**: Pass term from EloqStore through Shard to AsyncIoManager.

**Steps**:
1. Update `Shard` to access term from EloqStore:
   - Shard already has `store_` pointer
   - Access term via `store_->Term()`
   - Pass to IO manager during initialization
2. Update `Shard::Init()`:
   - Get term from store: `uint64_t term = store_->Term()`
   - Pass to IO manager initialization
   - Or store in Shard and pass later
3. Update IO manager initialization:
   - Pass term to `AsyncIoManager::Instance()` or constructor
   - Or set term after initialization
4. Ensure term available when needed:
   - Term needed before first file operations
   - Ensure term set before IO manager starts

**Files**: `shard.h`, `shard.cpp`

**Success Criteria**:
- [x] Term accessed from EloqStore in Shard
- [x] Term passed to IO manager
- [x] Term available when needed
- [ ] Unit tests verify term wiring

### 10.3 Store term in IO managers

**Goal**: Store term in IouringMgr and CloudStoreMgr.

**Steps**:
1. Add `uint64_t term_` member to `AsyncIoManager` base class or both managers:
   - Add to `IouringMgr` class
   - Add to `CloudStoreMgr` class
   - Or add to base `AsyncIoManager` if exists
2. Update constructors/initialization:
   - Accept term parameter in constructor
   - Or set via initialization method
   - Store in term_ member
3. Update `AsyncIoManager::Instance()`:
   - Accept term parameter
   - Pass to manager constructors
   - Store in manager instances
4. Ensure term used for filename generation:
   - Term used in ToFilename (Phase 5)
   - Term used in GetManifest (Phase 6)
   - Term available for all file operations

**Files**: `async_io_manager.h`, `async_io_manager.cpp`

**Success Criteria**:
- [x] Term stored in IO managers
- [x] Term available for filename generation
- [x] Term used in all file operations
- [ ] Unit tests verify term storage
- [ ] Integration tests with term injection

## Dependencies

- None (foundation for all other phases)

## Related Phases

- All other phases depend on term being available
- Phase 5: LruFD Term Awareness (uses term_)
- Phase 6: Cloud Download (uses term_)

## Design Notes

- Term injected at startup, not in KvOptions
- Default term=0 maintains legacy behavior
- Term per process/instance, not per table
- Cloud deployments must set term > 0
- Term stored in EloqStore, accessible via getter

## Testing

Create unit tests:
- Test EloqStore::Start(term) with different terms
- Test term wiring through Shard
- Test term storage in IO managers
- Test default term=0 behavior
- Integration tests with term injection

