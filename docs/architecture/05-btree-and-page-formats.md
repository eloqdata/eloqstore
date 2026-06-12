# B+-Tree and Page Formats

Source: `include/storage/page.h`, `include/storage/data_page.h`,
`include/storage/mem_cached_page.h`, `include/storage/page_manager.h`,
`include/storage/data_page_builder.h`, `include/storage/index_page_builder.h`,
and the matching `src/storage/*.cpp`.

## Tree shape

Each partition is a B+-tree of 4 KiB pages (`data_page_size`):

- **Index (non-leaf) pages** hold `key → child PageId` entries. *All* index
  pages are resident in memory while the partition is open — this is the
  design bet that buys one-I/O point reads.
- **Data (leaf) pages** hold sorted KV entries and form a doubly-linked list
  (`prev/next` PageIds in the header) for scans.
- **Overflow pages** store values too big for a leaf but below the
  very-large-value threshold; a leaf entry stores an encoded pointer array.
- A second, smaller tree per partition — the **TTL tree** (root
  `ttl_root_id_`) — indexes `expire_ts → key` so expiration scans don't touch
  the main tree (doc 08).

Every page begins with `checksum(8B) | page_type(1B)` (`include/storage/page.h`;
xxhash, verified on load unless `skip_verify_checksum`).

## On-disk encodings

Two builder classes define the canonical formats; readers
(`DataPageIter`, `IndexPageIter`, `PageRegionIter`) decode the same layout.
**Any format change must update builder and iterator(s) together, and remain
replayable by old data** unless a migration is designed.

`DataPage` layout (`include/storage/data_page.h`):

```
checksum(8B) | type(1B) | content_len(2B) | prev_page(4B) | next_page(4B)
| data blob | restart_array(N*2B) | restart_num(2B) | padding |
```

- Keys are prefix-compressed against their predecessor; every
  `data_page_restart_interval` entries a *restart point* stores the full key.
  The restart array enables binary search over regions.
- Each entry stores `timestamp` and optional `expire_ts`.
- The low bits of the value-length field encode value flavor
  (`ValLenBit`): Overflow, Expire-present, and two compression bits.
  Compression bits `0b11` means **very large value**: the value bytes are an
  encoded segment-pointer array, not inline data (doc 09).
- Values may be dictionary-compressed (zstd, per-partition dictionary stored
  in the manifest) or standalone-compressed (`include/compression.h`).

`OverflowPage` layout: `checksum | type | value_len(2B) | value … pointers(N*4B)
| num_pointers(1B)` at page end. A big value is chained: the leaf entry holds
the first `overflow_pointers` page ids; each overflow page can point onward.

Index pages on disk are built by `IndexPageBuilder` with the same
prefix-compression + restart scheme, entries being `key → PageId`.

## In-memory index pages are *not* the disk bytes

`MemCachedPage` (`include/storage/mem_cached_page.h`) is the cached form of an
index page (and of data pages when `enable_data_page_cache` is on). It carries
lifecycle state the disk format doesn't have:

- `page_id_` / `file_page_id_` — durable identity.
- `ref_cnt_` pin count (`Handle` is the RAII pin). Pinned pages can't be
  evicted; tasks pin every page on their root-to-leaf path.
- LRU links (`prev_/next_`) for `PageManager`'s active list.
- A `WaitingZone` so concurrent readers of a page being loaded wait for the
  single in-flight read instead of issuing duplicates.

**Swizzling**: inside a `MappingSnapshot`, an index page's child references
can be raw `MemCachedPage*` pointers instead of logical page ids (doc 06).
`PageManager::FindPage` returns the swizzled child directly on the hot path;
`MappingSnapshot::Unswizzling` writes the durable id back before a page can be
recycled.

## PageManager: the per-shard cache

`PageManager` (one per shard) owns:

- The cached-page pool (`buffer_pool_size` bytes), LRU eviction via the
  active list; only unpinned pages are evictable. When eviction can't find a
  victim, allocation fails up the stack as `KvError::OutOfMem` → the request
  is retried (doc 03).
- `RootMetaMgr` — the LRU of per-partition `RootMeta` (doc 06).
- `SeekIndex(mapping, root, key)` — the root-to-leaf descent used by all
  readers; returns the data-page id that may contain the key.
- Mapping arenas (`MappingArena`, `MappingChunkArena`) recycling the chunked
  storage behind mapping tables.

Data-page caching is optional (`enable_data_page_cache`): when on, leaf pages
share the same pool/LRU and `LoadDataPage` consults the cache (hits are
read-only views; `LoadDataPageForUpdate` copies out a private buffer). Hit and
miss counters surface through `EloqStore::DataCacheHits/Misses`.

`PagesPool` (`include/storage/page.h`) is the underlying aligned-buffer
allocator. Buffers may be io_uring-*registered* (fixed buffers); `Page`
steals the pointer's top bit to remember registration so I/O paths can choose
fixed vs normal opcodes.

## Copy-on-write writes

Writers never mutate a published page. `BatchWriteTask::Apply`
(`src/tasks/batch_write_task.cpp`) merges a sorted batch into the tree by:

1. `MakeCowRoot` — clone the current root metadata + mappers into a private
   `CowRootMeta` (doc 06).
2. Descend with an explicit index stack (`write_tree_stack.h`), load each
   affected leaf, merge entries via `DataPageBuilder`, splitting or merging
   leaves as needed (`leaf_triple_` keeps the three adjacent leaves so the
   doubly-linked leaf list stays consistent; `Redistribute` rebalances).
3. Allocate new logical/physical pages through the CoW mapper
   (`AllocatePage`), write new pages, free replaced ones into the snapshot's
   `to_free_file_pages_`.
4. Rebuild the affected index path bottom-up (`FinishIndexPage` /
   `FlushIndexPage`).
5. `UpdateMeta` → flush a manifest record, publish the new root
   (`PageManager::UpdateRoot`), signal compaction if amplification crossed the
   threshold.

Readers that started before step 5 keep their `MappingSnapshot::Ref` and old
root; freed file pages are only recycled when the last reference to the old
snapshot dies (doc 06). This is the lock-free read-during-write story.
