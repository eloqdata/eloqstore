# Design Doc: Zero-Copy Read and Write Paths for Very Large Values

## Motivation

KV cache reads/writes very large string values on the storage engine, from 1MB to 10MB. EloqStore uses B+-trees to store key-value pairs. When used for KV cache, it's beneficial to use zero-copy to pass these string values from the upper layers (an in-memory cache module and the networking module) to EloqStore, which writes them to stable storage, and to read them from EloqStore and pass them to upper layers. This project augments EloqStore to realize zero-copy of very large values for both point reads and batch writes. Scans are not considered in this project. The change should not break EloqStore's existing implementation, which excels at small key value pairs. 

## Overview

To realize zero copy across three modules (networking, in-memory cache and the storage engine EloqStore), a global memory area (one large chunk or several chunks) in each core is registered in the two io urings, one ring in the networking layer and the other in EloqStore. Note that current EloqStore also has registered buffers, but they are local to EloqStore and stores index and data pages. This global memory area is unique in that it span multiple modules and only stores very large strings. 

 -> When networking receives a write request, it parses the key and zero copies the value into one or more blocks (``struct IoBufferRef``) in the registered area. When this key-value pair is cached in the in-memory cache, the value is in the form of linked blocks (``class IoStringBuffer``), each referencing a block in the global registered area. The key-value is written to EloqStore via ``BatchWriteTask`` which uses  io uring fixed write to flush stable storage.

 -> When networking receives a read request, it parses the key and checks the in-memory cache. If it's a cache miss, it reads from EloqStore via ``ReadTask``, which searches the B+-tree tree, locates the position of the value and uses io uring fixed read to load the value into one or more chunks in the global registerd memory area. 

 ## Global Registered Memory

 The global registered memory spans multiple modules including EloqStore. For ease of development, we define it under the EloqStore folder (`include/` and `src/`). The class named `GlobalRegisteredMemory` represents a global memory area in each core. It exposes the following APIs:
 - MemChunks(), exposing one or more memory chunks owned by itself to the networking module or EloqStore. EloqStore registers these chunks in its io uring when initialized. Each chunk is expected to one or multiple GBs.
 - GetSegment(), returning a `std::pair<char *, uint32_t>` where the first element is the segment pointer and the second is the chunk index within `chunks_`. The caller computes the io_uring buffer index by adding its own registered buffer start index, since the same `GlobalRegisteredMemory` may be registered in multiple io_urings (e.g., one for EloqStore and one for networking) at different buffer index offsets. The size of the segment is fixed once configured. The configured size can vary from 128KB to 512KB. Once returned, the segment cannot be returned to other callers, until it is recycled explicitly via Recycle().
 - Recycle(segment, chunk_index), notifying that a memory segment can be recycled for future use. The chunk_index is the chunk index returned by GetSegment().

Both memory chunks and segments are 4KB aligned.

`GlobalRegisteredMemory` maintains a lock-free free-list stack. The list head is a single `std::atomic<uintptr_t>` member `head_`. Because x86-64 user-space pointers occupy only 48 bits of a 64-bit integer, the remaining 16 low bits are available when the pointer is shifted left by 16. Each entry in the free list encodes its successor pointer as a single `uintptr_t` with the following layout:

```
bits 63..16  — successor pointer shifted left 16
bits 15..1   — chunk_idx of the successor segment
bit  0       — Harris mark bit (1 = this node is being popped)
```

`head_` uses the same layout with the mark bit always 0. A value of 0 means the list is empty.

Successor pointers are stored in a **separate `successors_` array** (`std::unique_ptr<std::atomic<uintptr_t>[]>`, one slot per segment), never in the first bytes of the segment itself. Once a segment is returned to a caller, the caller may write arbitrary I/O data into it. If the successor were embedded in the segment's first 8 bytes, a stale thread could read that overwritten data as a successor, coincidentally pass the Harris mark-CAS, and write garbage into `head_`, causing a crash on the next pop. The separate `successors_` array is only ever written by `TryGetSegment` and `Recycle`, so it always contains a valid encoded value.

`GetSegment()` and `Recycle()` are thread-safe. In the vast majority of cases a single worker thread drives all modules on a given core — networking, the in-memory cache and EloqStore — so concurrent access to `GlobalRegisteredMemory` simply does not occur and no synchronisation is needed. Thread-safety is required only in rare cases where a module runs its own background thread to avoid being stalled while the main worker thread is occupied elsewhere (e.g. an in-memory cache eviction thread that recycles segments independently). The pop operation uses a Harris-style mark-then-CAS protocol: before swinging `head_`, the popping thread atomically marks bit 0 of the head segment's `successors_` slot, claiming exclusive ownership. Any other thread that observes a marked successor helps complete the head swing and retries for its own segment.

The class also a `std::function` member variable named `evict_func_`. Once set, GetSegment() calls it repeatly until finding an available memory segment. The caller of GetSegment() in EloqStore (and the networking module) must be a coroutine, so if failing to get an available segment, yield the coroutine. Since EloqStore coroutines and networking coroutines may vary, the yield function should be an input of GetSegment() (e.g., via templated parameters). The flow inside `GetSegment()` is:

1. Tries to get an available segment. If there is one, returns. 
2. If there is no available segment, calls `evict_func_()` (which is non-blocking). Then calls the input yield function and returns to 1.  

The `evict_func_` is usually set by in-memory cache module. The cache module is stateful in that it keeps very large values and their segments in memory for a long time and needs to evict very large values and recycle segments in the registered memory for new zero copy IO requests. EloqStore is stateless in that as long as each read and write task finishes, it returns any segments allocated when executing the task to the caller of the task. It's the caller's decision to either free them immediately or keep them in memory.

## Storing Very Large Strings

### Data Page Extension

EloqStore implements a crash-safe B+-tree. The tree has two types of pages, index pages and data pages. When supporting very large string values, index pages are no different. A data page stores a sequence of key-value pairs. The value stored in the data page currently can be either (i) a string representing the true value, or (ii) an array of pointers, pointing to data in separate pages. The second type represents an overflow value that cannot fit into a single data page and must span one or more pages. Overflow pages are in a linked list. The value content points to the first K pages of the overflow value. Index and data pages are stored together in one or more files called "data files". 

To accommodate very large values, we extend the data page such that the value content in the data page can also be an array of pointers. Each pointer points to a segment in a file, which is designated exclusively for storing very large strings. We refer to these files as "segment files". The size of the segment is same as the segment in `GlobalRegisteredMemory`. In contrast to overflow values, we assume the value content includes pointers to all segments of the very large string and there is no additional chaining in the end. We further assume that the maximal number of segments is bounded, so the value field always fits into one 4KB data page, when representing very large values. 

To distinguish different types of values in data page, we use the lowest 4 bits of the value length field to encode properties of the value. The 2nd and 3rd bits combined now represent the compression type: no compression, dictionary-based compression and standalone compression. The two bits can represent 4 states and currently we only use 3 of them to denote the compression type. We use the 4th state to denote that the value is a very large string and we should decode the value content as an array of pointers, pointing to segments in segment files.

### Segment Files

We introduce a new type of files, known as "segment files", for storing very large strings. While data files support two modes, one for append only and the other for random writes, segment files are append only and segment file Id's are monotonically increasing. Naming convention of segment files is similar to that of data files, as directed at `DataFileName` in `include/common.h`, except that the `FileNameData` is "segment" not "data". 

Allocating a new file segment is similar to allocating a file page in the append-only mode: each time a physical file segment is requested, it is allocated to the end of the current last segment file and the file segment Id increments. That is: file segment Id (or physical segment Id) always roll forward. File pages use `AppendAllocator` as the file page allocator. File segments need a similar allocator, except that the allocation unit is much larger, 4KB page vs. 256KB segment. The sizes can be differentiated in `KvOptions`, which is the input of the allocator. Parameters in `KvOptions` overwrite default parameters in classes. 

For each table partition, we maintain a collection of open file descriptors of data files, i.e., `std::unordered_map<TableIdent, PartitionFiles> tables_` in `include/async_io_manager.h`. Open file descriptors are cached and maintained in a LRU list. This collection is extended to include segment files. The key of the `fds_` hash map in `PartitionFiles` is `TypedFileId`, a strongly-typed wrapper around `FileId` that encodes the file type in its lowest bit: data files have LSB=0 and segment files have LSB=1. Two helper functions, `DataFileKey(FileId)` and `SegmentFileKey(FileId)`, construct a `TypedFileId` by shifting the on-disk file Id left by 1 bit and setting the appropriate LSB. `TypedFileId` also provides `IsDataFile()`, `IsSegmentFile()`, and `ToFileId()` to query the type and extract the original on-disk file Id. Sentinel values for the directory and manifest file descriptors use reserved high values in `TypedFileId` that cannot collide with encoded data/segment IDs. This encoding is local to `PartitionFiles`; on-disk file Ids use the original number and are differentiated by file name extensions, i.e., "data" or "segment". In other places where a `FileId` may refer to a data file or a segment file, the file type is made explicit at the call site via `DataFileKey()` or `SegmentFileKey()`.

### Segment Mapping

In current implementation, we use logical Id's to represent index and data pages. There is a mapping of typed `PageMapper` that maps logical Id's to physical Id's. We refer to this mapping as "data mapping". Both index and data pages are fixed-length, i.e., 4KB, and each file is also fixed-length, so dividing the physical Id by the number of pages per file gives us the page's file Id and the offset in the file. 

In supporting very large values, we use segments rather than pages in storing large values, i.e., a very large value/string span one or more segments. We adopt the logical ID design and introduce a new mapping called "segment mapping" of type `PageMapper` for segments that maps logical segment Id's to Id's of file segements. File segments are fixed-length (e.g., 128KB, 256KB or 512KB), and each file contains a fixed number of segments (e.g., 256 segments or 1024 segments).

The new segment mapping needs similar handling as the original page mapping. This includes: 

- When a read task starts, it gets a snapshot of the mapping, ensuring the task sees a consistent snapshot. 
- A write task (e.g., `BatchWriteTask`) creates a copy-on-write mapping of the snapshot. It updates the mapping in its own copy and only replaces the old mapping associated with ``RootMeta`` with its own version. 
- When replacing the old mapping with the new mapping, it's possible that some ongoing read tasks are still using the old mapping, which cannot be destructed immediately. So, we need to trace the reference count of a mapping instance and only call destruction when no one is using it. 

## Batch Write Task

EloqStore only supports batch writes via `BatchWriteTask`. Current `BatchWriteTask` takes input as a sorted array of key and value pairs. Both keys and values are of type `std::string`. To enable zero copy of very large values, the input value is extended to support `IoStringBuffer` (e.g., via `std::variant`).  

`BatchWriteTask` traverses the tree and gradually merges the input keys with keys in the data pages. The tree is copy-on-write, so merging creates a new data page at time when the page is full, flushes the new data page into data files and propogates the data page's logical Id to the parent index page. Current implementation already accommodates two types of values, short values and overflow values. 

We now support a third type: very large strings. The segment mapper in `CowRootMeta` is created lazily: `MakeCowRoot()` copies it from `RootMeta` if present, but for a newly created partition (where no segment mapper exists yet), the segment mapper is created on demand when the first very large value is encountered during the batch write. This avoids unnecessary overhead for partitions that never store very large values.

Similar to how overflow values are handled, when a very large value is to be added to the current data page, we allocate logical segment Id's from the segment mapping and new file segments. The segments in `IoStringBuffer` are flushed to file segments concurrently and the segment mapping is updated, with allocated logical segment Id's pointing to the file segments. The array of logical segment Id's are encoded as the value content, which is added to the data page. The whole process is similar to how overflow values are handled, i.e., `WriteOverflowValue()` and how it is called, except that very large strings are written segments using the segment mapping and there is no chaining of segments. 

When the very large value is overwritten or deleted, a function similar to `DelOverflowValue()` (i.e., `DelLargeValue()`) is called to free the logical segments in the segment mapping. File segments will be recycled later by the garbage collection process.

The encoding of the value content for large values is as follows: the first 4 bytes represents the actual length of the very large value in bytes. The length divided by the size of each segment gives us the number of segments K. Following the 4 bytes are K*sizeof(uint32_t) array that encodes the K segment Id's.

### Persist Segment Mapping

At the end of the batch write task, the updated data mapping is stored persistently in a file called "manifest". Updates to the data mapping are first stored as update/log records. The base mapping snapshot plus update/log records are used to restore the newest mapping in memory when the manifest is loaded. When the size of update/log records exceeds a threshold, the update/log records are truncated and the whole mapping snapshot is stored. 

In supporting very large values, the segment mapping is appended after the existing data mapping and FileIdTermMapping in the manifest record. The format for both log and snapshot records is:

```
[ data_mapping_bytes_len (Fixed32) | data_mapping_bytes ... ]
[ file_id_term_bytes_len (Fixed32) | file_id_term_bytes ... ]
[ segment_mapping_bytes_len (Fixed32) | segment_mapping_bytes ... ]
```

Each segment file maintains its own term entry in `FileIdTermMapping`, keyed by `SegmentFileKey(file_id)`. This is analogous to data files, which are keyed by `DataFileKey(file_id)`. The on-disk format stores `TypedFileId::value_` directly, so data file entries (even values) and segment file entries (odd values) are naturally distinguished. Older manifests without segment mapping are backward-compatible: if no trailing bytes remain after parsing the `FileIdTermMapping`, the segment mapping is assumed empty.

### Restore Segment Mapping on Load

When a table partition's metadata is loaded from persistent storage (e.g., `load_meta` lambda in `IndexPageManager::FindRoot()`), the `Replayer` replays the manifest to restore the page mapping. The segment mapping must be restored in the same way: `Replayer` should parse the segment mapping bytes from the manifest records and produce a segment `PageMapper`. The restored segment mapper is assigned to `meta->segment_mapper_`, and its `MappingSnapshot` is inserted into `meta->segment_mapping_snapshots_`.

### Crash Recovery

If the process crashes after writing segments to segment files but before persisting the updated manifest, there is nothing to recover. On restart, the old segment mapping and the old tail segment Id are restored from the manifest. As new file segments are allocated and the segment Id increments, the orphaned segments will be overwritten. No special cleanup is needed.

### Garbage Collection

At the end of `BatchWriteTask`, the page mapping is scanned to check if any data file's used pages fall below a threshold and thus needs to be compacted. This is done inside `CompactIfNeeded()` of `WriteTask::UpdateMeta()`. In addition to check the page mapping, the segment mapping should also be checked to compact segment files in the same way as the page mapping. 

## Read Task

A read task `ReadTask` performs a point read for a given key and stores the value in the input string, i.e., `Read()` in `include/tasks/read_task.h`. To support zero copy of very large values, the input `std::string &value` needs to be extended to support `IoStringBuffer`, i.e., by adding a new input parameter `IoStringBuffer &large_value` (the reason that the new parameter is in parallel to the old `std::string &value` is because given the input key, we don't know what type of the value is until we read the value length field in the data page).

The read task performs a binary search on index pages to locate the data page that includes the input key. Once locating the input key, the task starts decoding the lowest 4 bits of the value length to obtain the type of the value. With the extension of very large values, if the 2nd and 3rd bits indicate that this value is a very large string, the execution should treat the value content as pointers to segments. 

1. By decoding the value content, we know that the very large string span K segments and their logical segment Id's.
2. Resolve the K logical segment Id's to physical file segment Id's via the segment mapping snapshot.
3. Try to allocate K segments in `GlobalRegisteredMemory` via `GetSegment()`.
4. Add K segments to the input `IoStringBuffer`.
5. Read the K segments via `ReadSegments` of `AsyncIoManager` using io uring fixed reads, passing the physical file segment Id's and the allocated memory segments.

## AsyncIoManager

In the project, we use `AsyncIoManager` to abstract the IO system. To support segment files, two main APIs needed for extension are `ReadSegments` and `WriteSegments`, which are similar to their counterparts `ReadPages` and `WritePages`. There also need to be extensions or changes to existing APIs to cope with file operations on segment files.

The main class that implements the interfaces is `IouringMgr`. For ease of development, we do not consider other subclasses for now.

### Registering Global Memory in io_uring

`IouringMgr::BootstrapRing()` takes an optional `GlobalRegisteredMemory *` parameter. If set, the memory chunks from `GlobalRegisteredMemory::MemChunks()` are appended to the iovec array before calling `io_uring_register_buffers()`. A member variable `global_reg_mem_index_base_` records the iovec index of the first global memory chunk, so that when `IouringMgr` gets a segment via `GetSegment()`, it computes the io_uring buffer index as `global_reg_mem_index_base_` plus the chunk index returned by `GetSegment()`.
