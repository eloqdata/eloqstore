# Design Doc: Supporting of Metadata for Very Large Values

## Motivation

EloqStore supports zero copy of reading and writing very large string values. The design is described in `zero_copy_read.md` and the implementation plan is in `zero_copy_impl.md`. This feature adds the support of adding metadata to the very large values. The main motivations to logically distinguish metadata and values are twofold. First, KV Cache inherently needs to store metadata and very large values. Very large values are in special memory areas so that data movements across VRAM, DRAM and SSD incur the minimal overhead. We need to store values and metadata separately to not to incur additional data copying and break the zero-copy promise. Second, KV Cache at runtime needs to access metadata and values separately. Distinguishing them facilitates efficient access for two scenarios.  


## Data Page Value Encoding

For a key-value pair in a data page, we use the value length field to encode whether or not the value is a very large value. When the value is a very large value, the content was previously formatted as `[ uint32_t actual_length | K × uint32_t logical_segment_id ]`. To accommodate metadata, we extend the content format by repurposing the **highest bit** of the first uint32_t as a `has_metadata` flag:

- bit 31 of the first uint32_t: `has_metadata` flag (1 = a metadata trailer follows; 0 = no metadata).
- bits 30..0 of the first uint32_t: `actual_length` in bytes. This supports values up to 2 GiB - 1, well above the 10 MiB range that KV Cache cares about.

When `has_metadata == 0`, the rest of the encoded content is exactly the pre-metadata layout (`K × uint32_t logical_segment_id`), so existing on-disk records continue to decode without changes.

When `has_metadata == 1`, the layout is:

```
[ uint32_t word0 (bit 31 = 1, bits 30..0 = actual_length) ]
[ K × uint32_t logical_segment_id ]
[ uint16_t metadata_length ]
[ metadata_bytes (metadata_length bytes) ]
```

K is **not** stored in the encoded content. It is implied by `actual_length` and the configured segment size, so the decoder recovers it as `K = ceil(actual_length / segment_size)`. With K known, the segment array has a fixed length `K * sizeof(uint32_t)`, and the metadata trailer (`uint16_t metadata_length` followed by `metadata_length` bytes) sits immediately after the segments. Storing K redundantly in the content would just take space and require a consistency check. Metadata length is capped at 65535 bytes by the `uint16_t` field, which is generous for the small descriptors KV Cache attaches to a value.

## Pinned Memory of Very Large Values

In the original design, we use `IoStringBuffer` to represent very large strings to enable zero copy.  KV Cache needs a new data structure. KV Cache, the caller of read/write requests of EloqStore, allocates one or more chunks of pinned memory (via PyTorch) registered in GPU. Pinned memory ensures CPU bypassing when copying between VRAM and DRAM. To ensure zero copy of IO, the same pinned memory needs to be registered in OS kernel. 

EloqStore currently uses `GlobalRegisteredMemory` to abstract memory areas for very large values and has APIs to register the memory areas during bootstrap, i.e., `IouringMgr::BootstrapRing()`. `GlobalRegisteredMemory` further exposes management APIs of the memory areas for EloqStore to call. What's distinguishing with KV Cache is that KV Cache has its own management of the pinned memory and does not expose maanagement APIs to EloqStore. What's exposed to EloqStore are simply (1) one or more memory chunks to be registered in the OS kernel when EloqStore is initialized, and (2) when reading/ writing a key, a memory address in pinned memory to/from which the very large value is copied.  

To register pinned memory managed by KV cache, we extend `BootstrapRing` such that in addition to `GlobalRegisteredMemory`, an array of raw memory chunks (`std::vector<std::pair<char*, size_t>>`) can be registered as well. It's fair to assume that for a single instance of EloqStore, either `GlobalRegisteredMemory` or raw memory chunks can be registered. So, the input of `BootstrapRing` is now `std::variant`. Similar to `GlobalRegisteredMemory`, the IO manager needs to remember the memory chunk array, and the buffer index of the first chunk in the array. Unlike `GlobalRegisteredMemory` where its internal memory chunks are sharded, pinned memory chunks (`std::vector<std::pair<char*, size_t>>`) are shared among all shards/cores.

When `BootstrapRing` accepts pinned memory chunks as input, we need to internally maintain a private `GlobalRegisteredMemory` instance. This is because in addition to online reads and writes operating on pinned memory, EloqStore also runs background tasks such as garbage collection (GC) that involves reading and writing segments. We cannot use pinned memory to perform zero-copy reads and writes in GC, because its management is oblivious. Rather, EloqStore needs to maintain its own `GlobalRegisteredMemory` instance, if no external `GlobalRegisteredMemory` instance is passed in. The capacity of `GlobalRegisteredMemory` for each shard can be small, e.g., 16 or 32MB per shard. We can always ensure that GC never issues more on-the-fly reads/writes of segments than the allocated `GlobalRegisteredMemory`.

## Reading Metadata and Very Large Values Separately

KV Cache needs to access metadata and very large values separately. We extend the the read API to distinguish two different needs. Currently, a read task `ReadTask` performs a point read for a given key and stores the conventional value in `std::string &value` or the very large value in `IoStringBuffer`, i.e., `Read()` in `include/tasks/read_task.h`. To support the new feature for KV Cache, we continue to use `std::string &value` for reading metadata. We add a second `Read()` API, which takes input as a block of contiguous  memory, `char *` and `size_t`, into which the very large value is copied.

For the first `Read()` API that reads metadata, the execution path is same as conventional point reads. The only difference is that once the data page is located, only the metadata portion of the value field is copied into the input string.

For the second `Read` API that reads the very large value into contiguous pinned memory, the search path is same as before. Once the data page is located, execution decodes the logical pointers of segments and loads file segments into the contiguous memory concurrently. Since pinned memory has been registered as fixed buffers, loading is done via IO uring fixed reads. To determine the buffer index, we may use the input address `char *` to binary or linear search the array of pinned memory chunks to determine which chunk the address falls into. 

When decoding logical pointers, we also know the total number of segments. If the input `size_t` is less than (# of segments * segment size), it means the last segment is only partially read. The IO uring request of the last segment should be constructed to read the correct size. Even though the last segment is partially read, it's safe to assume that the read content is always 4KB aligned.  

## Writing Metadata and Very Large Values Separately

EloqStore uses ``BatchWriteTask` to write key-value pairs (`WriteDataEntry`). `WriteDataEntry` already contains `std::string val_` for a normal value and `IoStringBuffer large_val_` for a very large value. The two variables are mutually exclusive. We continue to use `std::string val_` for metadata, and change `large_val_` into `std::variant`, where the first option is `IoStringBuffer` and the second option is a pinned memory block `std::pair<const char *, size_t>`. `std::string val_` continue to be mutually exclusive with `IoStringBuffer` but can co-exist with `std::pair`. The first constructor of `WriteDataEntry` that takes input as `std::string val` has a new optional input `std::pair`. 

The write path is similar as before. The difference is how the value field of a key-value pair in a data page is constructed when `std::string` and `std::pair<std::pair<const char *, size_t>>` both exist. The first step is to persist the very large value and get the logical pointers of segments. `BatchWriteTask::WriteLargeValue()` is the function to write a very large value in `IoStringBuffer` and store logical pointers in the member variable `large_value_content_`. We need a similar function for a very large value in `std::pair<const char *, size_t>`. There can be a helper function that implements the common logic of both functions. After the very large value is written, the content of the value field can then be constructed including metadata and logical pointers.  

