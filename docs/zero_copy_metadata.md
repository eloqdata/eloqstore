# Design Doc: Supporting of Metadata for Very Large Values

## Motivation

EloqStore supports zero copy of reading and writing very large string values. The design is described in `zero_copy_read.md` and the implementation plan is in `zero_copy_impl.md`. This feature adds support of adding metadata to the very large string values. It also adds mew read/write APIs for the KV Cache library named LMcache, which needs to store metadata and very large string values. 

## Data Page Value Encoding

For a key-value pair in a data page, we use the value length field to encode if the value is a very large value. When the value is a very large value, the content is in the format of `[ uint32_t actual_length | K × uint32_t logical_segment_id ]`. To accommodate metadata, we extend the content format as follows. In the first uint32_t integer, we use the lower 16 bits to encode `actual_length` and the higher 16 bits to encode the length of metadata. This encoding ensures backward compatability such that if the metadata is absent, the higher 16 bits are zero, meaning the length of metadata is 0. It's reasonable to assume that the number of segments never exceeds 65535, because for KV Cache the value is on the order of one or tens of MB. The second part of the value content contains `K × uint32_t logical_segment_id`, followed by the metadata string if the metadata is present. 

## Representation of Very Large Value and Metadata

In the original design, we use `IoStringBuffer` to represent very large strings to enable zero copy.  LMcache needs a new data structure. LMcache, the caller of read/write requests of EloqStore, allocates one or more chunks of pinned memory (via PyTorch) registered in GPU. Pinned memory ensures CPU bypassing when copying between VRAM and DRAM. To ensure zero copy of IO, the same pinned memory needs to be registered in OS kernel. 

EloqStore currently uses `GlobalRegisteredMemory` to abstract memory areas for very large values and has APIs to register the memory areas during bootstrap, i.e., `IouringMgr::BootstrapRing()`. `GlobalRegisteredMemory` further exposes management APIs of the memory areas for EloqStore to call. What's distinguishing with LMcache is that LMcache has its own management of these memory areas, which are invisible to EloqStore. What's exposed to EloqStore are simply one or more chunks of memory areas, and for each read/write, a piece of memory in these areas to/from which the very large value is copied.  

We need to extend `BootstrapRing` such that in addition to `GlobalRegisteredMemory`, an array of raw memory chunks can be registered as well. It's fair to assume that for a single instance of EloqStore, either `GlobalRegisteredMemory` or raw memory chunks can be registered. So, the input of `BootstrapRing` is now `std::variant`. The IO manager needs to remember the memory chunk array. 