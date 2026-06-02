`internal.h`
- private runtime declarations shared across the SDK implementation
- `KVCacheManager::Impl`, `KVCacheWorker::Impl`, and helper declarations

`common.cpp`
- shared helpers, internal state, and IPC codec/runtime scaffolding

`manager.cpp`
- `KVCacheManager` lifecycle, background flush thread, save/load/contains handling
- scheduler-side local methods execute synchronously; no internal request thread

`worker.cpp`
- `KVCacheWorker` descriptor attach and IPC stub methods
