from __future__ import annotations

import ctypes
import ctypes.util
from ctypes import POINTER, byref, c_bool, c_char_p, c_double, c_size_t, c_uint8, c_uint64, cast, c_void_p
from dataclasses import dataclass, field
import mmap
import os
import time
from typing import Sequence

import torch

from ._errors import EloqStoreError
from ._ffi import (
    CGetResult,
    CKVCacheBufferHandle,
    CKVCacheRequestState,
    last_error,
    lib,
)


def _ok_status(status: int) -> None:
    """Raise EloqStoreError when a status-code API reports failure."""
    if status != 0:
        raise EloqStoreError(status, last_error())


def _ok(ok: bool, status: int = 1) -> None:
    """Raise EloqStoreError when a bool-returning FFI call fails."""
    if not ok:
        raise EloqStoreError(status, last_error())


def _encode(value: str | bytes) -> bytes:
    """Normalize Python text/bytes inputs to raw UTF-8 bytes."""
    return value if isinstance(value, bytes) else value.encode("utf-8")


def _uint8_ptr(value: bytes):
    """Expose immutable Python bytes as a `uint8_t*` for the C API."""
    return cast(c_char_p(value), POINTER(c_uint8))


def _decode_request_status(status: int) -> str:
    return {1: "pending", 2: "ready", 3: "failed"}.get(status, "failed")


def _shared_memory_path(shared_memory_name: str) -> str:
    return (
        shared_memory_name
        if shared_memory_name.startswith("/dev/shm/")
        else f"/dev/shm{shared_memory_name if shared_memory_name.startswith('/') else '/' + shared_memory_name}"
    )


def _derive_runtime_shape(
    memory_bytes: int,
    cpu_threads: int,
    block_payload_bytes: int,
) -> tuple[int, int, int, int, int]:
    entry_alignment = 4096
    if memory_bytes < entry_alignment:
        raise ValueError("memory_bytes must be at least 4096 bytes")
    if cpu_threads <= 0:
        raise ValueError("cpu_threads must be greater than zero")
    if block_payload_bytes <= 0:
        raise ValueError("block_payload_bytes must be greater than zero")

    entry_size = (block_payload_bytes + entry_alignment - 1) // entry_alignment * entry_alignment
    if entry_size > memory_bytes:
        raise ValueError("memory_bytes is too small for one aligned KV block payload")

    entry_count = memory_bytes // entry_size
    if entry_count < cpu_threads:
        raise ValueError("memory_bytes is too small for the requested cpu_threads")
    partition_count = cpu_threads
    submission_queue_depth = max(128, cpu_threads * 32)
    return (
        int(entry_size),
        int(entry_count),
        entry_alignment,
        int(partition_count),
        int(submission_queue_depth),
    )


@dataclass(slots=True)
class KVCacheRequestResult:
    request_id: int
    status: str
    offset: int
    length: int


@dataclass(slots=True)
class KVCacheSharedBuffer:
    descriptor: str
    shared_memory_name: str
    shm_path: str
    mapped_bytes: int
    entry_size: int
    entry_count: int
    entry_alignment: int
    shard_count: int
    partition_count: int
    fd: int | None = None
    mmap_obj: mmap.mmap | None = None
    tensor: torch.Tensor | None = None
    cuda_registered: bool = False
    cuda_base_ptr: int = 0

    @classmethod
    def from_descriptor(cls, descriptor: str) -> "KVCacheSharedBuffer":
        parts = descriptor.split("|")
        if len(parts) < 9:
            raise ValueError("buffer pool descriptor format is invalid")
        return cls(
            descriptor=descriptor,
            shared_memory_name=parts[0],
            shm_path=parts[1],
            mapped_bytes=int(parts[2]),
            entry_size=int(parts[3]),
            entry_count=int(parts[4]),
            entry_alignment=int(parts[5]),
            shard_count=int(parts[6]),
            partition_count=int(parts[8]),
        )

    def attach(self) -> None:
        if self.mmap_obj is not None:
            return
        fd = os.open(self.shm_path, os.O_RDWR)
        try:
            mm = mmap.mmap(
                fd,
                self.mapped_bytes,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
            )
        except Exception:
            os.close(fd)
            raise
        self.fd = fd
        self.mmap_obj = mm
        self.tensor = torch.frombuffer(memoryview(mm), dtype=torch.uint8, count=self.mapped_bytes)

    def register_cuda(self) -> None:
        if self.cuda_registered:
            return
        if self.mmap_obj is None:
            self.attach()
        if self.mmap_obj is None:
            raise RuntimeError("shared buffer is not attached")
        cudart = _load_cudart()
        if cudart is None:
            raise RuntimeError("Unable to load cudart for cudaHostRegister")
        base_ptr = ctypes.addressof(ctypes.c_char.from_buffer(self.mmap_obj))
        rc = cudart.cudaHostRegister(
            ctypes.c_void_p(base_ptr),
            ctypes.c_size_t(self.mapped_bytes),
            ctypes.c_uint(0),
        )
        if rc != 0:
            raise RuntimeError(f"cudaHostRegister failed with error code {rc}")
        self.cuda_registered = True
        self.cuda_base_ptr = base_ptr

    def slice(self, offset: int, length: int) -> memoryview:
        if self.mmap_obj is None:
            self.attach()
        if self.mmap_obj is None:
            raise RuntimeError("shared buffer is not attached")
        return memoryview(self.mmap_obj)[offset : offset + length].cast("B")

    def slice_tensor(self, offset: int, length: int) -> torch.Tensor:
        if self.tensor is None:
            self.attach()
        if self.tensor is None:
            raise RuntimeError("shared buffer tensor is not attached")
        return self.tensor.narrow(0, offset, length)

    def close(self) -> None:
        if self.cuda_registered and self.cuda_base_ptr:
            cudart = _load_cudart()
            if cudart is not None:
                cudart.cudaHostUnregister(ctypes.c_void_p(self.cuda_base_ptr))
        self.cuda_registered = False
        self.cuda_base_ptr = 0
        self.tensor = None
        if self.mmap_obj is not None:
            self.mmap_obj.close()
            self.mmap_obj = None
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _load_cudart():
    libname = ctypes.util.find_library("cudart")
    if not libname:
        return None
    cudart = ctypes.CDLL(libname)
    cudart.cudaHostRegister.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint]
    cudart.cudaHostRegister.restype = ctypes.c_int
    cudart.cudaHostUnregister.argtypes = [ctypes.c_void_p]
    cudart.cudaHostUnregister.restype = ctypes.c_int
    return cudart


@dataclass(slots=True)
class ClientOptions:
    store_paths: Sequence[str] = field(default_factory=list)
    table_name: str = "default"
    partition_id: int = 0
    branch: str = "main"
    term: int = 0
    partition_group_id: int = 0
    num_threads: int = 1


@dataclass(slots=True)
class _KVCacheRuntimeOptions:
    """Internal transport object shared by manager/worker wrappers.

    Public SDK callers should use `KVCacheManagerOptions` or
    `KVCacheWorkerOptions`. This low-level shape exists only because the native
    C/C++ layer still consumes one common options struct.
    """
    store_paths: Sequence[str] = field(default_factory=list)
    table_name: str = "default"
    branch: str = "main"
    ipc_path: str = ""
    shared_memory_name: str = ""
    term: int = 0
    partition_group_id: int = 0
    num_threads: int = 1
    partition_count: int = 1
    shared_memory_bytes: int = 0
    entry_size: int = 0
    entry_count: int = 0
    entry_alignment: int = 4096
    submission_queue_depth: int = 128

    def to_handle(self):
        """Materialize this Python options object as a native C options handle."""
        native = lib().CEloqStore_KVCacheOptions_Create()
        if not native:
            raise EloqStoreError(1, last_error())
        try:
            for path in self.store_paths:
                lib().CEloqStore_KVCacheOptions_AddStorePath(native, _encode(path))
            lib().CEloqStore_KVCacheOptions_SetTableName(
                native, _encode(self.table_name)
            )
            lib().CEloqStore_KVCacheOptions_SetBranch(native, _encode(self.branch))
            if self.ipc_path:
                lib().CEloqStore_KVCacheOptions_SetIpcPath(native, _encode(self.ipc_path))
            if self.shared_memory_name:
                lib().CEloqStore_KVCacheOptions_SetSharedMemoryName(
                    native, _encode(self.shared_memory_name)
                )
            lib().CEloqStore_KVCacheOptions_SetNumThreads(native, self.num_threads)
            lib().CEloqStore_KVCacheOptions_SetPartitionCount(native, self.partition_count)
            lib().CEloqStore_KVCacheOptions_SetTerm(native, self.term)
            lib().CEloqStore_KVCacheOptions_SetPartitionGroupId(
                native, self.partition_group_id
            )
            lib().CEloqStore_KVCacheOptions_SetSharedMemoryBytes(
                native, self.shared_memory_bytes
            )
            lib().CEloqStore_KVCacheOptions_SetEntrySize(native, self.entry_size)
            lib().CEloqStore_KVCacheOptions_SetEntryCount(native, self.entry_count)
            lib().CEloqStore_KVCacheOptions_SetEntryAlignment(
                native, self.entry_alignment
            )
            lib().CEloqStore_KVCacheOptions_SetSubmissionQueueDepth(
                native, self.submission_queue_depth
            )
            return native
        except Exception:
            lib().CEloqStore_KVCacheOptions_Destroy(native)
            raise


@dataclass(slots=True)
class KVCacheManagerOptions:
    """Options for the scheduler / engine-core-side manager runtime."""
    store_paths: Sequence[str] = field(default_factory=list)
    table_name: str = "default"
    branch: str = "main"
    ipc_path: str = ""
    shared_memory_name: str = ""
    term: int = 0
    partition_group_id: int = 0
    num_threads: int = 1
    partition_count: int = 1
    shared_memory_bytes: int = 0
    entry_size: int = 0
    entry_count: int = 0
    entry_alignment: int = 4096
    submission_queue_depth: int = 128

    @classmethod
    def from_budget(
        cls,
        *,
        store_paths: Sequence[str],
        table_name: str,
        memory_bytes: int,
        cpu_threads: int,
        block_payload_bytes: int,
        branch: str = "main",
        ipc_path: str = "",
        shared_memory_name: str = "",
        term: int = 0,
        partition_group_id: int = 0,
    ) -> "KVCacheManagerOptions":
        entry_size, entry_count, entry_alignment, partition_count, submission_queue_depth = (
            _derive_runtime_shape(memory_bytes, cpu_threads, block_payload_bytes)
        )
        return cls(
            store_paths=store_paths,
            table_name=table_name,
            branch=branch,
            ipc_path=ipc_path,
            shared_memory_name=shared_memory_name,
            term=term,
            partition_group_id=partition_group_id,
            num_threads=cpu_threads,
            partition_count=partition_count,
            shared_memory_bytes=memory_bytes,
            entry_size=entry_size,
            entry_count=entry_count,
            entry_alignment=entry_alignment,
            submission_queue_depth=submission_queue_depth,
        )

    def to_runtime_options(self) -> _KVCacheRuntimeOptions:
        """Convert the public manager config into the native transport shape."""
        return _KVCacheRuntimeOptions(
            store_paths=self.store_paths,
            table_name=self.table_name,
            branch=self.branch,
            ipc_path=self.ipc_path,
            shared_memory_name=self.shared_memory_name,
            term=self.term,
            partition_group_id=self.partition_group_id,
            num_threads=self.num_threads,
            partition_count=self.partition_count,
            shared_memory_bytes=self.shared_memory_bytes,
            entry_size=self.entry_size,
            entry_count=self.entry_count,
            entry_alignment=self.entry_alignment,
            submission_queue_depth=self.submission_queue_depth,
        )


@dataclass(slots=True)
class KVCacheWorkerOptions:
    """Options for the worker-side IPC stub and shared-memory attachment."""
    ipc_path: str = ""
    shared_memory_name: str = ""
    num_threads: int = 1
    partition_count: int = 1
    shared_memory_bytes: int = 0
    entry_size: int = 0
    entry_count: int = 0
    entry_alignment: int = 4096
    submission_queue_depth: int = 128

    @classmethod
    def from_budget(
        cls,
        *,
        memory_bytes: int,
        cpu_threads: int,
        block_payload_bytes: int,
        ipc_path: str = "",
        shared_memory_name: str = "",
    ) -> "KVCacheWorkerOptions":
        entry_size, entry_count, entry_alignment, partition_count, submission_queue_depth = (
            _derive_runtime_shape(memory_bytes, cpu_threads, block_payload_bytes)
        )
        return cls(
            ipc_path=ipc_path,
            shared_memory_name=shared_memory_name,
            num_threads=cpu_threads,
            partition_count=partition_count,
            shared_memory_bytes=memory_bytes,
            entry_size=entry_size,
            entry_count=entry_count,
            entry_alignment=entry_alignment,
            submission_queue_depth=submission_queue_depth,
        )

    def to_runtime_options(self) -> _KVCacheRuntimeOptions:
        """Convert the public worker config into the native transport shape."""
        return _KVCacheRuntimeOptions(
            ipc_path=self.ipc_path,
            shared_memory_name=self.shared_memory_name,
            num_threads=self.num_threads,
            partition_count=self.partition_count,
            shared_memory_bytes=self.shared_memory_bytes,
            entry_size=self.entry_size,
            entry_count=self.entry_count,
            entry_alignment=self.entry_alignment,
            submission_queue_depth=self.submission_queue_depth,
        )


@dataclass(slots=True)
class KVCacheBufferHandle:
    """A writable/readable slice inside the shared pinned buffer."""

    request_id: int
    offset: int
    length: int


@dataclass(slots=True)
class _TrackedKVRequest:
    request_id: int
    offset: int
    length: int
    status: str = "pending"


class KVCacheManager:
    """Python wrapper for the engine-core-side KV cache runtime.

    The real lifecycle, shared-memory ownership, queueing, and I/O live in the
    C++ runtime. This wrapper intentionally stays thin so Python never becomes
    part of the payload hot path.
    """

    def __init__(self, options: KVCacheManagerOptions):
        """Create a manager wrapper that owns one native KVCacheManager handle."""
        self._options = options
        self._tracked_requests: dict[int, _TrackedKVRequest] = {}
        self._buffer_pool_descriptor: str | None = None
        self._options_handle = options.to_runtime_options().to_handle()
        self._handle = lib().CEloqStore_KVCacheManager_Create(self._options_handle)
        if not self._handle:
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            raise EloqStoreError(1, last_error())

    def start(self) -> None:
        """Start the native manager runtime and allocate its shared-memory pool."""
        _ok(lib().CEloqStore_KVCacheManager_Start(self._handle))

    def register_io_uring_buffers(self) -> None:
        """Register the manager-owned shared-memory pool with native I/O paths."""
        _ok(lib().CEloqStore_KVCacheManager_RegisterIoUringBuffers(self._handle))

    def export_buffer_pool(self) -> str:
        """Return the manager-exported buffer-pool descriptor for workers."""
        raw = lib().CEloqStore_KVCacheManager_ExportBufferPool(self._handle)
        if not raw:
            raise EloqStoreError(1, last_error())
        try:
            value = cast(raw, c_char_p).value
            if value is None:
                raise EloqStoreError(1, "buffer pool descriptor is null")
            self._buffer_pool_descriptor = value.decode("utf-8")
            return self._buffer_pool_descriptor
        finally:
            lib().CEloqStore_FreeCString(raw)

    def begin_save(self, key: str, length: int) -> KVCacheBufferHandle:
        native = CKVCacheBufferHandle()
        _ok(
            lib().CEloqStore_KVCacheManager_BeginSave(
                self._handle,
                _encode(key),
                length,
                byref(native),
            )
        )
        tracked = _TrackedKVRequest(
            request_id=native.request_id,
            offset=native.offset_bytes,
            length=native.payload_bytes,
        )
        self._tracked_requests[tracked.request_id] = tracked
        return KVCacheBufferHandle(
            request_id=tracked.request_id,
            offset=tracked.offset,
            length=tracked.length,
        )

    def finish_save(self, request_id: int) -> None:
        _ok(lib().CEloqStore_KVCacheManager_FinishSave(self._handle, request_id))
        tracked = self._tracked_requests.get(request_id)
        if tracked is not None:
            tracked.status = "ready"

    def begin_load(self, key: str, length: int) -> int:
        native_request_id = c_uint64(0)
        _ok(lib().CEloqStore_KVCacheManager_BeginLoad(
            self._handle,
            _encode(key),
            length,
            byref(native_request_id),
        ))
        tracked = _TrackedKVRequest(
            request_id=native_request_id.value,
            offset=0,
            length=length,
        )
        self._tracked_requests[tracked.request_id] = tracked
        return tracked.request_id

    def check_request(self, request_id: int) -> str:
        native = CKVCacheRequestState()
        _ok(lib().CEloqStore_KVCacheManager_CheckRequest(self._handle, request_id, byref(native)))
        tracked = self._tracked_requests.get(request_id)
        status = _decode_request_status(native.status)
        if tracked is not None:
            tracked.status = status
            tracked.offset = native.offset_bytes
            tracked.length = native.payload_bytes
        return status

    def get_ready_buffer(self, request_id: int) -> tuple[int, int]:
        native = CKVCacheBufferHandle()
        _ok(lib().CEloqStore_KVCacheManager_GetReadyBuffer(self._handle, request_id, byref(native)))
        tracked = self._tracked_requests.get(request_id)
        if tracked is not None:
            tracked.offset = native.offset_bytes
            tracked.length = native.payload_bytes
            tracked.status = "ready"
        return native.offset_bytes, native.payload_bytes

    def contains_key(self, key: str) -> bool:
        """Probe whether one key exists through the native manager runtime."""
        out_exists = c_bool(False)
        _ok(
            lib().CEloqStore_KVCacheManager_ContainsKey(
                self._handle,
                _encode(key),
                byref(out_exists),
            )
        )
        return bool(out_exists.value)

    def contains_keys(self, keys: Sequence[str]) -> list[bool]:
        return [self.contains_key(key) for key in keys]

    def wait_requests(
        self, request_ids: Sequence[int], timeout_s: float = 30.0
    ) -> dict[int, KVCacheRequestResult]:
        pending = set(request_ids)
        results: dict[int, KVCacheRequestResult] = {}
        deadline = time.time() + timeout_s
        while pending:
            progressed = False
            for request_id in list(pending):
                status = self.check_request(request_id)
                if status == "pending":
                    continue
                pending.remove(request_id)
                progressed = True
                if status == "ready":
                    offset, length = self.get_ready_buffer(request_id)
                else:
                    tracked = self._tracked_requests.get(request_id)
                    offset = 0 if tracked is None else tracked.offset
                    length = 0 if tracked is None else tracked.length
                results[request_id] = KVCacheRequestResult(
                    request_id=request_id,
                    status=status,
                    offset=offset,
                    length=length,
                )
            if pending and not progressed:
                if time.time() >= deadline:
                    raise TimeoutError(f"timed out waiting for requests: {sorted(pending)}")
                time.sleep(0.001)
        return results

    def close(self) -> None:
        """Stop and destroy the native manager handle and its options handle."""
        if getattr(self, "_handle", None):
            lib().CEloqStore_KVCacheManager_Stop(self._handle)
            lib().CEloqStore_KVCacheManager_Destroy(self._handle)
            self._handle = None
        if getattr(self, "_options_handle", None):
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            self._options_handle = None
        self._tracked_requests.clear()
        self._buffer_pool_descriptor = None

    def __enter__(self) -> "KVCacheManager":
        """Support context-manager usage for deterministic cleanup."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close the manager wrapper on context-manager exit."""
        self.close()

    def __del__(self) -> None:
        """Best-effort fallback cleanup when the wrapper is garbage-collected."""
        self.close()


class KVCacheWorker:
    """Python wrapper for the worker-side control-plane stub.

    Workers attach to the manager-created buffer pool and forward IPC requests.
    Actual shared-memory mapping and CUDA registration are coordinated by the
    vLLM connector layer in the worker process.
    """

    def __init__(self, options: KVCacheWorkerOptions):
        """Create a worker wrapper that owns one native KVCacheWorker handle."""
        self._options = options
        self._tracked_requests: dict[int, _TrackedKVRequest] = {}
        self._shared_buffer: KVCacheSharedBuffer | None = None
        self._options_handle = options.to_runtime_options().to_handle()
        self._handle = lib().CEloqStore_KVCacheWorker_Create(self._options_handle)
        if not self._handle:
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            raise EloqStoreError(1, last_error())

    def attach_buffer_pool(
        self,
        descriptor: str,
        *,
        map_shared_buffer: bool = True,
        cuda_register: bool = False,
    ) -> None:
        """Load one manager-exported descriptor into the native worker stub."""
        _ok(
            lib().CEloqStore_KVCacheWorker_AttachBufferPool(
                self._handle, _encode(descriptor)
            )
        )
        self._shared_buffer = KVCacheSharedBuffer.from_descriptor(descriptor)
        if map_shared_buffer:
            self._shared_buffer.attach()
        if cuda_register and self._shared_buffer is not None:
            self._shared_buffer.register_cuda()

    def begin_save(self, key: str, length: int) -> KVCacheBufferHandle:
        native = CKVCacheBufferHandle()
        _ok(
            lib().CEloqStore_KVCacheWorker_BeginSave(
                self._handle,
                _encode(key),
                length,
                byref(native),
            )
        )
        tracked = _TrackedKVRequest(
            request_id=native.request_id,
            offset=native.offset_bytes,
            length=native.payload_bytes,
        )
        self._tracked_requests[tracked.request_id] = tracked
        return KVCacheBufferHandle(
            request_id=tracked.request_id,
            offset=tracked.offset,
            length=tracked.length,
        )

    def finish_save(self, request_id: int) -> None:
        _ok(lib().CEloqStore_KVCacheWorker_FinishSave(self._handle, request_id))
        tracked = self._tracked_requests.get(request_id)
        if tracked is not None:
            tracked.status = "ready"

    def begin_load(self, key: str, length: int) -> int:
        native_request_id = c_uint64(0)
        _ok(lib().CEloqStore_KVCacheWorker_BeginLoad(
            self._handle,
            _encode(key),
            length,
            byref(native_request_id),
        ))
        tracked = _TrackedKVRequest(
            request_id=native_request_id.value,
            offset=0,
            length=length,
        )
        self._tracked_requests[tracked.request_id] = tracked
        return tracked.request_id

    def check_request(self, request_id: int) -> str:
        native = CKVCacheRequestState()
        _ok(lib().CEloqStore_KVCacheWorker_CheckRequest(self._handle, request_id, byref(native)))
        tracked = self._tracked_requests.get(request_id)
        status = _decode_request_status(native.status)
        if tracked is not None:
            tracked.status = status
            tracked.offset = native.offset_bytes
            tracked.length = native.payload_bytes
        return status

    def get_ready_buffer(self, request_id: int) -> tuple[int, int]:
        native = CKVCacheBufferHandle()
        _ok(lib().CEloqStore_KVCacheWorker_GetReadyBuffer(self._handle, request_id, byref(native)))
        tracked = self._tracked_requests.get(request_id)
        if tracked is not None:
            tracked.offset = native.offset_bytes
            tracked.length = native.payload_bytes
            tracked.status = "ready"
        return native.offset_bytes, native.payload_bytes

    def wait_requests(
        self, request_ids: Sequence[int], timeout_s: float = 30.0
    ) -> dict[int, KVCacheRequestResult]:
        pending = set(request_ids)
        results: dict[int, KVCacheRequestResult] = {}
        deadline = time.time() + timeout_s
        while pending:
            progressed = False
            for request_id in list(pending):
                status = self.check_request(request_id)
                if status == "pending":
                    continue
                pending.remove(request_id)
                progressed = True
                if status == "ready":
                    offset, length = self.get_ready_buffer(request_id)
                else:
                    tracked = self._tracked_requests.get(request_id)
                    offset = 0 if tracked is None else tracked.offset
                    length = 0 if tracked is None else tracked.length
                results[request_id] = KVCacheRequestResult(
                    request_id=request_id,
                    status=status,
                    offset=offset,
                    length=length,
                )
            if pending and not progressed:
                if time.time() >= deadline:
                    raise TimeoutError(f"timed out waiting for requests: {sorted(pending)}")
                time.sleep(0.001)
        return results

    def shared_buffer(self) -> KVCacheSharedBuffer:
        if self._shared_buffer is None:
            raise RuntimeError("worker shared buffer is not attached")
        return self._shared_buffer

    def attached(self) -> bool:
        return self._shared_buffer is not None

    def close(self) -> None:
        """Detach and destroy the native worker handle and its options handle."""
        if getattr(self, "_shared_buffer", None) is not None:
            self._shared_buffer.close()
            self._shared_buffer = None
        if getattr(self, "_handle", None):
            lib().CEloqStore_KVCacheWorker_DetachBufferPool(self._handle)
            lib().CEloqStore_KVCacheWorker_Destroy(self._handle)
            self._handle = None
        if getattr(self, "_options_handle", None):
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            self._options_handle = None
        self._tracked_requests.clear()

    def __enter__(self) -> "KVCacheWorker":
        """Support context-manager usage for deterministic cleanup."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close the worker wrapper on context-manager exit."""
        self.close()

    def __del__(self) -> None:
        """Best-effort fallback cleanup when the wrapper is garbage-collected."""
        self.close()


class Client:
    """Thin Python wrapper for ordinary EloqStore CRUD operations.

    Unlike KVCacheManager/KVCacheWorker, this class does not participate in the
    shared-memory or IPC runtime; it is used for regular metadata CRUD.
    """

    def __init__(self, options: ClientOptions):
        """Create and start one ordinary EloqStore client session."""
        opts = lib().CEloqStore_Options_Create()
        if not opts:
            raise EloqStoreError(1, last_error())
        try:
            for path in options.store_paths:
                lib().CEloqStore_Options_AddStorePath(opts, _encode(path))
            lib().CEloqStore_Options_SetNumThreads(opts, options.num_threads)
            if not lib().CEloqStore_Options_Validate(opts):
                raise EloqStoreError(1, last_error() or "invalid client options")
            self._handle = lib().CEloqStore_Create(opts)
        finally:
            lib().CEloqStore_Options_Destroy(opts)
        if not self._handle:
            raise EloqStoreError(1, last_error())
        self._table = lib().CEloqStore_TableIdent_Create(
            _encode(options.table_name), options.partition_id
        )
        if not self._table:
            lib().CEloqStore_Destroy(self._handle)
            raise EloqStoreError(1, last_error())
        status = lib().CEloqStore_StartWithBranch(
            self._handle,
            _encode(options.branch),
            options.term,
            options.partition_group_id,
        )
        _ok_status(status)

    def put(self, key: str | bytes, value: bytes, timestamp: int = 0) -> None:
        """Write one key/value pair through the ordinary CRUD client."""
        key_bytes = _encode(key)
        value_bytes = bytes(value)
        status = lib().CEloqStore_Put(
            self._handle,
            self._table,
            _uint8_ptr(key_bytes),
            len(key_bytes),
            _uint8_ptr(value_bytes),
            len(value_bytes),
            c_uint64(timestamp),
        )
        _ok_status(status)

    def get(self, key: str | bytes) -> bytes | None:
        """Read one key/value pair through the ordinary CRUD client."""
        key_bytes = _encode(key)
        result = CGetResult()
        status = lib().CEloqStore_Get(
            self._handle,
            self._table,
            _uint8_ptr(key_bytes),
            len(key_bytes),
            byref(result),
        )
        _ok_status(status)
        try:
            if not result.found:
                return None
            return bytes(cast(result.value, POINTER(c_uint8))[: result.value_len])
        finally:
            lib().CEloqStore_FreeGetResult(byref(result))

    def delete(self, key: str | bytes, timestamp: int = 0) -> None:
        """Delete one key through the ordinary CRUD client."""
        key_bytes = _encode(key)
        status = lib().CEloqStore_Delete(
            self._handle,
            self._table,
            _uint8_ptr(key_bytes),
            len(key_bytes),
            c_uint64(timestamp),
        )
        _ok_status(status)

    def exists(self, key: str | bytes) -> bool:
        """Probe whether one key exists through the ordinary CRUD client."""
        key_bytes = _encode(key)
        out_exists = c_bool(False)
        status = lib().CEloqStore_Exists(
            self._handle,
            self._table,
            _uint8_ptr(key_bytes),
            len(key_bytes),
            byref(out_exists),
        )
        _ok_status(status)
        return bool(out_exists.value)

    def close(self) -> None:
        """Stop and destroy the native CRUD client session."""
        if getattr(self, "_table", None):
            lib().CEloqStore_TableIdent_Destroy(self._table)
            self._table = None
        if getattr(self, "_handle", None):
            lib().CEloqStore_Stop(self._handle)
            lib().CEloqStore_Destroy(self._handle)
            self._handle = None

    def __enter__(self) -> "Client":
        """Support context-manager usage for deterministic cleanup."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close the client wrapper on context-manager exit."""
        self.close()

    def __del__(self) -> None:
        """Best-effort fallback cleanup when the wrapper is garbage-collected."""
        self.close()
