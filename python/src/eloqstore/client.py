from __future__ import annotations

from ctypes import POINTER, byref, c_bool, c_char_p, c_size_t, c_uint8, c_uint64, cast, c_void_p
from dataclasses import dataclass, field
from typing import Sequence

from ._errors import EloqStoreError
from ._ffi import CGetResult, CKVCacheCompletion, CKVCacheRequest, last_error, lib


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
    slot_size: int = 0
    slot_count: int = 0
    slot_alignment: int = 4096
    submission_queue_depth: int = 128
    eager_io_uring_register: bool = True

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
            lib().CEloqStore_KVCacheOptions_SetSlotSize(native, self.slot_size)
            lib().CEloqStore_KVCacheOptions_SetSlotCount(native, self.slot_count)
            lib().CEloqStore_KVCacheOptions_SetSlotAlignment(
                native, self.slot_alignment
            )
            lib().CEloqStore_KVCacheOptions_SetSubmissionQueueDepth(
                native, self.submission_queue_depth
            )
            lib().CEloqStore_KVCacheOptions_SetEagerIoUringRegister(
                native, self.eager_io_uring_register
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
    slot_size: int = 0
    slot_count: int = 0
    slot_alignment: int = 4096
    submission_queue_depth: int = 128
    eager_io_uring_register: bool = True

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
            slot_size=self.slot_size,
            slot_count=self.slot_count,
            slot_alignment=self.slot_alignment,
            submission_queue_depth=self.submission_queue_depth,
            eager_io_uring_register=self.eager_io_uring_register,
        )


@dataclass(slots=True)
class KVCacheWorkerOptions:
    """Options for the worker-side IPC stub and shared-memory attachment."""
    ipc_path: str = ""
    shared_memory_name: str = ""
    num_threads: int = 1
    partition_count: int = 1
    shared_memory_bytes: int = 0
    slot_size: int = 0
    slot_count: int = 0
    slot_alignment: int = 4096
    submission_queue_depth: int = 128

    def to_runtime_options(self) -> _KVCacheRuntimeOptions:
        """Convert the public worker config into the native transport shape."""
        return _KVCacheRuntimeOptions(
            ipc_path=self.ipc_path,
            shared_memory_name=self.shared_memory_name,
            num_threads=self.num_threads,
            partition_count=self.partition_count,
            shared_memory_bytes=self.shared_memory_bytes,
            slot_size=self.slot_size,
            slot_count=self.slot_count,
            slot_alignment=self.slot_alignment,
            submission_queue_depth=self.submission_queue_depth,
        )


@dataclass(slots=True)
class KVCacheRequest:
    """Control-plane response for a reserved shared-memory slot."""
    request_id: int
    kind: int
    partition_id: int
    shard_id: int
    slot_id: int
    slot_generation: int
    payload_bytes: int


@dataclass(slots=True)
class KVCacheCompletion:
    """Completion record returned after manager-side I/O finishes."""
    request_id: int
    kind: int
    status: int
    partition_id: int
    shard_id: int
    slot_id: int
    slot_generation: int
    payload_bytes: int


class KVCacheManager:
    """Python wrapper for the engine-core-side KV cache runtime.

    The real lifecycle, shared-memory ownership, queueing, and I/O live in the
    C++ runtime. This wrapper intentionally stays thin so Python never becomes
    part of the payload hot path.
    """

    def __init__(self, options: KVCacheManagerOptions):
        """Create a manager wrapper that owns one native KVCacheManager handle."""
        self._options = options
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
            return value.decode("utf-8")
        finally:
            lib().CEloqStore_FreeCString(raw)

    def submit_save(
        self,
        key: str,
        partition_id: int,
        payload_bytes: int,
    ) -> KVCacheRequest:
        """Reserve a save slot through the native manager runtime."""
        native = CKVCacheRequest()
        _ok(
            lib().CEloqStore_KVCacheManager_SubmitSave(
                self._handle,
                _encode(key),
                partition_id,
                payload_bytes,
                byref(native),
            )
        )
        return KVCacheRequest(
            request_id=native.request_id,
            kind=native.kind,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def submit_load(
        self,
        key: str,
        partition_id: int,
        payload_bytes: int,
    ) -> KVCacheRequest:
        """Reserve a load slot through the native manager runtime."""
        native = CKVCacheRequest()
        _ok(
            lib().CEloqStore_KVCacheManager_SubmitLoad(
                self._handle,
                _encode(key),
                partition_id,
                payload_bytes,
                byref(native),
            )
        )
        return KVCacheRequest(
            request_id=native.request_id,
            kind=native.kind,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def mark_save_ready(self, request_id: int) -> None:
        """Mark one previously reserved save slot as ready for manager I/O."""
        _ok(lib().CEloqStore_KVCacheManager_MarkSaveReady(self._handle, request_id))

    def poll_completion(self) -> KVCacheCompletion | None:
        """Return one finished manager request, or None when no completion exists."""
        native = CKVCacheCompletion()
        ok = lib().CEloqStore_KVCacheManager_PollCompletion(
            self._handle, byref(native)
        )
        if not ok:
            if last_error():
                raise EloqStoreError(1, last_error())
            return None
        return KVCacheCompletion(
            request_id=native.request_id,
            kind=native.kind,
            status=native.status,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def contains_key(self, key: str, partition_id: int) -> bool:
        """Probe whether one key exists through the native manager runtime."""
        out_exists = c_bool(False)
        _ok(
            lib().CEloqStore_KVCacheManager_ContainsKey(
                self._handle, _encode(key), partition_id, byref(out_exists)
            )
        )
        return bool(out_exists.value)

    def close(self) -> None:
        """Stop and destroy the native manager handle and its options handle."""
        if getattr(self, "_handle", None):
            lib().CEloqStore_KVCacheManager_Stop(self._handle)
            lib().CEloqStore_KVCacheManager_Destroy(self._handle)
            self._handle = None
        if getattr(self, "_options_handle", None):
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            self._options_handle = None

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
        self._options_handle = options.to_runtime_options().to_handle()
        self._handle = lib().CEloqStore_KVCacheWorker_Create(self._options_handle)
        if not self._handle:
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            raise EloqStoreError(1, last_error())

    def attach_buffer_pool(self, descriptor: str) -> None:
        """Load one manager-exported descriptor into the native worker stub."""
        _ok(
            lib().CEloqStore_KVCacheWorker_AttachBufferPool(
                self._handle, _encode(descriptor)
            )
        )

    def recommend_partition(self, worker_lane: int) -> int:
        """Ask the native worker stub for a preferred partition."""
        return int(
            lib().CEloqStore_KVCacheWorker_RecommendPartition(
                self._handle, worker_lane
            )
        )

    def submit_save(
        self,
        key: str,
        partition_id: int,
        payload_bytes: int,
    ) -> KVCacheRequest:
        """Forward a save-slot reservation request to the manager over IPC."""
        native = CKVCacheRequest()
        _ok(
            lib().CEloqStore_KVCacheWorker_SubmitSave(
                self._handle,
                _encode(key),
                partition_id,
                payload_bytes,
                byref(native),
            )
        )
        return KVCacheRequest(
            request_id=native.request_id,
            kind=native.kind,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def submit_load(
        self,
        key: str,
        partition_id: int,
        payload_bytes: int,
    ) -> KVCacheRequest:
        """Forward a load-slot reservation request to the manager over IPC."""
        native = CKVCacheRequest()
        _ok(
            lib().CEloqStore_KVCacheWorker_SubmitLoad(
                self._handle,
                _encode(key),
                partition_id,
                payload_bytes,
                byref(native),
            )
        )
        return KVCacheRequest(
            request_id=native.request_id,
            kind=native.kind,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def mark_save_ready(self, request_id: int) -> None:
        """Tell the manager that the worker has finished filling one save slot."""
        _ok(lib().CEloqStore_KVCacheWorker_MarkSaveReady(self._handle, request_id))

    def poll_completion(self) -> KVCacheCompletion | None:
        """Poll one manager completion through the worker-side IPC stub."""
        native = CKVCacheCompletion()
        ok = lib().CEloqStore_KVCacheWorker_PollCompletion(self._handle, byref(native))
        if not ok:
            if last_error():
                raise EloqStoreError(1, last_error())
            return None
        return KVCacheCompletion(
            request_id=native.request_id,
            kind=native.kind,
            status=native.status,
            partition_id=native.partition_id,
            shard_id=native.shard_id,
            slot_id=native.slot_id,
            slot_generation=native.slot_generation,
            payload_bytes=native.payload_bytes,
        )

    def close(self) -> None:
        """Detach and destroy the native worker handle and its options handle."""
        if getattr(self, "_handle", None):
            lib().CEloqStore_KVCacheWorker_DetachBufferPool(self._handle)
            lib().CEloqStore_KVCacheWorker_Destroy(self._handle)
            self._handle = None
        if getattr(self, "_options_handle", None):
            lib().CEloqStore_KVCacheOptions_Destroy(self._options_handle)
            self._options_handle = None

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
        if not result.found:
            return None
        return bytes(cast(result.value, POINTER(c_uint8))[: result.value_len])

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
