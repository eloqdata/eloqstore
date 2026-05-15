from __future__ import annotations

import asyncio
from ctypes import POINTER, c_size_t, c_uint8, c_uint64, c_bool, byref
from dataclasses import dataclass, field
from time import time_ns
from typing import Any, Iterable, Mapping, Sequence

from ._errors import EloqStoreError
from ._ffi import (
    CGetResult,
    CIoStringFragment,
    CLargeValueResult,
    alloc_bytes,
    as_input_buffer,
    as_output_buffer,
    c_void_p,
    cast,
    lib,
    last_error,
)


def _to_bytes(data: str | bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, memoryview):
        return data.tobytes()
    raise TypeError(f"Expected str or bytes, got {type(data)!r}")


def _ok(status: int) -> None:
    if status != 0:
        raise EloqStoreError(status, last_error())


def _validate_uint(name: str, value: int, max_value: int) -> int:
    if not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value)!r}")
    if value < 0 or value > max_value:
        raise ValueError(f"{name} must be between 0 and {max_value}, got {value}")
    return value


def _predict_global_reg_mem_index_base(options: Options) -> int:
    page_align = 4096
    max_registered_bytes = 1 << 30
    buffer_pool_size = options.buffer_pool_size
    if buffer_pool_size is None:
        buffer_pool_size = 32 << 20
    data_page_size = options.data_page_size or 4096
    pool_bytes = (buffer_pool_size // data_page_size) * data_page_size
    page_iov_count = (
        0
        if pool_bytes <= 0
        else (pool_bytes + max_registered_bytes - 1) // max_registered_bytes
    )

    write_iov_count = 0
    if options.data_append_mode and buffer_pool_size > 0:
        write_buf_size = 1 << 20
        write_pool_bytes = min(int(buffer_pool_size * 0.05), buffer_pool_size)
        write_buf_size = (write_buf_size // page_align) * page_align
        write_pool_bytes = (write_pool_bytes // page_align) * page_align
        if write_buf_size > 0 and write_pool_bytes >= write_buf_size:
            write_iov_count = write_pool_bytes // write_buf_size

    return int(page_iov_count + write_iov_count)


@dataclass(slots=True)
class Options:
    # ── required ──
    store_paths: Sequence[str] = field(default_factory=list)
    # ── table / branch ──
    options_path: str | None = None
    table_name: str = "default"
    partition_id: int = 0
    branch: str = "main"
    term: int = 0
    partition_group_id: int = 0
    validate: bool = True
    # ── engine ──
    num_threads: int | None = None
    # ── B+Tree storage ──
    data_page_size: int | None = None  # bytes, max 65535 (uint16)
    pages_per_file_shift: int | None = None  # data file = page_size << shift
    data_append_mode: bool | None = None
    overflow_pointers: int | None = None  # max 128
    enable_compression: bool | None = None
    # ── resource limits ──
    buffer_pool_size: int | None = None  # bytes, index page cache per shard
    manifest_limit: int | None = None  # bytes, WAL file size limit
    fd_limit: int | None = None  # max open files
    # ── zero-copy large value path ──
    segment_size: int | None = None
    registered_memory_chunk_size: int | None = None
    segments_per_file_shift: int | None = None
    registered_memory: Any | None = None


class RegisteredMemory:
    def __init__(
        self,
        *,
        total_size: int,
        chunk_size: int = 1 << 30,
        segment_size: int = 256 << 10,
        reg_mem_index_base: int = 0,
    ) -> None:
        self._lib = lib()
        self.segment_size = _validate_uint("segment_size", segment_size, 2**32 - 1)
        self.chunk_size = _validate_uint("chunk_size", chunk_size, 2**64 - 1)
        self.total_size = _validate_uint("total_size", total_size, 2**64 - 1)
        self.reg_mem_index_base = _validate_uint(
            "reg_mem_index_base", reg_mem_index_base, 65535
        )
        self._handle = self._lib.CEloqStore_GlobalMemory_Create(
            self.segment_size, self.chunk_size, self.total_size
        )
        if not self._handle:
            raise EloqStoreError(1, last_error())

    @property
    def handle(self):
        return self._handle

    def allocate(self, size: int) -> LargeValueBuffer:
        _validate_uint("size", size, 2**64 - 1)
        handle = self._lib.CEloqStore_GlobalMemory_AllocateIoString(
            self._handle, size, self.reg_mem_index_base
        )
        if not handle:
            raise EloqStoreError(1, last_error())
        return LargeValueBuffer(self, handle, owns_handle=True, recycle_on_close=True)

    def free_segments(self) -> int:
        return int(self._lib.CEloqStore_GlobalMemory_FreeSegments(self._handle))

    def close(self) -> None:
        if self._handle:
            self._lib.CEloqStore_GlobalMemory_Destroy(self._handle)
            self._handle = None

    def __enter__(self) -> RegisteredMemory:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class LargeValueBuffer:
    def __init__(
        self,
        memory: RegisteredMemory,
        handle: Any,
        *,
        owns_handle: bool,
        recycle_on_close: bool,
    ) -> None:
        self._lib = lib()
        self.memory = memory
        self._handle = handle
        self._owns_handle = owns_handle
        self._recycle_on_close = recycle_on_close
        self._views: list[Any] | None = None

    @property
    def handle(self):
        return self._handle

    def __len__(self) -> int:
        if not self._handle:
            return 0
        return int(self._lib.CEloqStore_IoStringBuffer_Size(self._handle))

    def memoryviews(self) -> list[memoryview]:
        if self._views is None:
            self._views = []
            remaining = len(self)
            count = int(self._lib.CEloqStore_IoStringBuffer_FragmentCount(self._handle))
            for idx in range(count):
                frag = CIoStringFragment()
                if not self._lib.CEloqStore_IoStringBuffer_FragmentAt(
                    self._handle, idx, byref(frag)
                ):
                    raise RuntimeError("failed to inspect large-value fragment")
                frag_len = min(self.memory.segment_size, remaining)
                remaining -= frag_len
                array_type = c_uint8 * frag_len
                addr = cast(frag.data, c_void_p).value
                if addr is None:
                    raise RuntimeError("large-value fragment has null data")
                arr = array_type.from_address(addr)
                view = memoryview(arr)
                if view.format != "B":
                    view = view.cast("B")
                self._views.append(view)
        return self._views

    def tensor_views(self) -> list[Any]:
        import torch

        return [torch.frombuffer(view, dtype=torch.uint8) for view in self.memoryviews()]

    def copy_from(self, data: Any, offset: int = 0) -> None:
        view = memoryview(data).cast("B")
        if offset != 0:
            raise NotImplementedError("non-zero offset copy is not implemented yet")
        if len(view) > len(self):
            raise ValueError("source is larger than large-value buffer")
        copied = 0
        for dst in self.memoryviews():
            if copied >= len(view):
                break
            n = min(len(dst), len(view) - copied)
            dst[:n] = view[copied : copied + n]
            copied += n

    def to_bytes(self) -> bytes:
        return b"".join(bytes(view) for view in self.memoryviews())[: len(self)]

    def close(self) -> None:
        if not self._handle:
            return
        if self._recycle_on_close:
            self._lib.CEloqStore_IoStringBuffer_Recycle(
                self._handle, self.memory.handle, self.memory.reg_mem_index_base
            )
        if self._owns_handle:
            self._lib.CEloqStore_IoStringBuffer_Destroy(self._handle)
        self._handle = None
        self._views = None

    def __enter__(self) -> LargeValueBuffer:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class AsyncHandle:
    def __init__(self, handle: Any, memory: RegisteredMemory | None = None) -> None:
        self._lib = lib()
        self._handle = handle
        self._memory = memory

    def done(self) -> bool:
        if not self._handle:
            return True
        return bool(self._lib.CEloqStore_AsyncIsDone(self._handle))

    def wait(self) -> None:
        if not self._handle:
            return
        status = self._lib.CEloqStore_AsyncWait(self._handle)
        _ok(status)

    async def wait_async(self) -> None:
        while not self.done():
            await asyncio.sleep(0)
        self.wait()

    def result_large(self) -> LargeValueBuffer | None:
        if not self._handle:
            raise RuntimeError("async handle is closed")
        if self._memory is None:
            raise RuntimeError("async handle does not contain a large-value result")
        result = CLargeValueResult()
        status = self._lib.CEloqStore_AsyncGetLargeResult(
            self._handle, byref(result)
        )
        _ok(status)
        if not result.found:
            return None
        return LargeValueBuffer(
            self._memory,
            result.value,
            owns_handle=True,
            recycle_on_close=True,
        )

    def close(self) -> None:
        if self._handle:
            self._lib.CEloqStore_AsyncDestroy(self._handle)
            self._handle = None

    def __del__(self) -> None:
        self.close()


class Client:
    def __init__(self, options: Options):
        self._lib = lib()
        self._options = options
        self._closed = False
        self._opts_handle = None
        self._store_handle = None
        self._table_handle = None
        self._opts_handle = self._lib.CEloqStore_Options_Create()
        if not self._opts_handle:
            raise EloqStoreError(1, last_error())

        try:
            if options.options_path:
                ok = self._lib.CEloqStore_Options_LoadFromIni(
                    self._opts_handle, options.options_path.encode("utf-8")
                )
                if not ok:
                    raise EloqStoreError(1, last_error())

            for path in options.store_paths:
                self._lib.CEloqStore_Options_AddStorePath(
                    self._opts_handle, path.encode("utf-8")
                )

            if options.num_threads is not None:
                self._lib.CEloqStore_Options_SetNumThreads(
                    self._opts_handle,
                    _validate_uint("num_threads", options.num_threads, 65535),
                )
            if options.data_page_size is not None:
                self._lib.CEloqStore_Options_SetDataPageSize(
                    self._opts_handle,
                    _validate_uint("data_page_size", options.data_page_size, 65535),
                )
            if options.pages_per_file_shift is not None:
                self._lib.CEloqStore_Options_SetPagesPerFileShift(
                    self._opts_handle,
                    _validate_uint(
                        "pages_per_file_shift", options.pages_per_file_shift, 255
                    ),
                )
            if options.data_append_mode is not None:
                self._lib.CEloqStore_Options_SetDataAppendMode(
                    self._opts_handle, options.data_append_mode
                )
            if options.overflow_pointers is not None:
                self._lib.CEloqStore_Options_SetOverflowPointers(
                    self._opts_handle,
                    _validate_uint("overflow_pointers", options.overflow_pointers, 128),
                )
            if options.enable_compression is not None:
                self._lib.CEloqStore_Options_SetEnableCompression(
                    self._opts_handle, options.enable_compression
                )
            if options.buffer_pool_size is not None:
                self._lib.CEloqStore_Options_SetBufferPoolSize(
                    self._opts_handle,
                    _validate_uint(
                        "buffer_pool_size", options.buffer_pool_size, 2**64 - 1
                    ),
                )
            if options.manifest_limit is not None:
                self._lib.CEloqStore_Options_SetManifestLimit(
                    self._opts_handle,
                    _validate_uint("manifest_limit", options.manifest_limit, 2**32 - 1),
                )
            if options.fd_limit is not None:
                self._lib.CEloqStore_Options_SetFdLimit(
                    self._opts_handle,
                    _validate_uint("fd_limit", options.fd_limit, 2**32 - 1),
                )
            if options.segment_size is not None:
                self._lib.CEloqStore_Options_SetSegmentSize(
                    self._opts_handle,
                    _validate_uint("segment_size", options.segment_size, 2**32 - 1),
                )
            if options.registered_memory_chunk_size is not None:
                self._lib.CEloqStore_Options_SetRegisteredMemoryChunkSize(
                    self._opts_handle,
                    _validate_uint(
                        "registered_memory_chunk_size",
                        options.registered_memory_chunk_size,
                        2**64 - 1,
                    ),
                )
            if options.segments_per_file_shift is not None:
                self._lib.CEloqStore_Options_SetSegmentsPerFileShift(
                    self._opts_handle,
                    _validate_uint(
                        "segments_per_file_shift", options.segments_per_file_shift, 255
                    ),
                )
            if options.registered_memory is not None:
                options.registered_memory.reg_mem_index_base = (
                    _predict_global_reg_mem_index_base(options)
                )
                self._lib.CEloqStore_Options_SetGlobalRegisteredMemory(
                    self._opts_handle, 0, options.registered_memory.handle
                )

            if options.validate and not self._lib.CEloqStore_Options_Validate(
                self._opts_handle
            ):
                raise EloqStoreError(1, last_error())

            self._store_handle = self._lib.CEloqStore_Create(self._opts_handle)
            if not self._store_handle:
                raise EloqStoreError(1, last_error())

            self._table_handle = self._lib.CEloqStore_TableIdent_Create(
                options.table_name.encode("utf-8"), options.partition_id
            )
            if not self._table_handle:
                raise EloqStoreError(1, last_error())

            _ok(
                self._lib.CEloqStore_StartWithBranch(
                    self._store_handle,
                    options.branch.encode("utf-8"),
                    options.term,
                    options.partition_group_id,
                )
            )
            if options.registered_memory is not None:
                actual_base = int(
                    self._lib.CEloqStore_GlobalRegMemIndexBase(self._store_handle, 0)
                )
                if actual_base != 0:
                    options.registered_memory.reg_mem_index_base = actual_base
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._table_handle:
            self._lib.CEloqStore_TableIdent_Destroy(self._table_handle)
            self._table_handle = None
        if self._store_handle:
            if not self._lib.CEloqStore_IsStopped(self._store_handle):
                self._lib.CEloqStore_Stop(self._store_handle)
            self._lib.CEloqStore_Destroy(self._store_handle)
            self._store_handle = None
        if self._opts_handle:
            self._lib.CEloqStore_Options_Destroy(self._opts_handle)
            self._opts_handle = None

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def put(
        self,
        key: str | bytes,
        value: str | bytes | bytearray | memoryview | Any,
        *,
        timestamp: int | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        value_arr, value_ptr, value_len = as_input_buffer(value)
        _ok(
            self._lib.CEloqStore_Put(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                value_ptr,
                value_len,
                ts,
            )
        )

    def get(self, key: str | bytes) -> bytes | None:
        if self._closed:
            raise RuntimeError("store is closed")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        result = CGetResult()
        status = self._lib.CEloqStore_Get(
            self._store_handle, self._table_handle, key_ptr, key_len, result
        )
        _ok(status)
        try:
            if not result.found:
                return None
            return bytes(result.value[: result.value_len])
        finally:
            self._lib.CEloqStore_FreeGetResult(result)

    def get_into(self, key: str | bytes, out_buffer: Any) -> int | None:
        if self._closed:
            raise RuntimeError("store is closed")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        out_arr, out_ptr, out_len = as_output_buffer(out_buffer)
        result = CGetResult()
        status = self._lib.CEloqStore_GetInto(
            self._store_handle,
            self._table_handle,
            key_ptr,
            key_len,
            out_ptr,
            out_len,
            result,
        )
        _ok(status)
        if not result.found:
            return None
        return int(result.value_len)

    def allocate_large_value(self, size: int) -> LargeValueBuffer:
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        return self._options.registered_memory.allocate(size)

    def put_large(
        self,
        key: str | bytes,
        value: LargeValueBuffer,
        *,
        timestamp: int | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        status = self._lib.CEloqStore_PutLarge(
            self._store_handle,
            self._table_handle,
            key_ptr,
            key_len,
            value.handle,
            self._options.registered_memory.handle,
            self._options.registered_memory.reg_mem_index_base,
            ts,
        )
        value._recycle_on_close = False
        value.close()
        _ok(status)

    def batch_put_large(
        self,
        items: Iterable[tuple[str | bytes, LargeValueBuffer]],
        *,
        timestamp: int | None = None,
    ) -> None:
        pending = list(items)
        if not pending:
            return
        if self._closed:
            raise RuntimeError("store is closed")
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        req = self._lib.CEloqStore_BatchWrite_Create()
        if not req:
            for _, value in pending:
                value.close()
            raise EloqStoreError(1, last_error())

        key_arrays = []
        try:
            self._lib.CEloqStore_BatchWrite_SetTable(req, self._table_handle)
            for key, value in pending:
                key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
                key_arrays.append(key_arr)
                self._lib.CEloqStore_BatchWrite_AddLargeEntry(
                    req,
                    key_ptr,
                    key_len,
                    value.handle,
                    ts,
                    0,
                    0,
                )
            status = self._lib.CEloqStore_ExecBatchWrite(self._store_handle, req)
            self._lib.CEloqStore_BatchWrite_RecycleLargeEntries(
                req,
                self._options.registered_memory.handle,
                self._options.registered_memory.reg_mem_index_base,
            )
            for _, value in pending:
                value._recycle_on_close = False
                value.close()
            _ok(status)
        except Exception:
            for _, value in pending:
                value.close()
            raise
        finally:
            self._lib.CEloqStore_BatchWrite_Destroy(req)

    def batch_put_large_async(
        self,
        items: Iterable[tuple[str | bytes, LargeValueBuffer]],
        *,
        timestamp: int | None = None,
    ) -> AsyncHandle:
        pending = list(items)
        if self._closed:
            raise RuntimeError("store is closed")
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        req = self._lib.CEloqStore_BatchWrite_Create()
        if not req:
            for _, value in pending:
                value.close()
            raise EloqStoreError(1, last_error())

        key_arrays = []
        try:
            self._lib.CEloqStore_BatchWrite_SetTable(req, self._table_handle)
            for key, value in pending:
                key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
                key_arrays.append(key_arr)
                self._lib.CEloqStore_BatchWrite_AddLargeEntry(
                    req, key_ptr, key_len, value.handle, ts, 0, 0
                )
            handle = self._lib.CEloqStore_ExecBatchWriteAsync(
                self._store_handle,
                req,
                self._options.registered_memory.handle,
                self._options.registered_memory.reg_mem_index_base,
            )
            if not handle:
                raise EloqStoreError(1, last_error())
            for _, value in pending:
                value._recycle_on_close = False
                value.close()
            return AsyncHandle(handle)
        except Exception:
            for _, value in pending:
                value.close()
            self._lib.CEloqStore_BatchWrite_Destroy(req)
            raise

    async def abatch_put_large(
        self,
        items: Iterable[tuple[str | bytes, LargeValueBuffer]],
        *,
        timestamp: int | None = None,
    ) -> None:
        handle = self.batch_put_large_async(items, timestamp=timestamp)
        try:
            await handle.wait_async()
        finally:
            handle.close()

    def get_large(self, key: str | bytes) -> LargeValueBuffer | None:
        if self._closed:
            raise RuntimeError("store is closed")
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        result = CLargeValueResult()
        status = self._lib.CEloqStore_GetLarge(
            self._store_handle, self._table_handle, key_ptr, key_len, byref(result)
        )
        _ok(status)
        if not result.found:
            return None
        return LargeValueBuffer(
            self._options.registered_memory,
            result.value,
            owns_handle=True,
            recycle_on_close=True,
        )

    def get_large_async(self, key: str | bytes) -> AsyncHandle:
        if self._closed:
            raise RuntimeError("store is closed")
        if self._options.registered_memory is None:
            raise RuntimeError("registered_memory is required for large values")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        handle = self._lib.CEloqStore_GetLargeAsync(
            self._store_handle,
            self._table_handle,
            key_ptr,
            key_len,
            self._options.registered_memory.handle,
            self._options.registered_memory.reg_mem_index_base,
        )
        if not handle:
            raise EloqStoreError(1, last_error())
        return AsyncHandle(handle, self._options.registered_memory)

    async def aget_large(self, key: str | bytes) -> LargeValueBuffer | None:
        handle = self.get_large_async(key)
        try:
            await handle.wait_async()
            return handle.result_large()
        finally:
            handle.close()

    def exists(self, key: str | bytes) -> bool:
        if self._closed:
            raise RuntimeError("store is closed")
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        out = c_bool(False)
        _ok(
            self._lib.CEloqStore_Exists(
                self._store_handle, self._table_handle, key_ptr, key_len, byref(out)
            )
        )
        return bool(out)

    def delete(self, key: str | bytes, *, timestamp: int | None = None) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        _ok(
            self._lib.CEloqStore_Delete(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                ts,
            )
        )

    def batch_put(
        self,
        items: Mapping[str | bytes, Any] | Iterable[tuple[str | bytes, Any]],
        *,
        timestamp: int | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        pairs = list(items.items()) if isinstance(items, Mapping) else list(items)
        if not pairs:
            return

        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        key_ptrs = []
        key_lens = []
        value_ptrs = []
        value_lens = []
        key_arrays = []
        value_arrays = []

        for key, value in pairs:
            key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
            value_arr, value_ptr, value_len = as_input_buffer(value)
            key_arrays.append(key_arr)
            value_arrays.append(value_arr)
            key_ptrs.append(key_ptr)
            key_lens.append(key_len)
            value_ptrs.append(value_ptr)
            value_lens.append(value_len)

        KeyPtrArray = POINTER(c_uint8) * len(key_ptrs)
        ValuePtrArray = POINTER(c_uint8) * len(value_ptrs)
        LenArray = c_size_t * len(key_lens)

        _ok(
            self._lib.CEloqStore_PutBatch(
                self._store_handle,
                self._table_handle,
                KeyPtrArray(*key_ptrs),
                LenArray(*key_lens),
                ValuePtrArray(*value_ptrs),
                LenArray(*value_lens),
                len(pairs),
                ts,
            )
        )

    def batch_get(self, keys: Sequence[str | bytes]) -> list[bytes | None]:
        if self._closed:
            raise RuntimeError("store is closed")
        return [self.get(key) for key in keys]

    def batch_delete(
        self, keys: Sequence[str | bytes], *, timestamp: int | None = None
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        if not keys:
            return

        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        key_ptrs = []
        key_lens = []
        key_arrays = []
        for key in keys:
            key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
            key_arrays.append(key_arr)
            key_ptrs.append(key_ptr)
            key_lens.append(key_len)

        KeyPtrArray = POINTER(c_uint8) * len(key_ptrs)
        LenArray = c_size_t * len(key_lens)

        _ok(
            self._lib.CEloqStore_DeleteBatch(
                self._store_handle,
                self._table_handle,
                KeyPtrArray(*key_ptrs),
                LenArray(*key_lens),
                len(keys),
                ts,
            )
        )
