from __future__ import annotations

from ctypes import (
    POINTER,
    Structure,
    c_char,
    byref,
    c_bool,
    c_char_p,
    c_size_t,
    c_uint16,
    c_uint32,
    c_uint64,
    c_uint8,
    c_void_p,
    cast,
    cdll,
)
from pathlib import Path
import os
from typing import Any


class CGetResult(Structure):
    _fields_ = [
        ("value", POINTER(c_uint8)),
        ("value_len", c_size_t),
        ("timestamp", c_uint64),
        ("expire_ts", c_uint64),
        ("found", c_bool),
        ("owns_value", c_bool),
    ]


class CLargeValueResult(Structure):
    _fields_ = [
        ("value", c_void_p),
        ("value_len", c_size_t),
        ("timestamp", c_uint64),
        ("expire_ts", c_uint64),
        ("found", c_bool),
        ("kind", c_uint32),
    ]


class CIoStringFragment(Structure):
    _fields_ = [
        ("data", POINTER(c_uint8)),
        ("len", c_size_t),
        ("buf_index", c_uint16),
        ("chunk_index", c_uint32),
        ("offset", c_size_t),
    ]


def _candidate_library_paths() -> list[Path]:
    env = os.environ.get("ELOQSTORE_PY_LIB")
    candidates: list[Path] = []
    if env:
        candidates.append(Path(env))

    pkg_dir = Path(__file__).resolve().parent
    repo_root = pkg_dir.parents[2]
    candidates.extend(
        [
            pkg_dir / "libeloqstore_capi.so",
            pkg_dir / ".libs" / "libeloqstore_capi.so",
            repo_root / "build" / "libeloqstore_capi.so",
            repo_root / "Release" / "libeloqstore_capi.so",
            repo_root / "libeloqstore_capi.so",
        ]
    )
    return candidates


def load_library():
    for candidate in _candidate_library_paths():
        if candidate.exists():
            lib = cdll.LoadLibrary(str(candidate))
            _configure_library(lib)
            return lib
    raise FileNotFoundError(
        "Could not locate libeloqstore_capi.so. "
        "Build the `eloqstore_capi` target or set ELOQSTORE_PY_LIB."
    )


def _configure_library(lib) -> None:
    lib.CEloqStore_Options_Create.restype = c_void_p
    lib.CEloqStore_Options_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_Options_SetNumThreads.argtypes = [c_void_p, c_uint16]
    lib.CEloqStore_Options_SetBufferPoolSize.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_Options_SetDataPageSize.argtypes = [c_void_p, c_uint16]
    lib.CEloqStore_Options_SetManifestLimit.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_Options_SetFdLimit.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_Options_SetPagesPerFileShift.argtypes = [c_void_p, c_uint8]
    lib.CEloqStore_Options_SetOverflowPointers.argtypes = [c_void_p, c_uint8]
    lib.CEloqStore_Options_SetDataAppendMode.argtypes = [c_void_p, c_bool]
    lib.CEloqStore_Options_SetEnableCompression.argtypes = [c_void_p, c_bool]
    lib.CEloqStore_Options_SetSegmentSize.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_Options_SetRegisteredMemoryChunkSize.argtypes = [
        c_void_p,
        c_uint64,
    ]
    lib.CEloqStore_Options_SetSegmentsPerFileShift.argtypes = [c_void_p, c_uint8]
    lib.CEloqStore_Options_SetGlobalRegisteredMemory.argtypes = [
        c_void_p,
        c_uint32,
        c_void_p,
    ]
    lib.CEloqStore_Options_AddStorePath.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_Options_LoadFromIni.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_Options_LoadFromIni.restype = c_bool
    lib.CEloqStore_Options_Validate.argtypes = [c_void_p]
    lib.CEloqStore_Options_Validate.restype = c_bool

    lib.CEloqStore_Create.argtypes = [c_void_p]
    lib.CEloqStore_Create.restype = c_void_p
    lib.CEloqStore_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_StartWithBranch.argtypes = [c_void_p, c_char_p, c_uint64, c_uint32]
    lib.CEloqStore_StartWithBranch.restype = c_uint32
    lib.CEloqStore_Stop.argtypes = [c_void_p]
    lib.CEloqStore_IsStopped.argtypes = [c_void_p]
    lib.CEloqStore_IsStopped.restype = c_bool
    lib.CEloqStore_GlobalRegMemIndexBase.argtypes = [c_void_p, c_size_t]
    lib.CEloqStore_GlobalRegMemIndexBase.restype = c_uint16

    lib.CEloqStore_TableIdent_Create.argtypes = [c_char_p, c_uint32]
    lib.CEloqStore_TableIdent_Create.restype = c_void_p
    lib.CEloqStore_TableIdent_Destroy.argtypes = [c_void_p]

    lib.CEloqStore_Put.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        POINTER(c_uint8),
        c_size_t,
        c_uint64,
    ]
    lib.CEloqStore_Put.restype = c_uint32

    lib.CEloqStore_PutBatch.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(POINTER(c_uint8)),
        POINTER(c_size_t),
        POINTER(POINTER(c_uint8)),
        POINTER(c_size_t),
        c_size_t,
        c_uint64,
    ]
    lib.CEloqStore_PutBatch.restype = c_uint32

    lib.CEloqStore_Get.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        POINTER(CGetResult),
    ]
    lib.CEloqStore_Get.restype = c_uint32
    lib.CEloqStore_GetInto.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        POINTER(c_uint8),
        c_size_t,
        POINTER(CGetResult),
    ]
    lib.CEloqStore_GetInto.restype = c_uint32

    lib.CEloqStore_GetLarge.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        POINTER(CLargeValueResult),
    ]
    lib.CEloqStore_GetLarge.restype = c_uint32
    lib.CEloqStore_GetLargeAsync.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        c_void_p,
        c_uint16,
    ]
    lib.CEloqStore_GetLargeAsync.restype = c_void_p
    lib.CEloqStore_AsyncGetLargeResult.argtypes = [
        c_void_p,
        POINTER(CLargeValueResult),
    ]
    lib.CEloqStore_AsyncGetLargeResult.restype = c_uint32
    lib.CEloqStore_PutLarge.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        c_void_p,
        c_void_p,
        c_uint16,
        c_uint64,
    ]
    lib.CEloqStore_PutLarge.restype = c_uint32

    lib.CEloqStore_Exists.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t, POINTER(c_bool)]
    lib.CEloqStore_Exists.restype = c_uint32

    lib.CEloqStore_Delete.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        c_uint64,
    ]
    lib.CEloqStore_Delete.restype = c_uint32

    lib.CEloqStore_DeleteBatch.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(POINTER(c_uint8)),
        POINTER(c_size_t),
        c_size_t,
        c_uint64,
    ]
    lib.CEloqStore_DeleteBatch.restype = c_uint32

    lib.CEloqStore_BatchWrite_Create.restype = c_void_p
    lib.CEloqStore_BatchWrite_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_BatchWrite_SetTable.argtypes = [c_void_p, c_void_p]
    lib.CEloqStore_BatchWrite_AddLargeEntry.argtypes = [
        c_void_p,
        POINTER(c_uint8),
        c_size_t,
        c_void_p,
        c_uint64,
        c_uint32,
        c_uint64,
    ]
    lib.CEloqStore_BatchWrite_Clear.argtypes = [c_void_p]
    lib.CEloqStore_BatchWrite_RecycleLargeEntries.argtypes = [
        c_void_p,
        c_void_p,
        c_uint16,
    ]
    lib.CEloqStore_ExecBatchWrite.argtypes = [c_void_p, c_void_p]
    lib.CEloqStore_ExecBatchWrite.restype = c_uint32
    lib.CEloqStore_ExecBatchWriteAsync.argtypes = [c_void_p, c_void_p, c_void_p, c_uint16]
    lib.CEloqStore_ExecBatchWriteAsync.restype = c_void_p
    lib.CEloqStore_AsyncIsDone.argtypes = [c_void_p]
    lib.CEloqStore_AsyncIsDone.restype = c_bool
    lib.CEloqStore_AsyncWait.argtypes = [c_void_p]
    lib.CEloqStore_AsyncWait.restype = c_uint32
    lib.CEloqStore_AsyncStatus.argtypes = [c_void_p]
    lib.CEloqStore_AsyncStatus.restype = c_uint32
    lib.CEloqStore_AsyncDestroy.argtypes = [c_void_p]

    lib.CEloqStore_FreeGetResult.argtypes = [POINTER(CGetResult)]
    lib.CEloqStore_GetLastError.argtypes = [c_void_p]
    lib.CEloqStore_GetLastError.restype = c_char_p

    lib.CEloqStore_GlobalMemory_Create.argtypes = [c_uint32, c_uint64, c_uint64]
    lib.CEloqStore_GlobalMemory_Create.restype = c_void_p
    lib.CEloqStore_GlobalMemory_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_GlobalMemory_SegmentSize.argtypes = [c_void_p]
    lib.CEloqStore_GlobalMemory_SegmentSize.restype = c_uint32
    lib.CEloqStore_GlobalMemory_TotalSegments.argtypes = [c_void_p]
    lib.CEloqStore_GlobalMemory_TotalSegments.restype = c_size_t
    lib.CEloqStore_GlobalMemory_FreeSegments.argtypes = [c_void_p]
    lib.CEloqStore_GlobalMemory_FreeSegments.restype = c_size_t
    lib.CEloqStore_GlobalMemory_AllocateIoString.argtypes = [
        c_void_p,
        c_size_t,
        c_uint16,
    ]
    lib.CEloqStore_GlobalMemory_AllocateIoString.restype = c_void_p
    lib.CEloqStore_IoStringBuffer_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_IoStringBuffer_Recycle.argtypes = [c_void_p, c_void_p, c_uint16]
    lib.CEloqStore_IoStringBuffer_Size.argtypes = [c_void_p]
    lib.CEloqStore_IoStringBuffer_Size.restype = c_size_t
    lib.CEloqStore_IoStringBuffer_FragmentCount.argtypes = [c_void_p]
    lib.CEloqStore_IoStringBuffer_FragmentCount.restype = c_size_t
    lib.CEloqStore_IoStringBuffer_FragmentAt.argtypes = [
        c_void_p,
        c_size_t,
        POINTER(CIoStringFragment),
    ]
    lib.CEloqStore_IoStringBuffer_FragmentAt.restype = c_bool


_LIB = None


def lib():
    global _LIB
    if _LIB is None:
        _LIB = load_library()
    return _LIB


def last_error() -> str:
    raw = lib().CEloqStore_GetLastError(c_void_p())
    return raw.decode("utf-8") if raw else "unknown error"


def alloc_bytes(data: bytes):
    from ctypes import c_uint8 as _c_uint8

    if not data:
        arr = (_c_uint8 * 1)()
        return arr, arr, 0
    arr = (_c_uint8 * len(data)).from_buffer_copy(data)
    return arr, arr, len(data)


def as_input_buffer(data: Any):
    from ctypes import c_uint8 as _c_uint8

    if isinstance(data, str):
        data = data.encode("utf-8")
    if isinstance(data, bytes):
        return alloc_bytes(data)

    view = memoryview(data)
    if view.ndim != 1:
        raise TypeError("buffer must be 1-dimensional")
    if not view.contiguous:
        raise TypeError("buffer must be contiguous")
    byte_view = view.cast("B")
    if len(byte_view) == 0:
        arr = (_c_uint8 * 1)()
        return arr, arr, 0
    if byte_view.readonly:
        arr = (_c_uint8 * len(byte_view)).from_buffer_copy(byte_view)
        return (view, arr), arr, len(byte_view)
    arr = (_c_uint8 * len(byte_view)).from_buffer(byte_view)
    return (view, arr), arr, len(byte_view)


def as_output_buffer(data: Any):
    from ctypes import c_uint8 as _c_uint8

    view = memoryview(data)
    if view.ndim != 1:
        raise TypeError("output buffer must be 1-dimensional")
    if not view.contiguous:
        raise TypeError("output buffer must be contiguous")
    if view.readonly:
        raise TypeError("output buffer must be writable")
    byte_view = view.cast("B")
    if len(byte_view) == 0:
        arr = (_c_uint8 * 1)()
        return (view, arr), arr, 0
    arr = (_c_uint8 * len(byte_view)).from_buffer(byte_view)
    return (view, arr), arr, len(byte_view)
