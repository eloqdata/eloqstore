from __future__ import annotations

from ctypes import CDLL, POINTER, Structure, c_bool, c_char_p, c_size_t, c_uint16, c_uint32, c_uint64, c_uint8, c_void_p, cdll
from pathlib import Path
import os


class CGetResult(Structure):
    _fields_ = [
        ("value", POINTER(c_uint8)),
        ("value_len", c_size_t),
        ("timestamp", c_uint64),
        ("expire_ts", c_uint64),
        ("found", c_bool),
        ("owns_value", c_bool),
    ]


class CKVCacheBufferHandle(Structure):
    _fields_ = [
        ("request_id", c_uint64),
        ("offset_bytes", c_uint64),
        ("payload_bytes", c_uint32),
    ]


class CKVCacheRequestState(Structure):
    _fields_ = [
        ("request_id", c_uint64),
        ("status", c_uint8),
        ("offset_bytes", c_uint64),
        ("payload_bytes", c_uint32),
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
            _preload_packaged_dependencies(candidate)
            lib = cdll.LoadLibrary(str(candidate))
            _configure_library(lib)
            return lib
    raise FileNotFoundError(
        "Could not locate libeloqstore_capi.so. "
        "Build the `eloqstore_capi` target or set ELOQSTORE_PY_LIB."
    )


def _preload_packaged_dependencies(main_lib: Path) -> None:
    libs_dir = main_lib.parent if main_lib.parent.name == ".libs" else main_lib.parent / ".libs"
    if not libs_dir.exists():
        return
    mode = getattr(os, "RTLD_GLOBAL", 0) | getattr(os, "RTLD_NOW", 0)
    for dependency in sorted(libs_dir.glob("*.so*")):
        if dependency.name == main_lib.name:
            continue
        CDLL(str(dependency), mode=mode) if mode else cdll.LoadLibrary(str(dependency))


def _configure_library(lib) -> None:
    lib.CEloqStore_GetLastError.argtypes = [c_void_p]
    lib.CEloqStore_GetLastError.restype = c_char_p
    lib.CEloqStore_FreeCString.argtypes = [c_void_p]

    lib.CEloqStore_Options_Create.restype = c_void_p
    lib.CEloqStore_Options_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_Options_AddStorePath.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_Options_SetNumThreads.argtypes = [c_void_p, c_uint16]
    lib.CEloqStore_Options_SetBufferPoolSize.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_Options_SetDataPageSize.argtypes = [c_void_p, c_uint16]
    lib.CEloqStore_Options_SetManifestLimit.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_Options_SetFdLimit.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_Options_SetPagesPerFileShift.argtypes = [c_void_p, c_uint8]
    lib.CEloqStore_Options_SetOverflowPointers.argtypes = [c_void_p, c_uint8]
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
    lib.CEloqStore_TableIdent_Create.argtypes = [c_char_p, c_uint32]
    lib.CEloqStore_TableIdent_Create.restype = c_void_p
    lib.CEloqStore_TableIdent_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_Put.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t, POINTER(c_uint8), c_size_t, c_uint64]
    lib.CEloqStore_Put.restype = c_uint32
    lib.CEloqStore_Get.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t, POINTER(CGetResult)]
    lib.CEloqStore_Get.restype = c_uint32
    lib.CEloqStore_FreeGetResult.argtypes = [POINTER(CGetResult)]
    lib.CEloqStore_Delete.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t, c_uint64]
    lib.CEloqStore_Delete.restype = c_uint32
    lib.CEloqStore_Exists.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t, POINTER(c_bool)]
    lib.CEloqStore_Exists.restype = c_uint32

    lib.CEloqStore_KVCacheOptions_Create.restype = c_void_p
    lib.CEloqStore_KVCacheOptions_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheOptions_AddStorePath.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheOptions_SetTableName.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheOptions_SetBranch.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheOptions_SetIpcPath.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheOptions_SetSharedMemoryName.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheOptions_SetNumThreads.argtypes = [c_void_p, c_uint16]
    lib.CEloqStore_KVCacheOptions_SetPartitionCount.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetTerm.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_KVCacheOptions_SetPartitionGroupId.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetSharedMemoryBytes.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_KVCacheOptions_SetEntrySize.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetEntryCount.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetEntryAlignment.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetSubmissionQueueDepth.argtypes = [c_void_p, c_uint32]
    lib.CEloqStore_KVCacheOptions_SetEagerIoUringRegister.argtypes = [c_void_p, c_bool]

    lib.CEloqStore_KVCacheManager_Create.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_Create.restype = c_void_p
    lib.CEloqStore_KVCacheManager_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_Start.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_Start.restype = c_bool
    lib.CEloqStore_KVCacheManager_Stop.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_RegisterIoUringBuffers.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_RegisterIoUringBuffers.restype = c_bool
    lib.CEloqStore_KVCacheManager_ExportBufferPool.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheManager_ExportBufferPool.restype = c_void_p
    lib.CEloqStore_KVCacheManager_BeginSave.argtypes = [c_void_p, c_char_p, c_uint32, POINTER(CKVCacheBufferHandle)]
    lib.CEloqStore_KVCacheManager_BeginSave.restype = c_bool
    lib.CEloqStore_KVCacheManager_FinishSave.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_KVCacheManager_FinishSave.restype = c_bool
    lib.CEloqStore_KVCacheManager_BeginLoad.argtypes = [c_void_p, c_char_p, c_uint32, POINTER(c_uint64)]
    lib.CEloqStore_KVCacheManager_BeginLoad.restype = c_bool
    lib.CEloqStore_KVCacheManager_CheckRequest.argtypes = [c_void_p, c_uint64, POINTER(CKVCacheRequestState)]
    lib.CEloqStore_KVCacheManager_CheckRequest.restype = c_bool
    lib.CEloqStore_KVCacheManager_GetReadyBuffer.argtypes = [c_void_p, c_uint64, POINTER(CKVCacheBufferHandle)]
    lib.CEloqStore_KVCacheManager_GetReadyBuffer.restype = c_bool
    lib.CEloqStore_KVCacheManager_ContainsKey.argtypes = [c_void_p, c_char_p, POINTER(c_bool)]
    lib.CEloqStore_KVCacheManager_ContainsKey.restype = c_bool

    lib.CEloqStore_KVCacheWorker_Create.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheWorker_Create.restype = c_void_p
    lib.CEloqStore_KVCacheWorker_Destroy.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheWorker_AttachBufferPool.argtypes = [c_void_p, c_char_p]
    lib.CEloqStore_KVCacheWorker_AttachBufferPool.restype = c_bool
    lib.CEloqStore_KVCacheWorker_DetachBufferPool.argtypes = [c_void_p]
    lib.CEloqStore_KVCacheWorker_BeginSave.argtypes = [c_void_p, c_char_p, c_uint32, POINTER(CKVCacheBufferHandle)]
    lib.CEloqStore_KVCacheWorker_BeginSave.restype = c_bool
    lib.CEloqStore_KVCacheWorker_FinishSave.argtypes = [c_void_p, c_uint64]
    lib.CEloqStore_KVCacheWorker_FinishSave.restype = c_bool
    lib.CEloqStore_KVCacheWorker_BeginLoad.argtypes = [c_void_p, c_char_p, c_uint32, POINTER(c_uint64)]
    lib.CEloqStore_KVCacheWorker_BeginLoad.restype = c_bool
    lib.CEloqStore_KVCacheWorker_CheckRequest.argtypes = [c_void_p, c_uint64, POINTER(CKVCacheRequestState)]
    lib.CEloqStore_KVCacheWorker_CheckRequest.restype = c_bool
    lib.CEloqStore_KVCacheWorker_GetReadyBuffer.argtypes = [c_void_p, c_uint64, POINTER(CKVCacheBufferHandle)]
    lib.CEloqStore_KVCacheWorker_GetReadyBuffer.restype = c_bool


_LIB = None


def lib():
    global _LIB
    if _LIB is None:
        _LIB = load_library()
    return _LIB


def last_error() -> str:
    raw = lib().CEloqStore_GetLastError(None)
    return raw.decode("utf-8", errors="replace") if raw else ""
