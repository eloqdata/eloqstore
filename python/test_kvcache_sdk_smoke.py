from __future__ import annotations

import mmap
import os
import sys
import time
from multiprocessing import Pipe, Process
from pathlib import Path

from eloqstore import (
    KVCacheManager,
    KVCacheManagerOptions,
    KVCacheWorker,
    KVCacheWorkerOptions,
)


STORE_DIR = "/tmp/opencode/eloqstore-sdk-store"
IPC_PATH = "ipc:///tmp/eloqstore-sdk-smoke.sock"
SHM_NAME = "/eloqstore-sdk-smoke"
PAYLOAD = b"hello-eloqstore-kvcache"
KEY = "smoke:key"
PARTITION_ID = 0


def _make_manager_options() -> KVCacheManagerOptions:
    return KVCacheManagerOptions(
        store_paths=[STORE_DIR],
        table_name="sdk_smoke",
        branch="main",
        ipc_path=IPC_PATH,
        shared_memory_name=SHM_NAME,
        num_threads=2,
        partition_count=4,
        shared_memory_bytes=8 << 20,
        slot_size=1 << 20,
        slot_count=8,
        slot_alignment=4096,
        submission_queue_depth=32,
        eager_io_uring_register=True,
    )


def _make_worker_options() -> KVCacheWorkerOptions:
    return KVCacheWorkerOptions(
        ipc_path=IPC_PATH,
        shared_memory_name=SHM_NAME,
        num_threads=2,
        partition_count=4,
        shared_memory_bytes=8 << 20,
        slot_size=1 << 20,
        slot_count=8,
        slot_alignment=4096,
        submission_queue_depth=32,
    )


def _shm_path_from_descriptor(descriptor: str) -> tuple[str, int, int]:
    parts = descriptor.split("|")
    return parts[1], int(parts[2]), int(parts[3])


def _wait_completion(poller, request_id: int, timeout_s: float = 10.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        completion = poller.poll_completion()
        if completion is None:
            time.sleep(0.01)
            continue
        if completion.request_id == request_id:
            return completion
    raise TimeoutError(f"timed out waiting for completion request_id={request_id}")


def run_same_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    manager_options = _make_manager_options()
    worker_options = _make_worker_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()

        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shm_path, mapped_bytes, slot_size = _shm_path_from_descriptor(descriptor)
            fd = os.open(shm_path, os.O_RDWR)
            mm: mmap.mmap | None = None
            try:
                mm = mmap.mmap(
                    fd,
                    mapped_bytes,
                    flags=mmap.MAP_SHARED,
                    prot=mmap.PROT_READ | mmap.PROT_WRITE,
                )
                save_req = worker.submit_save(KEY, PARTITION_ID, len(PAYLOAD))
                print(f"same save_req={save_req}", flush=True)
                mm.seek(save_req.slot_id * slot_size)
                mm.write(PAYLOAD)
                worker.mark_save_ready(save_req.request_id)
                save_completion = _wait_completion(worker, save_req.request_id)
                print(f"same save_completion={save_completion}", flush=True)
                assert save_completion.status == 2, save_completion
                assert manager.contains_key(KEY, PARTITION_ID) is True

                load_req = worker.submit_load(KEY, PARTITION_ID, len(PAYLOAD))
                print(f"same load_req={load_req}", flush=True)
                load_completion = _wait_completion(worker, load_req.request_id)
                print(f"same load_completion={load_completion}", flush=True)
                assert load_completion.status == 2, load_completion
                mm.seek(load_req.slot_id * slot_size)
                loaded = mm.read(len(PAYLOAD))
                assert loaded == PAYLOAD, (loaded, PAYLOAD)
            finally:
                if mm is not None:
                    mm.close()
                os.close(fd)


def run_shard_eviction_same_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    manager_options = _make_manager_options()
    worker_options = _make_worker_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()

        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shm_path, mapped_bytes, slot_size = _shm_path_from_descriptor(descriptor)
            fd = os.open(shm_path, os.O_RDWR)
            mm: mmap.mmap | None = None
            try:
                mm = mmap.mmap(
                    fd,
                    mapped_bytes,
                    flags=mmap.MAP_SHARED,
                    prot=mmap.PROT_READ | mmap.PROT_WRITE,
                )
                shard0_capacity = (
                    manager_options.slot_count // manager_options.num_threads
                )
                keys = [f"evict:key:{i}" for i in range(shard0_capacity + 1)]
                payloads = [f"payload-{i}".encode("utf-8") for i in range(len(keys))]

                for key, payload in zip(keys, payloads):
                    req = worker.submit_save(key, PARTITION_ID, len(payload))
                    mm.seek(req.slot_id * slot_size)
                    mm.write(payload)
                    worker.mark_save_ready(req.request_id)
                    completion = _wait_completion(worker, req.request_id)
                    assert completion.status == 2, completion

                for key in keys:
                    assert manager.contains_key(key, PARTITION_ID) is True

                reloaded_key = keys[0]
                reloaded_payload = payloads[0]
                load_req = worker.submit_load(reloaded_key, PARTITION_ID, len(reloaded_payload))
                load_completion = _wait_completion(worker, load_req.request_id)
                assert load_completion.status == 2, load_completion
                mm.seek(load_req.slot_id * slot_size)
                loaded = mm.read(len(reloaded_payload))
                assert loaded == reloaded_payload, (loaded, reloaded_payload)
            finally:
                if mm is not None:
                    mm.close()
                os.close(fd)


def _manager_process(conn, descriptor_path: str) -> None:
    manager_options = _make_manager_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()
        Path(descriptor_path).write_text(descriptor, encoding="utf-8")
        conn.send("ready")
        conn.recv()


def run_two_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    parent_conn, child_conn = Pipe()
    descriptor_path = "/tmp/opencode/eloqstore-sdk-descriptor.txt"
    proc = Process(target=_manager_process, args=(child_conn, descriptor_path), daemon=True)
    proc.start()
    assert parent_conn.recv() == "ready"

    try:
        worker_options = _make_worker_options()
        descriptor = Path(descriptor_path).read_text(encoding="utf-8")
        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shm_path, mapped_bytes, slot_size = _shm_path_from_descriptor(descriptor)
            fd = os.open(shm_path, os.O_RDWR)
            mm: mmap.mmap | None = None
            try:
                mm = mmap.mmap(
                    fd,
                    mapped_bytes,
                    flags=mmap.MAP_SHARED,
                    prot=mmap.PROT_READ | mmap.PROT_WRITE,
                )
                save_req = worker.submit_save(KEY + ":mp", PARTITION_ID, len(PAYLOAD))
                print(f"mp save_req={save_req}", flush=True)
                mm.seek(save_req.slot_id * slot_size)
                mm.write(PAYLOAD)
                worker.mark_save_ready(save_req.request_id)
                save_completion = _wait_completion(worker, save_req.request_id)
                print(f"mp save_completion={save_completion}", flush=True)
                assert save_completion.status == 2, save_completion

                load_req = worker.submit_load(KEY + ":mp", PARTITION_ID, len(PAYLOAD))
                print(f"mp load_req={load_req}", flush=True)
                load_completion = _wait_completion(worker, load_req.request_id)
                print(f"mp load_completion={load_completion}", flush=True)
                assert load_completion.status == 2, load_completion
                mm.seek(load_req.slot_id * slot_size)
                loaded = mm.read(len(PAYLOAD))
                assert loaded == PAYLOAD, (loaded, PAYLOAD)
            finally:
                if mm is not None:
                    mm.close()
                os.close(fd)
    finally:
        parent_conn.send("stop")
        proc.join(timeout=10)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)
        Path(descriptor_path).unlink(missing_ok=True)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "same"
    if mode == "same":
        run_same_process()
        print("same-process sdk smoke passed")
        return 0
    if mode == "same-evict":
        run_shard_eviction_same_process()
        print("same-process shard-eviction sdk smoke passed")
        return 0
    if mode == "mp":
        run_two_process()
        print("two-process sdk smoke passed")
        return 0
    raise SystemExit(f"unknown mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
