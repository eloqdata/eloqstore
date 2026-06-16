#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
from __future__ import annotations

import copy
from dataclasses import dataclass, field
import re
import resource
from typing import TYPE_CHECKING, Any, cast

import torch

from vllm.config import VllmConfig
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
    SupportsHMA,
)
from vllm.distributed.kv_transfer.kv_connector.v1.metrics import KVConnectorPromMetrics
from vllm.distributed.kv_transfer.kv_connector.v1.metrics import KVConnectorStats
from vllm.logger import init_logger
from vllm.utils.hashing import safe_hash
from vllm.v1.core.sched.output import SchedulerOutput

try:
    from eloqstore import (
        KVCacheManager,
        KVCacheManagerOptions,
        KVCacheWorker,
        KVCacheWorkerOptions,
    )
except ImportError:  # pragma: no cover
    KVCacheManager = None
    KVCacheManagerOptions = None
    KVCacheWorker = None
    KVCacheWorkerOptions = None

if TYPE_CHECKING:
    from vllm.forward_context import ForwardContext
    from vllm.distributed.kv_transfer.kv_connector.v1.metrics import PromMetric, PromMetricT
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.request import Request

logger = init_logger(__name__)
_SCHEMA_VERSION = "v1"
_DEFAULT_IPC_PATH = "ipc:///tmp/eloqstore-vllm-kvcache.sock"
_DEFAULT_SHARED_MEMORY_NAME = "/eloqstore-vllm-kvcache"


def align_to_block_size(value: int, block_size: int) -> int:
    # The connector only stores and loads full vLLM blocks. Any tail that does
    # not fill a complete block must stay local to the current prefill step.
    return value - (value % block_size)


@dataclass
class EloqStoreConnectorStats(KVConnectorStats):
    """Connector-local counters.

    vLLM already provides the generic stats container. This subclass keeps the
    EloqStore-specific counters in one place so the scheduler and worker can
    both report through the same interface.
    """

    def __post_init__(self) -> None:
        if not self.data:
            self.reset()

    def reset(self) -> None:
        self.data = {
            "match_query_tokens": 0,
            "match_aligned_tokens": 0,
            "match_hit_tokens": 0,
            "match_miss_queries": 0,
            "match_hit_blocks": 0,
            "match_miss_blocks": 0,
            "match_unaligned_tail_tokens": 0,
            "match_reserved_tail_tokens": 0,
            "match_queries": 0,
            "load_blocks": 0,
            "load_payload_bytes": 0,
            "store_blocks": 0,
            "store_payload_bytes": 0,
        }

    def clone_and_reset(self) -> "EloqStoreConnectorStats":
        old = copy.copy(self)
        old.data = dict(self.data)
        self.reset()
        return old

    def aggregate(self, other: KVConnectorStats) -> KVConnectorStats:
        if other.is_empty():
            return self
        for key, value in other.data.items():
            self.data[key] = self.data.get(key, 0) + value
        return self

    def reduce(self) -> dict[str, int | float]:
        query_tokens = int(self.data["match_query_tokens"])
        aligned_tokens = int(self.data["match_aligned_tokens"])
        hit_tokens = int(self.data["match_hit_tokens"])
        hit_rate = 0.0 if query_tokens == 0 else hit_tokens / query_tokens
        aligned_hit_rate = 0.0 if aligned_tokens == 0 else hit_tokens / aligned_tokens
        return {
            **self.data,
            "match_hit_rate": round(hit_rate, 4),
            "match_aligned_hit_rate": round(aligned_hit_rate, 4),
        }

    def is_empty(self) -> bool:
        return not any(self.data.values())

    def record_match_query(
        self,
        *,
        query_tokens: int,
        aligned_tokens: int,
        hit_tokens: int,
        hit_blocks: int,
        miss_blocks: int,
        reserved_tail_tokens: int,
        unaligned_tail_tokens: int,
    ) -> None:
        self.data["match_queries"] += 1
        self.data["match_query_tokens"] += query_tokens
        self.data["match_aligned_tokens"] += aligned_tokens
        self.data["match_hit_tokens"] += hit_tokens
        self.data["match_hit_blocks"] += hit_blocks
        self.data["match_miss_blocks"] += miss_blocks
        self.data["match_reserved_tail_tokens"] += reserved_tail_tokens
        self.data["match_unaligned_tail_tokens"] += unaligned_tail_tokens
        if hit_tokens == 0:
            self.data["match_miss_queries"] += 1

    def record_load(self, blocks: int, payload_bytes: int) -> None:
        self.data["load_blocks"] += blocks
        self.data["load_payload_bytes"] += payload_bytes

    def record_store(self, blocks: int, payload_bytes: int) -> None:
        self.data["store_blocks"] += blocks
        self.data["store_payload_bytes"] += payload_bytes


@dataclass
class ReqMeta:
    """Serialized per-request work description shipped from scheduler to worker.

    The connector uses vLLM's own `block_hashes` as the authoritative block
    identity. The worker only needs the local `block_ids` plus the matching
    block-hash slice for the current request segment.
    """

    block_ids: torch.Tensor
    block_hashes: list[bytes]
    is_store: bool

    @staticmethod
    def make_meta(
        block_ids: list[int],
        block_hashes: list[bytes],
        block_size: int,
        is_store: bool,
        start_token: int = 0,
        token_limit: int = 0,
    ) -> "ReqMeta":
        valid_num_tokens = align_to_block_size(token_limit, block_size)
        block_ids_tensor = torch.as_tensor(block_ids, dtype=torch.long)
        start_block = start_token // block_size
        num_request_blocks = max(valid_num_tokens // block_size - start_block, 0)
        return ReqMeta(
            block_ids=block_ids_tensor[start_block : start_block + num_request_blocks],
            block_hashes=block_hashes[start_block : start_block + num_request_blocks],
            is_store=is_store,
        )


@dataclass
class _PendingLoadRequest:
    # Scheduler-side bookkeeping: this request has an external prefix available
    # and the worker should load `num_external_tokens` before local execution.
    request: "Request"
    num_external_tokens: int


@dataclass
class _PendingLoadBlock:
    # Worker-side runtime state after `start_load_kv()` has submitted a load.
    # Unlike `_PreparedBlockPlan`, this already has a runtime request id.
    block_key: str
    block_id: int
    payload_bytes: int
    request_id: int


@dataclass
class _PreparedBlockPlan:
    # Lightweight plan produced from connector metadata. This says which block
    # should be loaded or saved, but it does not allocate buffers or submit I/O.
    block_key: str
    block_id: int


@dataclass
class _BlockRuntimePayload:
    # Full runtime description of one block transfer.
    block_key: str
    block_id: int
    payload_bytes: int
    buffer_view: torch.Tensor | None = None
    request_id: int = 0
    buffer_offset: int = 0


@dataclass
class EloqStoreConnectorMetadata(KVConnectorMetadata):
    """Metadata transported by vLLM from scheduler to worker.

    Each request entry is intentionally compact: enough information to rebuild
    block keys and local block targets on the worker, but not enough to require
    shipping actual KV tensors through vLLM metadata.
    """

    requests: list[ReqMeta] = field(default_factory=list)

    def add_request(
        self,
        block_ids: list[int],
        block_hashes: list[bytes],
        block_size: int,
        is_store: bool,
        start_token: int = 0,
        token_limit: int = 0,
    ) -> None:
        self.requests.append(
            ReqMeta.make_meta(
                block_ids,
                block_hashes,
                block_size,
                is_store,
                start_token,
                token_limit,
            )
        )


class EloqStoreConnector(KVConnectorBase_V1, SupportsHMA):
    """vLLM KV connector backed by EloqStore.

    There are two distinct roles with different responsibilities:

    - scheduler: probe storage for reusable prefixes and emit transfer metadata
    - worker: marshal KV bytes between GPU tensors and the shared host buffer

    The code is organized around that split. Most of the complexity here comes
    from translating vLLM's request/block/layer model into byte payloads that
    the EloqStore runtime can load and save asynchronously.
    """

    def __init__(
        self,
        vllm_config: "VllmConfig",
        role: KVConnectorRole,
        kv_cache_config: "KVCacheConfig | None" = None,
    ):
        super().__init__(
            vllm_config=vllm_config,
            role=role,
            kv_cache_config=cast(Any, kv_cache_config),
        )
        if (
            KVCacheManager is None
            or KVCacheManagerOptions is None
            or KVCacheWorker is None
            or KVCacheWorkerOptions is None
        ):
            raise ImportError("eloqstore runtime SDK is required to use EloqStoreConnector")

        self._role = role
        self._block_size = vllm_config.cache_config.block_size
        self._model_name = self._resolve_model_name()
        self._table_name = self._resolve_table_name()
        self._layer_order = self._resolve_layer_order()
        self._block_payload_bytes = self._resolve_block_payload_bytes()
        self._layer_block_offsets: dict[str, tuple[int, int]] = {}
        # Worker-side view of live KV cache tensors, normalized into layer-major
        # form so all later save/load helpers can treat every layer uniformly.
        self._registered_kv_caches: dict[str, torch.Tensor] = {}
        # Scheduler-side staging of requests that should issue a load in the next
        # scheduler step.
        self._requests_need_load: dict[str, _PendingLoadRequest] = {}
        # Scheduler-side staging of requests whose newly computed blocks should
        # be persisted after the next worker execution step.
        self._requests_need_store: dict[str, "Request"] = {}
        self._stats = EloqStoreConnectorStats()
        self._runtime_options: Any | None = None
        self._kv_cache_manager: Any | None = None
        self._kv_cache_worker: Any | None = None
        self._buffer_pool_descriptor: str | None = None
        # Worker-side state for loads that have already been submitted to the
        # runtime and are waiting to be completed.
        self._pending_load_blocks: list[_PendingLoadBlock] = []
        # Worker-side state for blocks being assembled into the shared buffer for
        # an eventual save. The dict key is the storage block key.
        self._pending_save_blocks: dict[str, _BlockRuntimePayload] = {}
        # Plans derived from bound metadata. These exist before any I/O starts.
        self._prepared_load_blocks: list[_PreparedBlockPlan] = []
        self._prepared_save_blocks: list[_PreparedBlockPlan] = []
        # Failed load block ids reported back to vLLM so it can invalidate those
        # local blocks instead of treating them as usable external cache hits.
        self._load_error_block_ids: set[int] = set()
        self._init_runtime()

    @property
    def prefer_cross_layer_blocks(self) -> bool:
        # A single EloqStore payload concatenates the block bytes for every
        # registered layer, so vLLM can treat this connector as cross-layer.
        return True

    def _init_runtime(self) -> None:
        # The scheduler owns the shared buffer pool and the storage runtime.
        # Workers only create a lightweight client and attach to the exported
        # shared buffer later when it becomes available.
        options = self._build_runtime_options()
        self._runtime_options = options
        if self._role == KVConnectorRole.SCHEDULER:
            assert KVCacheManager is not None
            runtime = KVCacheManager(options)
            runtime.start()
            self._validate_memlock_budget(options.shared_memory_bytes)
            runtime.register_io_uring_buffers()
            self._buffer_pool_descriptor = runtime.export_buffer_pool()
            self._kv_transfer_config.kv_connector_extra_config[
                "shared_memory_descriptor"
            ] = self._buffer_pool_descriptor
            self._kv_cache_manager = runtime
            return

        assert KVCacheWorker is not None
        runtime = KVCacheWorker(options)
        self._kv_cache_worker = runtime

    def _resolve_worker_descriptor(self) -> str:
        # The scheduler exports the shared buffer descriptor through the generic
        # connector extra config, which is the only metadata path shared with the
        # worker before request-level connector metadata is bound.
        descriptor = self._kv_transfer_config.get_from_extra_config(
            "shared_memory_descriptor", None
        )
        if descriptor:
            return str(descriptor)
        return ""

    def shutdown(self) -> None:
        # Clear both runtime objects and in-flight state. The connector instance
        # may outlive a particular request, but it should not retain per-request
        # transfer state after shutdown.
        if self._kv_cache_worker is not None:
            self._kv_cache_worker.close()
            self._kv_cache_worker = None
        if self._kv_cache_manager is not None:
            self._kv_cache_manager.close()
            self._kv_cache_manager = None
        self._pending_load_blocks.clear()
        self._pending_save_blocks.clear()
        self._prepared_load_blocks.clear()
        self._prepared_save_blocks.clear()
        self._load_error_block_ids.clear()

    def bind_connector_metadata(self, connector_metadata: KVConnectorMetadata) -> None:
        super().bind_connector_metadata(connector_metadata)
        # Parse request metadata immediately so later worker hooks can stay small
        # and only deal with submitting/waiting for I/O.
        self._prepare_block_plans(connector_metadata)

    def clear_connector_metadata(self) -> None:
        super().clear_connector_metadata()
        # Prepared plans are scoped to one scheduler step. Pending runtime state
        # is cleared by the save/load completion paths instead.
        self._prepared_load_blocks.clear()
        self._prepared_save_blocks.clear()

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        # Preserve model layer order and precompute only the byte offsets for
        # each layer inside one concatenated block payload.
        self._registered_kv_caches = {
            layer_name: kv_caches[layer_name]
            for layer_name in self._layer_order
            if layer_name in kv_caches
        }
        self._layer_block_offsets.clear()
        offset = 0
        for layer_name, kv_layer in self._registered_kv_caches.items():
            layer_bytes = self._layer_payload_bytes(kv_layer, 0)
            self._layer_block_offsets[layer_name] = (offset, layer_bytes)
            offset += layer_bytes
        self._ensure_worker_runtime_attached()

    def register_cross_layers_kv_cache(
        self, kv_cache: torch.Tensor, attn_backend: type[Any]
    ) -> None:
        # Some attention backends expose the KV cache in a packed cross-layer
        # layout. Normalize that tensor into one view per layer so all later
        # copy helpers can reuse the same layer-major code path.
        try:
            stride_order = attn_backend.get_kv_cache_stride_order(
                include_num_layers_dimension=True
            )
            inv_order = [stride_order.index(i) for i in range(len(stride_order))]
            layer_major_kv_cache = kv_cache.permute(*inv_order)
        except (AttributeError, NotImplementedError):
            layer_major_kv_cache = kv_cache

        self._registered_kv_caches = {
            layer_name: layer_major_kv_cache[layer_idx]
            for layer_idx, layer_name in enumerate(self._layer_order)
        }
        self._layer_block_offsets.clear()
        offset = 0
        for layer_name, kv_layer in self._registered_kv_caches.items():
            layer_bytes = self._layer_payload_bytes(kv_layer, 0)
            self._layer_block_offsets[layer_name] = (offset, layer_bytes)
            offset += layer_bytes
        self._ensure_worker_runtime_attached()

    def start_load_kv(self, forward_context: "ForwardContext", **kwargs: Any) -> None:
        del forward_context, kwargs
        if self._role != KVConnectorRole.WORKER:
            raise RuntimeError("start_load_kv is worker-only for EloqStoreConnector")
        metadata = self._get_connector_metadata()
        assert isinstance(metadata, EloqStoreConnectorMetadata)
        self._ensure_worker_runtime_attached()
        self._pending_load_blocks.clear()
        self._load_error_block_ids.clear()
        # `_prepared_load_blocks` already exists at this point because it was
        # derived from scheduler metadata during `bind_connector_metadata()`. The
        # transition here is from "planned" to "submitted".
        for block_plan in self._prepared_load_blocks:
            # The scheduler has already filtered loadable blocks. The worker
            # only submits reads and waits later when one layer actually needs
            # the loaded bytes.
            payload = self._build_block_runtime_payload(
                block_plan.block_key,
                block_plan.block_id,
            )
            try:
                request_id = self._begin_load(
                    payload.block_key,
                    payload.payload_bytes,
                )
            except Exception as exc:
                logger.warning(
                    "EloqStore load submit failed for key=%s block=%s: %s",
                    payload.block_key,
                    payload.block_id,
                    exc,
                )
                self._load_error_block_ids.add(payload.block_id)
                continue
            self._pending_load_blocks.append(
                _PendingLoadBlock(
                    block_key=payload.block_key,
                    block_id=payload.block_id,
                    payload_bytes=payload.payload_bytes,
                    request_id=request_id,
                )
            )

    def wait_for_layer_load(self, layer_name: str) -> None:
        if self._role != KVConnectorRole.WORKER:
            raise RuntimeError(
                "wait_for_layer_load is worker-only for EloqStoreConnector"
            )
        if layer_name not in self._registered_kv_caches or not self._pending_load_blocks:
            return
        # Load stays fully asynchronous until the first layer actually needs
        # the bytes. At that point we synchronously wait for every submitted
        # block and scatter the shared-buffer bytes back into the layer tensors.
        self._wait_for_pending_loads()

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: Any,
        **kwargs: Any,
    ) -> None:
        del layer_name, kv_layer, attn_metadata, kwargs
        if self._role != KVConnectorRole.WORKER:
            raise RuntimeError("save_kv_layer is worker-only for EloqStoreConnector")
        metadata = self._get_connector_metadata() if self.has_connector_metadata() else None
        if not isinstance(metadata, EloqStoreConnectorMetadata):
            return
        self._ensure_worker_runtime_attached()
        # Save is now block-batched instead of layer-batched. The layer hook is
        # intentionally a no-op hot path; all bytes are gathered once in
        # `wait_for_save()` from the registered KV cache tensors.
        return

    def wait_for_save(self):
        if self._role != KVConnectorRole.WORKER:
            raise RuntimeError("wait_for_save is worker-only for EloqStoreConnector")
        self._ensure_worker_runtime_attached()
        for block_plan in self._prepared_save_blocks:
            payload = self._pending_save_blocks.get(block_plan.block_key)
            if payload is None:
                payload = self._build_block_runtime_payload(
                    block_plan.block_key,
                    block_plan.block_id,
                )
                buffer_handle = self._begin_save(
                    payload.block_key,
                    payload.payload_bytes,
                )
                payload.request_id = buffer_handle.request_id
                payload.buffer_offset = buffer_handle.offset
                self._bind_payload_buffer_view(payload)
                self._pending_save_blocks[block_plan.block_key] = payload

            self._copy_block_into_shared_buffer(payload, block_plan.block_id)

        pending_saves: dict[int, _BlockRuntimePayload] = {}
        for payload in self._pending_save_blocks.values():
            # `finish_save` tells the runtime that all bytes for this block have
            # been copied into the shared buffer and the request can become
            # visible to storage readers.
            self._finish_save(payload.request_id)
            pending_saves[payload.request_id] = payload
        self._wait_for_pending_saves(pending_saves)
        self._pending_save_blocks.clear()

    def get_block_ids_with_load_errors(self) -> set[int]:
        # vLLM consumes this once per step. Return-and-clear avoids reporting the
        # same load failure across unrelated future steps.
        invalid = set(self._load_error_block_ids)
        self._load_error_block_ids.clear()
        return invalid

    def _ensure_worker_runtime_attached(self) -> None:
        # The worker can be constructed before the scheduler has exported the
        # shared buffer descriptor. Attachment is therefore lazy and retried on
        # every hot-path entry until it succeeds.
        if self._role != KVConnectorRole.WORKER:
            return
        if self._kv_cache_worker is None:
            return
        if self._kv_cache_worker.attached():
            return
        descriptor = self._resolve_worker_descriptor()
        if not descriptor:
            return
        try:
            self._kv_cache_worker.attach_buffer_pool(
                descriptor,
                map_shared_buffer=True,
                cuda_register=torch.cuda.is_available(),
            )
        except Exception as exc:
            logger.warning("Unable to attach EloqStore shared buffer: %s", exc)

    def _prepare_block_plans(self, connector_metadata: KVConnectorMetadata) -> None:
        self._prepared_load_blocks.clear()
        self._prepared_save_blocks.clear()
        if not isinstance(connector_metadata, EloqStoreConnectorMetadata):
            return
        # Convert request-shaped metadata into flat block plans. This is the
        # boundary between scheduler reasoning and worker execution.
        for request in connector_metadata.requests:
            target = self._prepared_save_blocks if request.is_store else self._prepared_load_blocks
            for block_id, block_hash in self._iter_request_blocks(request):
                block_key = self._block_key(block_hash)
                target.append(
                    _PreparedBlockPlan(
                        block_key=block_key,
                        block_id=block_id,
                    )
                )

    def _validate_memlock_budget(self, shared_memory_bytes: int) -> None:
        # io_uring buffer registration pins the shared host buffer. Fail early
        # with a targeted error instead of letting runtime startup fail later
        # with a much less obvious resource error.
        if shared_memory_bytes <= 0:
            return
        try:
            soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        except (AttributeError, OSError, ValueError):
            return
        if soft_limit < 0 or shared_memory_bytes <= soft_limit:
            return
        soft_kib = soft_limit // 1024
        hard_kib = hard_limit // 1024 if hard_limit >= 0 else -1
        required_kib = (shared_memory_bytes + 1023) // 1024
        raise RuntimeError(
            "EloqStore shared pinned memory pool exceeds RLIMIT_MEMLOCK: "
            f"requested={required_kib} KiB soft_limit={soft_kib} KiB "
            f"hard_limit={hard_kib} KiB. Reduce memory_bytes or raise memlock "
            "before enabling io_uring buffer registration."
        )

    def _begin_save(
        self,
        key: str,
        payload_bytes: int,
    ):
        # Both scheduler and worker expose the same logical operation, but the
        # scheduler talks to the owning runtime while the worker talks to the
        # attached client runtime.
        if self._role == KVConnectorRole.SCHEDULER:
            return self._kv_cache_manager.begin_save(key, payload_bytes)
        self._ensure_worker_runtime_attached()
        if self._kv_cache_worker is None:
            raise RuntimeError("kv cache worker runtime is not available for begin_save")
        return self._kv_cache_worker.begin_save(key, payload_bytes)

    def _begin_load(
        self,
        key: str,
        payload_bytes: int,
    ) -> int:
        # Same role split as `_begin_save()`: scheduler owns the storage runtime,
        # worker forwards through the attached client object.
        if self._role == KVConnectorRole.SCHEDULER:
            return self._kv_cache_manager.begin_load(key, payload_bytes)
        self._ensure_worker_runtime_attached()
        if self._kv_cache_worker is None:
            raise RuntimeError("kv cache worker runtime is not available for begin_load")
        return self._kv_cache_worker.begin_load(key, payload_bytes)

    def _finish_save(self, request_id: int) -> None:
        # The worker only calls this after all layer slices for the block have
        # been written into the shared buffer.
        if self._role == KVConnectorRole.SCHEDULER:
            self._kv_cache_manager.finish_save(request_id)
            return
        if self._kv_cache_worker is None:
            raise RuntimeError("kv cache worker runtime is not available for finish_save")
        self._kv_cache_worker.finish_save(request_id)

    def _wait_for_pending_loads(self) -> None:
        # Wait for every submitted block as one batch, then restore each full
        # block payload back into the registered cross-layer KV cache view.
        pending = {block.request_id: block for block in self._pending_load_blocks}
        if self._kv_cache_worker is None:
            raise RuntimeError("kv cache worker runtime is not available for load wait")
        results = self._kv_cache_worker.wait_requests(list(pending))
        for request_id, result in results.items():
            block = pending[request_id]
            if result.status != "ready":
                logger.warning(
                    "EloqStore load failed for key=%s block=%s: status=%s",
                    block.block_key,
                    block.block_id,
                    result.status,
                )
                self._load_error_block_ids.add(block.block_id)
                continue
            self._copy_buffer_into_block(
                block_id=block.block_id,
                buffer_offset=result.offset,
                payload_bytes=result.length,
            )
            self._stats.record_load(1, result.length)
        self._pending_load_blocks.clear()

    def _wait_for_pending_saves(
        self,
        pending_saves: dict[int, _BlockRuntimePayload],
    ) -> None:
        # Save completion does not need a scatter step. Once storage marks the
        # payload ready, the block is fully published.
        if self._kv_cache_worker is None:
            raise RuntimeError("kv cache worker runtime is not available for save wait")
        results = self._kv_cache_worker.wait_requests(list(pending_saves))
        for request_id, result in results.items():
            payload = pending_saves[request_id]
            if result.status != "ready":
                logger.warning(
                    "EloqStore save failed for key=%s block=%s: status=%s",
                    payload.block_key,
                    payload.block_id,
                    result.status,
                )
                continue
            self._stats.record_store(1, payload.payload_bytes)

    def _buffer_slice(self, buffer_offset: int, payload_bytes: int) -> memoryview:
        self._ensure_worker_runtime_attached()
        if self._kv_cache_worker is None:
            raise RuntimeError("worker shared buffer is not attached")
        # Always operate on a flat byte view. The save/load path only needs raw
        # bytes; tensor shapes are restored later from the registered KV cache.
        return self._kv_cache_worker.shared_buffer().slice(buffer_offset, payload_bytes)

    def _buffer_slice_tensor(self, buffer_offset: int, payload_bytes: int) -> torch.Tensor:
        # Expose one shared-buffer slice as a CPU uint8 tensor without creating
        # an intermediate Python bytes object. The underlying buffer is the
        # shared pinned host region owned by the runtime.
        self._ensure_worker_runtime_attached()
        if self._kv_cache_worker is None:
            raise RuntimeError("worker shared buffer is not attached")
        return self._kv_cache_worker.shared_buffer().slice_tensor(buffer_offset, payload_bytes)

    def _bind_payload_buffer_view(self, payload: _BlockRuntimePayload) -> None:
        payload.buffer_view = self._buffer_slice_tensor(
            payload.buffer_offset,
            payload.payload_bytes,
        )

    def _extract_block_tensor(self, kv_layer: torch.Tensor, block_id: int) -> torch.Tensor:
        # vLLM KV tensors may be laid out either as [2, num_blocks, ...] for
        # separate K/V heads or [num_blocks, ...] for already packed tensors.
        if kv_layer.shape[0] == 2:
            return kv_layer[:, block_id, ...]
        return kv_layer[block_id, ...]

    def _layer_payload_bytes(self, kv_layer: torch.Tensor, block_id: int) -> int:
        block_tensor = self._extract_block_tensor(kv_layer, block_id)
        return int(block_tensor.numel() * block_tensor.element_size())

    def _build_block_layout(
        self,
        block_key: str,
        block_id: int,
    ) -> int:
        del block_key, block_id
        return self._block_payload_bytes

    def _build_block_runtime_payload(
        self,
        block_key: str,
        block_id: int,
    ) -> _BlockRuntimePayload:
        # One runtime payload now always corresponds to one full block-sized
        # shared-memory slot.
        payload_bytes = self._build_block_layout(block_key, block_id)
        return _BlockRuntimePayload(
            block_key=block_key,
            block_id=block_id,
            payload_bytes=payload_bytes,
        )

    def _copy_block_into_shared_buffer(
        self,
        payload: _BlockRuntimePayload,
        block_id: int,
    ) -> None:
        assert payload.buffer_view is not None
        for layer_name in self._layer_order:
            kv_layer = self._registered_kv_caches.get(layer_name)
            layer_layout = self._layer_block_offsets.get(layer_name)
            if kv_layer is None or layer_layout is None:
                continue
            offset, layer_bytes = layer_layout
            block_tensor = self._extract_block_tensor(kv_layer, block_id).detach().contiguous()
            cpu_tensor = block_tensor.to("cpu")
            payload.buffer_view.narrow(0, offset, layer_bytes).copy_(
                cpu_tensor.view(torch.uint8).reshape(-1)
            )

    def _copy_buffer_into_block(
        self,
        block_id: int,
        buffer_offset: int,
        payload_bytes: int,
    ) -> None:
        payload_tensor = self._buffer_slice_tensor(buffer_offset, payload_bytes)
        for layer_name in self._layer_order:
            kv_layer = self._registered_kv_caches.get(layer_name)
            layer_layout = self._layer_block_offsets.get(layer_name)
            if kv_layer is None or layer_layout is None:
                continue
            offset, layer_bytes = layer_layout
            block_tensor = self._extract_block_tensor(kv_layer, block_id)
            cpu_tensor = payload_tensor.narrow(0, offset, layer_bytes).view(
                block_tensor.dtype
            ).reshape(tuple(block_tensor.shape))
            block_tensor.copy_(cpu_tensor.to(device=block_tensor.device, dtype=block_tensor.dtype))

    def _get_num_matched_tokens_for_request(self, request: "Request") -> int:
        return self._get_num_matched_tokens_for_prompt(
            request.num_prompt_tokens,
            list(request.block_hashes),
        )

    def _get_num_matched_tokens_for_prompt(
        self,
        prompt_num_tokens: int,
        block_hashes: list[bytes],
    ) -> int:
        # Prefix matching is conservative: probe storage one full block at a
        # time and stop on the first miss so only the longest contiguous prefix
        # is considered externally reusable.
        max_probe_tokens = max(prompt_num_tokens - 1, 0)
        aligned_probe_tokens = align_to_block_size(max_probe_tokens, self._block_size)
        matched_tokens = 0
        hit_blocks = 0
        miss_blocks = 0
        probe_keys: list[tuple[int, str]] = []
        num_probe_blocks = min(aligned_probe_tokens // self._block_size, len(block_hashes))
        for block_index in range(num_probe_blocks):
            token_end = (block_index + 1) * self._block_size
            probe_keys.append((token_end, self._block_key(block_hashes[block_index])))
        existence = self._kv_cache_manager.contains_keys([key for _, key in probe_keys])
        for (token_end, _), exists in zip(probe_keys, existence, strict=False):
            if not exists:
                miss_blocks += 1
                break
            hit_blocks += 1
            matched_tokens = token_end
        self._stats.record_match_query(
            query_tokens=prompt_num_tokens,
            aligned_tokens=aligned_probe_tokens,
            hit_tokens=matched_tokens,
            hit_blocks=hit_blocks,
            miss_blocks=miss_blocks,
            reserved_tail_tokens=1 if prompt_num_tokens else 0,
            unaligned_tail_tokens=max(prompt_num_tokens - 1 - aligned_probe_tokens, 0),
        )
        return matched_tokens

    def get_metrics(self) -> dict[str, float | int]:
        return self._stats.reduce()

    def get_kv_connector_stats(self) -> EloqStoreConnectorStats | None:
        if self._stats.is_empty():
            return None
        return self._stats.clone_and_reset()

    @classmethod
    def build_kv_connector_stats(
        cls, data: dict[str, Any] | None = None
    ) -> EloqStoreConnectorStats | None:
        return EloqStoreConnectorStats(data=data) if data is not None else EloqStoreConnectorStats()

    @classmethod
    def build_prom_metrics(
        cls,
        vllm_config: VllmConfig,
        metric_types: dict[type["PromMetric"], type["PromMetricT"]],
        labelnames: list[str],
        per_engine_labelvalues: dict[int, list[object]],
    ) -> KVConnectorPromMetrics | None:
        del vllm_config, metric_types, labelnames, per_engine_labelvalues
        return None

    def get_num_new_matched_tokens(
        self,
        request: "Request",
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        if self._role != KVConnectorRole.SCHEDULER:
            raise RuntimeError(
                "get_num_new_matched_tokens is scheduler-only for EloqStoreConnector"
            )
        matched_tokens = self._get_num_matched_tokens_for_request(request)
        # EloqStore matching is synchronous today, so `False` advertises that no
        # later callback will update this decision asynchronously.
        return max(matched_tokens - num_computed_tokens, 0), False

    def update_state_after_alloc(
        self, request: "Request", blocks: "KVCacheBlocks", num_external_tokens: int
    ):
        del blocks
        # The scheduler records intent here and converts it into connector
        # metadata later in `build_connector_meta()` when it knows exactly which
        # requests were scheduled this step.
        if num_external_tokens > 0:
            self._requests_need_load[request.request_id] = _PendingLoadRequest(
                request=request,
                num_external_tokens=num_external_tokens,
            )
        elif request.num_computed_tokens < request.num_prompt_tokens:
            self._requests_need_store[request.request_id] = request

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        if self._role != KVConnectorRole.SCHEDULER:
            raise RuntimeError(
                "build_connector_meta is scheduler-only for EloqStoreConnector"
            )
        meta = EloqStoreConnectorMetadata()
        # New requests either load an externally matched prefix or, if there is
        # no reusable prefix, store any newly completed full blocks.
        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = new_req.prompt_token_ids or []
            mm_hashes = [f.identifier for f in new_req.mm_features]
            num_new_tokens = scheduler_output.num_scheduled_tokens[new_req.req_id]
            pending_load = self._requests_need_load.get(new_req.req_id)
            token_limit = min(new_req.num_computed_tokens + num_new_tokens, len(token_ids))
            if pending_load is not None:
                request = pending_load.request
                # The worker will load only the externally reusable prefix,
                # capped by the number of tokens actually scheduled this step.
                token_limit = min(token_limit, pending_load.num_external_tokens)
                meta.add_request(
                    block_ids=new_req.block_ids[0],
                    block_hashes=list(request.block_hashes),
                    block_size=self._block_size,
                    is_store=False,
                    start_token=0,
                    token_limit=token_limit,
                )
            elif token_limit > new_req.num_computed_tokens:
                request = self._requests_need_store.get(new_req.req_id)
                if request is None:
                    continue
                # No external load for this request, so publish any full blocks
                # that became complete during this scheduling step.
                meta.add_request(
                    block_ids=new_req.block_ids[0],
                    block_hashes=list(request.block_hashes),
                    block_size=self._block_size,
                    is_store=True,
                    start_token=new_req.num_computed_tokens,
                    token_limit=token_limit,
                )

        cached_reqs = scheduler_output.scheduled_cached_reqs
        # Cached requests only contribute save work for newly materialized local
        # blocks. There is no load path here because the reusable prefix would
        # already have been handled when the request first entered scheduling.
        for i, req_id in enumerate(cached_reqs.req_ids):
            num_computed_tokens = cached_reqs.num_computed_tokens[i]
            num_new_tokens = scheduler_output.num_scheduled_tokens[req_id]
            new_block_ids = cached_reqs.new_block_ids[i]
            request = self._requests_need_store.get(req_id)
            if request is None:
                continue
            if new_block_ids is None:
                # A scheduled step may advance token accounting without sealing a
                # new full block. Emit an empty store request so the next step can
                # continue from the updated start token without persisting bytes.
                meta.add_request(
                    block_ids=[],
                    block_hashes=[],
                    block_size=self._block_size,
                    is_store=True,
                    start_token=num_computed_tokens,
                    token_limit=num_computed_tokens,
                )
                continue
            total_tokens = min(
                num_computed_tokens + num_new_tokens,
                request.num_prompt_tokens,
            )
            meta.add_request(
                block_ids=new_block_ids[0],
                block_hashes=list(request.block_hashes),
                block_size=self._block_size,
                is_store=True,
                start_token=num_computed_tokens,
                token_limit=total_tokens,
            )
        self._requests_need_load.clear()
        return meta

    def request_finished(
        self,
        request: "Request",
        block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        return self.request_finished_all_groups(request, (block_ids,))

    def request_finished_all_groups(
        self,
        request: "Request",
        block_ids: tuple[list[int], ...],
    ) -> tuple[bool, dict[str, Any] | None]:
        del request, block_ids
        return False, None

    def _build_runtime_options(self) -> Any:
        # The public connector config is intentionally small. Everything else is
        # derived here so scheduler and worker can deterministically rendezvous
        # on the same runtime identity.
        extra = self._kv_transfer_config.kv_connector_extra_config
        store_paths = extra.get("store_paths") or []
        if isinstance(store_paths, str):
            store_paths = [store_paths]
        memory_bytes = int(extra.get("memory_bytes", 512 << 20))
        cpu_threads = int(extra.get("cpu_threads", 1))
        ipc_path = str(extra.get("ipc_path") or _DEFAULT_IPC_PATH)
        # One machine uses one EloqStore runtime here, so keep the shared-memory
        # name fixed across LLMs and restarts unless the user explicitly
        # overrides it.
        shm_name = str(extra.get("shared_memory_name") or _DEFAULT_SHARED_MEMORY_NAME)
        if self._role == KVConnectorRole.SCHEDULER:
            assert KVCacheManagerOptions is not None
            return KVCacheManagerOptions.from_budget(
                store_paths=list(store_paths),
                table_name=self._table_name,
                memory_bytes=memory_bytes,
                cpu_threads=cpu_threads,
                block_payload_bytes=self._block_payload_bytes,
                branch=str(extra.get("branch", "main")),
                ipc_path=ipc_path,
                shared_memory_name=shm_name,
                term=int(extra.get("term", 0)),
                partition_group_id=int(extra.get("partition_group_id", 0)),
            )
        assert KVCacheWorkerOptions is not None
        return KVCacheWorkerOptions.from_budget(
            memory_bytes=memory_bytes,
            cpu_threads=cpu_threads,
            block_payload_bytes=self._block_payload_bytes,
            ipc_path=ipc_path,
            shared_memory_name=shm_name,
        )

    def _resolve_model_name(self) -> str:
        # Prefer the externally served model name so table naming follows the
        # user-visible deployment identity when available.
        model_config = self._vllm_config.model_config
        served = getattr(model_config, "served_model_name", None)
        if isinstance(served, str) and served:
            return served
        model = getattr(model_config, "model", None)
        if isinstance(model, str) and model:
            return model
        return "unknown_model"

    def _resolve_table_name(self) -> str:
        # Keep a deterministic default table name, but allow explicit override so
        # multiple deployments can intentionally share or isolate storage.
        explicit = self._kv_transfer_config.get_from_extra_config("table_name", None)
        if explicit:
            return str(explicit)
        sanitized = re.sub(r"[^0-9A-Za-z._-]+", "_", self._model_name).strip("._-")
        if not sanitized:
            sanitized = "unknown_model"
        return f"vllm_kv__{sanitized}"

    def _resolve_layer_order(self) -> list[str]:
        # This order defines the byte layout inside every stored block payload.
        # Save and load must agree exactly, so the connector derives it once from
        # the configured KV cache groups.
        if self._kv_cache_config is None:
            return []
        return [
            layer_name
            for group in self._kv_cache_config.kv_cache_groups
            for layer_name in group.layer_names
        ]

    def _resolve_block_payload_bytes(self) -> int:
        # The shared host buffer is sized as one slot per vLLM KV block. Each
        # slot therefore needs to hold the concatenated bytes for one block
        # across every participating layer.
        if self._kv_cache_config is None:
            raise RuntimeError("kv cache config is required to derive block payload bytes")

        total_bytes = 0
        for group in self._kv_cache_config.kv_cache_groups:
            group_spec = group.kv_cache_spec
            per_layer_specs = getattr(group_spec, "kv_cache_specs", None)
            for layer_name in group.layer_names:
                spec = per_layer_specs.get(layer_name, group_spec) if per_layer_specs else group_spec
                total_bytes += int(spec.page_size_bytes)

        if total_bytes <= 0:
            raise RuntimeError("derived block payload bytes must be greater than zero")
        return total_bytes

    def _block_key(self, block_hash: bytes) -> str:
        # Reuse vLLM's own block hash as the sole block identity. This matches
        # the other block-hash-based KV cache implementations in vLLM and avoids
        # mixing in a second token/end-position-based key namespace.
        prefix_hash = safe_hash(block_hash, usedforsecurity=False).hexdigest()
        return f"kv:{_SCHEMA_VERSION}:{prefix_hash}"

    def _iter_request_blocks(
        self, request: ReqMeta
    ) -> list[tuple[int, bytes]]:
        blocks: list[tuple[int, bytes]] = []
        if request.block_ids.numel() == 0:
            return blocks
        block_count = min(request.block_ids.numel(), len(request.block_hashes))
        for local_index in range(block_count):
            blocks.append(
                (
                    int(request.block_ids[local_index]),
                    request.block_hashes[local_index],
                )
            )
        return blocks
