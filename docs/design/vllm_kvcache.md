# EloqStore vLLM KV Cache Design

This is the canonical design document for the EloqStore-backed vLLM KV cache
connector.

The implementation lives in:

```text
eloqstore/python/src/eloqstore/vllm_connector.py
```

`vllm` does not carry a second EloqStore connector implementation. It loads
`eloqstore.vllm_connector` through the standard external connector hook.

## Scope

This document only covers the current design. Old transport models and removed
configuration concepts are intentionally omitted.

## Ownership Boundary

The integration has three layers:

1. EloqStore engine

- embedded storage engine
- shard execution
- durable persistence and retrieval

2. EloqStore KV-cache runtime

- shared host buffer ownership
- block-sized shared-buffer slot layout derived from vLLM KV blocks
- shared-buffer attach and mapping for workers
- optional CUDA host registration for the attached shared buffer
- async save / load request execution
- batched request waiting
- worker attachment to the shared buffer

3. vLLM adapter

- scheduler-side prefix matching
- worker-side layer hooks
- GPU KV tensor marshaling
- vLLM metadata transport

Everything specific to the EloqStore-backed vLLM connector lives in the
`eloqstore` project. `vllm` only provides the generic connector interface and
dynamic loading path.

## User-Facing Configuration

The connector exposes only these settings:

- `store_paths`
- `memory_bytes`
- `cpu_threads`

Everything else is derived internally.

Internally, the shared host buffer is no longer treated as a small number of
generic variable-sized entries. The runtime now needs to match the vLLM block
model directly:

- one shared-memory slot corresponds to one vLLM KV block
- slot size is the aligned payload size of one serialized block across all
  participating layers
- slot count is `memory_bytes / slot_size`

This avoids the previous failure mode where a large `memory_bytes` budget was
split into only a small number of oversized entries, which exhausted the number
of allocatable cache slots long before the shared-memory byte budget was used.

One important runtime prerequisite is not a connector setting:

- the host `RLIMIT_MEMLOCK` must be large enough for the pinned memory that
  EloqStore registers with io_uring

In the verified GPU setup, stock vLLM could serve `Qwen3-4B` without issue, but
the EloqStore-backed server failed until memlock was raised. The relevant
operational details and the verified launch command live in:

- `docs/vllm_eloqstore_connector_local_install.md`

## Data Path

The hot path is only `save` and `load`:

```text
save:
GPU KV cache -> shared host buffer -> EloqStore -> storage

load:
storage -> shared host buffer -> GPU KV cache
```

There is one unavoidable GPU/CPU copy in each direction because vLLM KV tensors
are GPU-resident and EloqStore is a CPU-side storage engine.

Worker code only cares about bytes in the shared host buffer. Storage routing
and runtime bookkeeping stay internal.

## Save Flow

`save_kv_layer(layer_name, kv_layer, attn_metadata)` is called once per layer.

The connector keeps the layer hook cheap:

1. Build block plans for the current requests.
2. Append this layer's KV bytes into a per-block staging buffer.
3. Do not wait in the layer hook.

`wait_for_save()` is the synchronization point:

1. Mark staged block slots as ready in the shared buffer.
2. Return to vLLM without waiting for durable EloqStore persistence.
3. Let the runtime flush dirty block slots to EloqStore asynchronously.

This keeps the high-frequency per-layer path small and moves the wait to the
point where vLLM already expects synchronization, while durable persistence is a
separate publish path.

## Load Flow

`start_load_kv(forward_context)` is non-blocking:

1. Read scheduler metadata describing which blocks should be loaded.
2. Submit async EloqStore load requests for those blocks.
3. Record request state for later layer waits.

`wait_for_layer_load(layer_name)` is the synchronization point:

1. Wait until the blocks needed by this layer are ready in the shared buffer.
2. Copy bytes from the shared buffer back into the GPU KV tensor for this layer.

This matches vLLM's execution model: issue loads early, wait only when a layer
actually needs the data.

## Prefix Matching

The scheduler side probes the runtime chunk by chunk and returns the longest
contiguous reusable prefix.

Matching is conservative:

- resident in-memory slots are checked first
- only fully saved chunks are reusable
- persisted EloqStore lookup is only needed after an in-memory miss
- the connector can reserve local work for vLLM prefill when required by the
  current execution model

## Why The Connector Is Still Non-Trivial

The runtime SDK handles async save/load execution, shared-buffer ownership,
worker-side shared-buffer attachment, batched request waiting, and runtime
shape derivation from the exposed memory/thread budget.

The connector still has to translate between:

- vLLM requests, blocks, layers, and `slot_mapping`
- EloqStore byte payloads keyed by prompt-chunk identity

That translation is the bulk of the Python connector.

## Related Document

- install and local usage:
  `docs/vllm_eloqstore_connector_local_install.md`
