# EloqStore Architecture Docs

This directory is the canonical high-level documentation of EloqStore's design.
It exists so that a developer (or an AI coding assistant) can build an accurate
mental model of the engine without re-deriving it from 40k+ lines of C++.

## Maintenance policy

**These docs are part of the code.** When a change alters behavior described
here — a new request type, a manifest format change, a new KvOptions field, a
different GC rule, a renamed class — update the matching doc in the same PR.
Each doc lists the source files it describes; if you touch those files in a
way that changes the documented contract, the doc is stale until you fix it.

Docs describe *durable design*: ownership, invariants, formats, and contracts.
Do not record transient implementation trivia that the code states better.

## Reading order

| Doc | Covers |
|-----|--------|
| [01-overview.md](01-overview.md) | What EloqStore is, core concepts and glossary, repo layout, store modes |
| [02-runtime-and-lifecycle.md](02-runtime-and-lifecycle.md) | `EloqStore` ownership graph, `KvOptions`, startup/shutdown ordering |
| [03-request-api.md](03-request-api.md) | `KvRequest` family, sync/async completion, routing and fanout, retry rules |
| [04-execution-model.md](04-execution-model.md) | Shard threads, coroutine tasks, task pools, per-table write serialization |
| [05-btree-and-page-formats.md](05-btree-and-page-formats.md) | COW B+-tree, on-disk page encodings, in-memory cached pages, buffer pool |
| [06-persistence-and-recovery.md](06-persistence-and-recovery.md) | RootMeta, page mapping, manifest format, replay, terms, branches, archives, file naming |
| [07-io-stack.md](07-io-stack.md) | `AsyncIoManager` hierarchy: io_uring base, cloud overlay, standby overlay; FD cache; cloud transport |
| [08-data-lifecycle.md](08-data-lifecycle.md) | Read/scan/write paths, compaction, TTL, file GC, archives, prewarm |
| [09-large-values-zero-copy.md](09-large-values-zero-copy.md) | Segment files, `GlobalRegisteredMemory`, KV-cache pinned mode, metadata trailers |
| [10-bindings-and-embedding.md](10-bindings-and-embedding.md) | C ABI, Rust/Python SDKs, EloqModule embedding mode |

If you are about to modify code and want the minimum context: read 01, then
the doc matching the subsystem you are touching. 04 (execution model) is
prerequisite reading for any change inside `src/` — almost every correctness
property in this codebase leans on shard-thread affinity.

## Relationship to other docs

- `docs/zero_copy_*.md` are the original *design proposals* for the
  very-large-value feature. They predate the final implementation and contain
  superseded details (e.g. the free-list described there uses a Harris-mark
  scheme; the shipped code uses a version-tagged Treiber stack). Doc 09 here
  reflects the code as written.
- `.codex/knowledge-base/` is an older generated knowledge base; parts of it
  reference files that no longer exist. Prefer this directory.
