# EloqStore IO QoS: Foreground/Background IO Isolation

## Summary

EloqStore runs foreground tasks (ReadTask, ScanTask) and background tasks
(BatchWriteTask, BackgroundWrite/compaction, file GC) as coroutines on a
per-shard thread. CPU-side prioritization works: background tasks yield to a
low-priority ready queue and the scheduler bounds low-priority slices to
`max_processing_time_microseconds`. However, experiments show background tasks
still significantly degrade foreground read latency.

PR #455 (`perf(compaction/gc)`) addressed the CPU half of this problem: the
time-budgeted cooperative yield (`MaybeYield()` /
`eloqstore_yield_budget_us`, default 20µs) bounds how long any single
compaction/GC coroutine segment holds the worker thread, and GC unlink/close
SQE preparation is chunked into 128-op batches. What remains — and what this
document addresses — was that **CPU priority was lost at the IO boundary**.
Before M1/M2, one 20µs slice could still burst up to 128 page reads (compaction
move batches, `DoCompactDataFile`) plus writes into the io_uring ring, and
nothing distinguished those requests from foreground reads at the ring or
device level. Reads were not budgeted at all; writes were budgeted only
per-task. Yielding controlled when background IO was *issued*, not how much of
it queued at the device once issued.

This document defines three per-shard mechanisms. M1 and M2 are implemented;
M3 remains a measurement-gated follow-up:

- **M1**: per-shard caps on in-flight page IO, with **separate caps for
  reads and writes** — they are different device resources and must be
  tunable independently.
- **M2**: foreground/background classification with a background sub-budget
  on the read cap (all page writes come from write tasks, i.e. background,
  so the write cap needs no split).
- **M3**: a bytes/sec rate limiter on background writes (follow-up, driven by
  measurement; complements the write cap, which bounds queue depth but not
  sustained throughput).

All caps and limits in this document are **per shard**, consistent with the
rest of EloqStore.

Related maintained documentation: `docs/architecture/04-execution-model.md`
(scheduler, ready queues, yield discipline), `07-io-stack.md` (IouringMgr,
page/segment IO, pools), `08-data-lifecycle.md` (compaction/GC cadence).
Implementing this design requires updating those docs in the same change
(see CLAUDE.md).

## Baseline and Current Mechanisms

### Pre-QoS baseline

The table and gaps below describe the baseline before M1/M2, not the current
implementation.

| Baseline mechanism | Location | Scope |
|---|---|---|
| Two ready queues (high / low priority), 400µs low-priority slice | `Shard::ExecuteReadyTasks`, `KvTask::YieldToLowPQ` | CPU only |
| Time-budgeted cooperative yield: `MaybeYield()` once a coroutine segment exceeds `eloqstore_yield_budget_us` (20µs) — added by #455 | `MaybeYield`, `Shard::CurResumeElapsedUs` | CPU only |
| GC unlink/close SQE prep chunked into 128-op batches, `WaitIo` per chunk — added by #455 | `IouringMgr::CloseFiles` / `DeleteFiles` | Metadata IO burst size |
| Per-write-task in-flight cap: `WaitWrite()` when `inflight_io_ >= max_write_batch_pages` (32) | `WriteTask::WritePage` | Writes only, per task |
| `WriteReqPool` sized by `max_inflight_write` (default 32768) | `IouringMgr` | Writes, effectively unbounded |
| Segment compaction: ≤ `max_segments_batch` (8) in-flight 256KB segments per batch, buffers shared with foreground via `GlobalRegisteredMemory`; yields every `segment_compact_yield_every` segments | `BackgroundWrite::DoCompactSegmentFile` | Segment IO, per task |
| SQ ring capacity `io_queue_size` (4096); `GetSQE` blocks on full ring | `IouringMgr::GetSQE`, `waiting_sqe_` | Backpressure, not QoS |
| `max_write_concurrency` bounds concurrent write tasks (incl. compaction: `GetBackgroundWrite` returns null at the limit) | `TaskManager` | Default unlimited in local mode |
| Cloud slots (`max_cloud_concurrency`), cloud buffer pool | `CloudStoreMgr` | Cloud HTTP, not disk |

### Baseline gaps addressed by M1/M2

1. **Reads had no budget anywhere.** Page compaction
   (`DoCompactDataFile`) issues bursts of up to `max_read_pages_batch` (128)
   page reads per move batch through the same `ReadPages` path as foreground
   reads.
2. **There was no shard-global in-flight IO counter.** Only per-task
   `inflight_io_` and `prepared_sqe_` existed. The effective global bounds
   (4096 SQEs, 32K
   in-flight writes) are far beyond the queue depth at which NVMe read
   latency degrades.
3. **There was no FG/BG tagging of IO.** SQEs were indistinguishable once
   submitted; `sqe->ioprio` was never set. A background task scheduled for one 20µs
   slice can leave 128+ requests queued at the device ahead of every
   subsequent foreground read — the yield discipline of #455 bounds CPU
   occupation per slice, not the IO submitted within it.

The current M1/M2 implementation uses a per-shard default of 64 configured
data pages for reads and a 25% background read slice. The write budget is
available but retains its effectively-unbounded 32768-page default; 512 remains
an explicit opt-in value. The old `max_write_batch_pages` throttle has no
effect; M3 is still deferred.

## Device-Level Rationale

EloqStore's pattern in append mode is **sequential writes vs. random reads**,
which makes the interference mechanism clean:

- Reads and writes are asymmetric on NVMe. A 4KB write is acknowledged from
  the controller buffer (~10–20µs); a 4KB read must touch NAND (~60–100µs).
  The deferred cost of writes is NAND program time (~1–3ms per TLC program)
  and die occupancy.
- **Die collisions dominate.** A random read landing on a die that is
  mid-program waits for it (unless the drive supports program-suspend). To
  first order, if background write throughput is fraction ρ of the device's
  sustained write bandwidth, then P(read collides) ≈ ρ, and collided reads
  pay ~0.5–1.5ms. Median read latency stays flat; tail latency degrades
  roughly linearly with write **bytes/sec** — not with in-flight write count.
- Sequential appends keep drive-internal write amplification near 1
  (whole-file GC invalidates large contiguous regions), so there is no
  hidden GC multiplier — provided deleted file space is actually TRIMmed.

Consequences for the design:

- An in-flight cap (M1/M2) bounds burst queueing at the ring/device, but the
  control that maps to the physics of sustained interference is a **write
  bytes/sec budget** (M3).
- **Pace, don't burst**: the same average MB/s issued as evenly spaced 1MB
  merged writes produces better read tails than periodic multi-MB bursts.
  Keep the 1MB merge unit; throttle frequency, not size.

## Design

### M1: Per-shard in-flight page-IO caps, reads and writes separate

Two per-shard counters of in-flight **page** IO (in configured
`data_page_size` units), with independent caps, enforced at the page-IO entry
points of `IouringMgr`.
Reads and writes are deliberately **not** mixed in one budget: they are
different device resources (reads need deep queue depth for IOPS; writes
saturate bandwidth at shallow depth and their interference tracks sustained
bytes/sec), so a shared cap would couple two knobs that must be tuned
separately.

- Read budget (`inflight_read_pages_` ≤ `max_inflight_read`):
  `ReadPage` / `ReadPages`, cost 1 per page.
- Write budget (`inflight_write_pages_` ≤ `max_inflight_write`, option
  redefined — see Interaction with Existing Knobs):
  `WritePage`, cost 1; `SubmitMergedWrite`, cost = bytes rounded up to
  `data_page_size` (a 1MB merged write counts as 256 pages at the default 4KB
  page size), so the cap means the same thing in append and non-append mode.
Segment IO (`ReadSegments` / `WriteSegments`, zero-copy large values) is
**out of scope** — see Non-Goals.

Metadata operations (open, statx, rename, unlink, mkdir), manifest IO, and bulk
file/snapshot paths (`ReadFile`, `ReadFilePrefix`, `WriteSnapshot`) are exempt;
metadata burst size is already bounded by the 128-op chunking from #455.
`fdatasync` is not counted initially but is instrumented (see Evaluation).

Enforcement follows the existing idiom, per class (read or write). The
in-flight counter holds only previously admitted, not-yet-completed IO; each
request is counted exactly once, from admission to completion. For a new
request of size `cost` (page-units, not yet counted):

```text
acquire(class, cost):                 // at the page-IO entry point
    while inflight[class] != 0 and inflight[class] + cost > cap[class]:
        wait on waiting_zone[class] (FIFO)
    inflight[class] += cost           // admitted; prep SQE(s) and submit

release(class, cost):                 // in PollComplete, on completion
    inflight[class] -= cost
    wake waiters of that class
```

Blocking-on-acquire is preferred over yield-and-retry: it is cheaper and
guarantees FIFO fairness.

Rules:

- Acquire the budget immediately before SQE preparation, with **no yield
  points between acquire and submit**.
- Release happens in `PollComplete`, which runs regardless of task
  scheduling, so budget release never depends on the blocked task running —
  no deadlock. Paths that hold budget and then block on another pool (e.g.
  write buffer pool) must acquire in a consistent order; audit during
  implementation.

### M2: Foreground/background classification and BG sub-budget

Every page IO is issued from a `KvTask` (`ThdTask()`), so classification is a
task-type predicate. Add `KvTask::IsBackground()`:

- Background: `BatchWrite`, `BackgroundWrite`, `EvictFile`, `Prewarm`.
- Foreground: `Read`, `Scan`, `ListObject`, `ListStandbyPartition`, `Reopen`.

Do **not** reuse `ReadOnly()` — `EvictFile` and `Prewarm` are read-only but
background. They do not currently issue budgeted page reads: local GC uses
`ReadFile`, while prewarm/download uses whole-file bulk IO; both remain exempt.

The FG/BG split only applies to the **read** budget. All page writes are
issued by write tasks (`BatchWrite`, `BackgroundWrite`), which are background
by the classification above — so the write cap of M1 *is* the background
write budget, and needs no further split. (If a genuinely foreground write
class ever appears, split the write budget then.)

Read budget structure:

- Foreground reads: `inflight_read_pages_ ≤ max_inflight_read`.
- Background page reads (compaction move batches and batch-write tree-traversal
  reads): additionally
  `bg_inflight_read_pages_ ≤ bg_read_limit` (a fraction of
  `max_inflight_read`, e.g. 25%).

Background never exceeds its sub-budget. Foreground can consume the entire
read budget **while background has no pending demand**; once a background
acquisition enters the wait path, its unused entitlement
(`bg_read_limit − bg_inflight`) stays reserved through admission, including
the wake-to-admit gap — new foreground admissions leave it alone. Without the
reservation, sustained foreground saturation would starve background
forever (every freed unit would be re-acquired by a foreground waiter and
compaction could never bootstrap its share), letting space amplification
grow unboundedly. With it, the ratio is genuinely maintained under
contention: background ramps to its slice, foreground keeps the rest, and
foreground reclaims the whole budget the moment background demand drains.

Two read `WaitingZone`s (one per class, FIFO within class). On release,
background waiters are woken first while the sub-budget has room (those
units are reserved for them anyway), and foreground is **always** woken as
well — not merely handed leftover credits. The leftover-only scheme shipped
first and starved the foreground class under write storms (found 2026-07-11
on Azure NVMe): background forms a saturated treadmill whose queue never
empties, so every release re-donates its credit to background; once any
foreground read queued (admission is FIFO-behind-waiters) and the last
foreground in-flight's credit landed in a background dip, the foreground
class had **no remaining wake source** until background's queue drained —
observed as multi-second foreground gate stalls (p999 120–312 ms, gate
waits up to 3.7 s). Over-waking is safe by construction: woken tasks
re-check the admission condition and re-wait.

The ratio starts **static**. An adaptive policy (shrink `bg_read_limit`
toward a floor when foreground waiters exist, grow toward the full budget
when the shard is idle) is a possible follow-up once counters justify it.

### M3: Background write rate limiter (follow-up)

A per-shard token bucket in bytes, refilled at `bg_write_rate_limit` bytes/sec:

- `WritePage` / `SubmitMergedWrite` from background tasks debit the bucket
  and block on a `WaitingZone` when empty.
- Bucket capacity is small (a few refill intervals worth) so a full bucket
  cannot discharge as a burst; this paces merged writes evenly.
- 0 = disabled (default). Enable after the calibration sweep (below) shows
  the tail-latency knee for the target device.

M3 is deferred until experiments confirm that in-flight capping alone (M1+M2)
is insufficient — which the die-collision model predicts for sustained
compaction, since interference tracks bytes/sec rather than queue depth.

### New options (per shard)

```cpp
uint32_t max_inflight_read = 64;     // configured data pages; 0 = disabled
uint32_t bg_read_ratio = 25;         // percent of max_inflight_read
uint32_t max_inflight_write = 32768; // configured data pages; redefined option
                                     // effectively unbounded by default
uint64_t bg_write_rate_limit = 0;    // bytes/sec; 0 = disabled (M3)
```

The WSL interference sweeps (2026-07-03) initially selected a read cap of 32
and `bg_read_ratio = 25`. Azure NVMe validation (2026-07-11) then showed real
foreground queueing at 32 while 64–128 were indistinguishable, so the shipped
read default is 64 and the policy ratio remains 25%. A later Azure NVMe
acceptance run failed the p99.9 target with write cap 512, so the shipped write
default remains 32768 and smaller caps are opt-in. Real-device calibration (the
QD sweep below) should still re-derive the read cap per device; the ratio is
policy and should transfer.

### Sizing contract: device knob × policy knob

The two read-side options deliberately live at different levels:

- **`max_inflight_read` is device calibration.** Size it from the device's
  bandwidth-delay product: `c × max_random_read_IOPS × t_read(unloaded) /
  num_threads`, with c ≈ 2–4 for die-imbalance and burst headroom (Azure
  local-NVMe calibration 2026-07-11 measured the knee at c ≈ 5–7: budgets
  64–128 for 16 shards on 8 × ~250K-IOPS devices at ~150 µs loaded latency,
  indistinguishable within cloud-environment noise; 32 showed genuine
  foreground queueing under 128 concurrent readers — the budget must also
  cover peak per-shard foreground concurrency, not only the BDP). Below
  the BDP the cap costs read throughput; far above it the cap stops
  representing the device queue and the ratio contract below degrades.
  Validation signal: foreground `read_blocked` should stay ≈ 0 under
  representative load — nonzero means undersized.
- **`bg_read_ratio` is policy, and is deliberately a ratio.** A foreground
  read's worst-case queueing behind background is `bg_cap / IOPS`; with the
  cap BDP-sized this equals `ratio × c × t_read` — the device's IOPS
  cancels. Relative tail inflation (tail as a multiple of the device's own
  base latency) is therefore a function of the ratio alone: a slower device
  serves proportionally slower reads with the **same tail-to-median shape**.
  That predictability is the operator-facing contract, it survives hardware
  changes without retuning, and it is why the option must not be an
  absolute page count.
- Background demand scales with the same device parameters in device-bound
  deployments, so the ratio budget and the demand shrink and grow together.
  If measured demand persistently exceeds the budget
  (`bg_read_blocked_us` ≈ wall time), compaction is being deferred: the
  remedy is raising `max_inflight_read` (the device has headroom the
  calibration missed), not the ratio — raising the ratio spends the tail
  contract.
- **The background slice needs an absolute floor.** Batch writes issue
  their own read-modify-write page fetches under the background class, so
  a tiny `ratio × cap` throttles ingest itself, not just compaction:
  measured at bg_cap = 3 (ratio 10 of cap 32), write throughput halved and
  the paced foreground collapsed with it. Until the engine enforces
  `bg_cap = max(floor, ratio × cap)` (floor ≈ 8 on tested devices), do not
  configure combinations that yield bg_cap below ~8.

## Interaction with Existing Knobs

- **`io_queue_size` (SQ ring) stays.** With both caps ≪ 4096, tasks
  should essentially never block in `GetSQE`; the ring cap becomes a sanity
  bound rather than a throttle.
- **`max_write_batch_pages` throttle retires.** *(Implemented, plan commit
  4.)* The `inflight_io_ >= cap → WaitWrite()` branch in
  `WriteTask::WritePage` is subsumed by M1/M2 and is strictly worse (it
  drains to zero and restarts, producing a sawtooth; budget acquisition
  keeps a steady level). Keep: the CPU yields between page builds
  (`YieldToLowPQ()` / `MaybeYield()` — the #455 time-budgeted discipline is
  orthogonal and stays), and the terminal `WaitWrite()` before
  `UpdateMeta`/`SyncData` (error collection and durability ordering).
  One consequence surfaced by tests: with `enable_data_page_cache`, a write
  task's write-promotion pins on cached pages are now bounded by
  `max_inflight_write` rather than by the per-task drain, so pathologically
  small buffer pools must account for in-flight-write pins (production
  pools dwarf the ≤ max_inflight_write pages of pins; only tests noticed).
- **`max_inflight_write` is redefined, not retired.** It is the M1 write cap
  (configured data-page units). The default remains 32768, effectively
  unbounded; 512 is an explicit opt-in value until the release acceptance
  target is met.
  In-flight pages are bounded by `max(cap, one request's cost)` because one
  oversized request may run alone to guarantee progress. `WriteReqPool` stays
  numerically sized to the option, but counts request objects rather than page
  units; it is therefore a conservative allocation bound, not the QoS bound
  itself. Note this is a behavioral change for deployments that set the old
  option explicitly.
- **`max_write_concurrency` is no longer an IO-QoS knob.** Note it is
  enforced **per shard** (each shard's `TaskManager` counts its own active
  write tasks against the store-wide option value), so the device-wide task
  bound is `max_write_concurrency × num_threads`. It remains as a
  cross-partition admission/memory bound (coroutine stacks, 1MB write
  buffers, CoW roots with retained mapping snapshots — per-partition writes
  are already serialized by `PendingWriteQueue`). Local-mode guidance:
  a small constant (k = 2–4). Rationale:
  - k = 1 forfeits intra-shard pipelining: each batch's serial IO tail
    (final drain + fdatasync + manifest append) becomes dead time for that
    shard's queued partitions. k = 2 recovers most of it.
  - With 4–8 shards per device, device-level parallelism, utilization, and
    stream mixing are set by shard count, not k; k is purely an intra-shard
    pipelining knob. Per-shard k also provides skew robustness when write
    load concentrates on few shards.
  - Small k additionally staggers fdatasync phases, avoiding fsync bursts.
  - Cloud mode keeps `max_cloud_concurrency`-scale values: the terminal
    stall is an upload (tens of ms), so higher k is required to keep the
    shard's write pipeline busy.

## Multi-Shard Considerations

All budgets are per shard, but the device is shared by all shards
(typically 4–8 per NVMe device):

- Size per-shard budgets as device budget / num_threads. Static division is
  safe (aggregate background pressure is bounded regardless of which shards
  are busy) but conservative: a lone compacting shard gets 1/S of the
  background budget. Acceptable initially.
- Follow-up: a shared global token bucket (atomic counter) for the
  background budget. Cross-shard wake-up is handled by each shard's
  `WorkLoop` re-checking the bucket once per round and waking its local
  waiters — no cross-thread coroutine resumption needed.
- Cloud mode: NIC bandwidth is shared between background uploads and
  foreground cache-miss downloads; the same FG/BG discipline should
  eventually apply to cloud slots. Whole-file upload, prewarm/download, and
  snapshot disk IO remains exempt from the page-IO budget, including
  `ReadFile`, `ReadFilePrefix`, and `WriteSnapshot` bulk paths.

## Non-Goals

- **Segment IO (zero-copy large values) is not budgeted.** Segments exist to
  serve very large values, a workload where throughput matters rather than
  tail latency and fairness, so FG/BG isolation is not a goal there.
  Segment IO is counted against neither `max_inflight_read` nor
  `max_inflight_write`, and is not classified;
  segment compaction keeps its existing bounds (`max_segments_batch`
  in-flight segments per batch via the shared `GlobalRegisteredMemory` pool,
  `segment_compact_yield_every`). Revisit only if large-value traffic is
  ever mixed with latency-sensitive point reads on the same store.
- No `sqe->ioprio` reliance: only honored by mq-deadline/BFQ schedulers
  (NVMe defaults to `none`). May be added as a one-line supplement, never as
  the mechanism.
- No deferred BG staging queue (strict priority instead of a ratio): more
  complexity; revisit only if M1–M3 prove insufficient.
- No cross-shard coordination in the first iteration.

## Observability

Export per-shard counters from day one; tuning must be measurement-driven:

- Current and high-watermark pages for total reads, the BG-read subset, and
  writes; foreground read usage is total minus BG.
- Cumulative blocked time and counts per admission class: `read_` is foreground
  waits only, `bg_read_` is background waits, and `write_` is all write waits.
- Background write bytes/sec (actual, vs. limit when M3 lands).
- fdatasync count and cumulative batch wall time.

## Evaluation Plan

0. **Re-baseline on top of #455.** The interference experiments that
   motivated this design predate the time-budgeted yield and GC chunking.
   Re-run the read-vs-compaction benchmark first to quantify how much
   foreground tail latency remains attributable to device-side queueing
   (this design's target) vs. worker-thread stalls (already fixed). The
   remaining gap sets the success criterion for M1+M2.
1. **Device calibration sweep** (per target device, preconditioned drive):
   fixed 4KB random-read load at target QD, step up sequential write MB/s,
   plot read p99/p99.9 vs. write rate. The knee is the background budget.
   Expect a near-linear tail-latency curve for the sequential-write pattern.
   Run long enough to exhaust any SLC cache so consumer-drive folding
   behavior does not confound results.
2. **Isolate fdatasync impact**: repeat the interference experiment with
   sync counts instrumented; if fsync stalls dominate, sync spacing/batching
   matters more than page-IO budgets and no in-flight cap will fix it.
3. **Verify TRIM**: confirm the filesystem issues discards for GC-deleted
   files (`discard` mount option or periodic `fstrim`); otherwise the
   WAF ≈ 1 assumption silently breaks on aged drives.
4. **Historical staged rollout** (M1/M2 and throttle retirement are complete):
   - Land M1+M2 with existing throttles left in place (loosened).
   - Re-run the read-vs-compaction benchmark; confirm the new mechanism
     alone protects foreground p99.
   - Remove the `max_write_batch_pages` throttle, but keep
     `max_inflight_write` effectively unbounded by default until a calibrated
     QoS value meets the release acceptance target.
   - Sweep `max_write_concurrency` ∈ {1, 2, 4, 8} in local mode; pick the
     throughput knee (expected at 2–4).
   - Evaluate M3 against the calibration curve; enable if M1+M2 leave a
     sustained-write tail-latency gap.
