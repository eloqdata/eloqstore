# IO QoS: Implementation and Testing Plan

Companion to `io_qos.md` (the design). This document maps M1/M2 (and the M3
follow-up) onto concrete code changes, commit sequence, and tests. File and
symbol references are as of `main` @ 8f5f699.

## Commit sequence

Five commits, each independently landable and revertible. Docs
(`docs/architecture/04-execution-model.md`, `07-io-stack.md`,
`08-data-lifecycle.md`) are updated in the same commit as the behavior they
describe (CLAUDE.md requirement).

| # | Content | Risk |
|---|---|---|
| 1 | M1: budget state, acquire/release plumbing, options, stats getter | Core; behavior-neutral at default caps until tightened |
| 2 | M2: `IsBackground()`, BG read sub-budget, wake policy | Small delta on top of 1 |
| 3 | Interference benchmark + fio calibration script | Test-only |
| 4 | Retire `max_write_batch_pages`; keep write QoS opt-in until acceptance passes | Behavioral; gated on benchmark results |
| 5 | M3 rate limiter (deferred; only if step-4 benchmarks show a sustained-write gap) | Optional |

## Commit 1 — M1: in-flight page-IO budgets

> **Status: implemented** (2026-07-02). Deviations from the plan below:
> `IoQosStats` includes fdatasync counters; the oversized-request escape
> (`Acquire` admits a request costlier than the cap once the budget drains)
> was added so small configured caps cannot deadlock against ~1MB merged
> writes; tests use `REQUIRE` (glog's `CHECK` macro shadows Catch2's).

### State (in `IouringMgr`, per shard; `MemStoreMgr` untouched)

```cpp
// include/async_io_manager.h
class IoBudget
{
public:
    void Acquire(uint32_t cost);      // blocks ThdTask() on waiting_ (FIFO)
    void Release(uint32_t cost);      // decrement + WakeN; called by PollComplete
    // stats: current_, high_watermark_, total_blocked_count_, ...
private:
    uint32_t inflight_{0};
    uint32_t cap_{0};                 // 0 = disabled (Acquire is a no-op)
    WaitingZone waiting_;
};

IoBudget read_budget_;                // cap = max_inflight_read
IoBudget write_budget_;               // cap = max_inflight_write (redefined)
```

Follow the trailing-underscore member convention. `Acquire` loops
`while (inflight_ + cost > cap_) waiting_.Wait(ThdTask());` — the counter
holds only admitted, not-yet-completed IO (see design doc pseudocode).

### Release-side classification: new `UserDataType`s

`PollComplete` must know each CQE's class and cost. `EncodeUserData` packs
the type into the low 8 bits (`async_io_manager.cpp:1519`), so there is
ample room. Current types: `KvTask`, `BaseReq`, `WriteReq`,
`MergedWriteReq`. Add:

- `KvTaskPageRead` — single-page read issued via the `KvTask` path
  (`IouringMgr::ReadPage`). Cost 1 configured data page. Handled identically to
  `KvTask` in
  `PollComplete`, plus `read_budget_.Release(1)`.
- `BaseReqPageRead` — per-page read in a batch (`IouringMgr::ReadPages`).
  Handled identically to `BaseReq`, plus `read_budget_.Release(1)`.

Metadata ops keep `KvTask`/`BaseReq` and are never charged. Write costs are
already derivable in configured data-page units: `WriteReq` = 1;
`MergedWriteReq` = `bytes_ / data_page_size` (round up; assert page alignment).

FG/BG at release time (needed by commit 2) comes from the task pointer every
req type already carries: `task->IsBackground()`.

### Acquire points and ordering

Rule (from the design): acquire the budget **last**, immediately before
`GetSQE`, after every other blocking acquisition (FD open, req-pool alloc,
write-buffer alloc). This gives a consistent global order: FD/mutex →
pools/buffers → budget → SQE, and guarantees a task never holds budget while
blocked on another pool.

To be precise about the two phases: *waiting inside* `Acquire` is expected —
the task parks on the budget's `WaitingZone` holding no budget (the counter
is only incremented on admission), and `PollComplete` wakes it regardless of
scheduling. The "no yield points" rule applies *after admission*: between
incrementing the counter and submitting the SQE there must be no voluntary
yield (`YieldToLowPQ`/`MaybeYield`) and no blocking on any resource whose
release could depend on this task. `GetSQE`'s ring-full wait is the one
tolerated block in that window: benign (slots are freed by kernel progress,
never by the blocked task) and unreachable in practice with caps ≪
`io_queue_size`.

| Entry point | Change |
|---|---|
| `IouringMgr::ReadPage` (`async_io_manager.cpp` ~484) | `read_budget_.Acquire(1)` at the top of the `read_page` lambda's retry loop, before `GetSQE`. Release is per-CQE, so each retry re-acquires. |
| `IouringMgr::ReadPages` (~565) | Acquire **per page** inside `send_req`, not per batch. Rationale: a 128-page compaction batch must not deadlock against a BG sub-budget of 64 (commit 2); per-page acquisition lets the task block mid-batch while already-submitted pages complete. This supersedes the "atomic batch acquire" idea discussed during design review. |
| `IouringMgr::WritePage` (~742) | `write_budget_.Acquire(1)` after `write_req_pool_->Alloc`, before `GetSQE`. |
| `IouringMgr::SubmitMergedWrite` (~777) | `write_budget_.Acquire(ceil(bytes / data_page_size))` after `merged_write_req_pool_->Alloc`, before `GetSQE`. |

Exempt (unchanged): all metadata ops, manifest IO, `ReadFile` /
`ReadFilePrefix` / `WriteSnapshot` bulk paths, `Fdatasync` (instrumented only),
`ReadSegments` / `WriteSegments` (out of scope per design Non-Goals).

### Release point

In `PollComplete` (`async_io_manager.cpp` ~2010), per CQE, after the
existing per-type handling: `Release(cost)` on the matching budget. Wake
policy in commit 1 is a plain `WakeN` on the released budget's zone.

### Options and validation

- `kv_options.h`: add `max_inflight_read` (initially 256; shipped default 64);
  redefine `max_inflight_write` in configured data-page units (keep old default
  32768 through release acceptance; lower caps remain opt-in). Add INI parsing
  in `kv_options.cpp` and equality-operator entries.
- `eloq_store.cpp` option validation: `max_inflight_write != 0` already
  enforced; nothing else hard-fails. `max_inflight_read == 0` disables the
  read budget (documented).
- `WriteReqPool` stays sized by `max_inflight_write` (unchanged line in
  `IouringMgr` ctor) as a conservative request-object allocation bound. QoS
  counts configured page units, and a merged request may consume multiple
  units.

### Stats

`struct IoQosStats` (per budget: current, high-watermark, blocked count,
cumulative blocked µs) + `IouringMgr::GetIoQosStats()`. `read_` blocked fields
count foreground waits only, `bg_read_` counts background waits, and `write_`
counts all write waits. The `ELOQSTORE_WITH_TXSERVICE` meter registers gauges
for current total-read, BG-read, and write usage behind the existing
`EnableMetrics()` guard; full stats remain reachable through the store's shard
accessors. Add an fdatasync counter + latency accumulator in
`SyncFiles`/`FdatasyncFiles` in the same commit (evaluation step 2 needs
it).

### Shutdown / abort audit (do during code review of commit 1)

- Tasks blocked in a budget `WaitingZone` are `TaskStatus::Blocked`, so
  `TaskManager::NumActive() > 0` keeps `Shard::WorkLoop` spinning and
  `PollComplete` continues releasing budget — no shutdown hang. Same
  property as the existing `WriteReqPool::waiting_`.
- `AbortWrite` calls `WaitIo()` first; all of the aborting task's CQEs drain
  through `PollComplete`, releasing their budget. No manual cleanup needed.
- Kill-point paths (`TEST_KILL_POINT_WEIGHT("WritePage", ...)`) fire before
  budget acquisition — keep it that way.

## Commit 2 — M2: FG/BG read sub-budget

> **Status: implemented** (2026-07-02). As planned, with two refinements:
> (a) the sub-budget lives inside `IoBudget` (`SetBgCap` + `Acquire/Release`
> taking a `background` flag) rather than as a separate class, so the write
> budget reuses the same type with no sub-budget configured; per-class
> oversized-request escape and FIFO zones as described; stats gained a
> `bg_read_` slice and a BG-read inflight gauge. (b) Waking FG first was found
> to starve background under sustained foreground saturation. Demand is now
> tracked from first wait through admission, so the unused BG entitlement stays
> reserved across wake-to-admit; release wakes BG first and always wakes FG.

- `KvTask::IsBackground()` in `tasks/task.h`: `BatchWrite`,
  `BackgroundWrite`, `EvictFile`, `Prewarm` → true; explicitly not
  `ReadOnly()`.
- Extend the read budget: `bg_inflight_` counter and a second `WaitingZone`
  (`bg_waiting_`). Acquire for a BG task additionally checks
  `bg_inflight_ + cost > bg_cap_`. Release decrements both counters when
  the completing request's `task->IsBackground()`.
- Wake policy: on read-budget release, wake BG waiters while
  `bg_inflight_ < bg_cap_`, and always wake FG as well. Spurious wakes are safe
  (acquire re-checks in its loop).
- Option `bg_read_ratio = 25` (percent, clamp 1–100);
  `bg_cap_ = max_inflight_read * ratio / 100`, min 1 page... but see next
  line: `bg_cap_` must be ≥ 1 and the per-page acquisition from commit 1
  guarantees any batch size makes progress through an arbitrarily small
  sub-budget.

## Commit 3 — benchmark and calibration tooling

> **Status: implemented** (2026-07-02). Delivered as a standalone binary
> `benchmark/interference_bench.cpp` (+ `opts_interference.ini`) rather than
> extending `eloq_store_bm`, and `scripts/io_calibration_sweep.sh`. Two
> deviations from the sketch below: (a) the storm is a **rotating strided
> partial overwrite** (3-of-5 keys, ~3000B one-KV-per-page values, shifted
> each round) — a bulk overwrite leaves files fully dead and compaction
> generates no read traffic at all; (b) latency percentiles are exact
> (raw-sample sort at phase end), not sliding-window. The report includes
> per-shard IoQosStats deltas (BG-read watermark/blocked, fdatasync
> count/latency), giving the sweep direct visibility into whether the
> budgets engaged. Smoke-verified: storm lifts read p99 ~2.3× at toy scale,
> bg_read watermark pins at the configured sub-budget cap.

- **Interference benchmark**: mixed scenario: steady point-read load at
  fixed QD on N partitions while a write/compaction storm runs. Report read
  p50/p99/p99.9 and BG write MB/s. Accept knob overrides via the existing
  `opts_*.ini` mechanism so sweeps are scriptable.
- **fio calibration script** (`scripts/io_calibration_sweep.sh`): fixed
  4KB randread job + stepped sequential-write job against the target
  device; emits the p99-vs-write-MB/s curve (design evaluation step 1).
  Document drive preconditioning and SLC-exhaustion runtime in the script
  header.
- Run design evaluation step 0 (re-baseline on top of #455) with this
  benchmark before tightening any default.

## Commit 4 — retire superseded throttles (gated on benchmarks)

> **Status: implemented** (2026-07-03) except for default tightening. The
> throttle retirement shipped in the branch, but the later Azure NVMe
> acceptance run rejected making the calibrated write cap the default.
> Details: `WritePage` throttle branch removed (budget admission comment
> left in its place); `max_write_batch_pages` deprecated — parsed with a
> LOG(WARNING), validation removed so all values are accepted, field documented
> as no-effect;
> The WSL sweep initially selected `max_inflight_write = 512` from
> {512, 2048, 32768} (512 bound marginally — hwm pinned,
> 6–26 blocks/30s — at equal-or-best write throughput and read tails;
> natural demand ceiling was 2048 = 8 concurrent 1MB merged writes).
> The later Azure NVMe acceptance run did not justify making that value the
> default, so the final default remains 32768 and 512 is opt-in. One behavioral
> consequence of selecting a smaller value: with
> enable_data_page_cache, write-promotion pins are now bounded by
> max_inflight_write instead of the per-task drain; the data_page_cache OOM
> test was recalibrated to `max_inflight_write = 1` (tiny 2-slot pool).
> The read default moved from 256 to 32 in the initial WSL campaign, then to
> the shipped value 64 after Azure NVMe validation; bg_read_ratio stays 25.

The implemented change was gated on the interference benchmark confirming
that M1+M2 alone protect foreground p99 (design evaluation step 4):

- Removed the `inflight_io_ >= max_write_batch_pages → WaitWrite()` branch in
  `WriteTask::WritePage` (`write_task.cpp` ~304). Keep the CPU yields and
  the terminal `WaitWrite()`.
- Kept `max_inflight_write` at 32768 by default; calibrated lower values such
  as 512 are explicit opt-ins. Deployments setting the option must interpret
  it under its new page-budget semantics.
- Deprecated `max_write_batch_pages` in `kv_options.h` (keep parsing,
  ignore, log a warning) for one release before deletion.

## Commit 5 — M3 rate limiter (deferred)

Sketch only; build if commit-4 benchmarks show sustained-compaction tail
degradation that inflight caps don't remove: token bucket (bytes) in
`IouringMgr`, refilled from the shard `WorkLoop` clock each round
(`ReadTimeMicroseconds` is already sampled per round), debited in
`WritePage`/`SubmitMergedWrite` when `task->IsBackground()`, bucket capacity
≤ 4 refill intervals, `bg_write_rate_limit = 0` disables.

## Testing plan

> **Status:** unit-test items 1–6 and the later regression cases are implemented
> in `tests/io_qos.cpp` (16 cases). Notes: item 4's "two concurrent merged
> writes" requires multiple partitions' write tasks — a single task's
> flushes don't overlap (it yields per page while filling the next buffer);
> item 5 uses the read-only-directory injection (kill points SIGTERM the
> process — they are crash-recovery tooling, not error injection); item 6
> verifies Stop() drains tasks queued behind a 1-page budget. Regression:
> full suite run (memlock binaries via systemd-run on WSL); db_stress with
> tiny caps (read 4 / bg 1 / write 8, so append merged writes exercise the
> oversized-admission path every flush) run ≥100 iterations across
> non-append + append modes — db_stress needs `active_width <= max_key`
> or its sliding window indexes out of bounds (harness limitation, debug
> builds assert). ASAN: two modes exist — `WITH_ASAN=ON` (fully sound,
> including coroutine stacks) hard-requires a locally built
> `libboost_context-asan` (Boost.Context compiled with BOOST_USE_ASAN +
> ucontext; not a distro package); alternatively, injecting
> `-fsanitize=address` via CMAKE_CXX_FLAGS with WITH_ASAN=OFF links stock
> boost and gives full **heap** coverage (UAF/overflow/leaks — what the
> budget/pool plumbing needs) with degraded stack coverage inside coroutine
> frames. The QoS suites were run under the flag-injection mode on this
> box.
>
> Results (2026-07-03, WSL box): full suite 32/32 binaries green (memlock
> suites via systemd-run; persist's one failure was cross-load contention,
> 3/3 clean serially). db_stress: 120/120 iterations pass (60 non-append +
> 60 append, tiny caps). One real commit-4 fallout found by the sweep:
> data_page_cache "OOM retry under concurrent writes" became ~1-in-5 flaky
> — it runs append mode with a pool too small for a write-buffer pool, so
> writes take the non-append path where the removed per-task drain used to
> bound write-promotion pins; fixed by bounding pins via
> `max_inflight_write = 32` in the test (12/12 pass after). Same class of
> consequence as the other recalibrated OOM test; production pools are
> orders of magnitude above the ≤ max_inflight_write pin bound.

### Unit tests (new `tests/io_qos.cpp`, Catch2, wired into `tests/CMakeLists.txt`)

Use `tests/common.h` presets; add a `qos_opts` preset with deliberately tiny
caps to make blocking paths hot:

1. **Accounting invariants**: mixed read/write workload on `default_opts` +
   `append_opts` variants with `max_inflight_read = 4`,
   `max_inflight_write = 8`; after quiesce, assert both budgets' current
   == 0 and high-watermark ≤ cap. Repeat with caps disabled (0) — stats
   stay zero, results identical.
2. **Oversized batch vs. tiny cap**: values with >128 overflow pages (forces
   `GetOverflowValue`'s 128-page `ReadPages` batches) under
   `max_inflight_read = 4` — completes, no deadlock (validates per-page
   acquisition).
3. **BG sub-budget**: issue a `CompactRequest` concurrently with a stream of
   point reads under small caps; assert BG high-watermark ≤ `bg_cap_`,
   foreground reads complete, and compaction finishes (no starvation in
   either direction).
4. **Merged-write cost**: append mode with `max_inflight_write = 256` and
   1MB write buffers — two merged writes cannot be in flight together
   (watermark ≤ 256); with 512 they can.
5. **Error paths**: kill-point injection on `WritePage` (existing
   `TEST_KILL_POINT_WEIGHT`) and a failing write (read-only dir trick or
   fault injection as in `large_value_fault_injection.cpp`) — budget
   returns to zero after `AbortWrite`.
6. **Shutdown while blocked**: stop the store while tasks are queued behind
   a 1-page budget; store drains and stops cleanly.

### Regression and stress

- Full `ctest --test-dir build/tests/` with default options (must be
  behavior-neutral before commit 4) and again with the tiny-cap `qos_opts`
  exported via an env override if practical.
- `db_stress` run with tiny caps in both append and non-append modes;
  repeat-run (≥100 iterations) to shake out wake/ordering races — budget
  bugs manifest as rare hangs, same failure mode as the historical
  free-list ABA issue, so favor iteration count over single-run length.
- ASAN build (`build-asan/`) for the new plumbing.
- On WSL2 dev machines, launch memlock-sensitive suites via
  `systemd-run --user --pipe --wait --property=LimitMEMLOCK=2G` (ulimit is
  unreliable there); do not shrink test parameters to dodge it.

### Performance acceptance (per target device, after commit 3)

- **User-confirmed product target**: read p99.9 below 10 ms during concurrent
  write, compaction, and GC.
- **Measured status: FAIL / not release-ready.**
  - In the 2026-07-16 same-binary campaign, median read p99.9 was
    19.779 ms with read QoS disabled and 18.711 ms with read QoS enabled.
    Both conditions retained the 512-page write budget, so that campaign did
    not isolate its effect. It ran at
    `c625004a32f474f446ba8adeba2d2d68f93dcee7` on `/dev/nvme1n1`
    (`Microsoft NVMe Direct Disk v2`).
  - The 2026-07-17 three-condition follow-up used three balanced-order,
    fresh-store repetitions at
    `42b7f94084f196264615a77ccb20a35c284f5d12`. Median logical write
    throughput was 19,473 key-ops/s with write cap 32768, 19,337 with write
    cap 512 (-0.7%), and 19,693 with the full 64/25%/512 policy. The 512 cap
    reached its high-water mark and recorded a median 53 write blocks, but was
    not sustained-throughput-discriminating. Median read p99.9 was
    17.111/16.268/15.751 ms respectively: the full policy improved 7.9% versus
    32768 but still missed the 10 ms target by 57.5%. All nine runs had
    concurrent compaction/GC and passed their validity gates.
  - The tested configurations set their caps explicitly and ran with
    `ELOQ_IO_STATS` disabled, so the subsequent default restoration and
    opt-in timing cleanup do not change the exercised data path.
- **Outstanding no-regression guards**: pure-read throughput and pure-write
  throughput within noise (±3%) of the pre-QoS baseline at default caps — the
  hot-path cost is two counter checks per IO, so any regression indicates a
  wake storm or false blocking.
- **Outstanding sweeps**: `max_inflight_read` ∈ {64, 128, 256, 512},
  `bg_read_ratio` ∈
  {10, 25, 50}, `max_inflight_write` ∈ {256, 512, 1024},
  `max_write_concurrency` ∈ {1, 2, 4, 8}; record read tails + BG
  throughput; pick defaults at the knees.

### Documentation updates (same commits as code)

- `07-io-stack.md`: budgets, new UserDataTypes, acquire/release points.
- `04-execution-model.md`: budget blocking as a new task wait state,
  interaction with `WaitingZone`/`MaybeYield`.
- `08-data-lifecycle.md`: compaction/GC now run under the BG read budget.
- `io_qos.md`: mark mechanisms as implemented as they land.
