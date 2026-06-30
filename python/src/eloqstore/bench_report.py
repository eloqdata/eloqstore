#!/usr/bin/env python3
"""Benchmark report generator for EloqStore vLLM KV Cache integration.

Reads stats JSON files produced by ``benchmark_serving_multi_turn.py``
(via its ``--stats-json-output`` flag) and prints comparison tables.

Usage as a library::

    from eloqstore.bench_report import load_stats, compare_systems

    eloq = load_stats("eloq_stats.json")
    offload = load_stats("offload_stats.json")
    compare_systems({"EloqStore": eloq, "CPU Offloading": offload})

Usage as a script::

    python scripts/bench_report.py eloq_stats.json offload_stats.json

    python scripts/bench_report.py --passes cold.json warm.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _ms(v: float) -> str:
    return f"{v:.2f}ms"


def _reqs(v: float) -> str:
    return f"{v:.3f}"


def _pct(v: float) -> str:
    return f"{v:.1%}"


def _x(v: float) -> str:
    return f"{v:.2f}x"


# ---------------------------------------------------------------------------
# stats loading
# ---------------------------------------------------------------------------


def load_stats(path: str | Path) -> list[dict[str, Any]]:
    """Load per-request stats from a JSON file written by
    ``benchmark_serving_multi_turn.py --stats-json-output``."""
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected a list of request stats, got {type(data)}")
    return data


def _summary(requests: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute aggregate statistics for one run."""
    if not requests:
        return {}
    ttft = [r["ttft_ms"] for r in requests]
    latency = [r["latency_ms"] for r in requests]
    by_turn: dict[int, list[float]] = {}
    for r in requests:
        t = r.get("input_num_turns", 1)
        by_turn.setdefault(t, []).append(r["ttft_ms"])

    return {
        "count": len(requests),
        "ttft_mean": mean(ttft),
        "ttft_median": median(ttft),
        "ttft_std": stdev(ttft) if len(ttft) > 1 else 0.0,
        "latency_mean": mean(latency),
        "latency_median": median(latency),
        "by_turn": {t: {"ttft_mean": mean(v), "ttft_median": median(v), "n": len(v)}
                    for t, v in sorted(by_turn.items())},
    }


# ---------------------------------------------------------------------------
# comparison tables
# ---------------------------------------------------------------------------


def compare_passes(
    passes: dict[str, list[dict[str, Any]]],
    title: str = "Cold vs Warm Cache",
) -> None:
    """Compare cold vs warm runs of the same system.

    ``passes`` maps a pass label (e.g. ``"Cold"``, ``"Warm"``) to its
    request list.
    """
    summaries = {label: _summary(reqs) for label, reqs in passes.items()}

    sep = "=" * 75
    print(sep)
    print(f"  {title}")
    print(sep)

    # header
    labels = list(passes.keys())
    cols = ["Metric"] + [f"{l:>16}" for l in labels]
    if len(labels) == 2:
        cols.append("Speedup")
    print("  " + "  ".join(cols))
    print("  " + "  ".join(["-" * 20] + ["-" * 16] * len(labels) + (
        ["-" * 10] if len(labels) == 2 else [])))

    # body
    metrics = [
        ("requests count", "count", lambda v: f"{v}"),
        ("ttft mean", "ttft_mean", _ms),
        ("ttft median", "ttft_median", _ms),
        ("latency mean", "latency_mean", _ms),
        ("latency median", "latency_median", _ms),
    ]
    for name, key, fmt in metrics:
        vals = [summaries[l].get(key, 0) for l in labels]
        row = f"  {name:<20}" + "".join(f"  {fmt(v):>14}" for v in vals)
        if len(vals) == 2 and vals[0] and vals[1]:
            s = vals[1] / vals[0] if key.endswith("mean") else vals[0] / vals[1]
            row += f"  {_x(s):>8}"
        print(row)

    # per-turn
    print()
    for label in labels:
        s = summaries[label].get("by_turn", {})
        if not s:
            continue
        print(f"  {label} TTFT by turn:")
        for turn, info in s.items():
            print(f"    Turn {turn}: mean={_ms(info['ttft_mean'])}  "
                  f"median={_ms(info['ttft_median'])}  n={info['n']}")

    # speedup
    if len(labels) == 2:
        a, b = summaries[labels[0]], summaries[labels[1]]
        if a.get("ttft_mean") and b.get("ttft_mean"):
            s = a["ttft_mean"] / b["ttft_mean"]
            print(f"\n  Cache speedup ({labels[0]} -> {labels[1]}): {_x(s)}")
    print()


def compare_systems(
    systems: dict[str, list[dict[str, Any]]],
    title: str = "System Comparison",
) -> None:
    """Compare different systems (e.g. EloqStore vs CPU Offloading).

    ``systems`` maps a system name to its request list.
    """
    summaries = {name: _summary(reqs) for name, reqs in systems.items()}

    sep = "=" * 75
    print(sep)
    print(f"  {title}")
    print(sep)

    names = list(systems.keys())
    cols = ["Metric"] + [f"{n:>16}" for n in names] + ["Winner"]
    print("  " + "  ".join(cols))
    print("  " + "  ".join(["-" * 20] + ["-" * 16] * len(names) + ["-" * 15]))

    metrics = [
        ("requests count", "count", lambda v: f"{v}"),
        ("ttft mean", "ttft_mean", _ms),
        ("ttft median", "ttft_median", _ms),
        ("latency mean", "latency_mean", _ms),
        ("latency median", "latency_median", _ms),
    ]
    for name, key, fmt in metrics:
        vals = [summaries[n].get(key, 0) for n in names]
        rows = f"  {name:<20}" + "".join(f"  {fmt(v):>14}" for v in vals)
        # determine winner (lower = better for all these metrics)
        if len(vals) == 2 and vals[0] and vals[1]:
            winner_idx = 0 if vals[0] < vals[1] else 1
            ratio = max(vals) / min(vals)
            rows += f"  {names[winner_idx]} {_x(ratio):>8}"
        print(rows)

    # per-turn
    print()
    for name in names:
        s = summaries[name].get("by_turn", {})
        if not s:
            continue
        print(f"  {name} TTFT by turn:")
        for turn, info in s.items():
            print(f"    Turn {turn}: mean={_ms(info['ttft_mean'])}  "
                  f"median={_ms(info['ttft_median'])}  n={info['n']}")
    print()


def print_report(
    eloq_cold: list[dict[str, Any]] | None = None,
    eloq_warm: list[dict[str, Any]] | None = None,
    offload_cold: list[dict[str, Any]] | None = None,
    offload_warm: list[dict[str, Any]] | None = None,
) -> None:
    """Print a full, multi-section comparison report."""
    # Workload info
    if eloq_cold:
        r = eloq_cold[0]
        turns = r.get("input_num_turns", 1)
        tokens = r.get("input_num_tokens", 0)
        cached = r.get("approx_cached_percent", 0)
        print(f"Workload: {len(eloq_cold)} requests  "
              f"first turns ~{tokens} tokens  "
              f"cached ~{cached}%")

    print()

    # Cold comparison
    if eloq_cold and offload_cold:
        compare_systems(
            {"EloqStore": eloq_cold, "CPU Offloading": offload_cold},
            title="COLD CACHE (First Run)",
        )

    # Warm comparison
    if eloq_warm and offload_warm:
        compare_systems(
            {"EloqStore": eloq_warm, "CPU Offloading": offload_warm},
            title="WARM CACHE (Second Run)",
        )

    # EloqStore cold vs warm
    if eloq_cold and eloq_warm:
        compare_passes(
            {"Cold": eloq_cold, "Warm": eloq_warm},
            title="EloqStore Cold vs Warm",
        )

    # CPU offloading cold vs warm
    if offload_cold and offload_warm:
        compare_passes(
            {"Cold": offload_cold, "Warm": offload_warm},
            title="CPU Offloading Cold vs Warm",
        )

    # Key takeaways
    if eloq_cold and eloq_warm and offload_cold and offload_warm:
        e_cold = _summary(eloq_cold)
        e_warm = _summary(eloq_warm)
        o_cold = _summary(offload_cold)
        o_warm = _summary(offload_warm)

        es = e_cold["ttft_mean"] / e_warm["ttft_mean"] if e_warm["ttft_mean"] else 0
        os_ = o_cold["ttft_mean"] / o_warm["ttft_mean"] if o_warm["ttft_mean"] else 0

        print("=" * 75)
        print("  KEY TAKEAWAYS")
        print("=" * 75)
        print(f"  1. EloqStore cache speedup:  {_x(es)} ({_ms(e_cold['ttft_mean'])} -> {_ms(e_warm['ttft_mean'])})")
        print(f"  2. CPU offloading speedup:   {_x(os_)} ({_ms(o_cold['ttft_mean'])} -> {_ms(o_warm['ttft_mean'])})")
        print(f"  3. With warm cache: EloqStore is {'faster' if e_warm['ttft_mean'] < o_warm['ttft_mean'] else 'slower'} "
              f"({_ms(e_warm['ttft_mean'])} vs {_ms(o_warm['ttft_mean'])})")
        print(f"  4. Without cache: CPU offloading is {'faster' if o_cold['ttft_mean'] < e_cold['ttft_mean'] else 'slower'} "
              f"({_ms(o_cold['ttft_mean'])} vs {_ms(e_cold['ttft_mean'])})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate comparison reports from vLLM multi-turn benchmark stats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Compare two systems (cold runs)
  bench_report.py eloq_stats.json offload_stats.json

  # Compare cold vs warm passes for one system
  bench_report.py --passes cold.json warm.json

  # Full 4-way comparison
  bench_report.py --eloq-cold ec.json --eloq-warm ew.json \\
                   --offload-cold oc.json --offload-warm ow.json
""",
    )

    parser.add_argument("files", nargs="*", help="Stats JSON files to compare")
    parser.add_argument(
        "--passes", nargs="+",
        help="Compare passes (cold/warm) of the same system",
    )
    parser.add_argument("--eloq-cold", help="EloqStore cold run stats JSON")
    parser.add_argument("--eloq-warm", help="EloqStore warm run stats JSON")
    parser.add_argument("--offload-cold", help="CPU offloading cold run stats JSON")
    parser.add_argument("--offload-warm", help="CPU offloading warm run stats JSON")

    args = parser.parse_args()

    ec = load_stats(args.eloq_cold) if args.eloq_cold else None
    ew = load_stats(args.eloq_warm) if args.eloq_warm else None
    oc = load_stats(args.offload_cold) if args.offload_cold else None
    ow = load_stats(args.offload_warm) if args.offload_warm else None

    if args.passes:
        passes = {}
        for i, p in enumerate(args.passes):
            passes[f"Pass {i+1}"] = load_stats(p)
        compare_passes(passes)
    elif ec or ew or oc or ow:
        print_report(eloq_cold=ec, eloq_warm=ew, offload_cold=oc, offload_warm=ow)
    elif len(args.files) >= 2:
        systems = {}
        for i, f in enumerate(args.files):
            label = Path(f).stem
            systems[label] = load_stats(f)
        compare_systems(systems)
    elif len(args.files) == 1:
        data = load_stats(args.files[0])
        s = _summary(data)
        print(json.dumps(s, indent=2))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
