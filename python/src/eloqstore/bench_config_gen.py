#!/usr/bin/env python3
"""Generate synthetic vLLM multi-turn benchmark conversation files."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Preset:
    output_name: str
    conversations: int
    first_turn_repeats: int
    description: str


PRESETS: dict[str, Preset] = {
    "fit-5g": Preset(
        output_name="fit_5g_conversations.json",
        conversations=3,
        first_turn_repeats=600,
        description="~3.4 GB Qwen3-4B KV cache footprint with a 5 GB cache",
    ),
    "overflow-5g": Preset(
        output_name="overflow_5g_conversations.json",
        conversations=7,
        first_turn_repeats=600,
        description="~7.9 GB Qwen3-4B KV cache footprint with a 5 GB cache",
    ),
    "high-hit": Preset(
        output_name="high_hit_conversations.json",
        conversations=64,
        first_turn_repeats=92,
        description="many shorter conversations with an aggregate footprint above 5 GB",
    ),
}


def _conversation(conv_id: int, first_turn_repeats: int) -> dict[str, Any]:
    analysis = "This is a detailed analysis of the topic. "
    ack = f"Acknowledged. The analysis for conversation {conv_id} looks correct. "
    finding = f"The main finding for conv {conv_id} is as stated above. "

    return {
        "id": f"conv_{conv_id}",
        "messages": [
            {
                "role": "user",
                "content": f"Conversation {conv_id}: " + analysis * first_turn_repeats,
            },
            {
                "role": "assistant",
                "content": ack * 5,
            },
            {
                "role": "user",
                "content": f"What was the main finding in conversation {conv_id}?",
            },
            {
                "role": "assistant",
                "content": finding * 3,
            },
        ],
    }


def build_conversations(
    conversations: int,
    first_turn_repeats: int,
) -> list[dict[str, Any]]:
    if conversations <= 0:
        raise ValueError("conversations must be positive")
    if first_turn_repeats <= 0:
        raise ValueError("first-turn repeats must be positive")
    return [
        _conversation(conv_id, first_turn_repeats)
        for conv_id in range(conversations)
    ]


def write_preset(name: str, output_dir: Path) -> Path:
    preset = PRESETS[name]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / preset.output_name
    output_path.write_text(
        json.dumps(
            build_conversations(
                preset.conversations,
                preset.first_turn_repeats,
            ),
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic vLLM multi-turn benchmark inputs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("bench_configs"),
        help="Directory where generated JSON files are written.",
    )
    parser.add_argument(
        "--preset",
        choices=[*PRESETS.keys(), "all"],
        default="all",
        help="Benchmark preset to generate.",
    )
    args = parser.parse_args()

    names = PRESETS.keys() if args.preset == "all" else [args.preset]
    for name in names:
        path = write_preset(name, args.output_dir)
        preset = PRESETS[name]
        print(
            f"wrote {path} "
            f"({preset.conversations} conversations, {preset.description})",
        )


if __name__ == "__main__":
    main()
