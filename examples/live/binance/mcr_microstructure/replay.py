#!/usr/bin/env python3
"""Replay canonical MCR microstructure events through the exact live feature engine."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from examples.live.binance.mcr_microstructure.engine import BookEvent, MicrostructureEngine, TradeEvent
from examples.live.binance.mcr_microstructure.recorder import JsonlRecorder


def replay_file(input_path: str | Path, output_path: str | Path, *, depth: int = 10) -> int:
    engine = MicrostructureEngine(depth=depth)
    count = 0
    with Path(input_path).open("r", encoding="utf-8") as source, JsonlRecorder(output_path) as sink:
        for line_no, line in enumerate(source, start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            kind = row.get("kind")
            try:
                if kind == "book":
                    feature = engine.on_book(BookEvent.from_dict(row))
                elif kind == "trade":
                    feature = engine.on_trade(TradeEvent.from_dict(row))
                else:
                    raise ValueError(f"unsupported kind={kind!r}")
            except Exception as exc:
                raise ValueError(f"invalid event at line {line_no}: {exc}") from exc
            sink.write(feature)
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="canonical_events.jsonl")
    parser.add_argument("--output", required=True, help="derived_features.replayed.jsonl")
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args()
    count = replay_file(args.input, args.output, depth=args.depth)
    print(f"Replayed {count} canonical events -> {args.output}")


if __name__ == "__main__":
    main()
