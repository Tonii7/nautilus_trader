#!/usr/bin/env python3
"""One-command post-capture audit for MCR US30/YM Rithmic sessions.

Runs the existing deterministic MCR capture audit and adds:
- session metadata/summary display;
- event-to-init timing distribution;
- negative-delay count;
- per-stream event-time monotonicity;
- cross-stream arrival-order regressions.

This is transport/data-quality analysis, not an alpha test.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path

from examples.live.binance.mcr_microstructure.audit_capture import audit_capture


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _quantile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, math.ceil(len(ordered) * p) - 1)
    return ordered[idx]


def _latest_session(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(root)
    candidates = sorted(
        p for p in root.iterdir()
        if p.is_dir() and (p / "canonical_events.jsonl").exists()
    )
    if not candidates:
        raise FileNotFoundError(f"No capture sessions found under {root}")
    return candidates[-1]


def _timing_report(name: str, rows: list[dict[str, object]]) -> None:
    delays = [
        (int(x["ts_init_ns"]) - int(x["ts_event_ns"])) / 1e6
        for x in rows
        if x.get("ts_init_ns") is not None
    ]
    print(f"\n=== {name} EVENT-TO-INIT ===")
    print("rows       :", len(rows))
    print("timed_rows :", len(delays))
    if not delays:
        return
    print("negative   :", sum(x < 0 for x in delays))
    print("min_ms     :", round(min(delays), 3))
    print("p50_ms     :", round(statistics.median(delays), 3))
    print("p95_ms     :", round(_quantile(delays, 0.95), 3))
    print("p99_ms     :", round(_quantile(delays, 0.99), 3))
    print("max_ms     :", round(max(delays), 3))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", help="exact capture session directory")
    parser.add_argument(
        "--root",
        default="mcr_rithmic_us30_data",
        help="capture root used when --dir is omitted",
    )
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args()

    session = Path(args.dir) if args.dir else _latest_session(Path(args.root))
    print("Session:", session.resolve())

    metadata_path = session / "session_metadata.json"
    summary_path = session / "session_summary.json"

    if metadata_path.exists():
        print("\n=== SESSION METADATA ===")
        print(metadata_path.read_text(encoding="utf-8").strip())
    if summary_path.exists():
        print("\n=== SESSION SUMMARY ===")
        print(summary_path.read_text(encoding="utf-8").strip())

    counts = audit_capture(session, depth=args.depth)
    print("\n=== DETERMINISTIC AUDIT ===")
    print("MCR capture audit: PASS")
    for key, value in counts.items():
        print(f"{key}: {value}")

    derived = _load_jsonl(session / "derived_features.jsonl")
    books = [x for x in derived if x.get("kind") == "book_features"]
    trades = [x for x in derived if x.get("kind") == "trade_features"]

    _timing_report("BOOK", books)
    _timing_report("TRADE", trades)

    print("\n=== EVENT-TIME ORDER ===")
    for name, data in (("book", books), ("trade", trades)):
        regressions = 0
        max_ms = 0.0
        for prev, cur in zip(data, data[1:]):
            if int(cur["ts_event_ns"]) < int(prev["ts_event_ns"]):
                regressions += 1
                delta = (
                    int(prev["ts_event_ns"]) - int(cur["ts_event_ns"])
                ) / 1e6
                max_ms = max(max_ms, delta)
        print(f"{name}_internal_regressions:", regressions)
        print(f"{name}_max_regression_ms:", round(max_ms, 3))

    cross = 0
    cross_max = 0.0
    for prev, cur in zip(derived, derived[1:]):
        if int(cur["ts_event_ns"]) < int(prev["ts_event_ns"]):
            cross += 1
            delta = (
                int(prev["ts_event_ns"]) - int(cur["ts_event_ns"])
            ) / 1e6
            cross_max = max(cross_max, delta)

    print("arrival_adjacent_event_time_regressions:", cross)
    print("arrival_largest_regression_ms:", round(cross_max, 3))
    print("\n=== COMPLETE ===")


if __name__ == "__main__":
    main()
