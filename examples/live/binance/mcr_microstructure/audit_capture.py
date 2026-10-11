#!/usr/bin/env python3
"""Audit a captured MCR Binance sandbox session.

Checks structural invariants only.  Passing this audit does not imply predictive edge.
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from examples.live.binance.mcr_microstructure.replay import replay_file


D = Decimal


def _load(path: Path) -> list[dict[str, object]]:
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON at {path}:{line_no}: {exc}") from exc
    return rows


def _assert_monotonic(rows: list[dict[str, object]], kind: str) -> None:
    timestamps = [int(r["ts_event_ns"]) for r in rows if r.get("kind") == kind]
    if any(curr < prev for prev, curr in zip(timestamps, timestamps[1:])):
        raise AssertionError(f"{kind} ts_event_ns is not non-decreasing")


def audit_capture(directory: str | Path, *, depth: int = 10) -> dict[str, int]:
    directory = Path(directory)
    canonical_path = directory / "canonical_events.jsonl"
    derived_path = directory / "derived_features.jsonl"
    if not canonical_path.exists():
        raise FileNotFoundError(canonical_path)
    if not derived_path.exists():
        raise FileNotFoundError(derived_path)

    canonical = _load(canonical_path)
    derived = _load(derived_path)
    if len(canonical) != len(derived):
        raise AssertionError(
            f"row count mismatch: canonical={len(canonical)}, derived={len(derived)}"
        )

    book_features = [r for r in derived if r.get("kind") == "book_features"]
    trade_features = [r for r in derived if r.get("kind") == "trade_features"]
    if not book_features:
        raise AssertionError("no book feature rows captured")
    if not trade_features:
        raise AssertionError("no trade feature rows captured")

    for i, row in enumerate(book_features, start=1):
        for key in ("bbo_imbalance", "depth_imbalance"):
            value = row.get(key)
            if value is None:
                continue
            value_d = D(str(value))
            if value_d < D("-1") or value_d > D("1"):
                raise AssertionError(f"{key} out of range at book row {i}: {value}")
        if D(str(row["spread"])) <= 0:
            raise AssertionError(f"non-positive spread at book row {i}: {row['spread']}")

    _assert_monotonic(canonical, "book")
    _assert_monotonic(canonical, "trade")

    replayed_path = directory / "derived_features.replayed.audit.jsonl"
    if replayed_path.exists():
        replayed_path.unlink()
    replay_file(canonical_path, replayed_path, depth=depth)
    replayed = _load(replayed_path)
    if replayed != derived:
        for idx, (a, b) in enumerate(zip(replayed, derived), start=1):
            if a != b:
                raise AssertionError(f"replay mismatch at row {idx}: replayed={a} live={b}")
        raise AssertionError("replay mismatch: unequal output length")

    return {
        "canonical_rows": len(canonical),
        "derived_rows": len(derived),
        "book_rows": len(book_features),
        "trade_rows": len(trade_features),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", required=True, help="sandbox capture directory")
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args()
    counts = audit_capture(args.dir, depth=args.depth)
    print("MCR capture audit: PASS")
    for key, value in counts.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
