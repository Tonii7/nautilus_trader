from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from examples.live.binance.mcr_microstructure.audit_capture import audit_capture
from examples.live.binance.mcr_microstructure.engine import BookEvent, Level, MicrostructureEngine, TradeEvent
from examples.live.binance.mcr_microstructure.recorder import JsonlRecorder
from examples.live.binance.mcr_microstructure.replay import replay_file


D = Decimal


def book(ts: int, bids, asks, seq: int) -> BookEvent:
    return BookEvent(
        instrument_id="BTCUSDT-PERP.BINANCE",
        ts_event_ns=ts,
        ts_init_ns=ts + 1,
        sequence=seq,
        bids=tuple(Level(p, s) for p, s in bids),
        asks=tuple(Level(p, s) for p, s in asks),
    )


class TestMicrostructureEngine(unittest.TestCase):
    def test_balanced_bbo_has_zero_imbalance_and_mid_microprice(self):
        engine = MicrostructureEngine(depth=2)
        row = engine.on_book(book(1, [(100, 5), (99, 3)], [(101, 5), (102, 3)], 1))
        self.assertEqual(row["bbo_imbalance"], "0")
        self.assertEqual(row["mid"], "100.5")
        self.assertEqual(row["microprice"], "100.5")
        self.assertEqual(row["depth_imbalance"], "0")
        self.assertIsNone(row["best_level_ofi"])

    def test_heavier_bid_shifts_microprice_up(self):
        engine = MicrostructureEngine(depth=1)
        row = engine.on_book(book(1, [(100, 9)], [(101, 1)], 1))
        self.assertEqual(row["bbo_imbalance"], "0.8")
        self.assertEqual(row["microprice"], "100.9")
        self.assertGreater(D(row["microprice"]), D(row["mid"]))

    def test_best_level_ofi_same_prices_uses_queue_size_changes(self):
        engine = MicrostructureEngine(depth=1)
        engine.on_book(book(1, [(100, 5)], [(101, 5)], 1))
        row = engine.on_book(book(2, [(100, 7)], [(101, 4)], 2))
        self.assertEqual(row["best_level_bid_ofi"], "2")
        self.assertEqual(row["best_level_ask_ofi"], "1")
        self.assertEqual(row["best_level_ofi"], "3")
        self.assertEqual(row["cumulative_best_level_ofi"], "3")

    def test_best_level_ofi_price_moves(self):
        engine = MicrostructureEngine(depth=1)
        engine.on_book(book(1, [(100, 5)], [(102, 4)], 1))
        row = engine.on_book(book(2, [(101, 6)], [(103, 3)], 2))
        # Bid improved: +6. Ask worsened upward: +previous ask size=+4.
        self.assertEqual(row["best_level_ofi"], "10")

    def test_depth_imbalance_n_levels(self):
        engine = MicrostructureEngine(depth=2)
        row = engine.on_book(book(1, [(100, 6), (99, 4)], [(101, 2), (102, 3)], 1))
        # (10 - 5) / (10 + 5) = 1/3
        self.assertEqual(D(row["depth_imbalance"]), D(1) / D(3))

    def test_mlofi_rankwise_vector_and_sum(self):
        engine = MicrostructureEngine(depth=2)
        engine.on_book(book(1, [(100, 5), (99, 4)], [(101, 5), (102, 4)], 1))
        row = engine.on_book(book(2, [(100, 6), (99, 2)], [(101, 3), (102, 5)], 2))
        # L1: +1 bid +2 ask = +3; L2: -2 bid -1 ask = -3.
        self.assertEqual(row["mlofi"], ["3", "-3"])
        self.assertEqual(row["mlofi_sum"], "0")
        self.assertEqual(row["cumulative_mlofi"], ["3", "-3"])

    def test_trade_delta_and_cumulative_delta(self):
        engine = MicrostructureEngine(depth=2)
        buy = engine.on_trade(TradeEvent("BTCUSDT-PERP.BINANCE", 1, 2, "a", 100, 2, "BUYER"))
        sell = engine.on_trade(TradeEvent("BTCUSDT-PERP.BINANCE", 3, 4, "b", 100, 0.5, "SELLER"))
        none = engine.on_trade(TradeEvent("BTCUSDT-PERP.BINANCE", 5, 6, "c", 100, 1, "NO_AGGRESSOR"))
        self.assertEqual(buy["trade_delta"], "2")
        self.assertEqual(sell["trade_delta"], "-0.5")
        self.assertEqual(sell["cumulative_trade_delta"], "1.5")
        self.assertEqual(none["trade_delta"], "0")
        self.assertEqual(none["cumulative_trade_delta"], "1.5")

    def test_rejects_crossed_book(self):
        with self.assertRaisesRegex(ValueError, "locked or crossed"):
            book(1, [(101, 1)], [(101, 1)], 1)

    def test_capture_audit_passes_for_deterministic_session(self):
        events = [
            book(1, [(100, 5), (99, 2)], [(101, 4), (102, 3)], 1),
            TradeEvent("BTCUSDT-PERP.BINANCE", 2, 3, "t1", 101, 0.25, "BUYER"),
            book(3, [(100, 7), (99, 2)], [(101, 3), (102, 3)], 2),
            TradeEvent("BTCUSDT-PERP.BINANCE", 4, 5, "t2", 100, 0.10, "SELLER"),
        ]
        engine = MicrostructureEngine(depth=2)
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with JsonlRecorder(td / "canonical_events.jsonl") as canonical, JsonlRecorder(td / "derived_features.jsonl") as derived:
                for event in events:
                    canonical.write(event.to_dict())
                    if isinstance(event, BookEvent):
                        derived.write(engine.on_book(event))
                    else:
                        derived.write(engine.on_trade(event))
            counts = audit_capture(td, depth=2)
        self.assertEqual(counts["canonical_rows"], 4)
        self.assertEqual(counts["book_rows"], 2)
        self.assertEqual(counts["trade_rows"], 2)

    def test_replay_is_deterministic_against_live_engine(self):
        events = [
            book(1, [(100, 5), (99, 2)], [(101, 4), (102, 3)], 1),
            TradeEvent("BTCUSDT-PERP.BINANCE", 2, 3, "t1", 101, 0.25, "BUYER"),
            book(3, [(100, 7), (99, 2)], [(101, 3), (102, 3)], 2),
            TradeEvent("BTCUSDT-PERP.BINANCE", 4, 5, "t2", 100, 0.10, "SELLER"),
        ]

        live_engine = MicrostructureEngine(depth=2)
        expected = []
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            canonical = td / "canonical.jsonl"
            replayed = td / "replayed.jsonl"
            with JsonlRecorder(canonical) as rec:
                for event in events:
                    rec.write(event.to_dict())
                    if isinstance(event, BookEvent):
                        expected.append(live_engine.on_book(event))
                    else:
                        expected.append(live_engine.on_trade(event))

            count = replay_file(canonical, replayed, depth=2)
            actual = [json.loads(line) for line in replayed.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(count, len(events))
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
