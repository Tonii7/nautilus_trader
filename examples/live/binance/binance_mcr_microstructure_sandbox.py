#!/usr/bin/env python3
"""Public-data BTCUSDT-PERP microstructure sandbox for MCR/Nautilus.

No API key, secret, execution client or order submission is used.

Pipeline:
    Binance USDT perpetual public feed
      -> Nautilus Binance adapter
      -> managed L2_MBP order book + TradeTick
      -> venue-neutral canonical events
      -> MCR microstructure engine
      -> canonical_events.jsonl + derived_features.jsonl

Environment variables:
    MCR_BINANCE_INSTRUMENT   default BTCUSDT-PERP.BINANCE
    MCR_BINANCE_DEPTH        default 10
    MCR_BINANCE_OUTPUT_DIR   default mcr_binance_sandbox_data
    MCR_BINANCE_LOG_EVERY    default 100
"""

from __future__ import annotations

import os
from pathlib import Path

from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceDataClientFactory
from nautilus_trader.adapters.binance import BinanceEnvironment
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.common import Environment
from nautilus_trader.config import StrategyConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BookType, InstrumentId, OrderBookDeltas, TradeTick
from nautilus_trader.model import TraderId
from nautilus_trader.trading import Strategy

from examples.live.binance.mcr_microstructure.engine import BookEvent, Level, MicrostructureEngine, TradeEvent
from examples.live.binance.mcr_microstructure.recorder import JsonlRecorder


class McrMicrostructureConfig(StrategyConfig):
    _CUSTOM_FIELDS = ("instrument_id", "depth", "output_dir", "log_every")

    def __new__(cls, *args, **kwargs):
        for key in cls._CUSTOM_FIELDS:
            kwargs.pop(key, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        instrument_id: str,
        depth: int = 10,
        output_dir: str = "mcr_binance_sandbox_data",
        log_every: int = 100,
        **kwargs,
    ) -> None:
        super().__init__()
        self.instrument_id = instrument_id
        self.depth = int(depth)
        self.output_dir = output_dir
        self.log_every = max(int(log_every), 1)


class McrMicrostructureStrategy(Strategy):
    def __init__(self, config: McrMicrostructureConfig) -> None:
        super().__init__(config)
        self._instrument_id = InstrumentId.from_str(config.instrument_id)
        self._depth = int(config.depth)
        self._engine = MicrostructureEngine(depth=self._depth)
        output = Path(config.output_dir)
        self._canonical = JsonlRecorder(output / "canonical_events.jsonl")
        self._derived = JsonlRecorder(output / "derived_features.jsonl")
        self._log_every = int(config.log_every)
        self._book_rows = 0
        self._trade_rows = 0
        self._invalid_book_rows = 0

    @staticmethod
    def _int_or_none(value) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def on_start(self) -> None:
        instrument = self.cache.instrument(self._instrument_id)
        if instrument is None:
            self.log.error(f"Instrument not loaded: {self._instrument_id}")
            self.stop()
            return

        self.log.info(
            "MCR Binance sandbox started: "
            f"instrument={self._instrument_id}, depth={self._depth}, data-only=True"
        )
        self.subscribe_book_deltas(self._instrument_id, BookType.L2_MBP, managed=True)
        self.subscribe_trades(self._instrument_id)

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        book = self.cache.order_book(self._instrument_id)
        if book is None:
            return

        bid_map = book.bids_to_dict(self._depth)
        ask_map = book.asks_to_dict(self._depth)
        if not bid_map or not ask_map:
            return

        try:
            event = BookEvent(
                instrument_id=str(self._instrument_id),
                ts_event_ns=int(deltas.ts_event),
                ts_init_ns=int(deltas.ts_init),
                sequence=int(deltas.sequence),
                bids=tuple(Level(price, size) for price, size in bid_map.items()),
                asks=tuple(Level(price, size) for price, size in ask_map.items()),
            )
            feature = self._engine.on_book(event)
        except ValueError as exc:
            self._invalid_book_rows += 1
            if self._invalid_book_rows == 1 or self._invalid_book_rows % self._log_every == 0:
                self.log.warning(
                    f"Skipped invalid managed-book state #{self._invalid_book_rows}: {exc}"
                )
            return

        self._canonical.write(event.to_dict())
        self._derived.write(feature)
        self._book_rows += 1

        if self._book_rows % self._log_every == 0:
            self.log.info(
                "MCR book "
                f"rows={self._book_rows} "
                f"bbo_imbalance={feature['bbo_imbalance']} "
                f"depth_imbalance={feature['depth_imbalance']} "
                f"ofi={feature['best_level_ofi']} "
                f"mlofi_sum={feature['mlofi_sum']}"
            )

    def on_trade(self, trade: TradeTick) -> None:
        event = TradeEvent(
            instrument_id=str(trade.instrument_id),
            ts_event_ns=int(trade.ts_event),
            ts_init_ns=self._int_or_none(getattr(trade, "ts_init", None)),
            trade_id=str(trade.trade_id),
            price=trade.price.as_decimal(),
            size=trade.size.as_decimal(),
            aggressor_side=str(trade.aggressor_side),
        )
        feature = self._engine.on_trade(event)
        self._canonical.write(event.to_dict())
        self._derived.write(feature)
        self._trade_rows += 1

        if self._trade_rows % self._log_every == 0:
            self.log.info(
                "MCR trades "
                f"rows={self._trade_rows} "
                f"trade_delta={feature['trade_delta']} "
                f"cum_delta={feature['cumulative_trade_delta']}"
            )

    def on_stop(self) -> None:
        try:
            self.unsubscribe_book_deltas(self._instrument_id)
            self.unsubscribe_trades(self._instrument_id)
        finally:
            self._canonical.close()
            self._derived.close()
            self.log.info(
                "MCR Binance sandbox stopped: "
                f"book_rows={self._book_rows}, trade_rows={self._trade_rows}, "
                f"invalid_book_rows={self._invalid_book_rows}"
            )


INSTRUMENT_ID = os.getenv("MCR_BINANCE_INSTRUMENT", "BTCUSDT-PERP.BINANCE")
DEPTH = int(os.getenv("MCR_BINANCE_DEPTH", "10"))
OUTPUT_DIR = os.getenv("MCR_BINANCE_OUTPUT_DIR", "mcr_binance_sandbox_data")
LOG_EVERY = int(os.getenv("MCR_BINANCE_LOG_EVERY", "100"))

instrument_id = InstrumentId.from_str(INSTRUMENT_ID)

builder = (
    LiveNode.builder(
        "MCR-BINANCE-SANDBOX-001",
        TraderId.from_str("MCR-BINANCE-SANDBOX-001"),
        Environment.LIVE,
    )
    .with_reconciliation(False)
    .with_timeout_connection(20)
    .with_timeout_disconnection_secs(10)
    .with_delay_post_stop_secs(1)
    .add_data_client(
        None,
        BinanceDataClientFactory(),
        BinanceDataClientConfig(
            product_type=BinanceProductType.USD_M,
            environment=BinanceEnvironment.LIVE,
            instrument_provider=BinanceInstrumentProviderConfig(
                load_all=False,
                load_ids=[str(instrument_id)],
            ),
        ),
    )
)

node = builder.build()
strategy = McrMicrostructureStrategy(
    McrMicrostructureConfig(
        instrument_id=str(instrument_id),
        depth=DEPTH,
        output_dir=OUTPUT_DIR,
        log_every=LOG_EVERY,
    )
)
node.add_strategy(strategy)

if __name__ == "__main__":
    print("MCR Binance microstructure sandbox")
    print(f"Instrument : {INSTRUMENT_ID}")
    print(f"Depth      : {DEPTH}")
    print(f"Output     : {Path(OUTPUT_DIR).resolve()}")
    print("Execution  : DISABLED")
    print("Credentials: NOT REQUIRED")
    print("Press CTRL+C to stop.\n")
    try:
        node.run()
    finally:
        node.dispose()
