#!/usr/bin/env python3
"""MCR US30/YM microstructure capture through the Nautilus Rithmic adapter.

Purpose
-------
Capture exchange-traded E-mini Dow (YM) market microstructure as the primary
US30 research feed. This is a DATA-ONLY research collector: no execution client
is created and no order submission path is present.

Pipeline
--------
Rithmic / CBOT YM
    -> Nautilus Rithmic data client
    -> L2_MBP OrderBookDeltas + TradeTick
    -> venue-neutral BookEvent / TradeEvent
    -> existing MCR MicrostructureEngine v1
    -> canonical_events.jsonl + derived_features.jsonl

Environment variables
---------------------
MCR_RITHMIC_PROFILE        optional Rithmic profile name
MCR_RITHMIC_ENV_FILE       optional path to Rithmic .env; defaults to repo .env
MCR_RITHMIC_PRODUCT        default YM
MCR_RITHMIC_EXCHANGE       default CBOT
MCR_RITHMIC_INSTRUMENT     optional exact Nautilus InstrumentId override
MCR_RITHMIC_DEPTH          default 10
MCR_RITHMIC_LOG_EVERY      default 100
MCR_RITHMIC_RUN_SECONDS    default 0 (run until CTRL+C)
MCR_RITHMIC_OUTPUT_DIR     optional exact output directory

Important
---------
YM is the exchange-traded E-mini Dow futures contract used as the US30 research
instrument. It is not the IC Markets US30 CFD. The front-month contract is
resolved dynamically unless an exact instrument override is provided.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nautilus_trader._libnautilus.common import Environment, LogColor
from nautilus_trader._libnautilus.model import BookType, ClientId, OrderBook, StrategyId
from nautilus_trader._libnautilus.model import InstrumentId as PyInstrumentId
from nautilus_trader._libnautilus.trading import Strategy, StrategyConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId

from examples.live.binance.mcr_microstructure.engine import (
    BookEvent,
    Level,
    MicrostructureEngine,
    TradeEvent,
)
from examples.live.binance.mcr_microstructure.recorder import JsonlRecorder
from examples.live.rithmic.rithmic_live_node_helpers import (
    TRADER_ID,
    RithmicDataClientFactory,
    build_data_client_config,
    build_data_client_id,
    load_rithmic_env_file,
    resolve_instrument_id,
    schedule_stop,
)


PRODUCT_CODE = os.getenv("MCR_RITHMIC_PRODUCT", "YM").strip().upper()
EXCHANGE = os.getenv("MCR_RITHMIC_EXCHANGE", "CBOT").strip().upper()
PROFILE = os.getenv("MCR_RITHMIC_PROFILE") or None
ENV_FILE = os.getenv("MCR_RITHMIC_ENV_FILE") or None
DEPTH = int(os.getenv("MCR_RITHMIC_DEPTH", "10"))
LOG_EVERY = max(int(os.getenv("MCR_RITHMIC_LOG_EVERY", "100")), 1)
RUN_SECONDS = int(os.getenv("MCR_RITHMIC_RUN_SECONDS", "0"))
INSTRUMENT_OVERRIDE = os.getenv("MCR_RITHMIC_INSTRUMENT") or None
OUTPUT_OVERRIDE = os.getenv("MCR_RITHMIC_OUTPUT_DIR") or None


def _default_output_dir() -> Path:
    session = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("mcr_rithmic_us30_data") / session


def _coerce_instrument_id(value: str | PyInstrumentId) -> PyInstrumentId:
    if isinstance(value, PyInstrumentId):
        return value
    return PyInstrumentId.from_str(value)


def _coerce_client_id(value: str | ClientId) -> ClientId:
    if isinstance(value, ClientId):
        return value
    return ClientId(value)


def _coerce_strategy_id(value: str | StrategyId) -> StrategyId:
    if isinstance(value, StrategyId):
        return value
    return StrategyId(value)


def _as_levels(mapping, *, bids: bool) -> tuple[Level, ...]:
    """Normalize a Nautilus order-book mapping into strict price order."""
    ordered = sorted(
        mapping.items(),
        key=lambda item: Decimal(str(item[0])),
        reverse=bids,
    )
    return tuple(Level(str(price), str(size)) for price, size in ordered)


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class RithmicMcrUs30Config(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | PyInstrumentId,
        data_client_id: str | ClientId,
        output_dir: str,
        depth: int = 10,
        log_every: int = 100,
        strategy_id: str | StrategyId = "MCR-US30-RITHMIC-001",
        log_events: bool = True,
        log_commands: bool = False,
    ):
        parsed_instrument_id = _coerce_instrument_id(instrument_id)
        parsed_strategy_id = _coerce_strategy_id(strategy_id)

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=False,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.data_client_id = _coerce_client_id(data_client_id)
        config.output_dir = str(output_dir)
        config.depth = int(depth)
        config.log_every = max(int(log_every), 1)
        return config


class RithmicMcrUs30Strategy(Strategy):
    def __init__(self, config: RithmicMcrUs30Config):
        super().__init__(config)
        self.config = config
        self._book = OrderBook(config.instrument_id, BookType.L2_MBP)
        self._engine = MicrostructureEngine(depth=config.depth)

        output = Path(config.output_dir)
        output.mkdir(parents=True, exist_ok=True)
        self._output = output
        self._canonical = JsonlRecorder(output / "canonical_events.jsonl")
        self._derived = JsonlRecorder(output / "derived_features.jsonl")

        self._book_rows = 0
        self._trade_rows = 0
        self._invalid_book_rows = 0
        self._instrument_rows = 0

    def on_start(self):
        self.log.info(
            "MCR US30/Rithmic capture started: "
            f"instrument={self.config.instrument_id}, depth={self.config.depth}, "
            "book_type=L2_MBP, data-only=True",
            LogColor.CYAN,
        )

        self.request_instrument(
            instrument_id=self.config.instrument_id,
            client_id=self.config.data_client_id,
        )
        self.subscribe_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
            depth=self.config.depth,
            client_id=self.config.data_client_id,
        )
        self.subscribe_trades(
            instrument_id=self.config.instrument_id,
            client_id=self.config.data_client_id,
        )

    def on_instrument(self, instrument):
        if instrument.id != self.config.instrument_id:
            return
        self._instrument_rows += 1
        self.log.info(f"YM instrument ready: {instrument.id}", LogColor.CYAN)

    def on_book_deltas(self, deltas):
        self._book.apply_deltas(deltas)

        bid_map = self._book.bids_to_dict(self.config.depth)
        ask_map = self._book.asks_to_dict(self.config.depth)
        if not bid_map or not ask_map:
            return

        try:
            event = BookEvent(
                instrument_id=str(self.config.instrument_id),
                ts_event_ns=int(deltas.ts_event),
                ts_init_ns=_int_or_none(getattr(deltas, "ts_init", None)),
                sequence=_int_or_none(getattr(deltas, "sequence", None)),
                bids=_as_levels(bid_map, bids=True),
                asks=_as_levels(ask_map, bids=False),
            )
            feature = self._engine.on_book(event)
        except ValueError as exc:
            self._invalid_book_rows += 1
            if (
                self._invalid_book_rows == 1
                or self._invalid_book_rows % self.config.log_every == 0
            ):
                self.log.warning(
                    "Skipped invalid YM managed-book state "
                    f"#{self._invalid_book_rows}: {exc}"
                )
            return

        self._canonical.write(event.to_dict())
        self._derived.write(feature)
        self._book_rows += 1

        if self._book_rows % self.config.log_every == 0:
            self.log.info(
                "MCR YM book "
                f"rows={self._book_rows} "
                f"bbo_imbalance={feature['bbo_imbalance']} "
                f"depth_imbalance={feature['depth_imbalance']} "
                f"ofi={feature['best_level_ofi']} "
                f"mlofi_sum={feature['mlofi_sum']}"
            )

    def on_trade(self, trade):
        event = TradeEvent(
            instrument_id=str(trade.instrument_id),
            ts_event_ns=int(trade.ts_event),
            ts_init_ns=_int_or_none(getattr(trade, "ts_init", None)),
            trade_id=str(trade.trade_id),
            price=trade.price.as_decimal(),
            size=trade.size.as_decimal(),
            aggressor_side=str(trade.aggressor_side),
        )
        feature = self._engine.on_trade(event)
        self._canonical.write(event.to_dict())
        self._derived.write(feature)
        self._trade_rows += 1

        if self._trade_rows % self.config.log_every == 0:
            self.log.info(
                "MCR YM trades "
                f"rows={self._trade_rows} "
                f"trade_delta={feature['trade_delta']} "
                f"cum_delta={feature['cumulative_trade_delta']}"
            )

    def on_stop(self):
        try:
            self.unsubscribe_book_deltas(
                instrument_id=self.config.instrument_id,
                client_id=self.config.data_client_id,
            )
            self.unsubscribe_trades(
                instrument_id=self.config.instrument_id,
                client_id=self.config.data_client_id,
            )
        finally:
            self._canonical.close()
            self._derived.close()

            summary = {
                "schema_version": "MCR_RITHMIC_US30_SESSION_V1",
                "instrument_id": str(self.config.instrument_id),
                "depth": int(self.config.depth),
                "book_type": "L2_MBP",
                "book_rows": self._book_rows,
                "trade_rows": self._trade_rows,
                "invalid_book_rows": self._invalid_book_rows,
                "instrument_rows": self._instrument_rows,
                "execution_enabled": False,
            }
            (self._output / "session_summary.json").write_text(
                json.dumps(summary, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.log.info(
                "MCR US30/Rithmic capture stopped: "
                f"book_rows={self._book_rows}, trade_rows={self._trade_rows}, "
                f"invalid_book_rows={self._invalid_book_rows}"
            )


def main() -> None:
    if DEPTH <= 0:
        raise ValueError("MCR_RITHMIC_DEPTH must be positive")
    if RUN_SECONDS < 0:
        raise ValueError("MCR_RITHMIC_RUN_SECONDS cannot be negative")

    load_rithmic_env_file(ENV_FILE)

    explicit_id = (
        InstrumentId.from_str(INSTRUMENT_OVERRIDE)
        if INSTRUMENT_OVERRIDE
        else None
    )
    instrument_id = resolve_instrument_id(
        PROFILE,
        explicit_id,
        PRODUCT_CODE,
        EXCHANGE,
    )
    data_client_id = build_data_client_id(PROFILE)
    output_dir = Path(OUTPUT_OVERRIDE) if OUTPUT_OVERRIDE else _default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "schema_version": "MCR_RITHMIC_US30_SESSION_V1",
        "source": "RITHMIC",
        "research_market": "US30",
        "exchange_traded_instrument": "E-mini Dow futures (YM)",
        "product_code": PRODUCT_CODE,
        "exchange": EXCHANGE,
        "resolved_instrument_id": instrument_id.value,
        "depth": DEPTH,
        "book_type": "L2_MBP",
        "execution_enabled": False,
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }
    (output_dir / "session_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    node = (
        LiveNode.builder("MCR-US30-RITHMIC-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(False)
        .with_timeout_connection(20)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(1)
        .add_data_client(
            data_client_id,
            RithmicDataClientFactory(),
            build_data_client_config(PROFILE),
        )
        .build()
    )

    strategy = RithmicMcrUs30Strategy(
        RithmicMcrUs30Config(
            instrument_id=instrument_id.value,
            data_client_id=data_client_id,
            output_dir=str(output_dir),
            depth=DEPTH,
            log_every=LOG_EVERY,
        )
    )
    node.add_strategy(strategy)

    print("MCR US30 / Rithmic microstructure capture")
    print(f"Product       : {PRODUCT_CODE}")
    print(f"Exchange      : {EXCHANGE}")
    print(f"Instrument    : {instrument_id}")
    print(f"Data client   : {data_client_id}")
    print(f"Depth         : {DEPTH}")
    print(f"Output        : {output_dir.resolve()}")
    print("Book type     : L2_MBP")
    print("Execution     : DISABLED")
    print(f"Run seconds   : {RUN_SECONDS or 'until CTRL+C'}")
    print("Press CTRL+C to stop.\n")

    timer = None
    try:
        timer = schedule_stop(node, RUN_SECONDS)
        node.run()
    finally:
        if timer is not None:
            timer.cancel()
        node.dispose()


if __name__ == "__main__":
    main()
