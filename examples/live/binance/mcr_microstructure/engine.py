"""Deterministic, venue-neutral microstructure feature engine.

Version: MCR-BINANCE-SANDBOX-V1

Scientific contract
-------------------
Inputs are canonical market observations only.  No trade signal, threshold or
future price information enters this module.  All calculations are causal and
use current/previous observations only.

Implemented primitives:
- BBO imbalance
- microprice
- N-level depth imbalance
- Best-Level OFI (Cont-style event increment)
- rank-wise Multi-Level OFI (MLOFI-v1)
- signed trade delta and cumulative trade delta

Decimal arithmetic is used throughout so a captured event stream can be replayed
without floating-point serialization drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

_ZERO = Decimal("0")
_TWO = Decimal("2")


def _d(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class Level:
    price: Decimal
    size: Decimal

    def __init__(self, price: Decimal | str | int | float, size: Decimal | str | int | float):
        object.__setattr__(self, "price", _d(price))
        object.__setattr__(self, "size", _d(size))
        if self.size < _ZERO:
            raise ValueError("level size cannot be negative")

    def to_dict(self) -> dict[str, str]:
        return {"price": str(self.price), "size": str(self.size)}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "Level":
        return cls(price=str(data["price"]), size=str(data["size"]))


@dataclass(frozen=True, slots=True)
class BookEvent:
    instrument_id: str
    ts_event_ns: int
    ts_init_ns: int | None
    sequence: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    def __post_init__(self) -> None:
        if not self.instrument_id:
            raise ValueError("instrument_id is required")
        if self.ts_event_ns < 0:
            raise ValueError("ts_event_ns cannot be negative")
        if not self.bids or not self.asks:
            raise ValueError("both bid and ask depth are required")
        if any(a.price <= b.price for a, b in zip(self.bids, self.bids[1:])):
            raise ValueError("bids must be strictly descending by price")
        if any(a.price >= b.price for a, b in zip(self.asks, self.asks[1:])):
            raise ValueError("asks must be strictly ascending by price")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("book is locked or crossed")

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "book",
            "instrument_id": self.instrument_id,
            "ts_event_ns": self.ts_event_ns,
            "ts_init_ns": self.ts_init_ns,
            "sequence": self.sequence,
            "bids": [x.to_dict() for x in self.bids],
            "asks": [x.to_dict() for x in self.asks],
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "BookEvent":
        bids = data.get("bids")
        asks = data.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            raise ValueError("book event requires bids and asks arrays")
        return cls(
            instrument_id=str(data["instrument_id"]),
            ts_event_ns=int(data["ts_event_ns"]),
            ts_init_ns=None if data.get("ts_init_ns") is None else int(data["ts_init_ns"]),
            sequence=None if data.get("sequence") is None else int(data["sequence"]),
            bids=tuple(Level.from_dict(x) for x in bids),
            asks=tuple(Level.from_dict(x) for x in asks),
        )


@dataclass(frozen=True, slots=True)
class TradeEvent:
    instrument_id: str
    ts_event_ns: int
    ts_init_ns: int | None
    trade_id: str
    price: Decimal
    size: Decimal
    aggressor_side: str

    def __init__(
        self,
        instrument_id: str,
        ts_event_ns: int,
        ts_init_ns: int | None,
        trade_id: str,
        price: Decimal | str | int | float,
        size: Decimal | str | int | float,
        aggressor_side: str,
    ):
        side = aggressor_side.strip().upper()
        if side not in {"BUYER", "SELLER", "NO_AGGRESSOR"}:
            raise ValueError(f"unsupported aggressor_side={aggressor_side!r}")
        price_d = _d(price)
        size_d = _d(size)
        if size_d <= _ZERO:
            raise ValueError("trade size must be positive")
        if ts_event_ns < 0:
            raise ValueError("ts_event_ns cannot be negative")
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "ts_event_ns", int(ts_event_ns))
        object.__setattr__(self, "ts_init_ns", None if ts_init_ns is None else int(ts_init_ns))
        object.__setattr__(self, "trade_id", trade_id)
        object.__setattr__(self, "price", price_d)
        object.__setattr__(self, "size", size_d)
        object.__setattr__(self, "aggressor_side", side)

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "trade",
            "instrument_id": self.instrument_id,
            "ts_event_ns": self.ts_event_ns,
            "ts_init_ns": self.ts_init_ns,
            "trade_id": self.trade_id,
            "price": str(self.price),
            "size": str(self.size),
            "aggressor_side": self.aggressor_side,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "TradeEvent":
        return cls(
            instrument_id=str(data["instrument_id"]),
            ts_event_ns=int(data["ts_event_ns"]),
            ts_init_ns=None if data.get("ts_init_ns") is None else int(data["ts_init_ns"]),
            trade_id=str(data["trade_id"]),
            price=str(data["price"]),
            size=str(data["size"]),
            aggressor_side=str(data["aggressor_side"]),
        )


def _imbalance(bid_size: Decimal, ask_size: Decimal) -> Decimal | None:
    total = bid_size + ask_size
    if total <= _ZERO:
        return None
    return (bid_size - ask_size) / total


def _best_level_ofi(prev: Level, curr: Level, *, side: str) -> Decimal:
    """Return one-side OFI contribution, positive = buy pressure.

    For bids:
      price improves -> +current size
      same price     -> current size - previous size
      price worsens  -> -previous size

    For asks, the sign convention is inverted so lower asks / shrinking ask size
    produce positive pressure and higher asks / growing ask size produce negative.
    """

    if side == "bid":
        if curr.price > prev.price:
            return curr.size
        if curr.price == prev.price:
            return curr.size - prev.size
        return -prev.size

    if side == "ask":
        if curr.price < prev.price:
            return -curr.size
        if curr.price == prev.price:
            return prev.size - curr.size
        return prev.size

    raise ValueError(f"unknown side={side!r}")


def _rankwise_mlofi(
    prev_levels: Sequence[Level],
    curr_levels: Sequence[Level],
    *,
    side: str,
    depth: int,
) -> tuple[Decimal | None, ...]:
    values: list[Decimal | None] = []
    for i in range(depth):
        if i >= len(prev_levels) or i >= len(curr_levels):
            values.append(None)
            continue
        values.append(_best_level_ofi(prev_levels[i], curr_levels[i], side=side))
    return tuple(values)


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _sum_present(values: Iterable[Decimal | None]) -> Decimal | None:
    present = [x for x in values if x is not None]
    if not present:
        return None
    return sum(present, _ZERO)


class MicrostructureEngine:
    """Causal state machine for a single instrument."""

    SCHEMA_VERSION = "MCR_MICROSTRUCTURE_V1"
    MLOFI_DEFINITION = "RANKWISE_PIECEWISE_OFI_V1"

    def __init__(self, depth: int = 10):
        if depth <= 0:
            raise ValueError("depth must be positive")
        self.depth = int(depth)
        self._prev_book: BookEvent | None = None
        self._cumulative_trade_delta = _ZERO
        self._cumulative_best_level_ofi = _ZERO
        self._cumulative_mlofi = [_ZERO for _ in range(self.depth)]
        self._book_events = 0
        self._trade_events = 0

    def reset(self) -> None:
        self._prev_book = None
        self._cumulative_trade_delta = _ZERO
        self._cumulative_best_level_ofi = _ZERO
        self._cumulative_mlofi = [_ZERO for _ in range(self.depth)]
        self._book_events = 0
        self._trade_events = 0

    def on_book(self, event: BookEvent) -> dict[str, object]:
        self._book_events += 1
        bid0 = event.bids[0]
        ask0 = event.asks[0]
        spread = ask0.price - bid0.price
        mid = (bid0.price + ask0.price) / _TWO
        bbo_imbalance = _imbalance(bid0.size, ask0.size)

        bbo_total = bid0.size + ask0.size
        microprice = None
        if bbo_total > _ZERO:
            microprice = (ask0.price * bid0.size + bid0.price * ask0.size) / bbo_total

        bid_depth = sum((x.size for x in event.bids[: self.depth]), _ZERO)
        ask_depth = sum((x.size for x in event.asks[: self.depth]), _ZERO)
        depth_imbalance = _imbalance(bid_depth, ask_depth)

        best_level_ofi: Decimal | None = None
        bid_ofi: Decimal | None = None
        ask_ofi: Decimal | None = None
        bid_mlofi: tuple[Decimal | None, ...] = tuple(None for _ in range(self.depth))
        ask_mlofi: tuple[Decimal | None, ...] = tuple(None for _ in range(self.depth))
        mlofi: tuple[Decimal | None, ...] = tuple(None for _ in range(self.depth))
        mlofi_sum: Decimal | None = None

        prev = self._prev_book
        if prev is not None:
            if prev.instrument_id != event.instrument_id:
                raise ValueError("engine cannot mix instruments")
            if event.ts_event_ns < prev.ts_event_ns:
                raise ValueError("out-of-order book event")

            bid_ofi = _best_level_ofi(prev.bids[0], bid0, side="bid")
            ask_ofi = _best_level_ofi(prev.asks[0], ask0, side="ask")
            best_level_ofi = bid_ofi + ask_ofi
            self._cumulative_best_level_ofi += best_level_ofi

            bid_mlofi = _rankwise_mlofi(prev.bids, event.bids, side="bid", depth=self.depth)
            ask_mlofi = _rankwise_mlofi(prev.asks, event.asks, side="ask", depth=self.depth)
            mlofi_list: list[Decimal | None] = []
            for i, (b, a) in enumerate(zip(bid_mlofi, ask_mlofi)):
                if b is None or a is None:
                    mlofi_list.append(None)
                    continue
                value = b + a
                mlofi_list.append(value)
                self._cumulative_mlofi[i] += value
            mlofi = tuple(mlofi_list)
            mlofi_sum = _sum_present(mlofi)

        self._prev_book = event

        return {
            "schema_version": self.SCHEMA_VERSION,
            "kind": "book_features",
            "instrument_id": event.instrument_id,
            "ts_event_ns": event.ts_event_ns,
            "ts_init_ns": event.ts_init_ns,
            "sequence": event.sequence,
            "book_event_no": self._book_events,
            "depth": self.depth,
            "best_bid_price": str(bid0.price),
            "best_ask_price": str(ask0.price),
            "best_bid_size": str(bid0.size),
            "best_ask_size": str(ask0.size),
            "spread": str(spread),
            "mid": str(mid),
            "bbo_imbalance": _decimal_or_none(bbo_imbalance),
            "microprice": _decimal_or_none(microprice),
            "microprice_minus_mid": _decimal_or_none(None if microprice is None else microprice - mid),
            "bid_depth_size": str(bid_depth),
            "ask_depth_size": str(ask_depth),
            "depth_imbalance": _decimal_or_none(depth_imbalance),
            "best_level_bid_ofi": _decimal_or_none(bid_ofi),
            "best_level_ask_ofi": _decimal_or_none(ask_ofi),
            "best_level_ofi": _decimal_or_none(best_level_ofi),
            "cumulative_best_level_ofi": str(self._cumulative_best_level_ofi),
            "mlofi_definition": self.MLOFI_DEFINITION,
            "bid_mlofi": [_decimal_or_none(x) for x in bid_mlofi],
            "ask_mlofi": [_decimal_or_none(x) for x in ask_mlofi],
            "mlofi": [_decimal_or_none(x) for x in mlofi],
            "mlofi_sum": _decimal_or_none(mlofi_sum),
            "cumulative_mlofi": [str(x) for x in self._cumulative_mlofi],
        }

    def on_trade(self, event: TradeEvent) -> dict[str, object]:
        self._trade_events += 1
        if event.aggressor_side == "BUYER":
            signed_delta = event.size
        elif event.aggressor_side == "SELLER":
            signed_delta = -event.size
        else:
            signed_delta = _ZERO

        self._cumulative_trade_delta += signed_delta

        return {
            "schema_version": self.SCHEMA_VERSION,
            "kind": "trade_features",
            "instrument_id": event.instrument_id,
            "ts_event_ns": event.ts_event_ns,
            "ts_init_ns": event.ts_init_ns,
            "trade_event_no": self._trade_events,
            "trade_id": event.trade_id,
            "price": str(event.price),
            "size": str(event.size),
            "aggressor_side": event.aggressor_side,
            "trade_delta": str(signed_delta),
            "cumulative_trade_delta": str(self._cumulative_trade_delta),
        }
