# MCR Binance Microstructure Sandbox v1

## Status

Drop-in implementation prepared against:

- repository: `Tonii7/nautilus_trader`
- base branch: `mcr-rithmic-baseline`
- base commit: `1c9054efb2e445fa071408a13554836bf187688b`
- target sandbox branch: `mcr-binance-sandbox-v1`

The Rithmic adapter is **not modified** by this sandbox.

## Purpose

Use Binance `BTCUSDT-PERP` public data as an always-available microstructure proving ground before the
same venue-neutral feature contract is attached to Rithmic/YM.

This is a **research data pipeline, not a trading strategy**.  It does not place orders, define alpha,
or tune thresholds.

## Data path

```text
Binance USDT Futures public feed
        |
        v
Nautilus BinanceDataClient (no credentials)
        |
        +--> managed L2_MBP order book ----+
        |                                   |
        +--> TradeTick ---------------------+--> canonical events
                                                   |
                                                   v
                                      MicrostructureEngine v1
                                                   |
                                                   +--> BBO imbalance
                                                   +--> microprice
                                                   +--> N-level depth imbalance
                                                   +--> BestLevelOFI
                                                   +--> MLOFI-v1
                                                   +--> trade delta
                                                   +--> cumulative trade delta
```

## Canonical event contract

`canonical_events.jsonl` contains only normalized current observations required for deterministic
replay.  Decimal prices and quantities are serialized as strings to avoid float drift.

The word "canonical" is intentional: these are not raw Binance wire packets.  They are the raw inputs
to the MCR feature engine after Nautilus has normalized/reconstructed the feed.

## Feature definitions

For best bid size `B` and best ask size `A`:

`BBO_IMBALANCE = (B - A) / (B + A)`

For best bid price `Pb`, best ask price `Pa`:

`MICROPRICE = (Pa*B + Pb*A) / (B + A)`

For top `N` levels:

`DEPTH_IMBALANCE_N = (sum_bid_N - sum_ask_N) / (sum_bid_N + sum_ask_N)`

Best-level OFI uses the standard piecewise queue-change convention.  Positive values mean increasing
buy-side pressure; negative values mean increasing sell-side pressure.

`MLOFI-v1` applies that same piecewise OFI rule independently by rank for levels `1..N`.  This is a
pre-registered engineering primitive, **not** evidence of predictive edge.

Trade delta:

- buyer aggressor: `+size`
- seller aggressor: `-size`
- no known aggressor: `0`

## Files

```text
examples/live/binance/binance_mcr_microstructure_sandbox.py
examples/live/binance/MCR_MICROSTRUCTURE_SANDBOX.md
examples/live/binance/mcr_microstructure/__init__.py
examples/live/binance/mcr_microstructure/engine.py
examples/live/binance/mcr_microstructure/recorder.py
examples/live/binance/mcr_microstructure/replay.py
examples/live/binance/mcr_microstructure/audit_capture.py
examples/live/binance/mcr_microstructure/tests/__init__.py
examples/live/binance/mcr_microstructure/tests/test_engine.py
```

## Run tests

From the repository root:

```bash
python -m unittest examples.live.binance.mcr_microstructure.tests.test_engine -v
```

## Run live public-data sandbox

```bash
python examples/live/binance/binance_mcr_microstructure_sandbox.py
```

No Binance API key or secret is required.  Stop with `CTRL+C`.

Optional configuration:

```powershell
$env:MCR_BINANCE_INSTRUMENT="BTCUSDT-PERP.BINANCE"
$env:MCR_BINANCE_DEPTH="10"
$env:MCR_BINANCE_OUTPUT_DIR="mcr_binance_sandbox_data"
$env:MCR_BINANCE_LOG_EVERY="100"
python examples/live/binance/binance_mcr_microstructure_sandbox.py
```

Output:

```text
mcr_binance_sandbox_data/
  canonical_events.jsonl
  derived_features.jsonl
```

## Deterministic replay

```bash
python -m examples.live.binance.mcr_microstructure.replay \
  --input mcr_binance_sandbox_data/canonical_events.jsonl \
  --output mcr_binance_sandbox_data/derived_features.replayed.jsonl \
  --depth 10
```

The replay path executes the exact same `MicrostructureEngine` used by the live strategy.

## Gate before Rithmic/YM port

Sandbox v1 is considered transport/feature-ready only after:

1. unit tests pass;
2. public Binance L2 + trades produce both output files;
3. canonical event timestamps are monotonic enough for the chosen stream semantics;
4. no persistent locked/crossed reconstructed books are observed;
5. replay of the captured canonical stream reproduces the live-derived feature rows;
6. feature range/invariant audit passes (`imbalance in [-1,1]`, signed OFI/delta finite);
7. no execution client, order submission or credentials are present.

After these gates, the same canonical `BookEvent`/`TradeEvent` adapter boundary can be implemented for
Rithmic/YM without changing feature formulas.

## Post-capture audit

After stopping a live capture:

```bash
python -m examples.live.binance.mcr_microstructure.audit_capture \
  --dir mcr_binance_sandbox_data \
  --depth 10
```

The audit fails if canonical/derived row counts diverge, no book/trade data was captured,
imbalance leaves `[-1, 1]`, spread is non-positive, per-stream event timestamps regress, or replay
does not exactly reproduce the live-derived rows.

## Scientific limits of the Binance sandbox

Do not over-interpret this stage:

- Binance Futures order-book data is **L2 MBP**, not CME/Rithmic MBO. It cannot validate true
  per-order queue position, order identity, or MBO-specific replenishment logic.
- Binance Futures real-time trades use the venue `aggTrade` stream. `trade_delta` here is therefore
  signed **aggregated aggressive trade volume**, not a claim that every underlying fill is visible as
  an independent tape event.
- Crypto market structure and 24/7 session behavior are not a proxy for YM/CME behavior.
- The purpose of this sandbox is to validate transport, canonicalization, state transitions,
  deterministic replay and feature implementations before the Rithmic/YM port.
