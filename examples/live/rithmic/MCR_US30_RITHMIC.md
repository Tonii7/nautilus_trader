# MCR US30 / Rithmic Microstructure v1

## Goal

Make **US30 research primary** by collecting the exchange-traded E-mini Dow
futures contract (`YM`) from Rithmic and feeding the exact same deterministic
MCR microstructure engine already validated with the Binance sandbox.

This collector is **data-only**. It does not construct an execution client and
cannot submit orders.

## Important instrument distinction

`US30` at a CFD broker and `YM` on CME Group are not the same instrument.

For the MCR/Rithmic laboratory, the exchange-traded research instrument is:

- product: `YM`
- market: E-mini Dow Jones Industrial Average futures
- exchange: `CBOT`
- data transport: `Rithmic`
- Nautilus book subscription: `L2_MBP`

The script resolves the Rithmic front month dynamically. Do not hard-code a
quarterly contract unless troubleshooting requires an exact instrument override.

## Files

```text
examples/live/rithmic/rithmic_mcr_us30_microstructure.py
examples/live/rithmic/rithmic_mcr_us30_audit.py
```

It reuses the already validated venue-neutral primitives in:

```text
examples/live/binance/mcr_microstructure/
```

No feature formula is changed in the US30 port.

## Output

Every run creates a separate session directory by default:

```text
mcr_rithmic_us30_data/
  YYYYMMDDTHHMMSSZ/
    session_metadata.json
    canonical_events.jsonl
    derived_features.jsonl
    session_summary.json
```

This prevents two live sessions from being appended into one replay stream.

## Configuration

Optional PowerShell environment variables:

```powershell
$env:MCR_RITHMIC_PROFILE=""
$env:MCR_RITHMIC_PRODUCT="YM"
$env:MCR_RITHMIC_EXCHANGE="CBOT"
$env:MCR_RITHMIC_DEPTH="10"
$env:MCR_RITHMIC_LOG_EVERY="100"
$env:MCR_RITHMIC_RUN_SECONDS="30"
```

Optional exact instrument override:

```powershell
$env:MCR_RITHMIC_INSTRUMENT="<exact InstrumentId returned by Rithmic>"
```

Normally leave this unset so the front-month resolver chooses the active
contract.

## First live validation

From the Nautilus repository root:

```powershell
.\.venv\Scripts\python.exe -m examples.live.rithmic.rithmic_mcr_us30_microstructure
```

For the first check, set:

```powershell
$env:MCR_RITHMIC_RUN_SECONDS="30"
```

The expected success criteria are:

1. instrument resolves and becomes ready;
2. `book_rows > 0`;
3. `trade_rows > 0`;
4. `invalid_book_rows` is zero or rare/transient;
5. canonical and derived files are both created.

## Replay / invariant audit

Run the existing deterministic audit against the exact session folder:

```powershell
.\.venv\Scripts\python.exe -m examples.live.binance.mcr_microstructure.audit_capture `
  --dir "mcr_rithmic_us30_data\<SESSION>" `
  --depth 10
```

Expected:

```text
MCR capture audit: PASS
```

The audit checks row-count parity, positive spread, imbalance ranges,
per-stream event-time monotonicity, and exact deterministic replay.

## Research semantics

Keep two clocks:

- `ts_event_ns`: exchange/event time used for research alignment;
- `ts_init_ns`: Nautilus initialization/arrival-side time used for transport
  diagnostics.

Do not sort or rewrite the raw canonical capture. Preserve arrival order there.
Build separate event-time research views later.

## Current scientific status

This implementation ports the **measurement infrastructure**, not an alpha
claim.

The Binance sandbox established that:
- the canonical/derived replay path is deterministic;
- book and trade streams must be aligned by event time for research;
- MLOFI is a promising research primitive worth testing on the primary US30/YM
  market.

None of the Binance findings are assumed to transfer to YM. YM must be
measured independently.


## One-command latest-session audit

After a capture, from the Nautilus repo root:

```powershell
.\.venv\Scripts\python.exe -m examples.live.rithmic.rithmic_mcr_us30_audit
```

It automatically selects the latest timestamped session and reports:
- deterministic replay PASS/FAIL;
- row counts;
- event-to-init p50/p95/p99;
- negative timestamp delays;
- per-stream event-time regressions;
- cross-stream arrival-order regressions.
