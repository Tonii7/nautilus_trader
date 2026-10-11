"""MCR microstructure primitives used by the Binance public-data sandbox.

The package is deliberately venue-neutral.  The live Binance strategy translates
Nautilus objects into these canonical events; replay feeds the exact same events
back into the same feature engine.
"""

from .engine import BookEvent, Level, MicrostructureEngine, TradeEvent
from .recorder import JsonlRecorder

__all__ = [
    "BookEvent",
    "JsonlRecorder",
    "Level",
    "MicrostructureEngine",
    "TradeEvent",
]
