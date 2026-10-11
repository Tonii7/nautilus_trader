"""Append-only JSONL recorder for canonical events and derived features."""

from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Mapping


class JsonlRecorder:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8", newline="\n")
        self._lock = Lock()

    def write(self, row: Mapping[str, object]) -> None:
        payload = json.dumps(
            dict(row),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self._lock:
            self._fh.write(payload + "\n")
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.flush()
                self._fh.close()

    def __enter__(self) -> "JsonlRecorder":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
