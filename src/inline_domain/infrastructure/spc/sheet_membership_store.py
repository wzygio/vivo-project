"""Atomic, week-sharded JSON snapshots; no Excel, database or report construction."""
from datetime import date
import json
import os
from pathlib import Path
import re
import tempfile

from src.shared_kernel.infrastructure.file_lock import publication_lock


class SpcSheetMembershipJsonStore:
    def __init__(self, directory: str | Path, *, max_bytes: int = 64 * 1024 * 1024) -> None:
        self.directory = Path(directory)
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("Membership capacity must be positive")
        self.max_bytes = max_bytes

    def _path(self, product: str, start_date: date) -> Path:
        if not isinstance(product, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", product):
            raise ValueError("Invalid membership product")
        if not isinstance(start_date, date) or start_date.weekday() != 0:
            raise ValueError("Invalid membership week")
        iso = start_date.isocalendar()
        return self.directory / product / f"{iso.year}-W{iso.week:02d}.json"

    def read(self, product: str, start_date: date) -> dict | None:
        path = self._path(product, start_date)
        try:
            before = path.stat()
        except FileNotFoundError:
            return None
        if before.st_size > self.max_bytes:
            raise ValueError("Membership snapshot exceeds capacity")
        with path.open("rb") as stream:
            raw = stream.read(self.max_bytes + 1)
        after = path.stat()
        if len(raw) > self.max_bytes or (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
            raise ValueError("Membership snapshot changed during read")
        snapshot = json.loads(raw)
        if not isinstance(snapshot, dict):
            raise ValueError("Invalid membership snapshot")
        return snapshot

    def publish(self, snapshot: dict) -> None:
        product = snapshot["product"]
        start = date.fromisoformat(snapshot["coverage_start"])
        path = self._path(product, start)
        raw = (json.dumps(snapshot, ensure_ascii=False, allow_nan=False, sort_keys=True,
                          separators=(",", ":")) + "\n").encode("utf-8")
        if len(raw) > self.max_bytes:
            raise ValueError("Membership snapshot exceeds capacity")
        path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(path):
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".membership-", suffix=".tmp", delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
