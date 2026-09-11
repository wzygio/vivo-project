"""Latest report-day boundary, independent of a caller's selected date range."""

from dataclasses import dataclass
from datetime import time

import pandas as pd


@dataclass(frozen=True)
class ReportCutoffPolicy:
    """Keep facts through today's configured time on the display time axis."""

    latest_day_time: str = "12:00:00"

    def __post_init__(self) -> None:
        if not isinstance(self.latest_day_time, str):
            raise ValueError("report_cutoff.latest_day_time must be HH:MM:SS")
        try:
            parsed = time.fromisoformat(self.latest_day_time)
        except ValueError as exc:
            raise ValueError("report_cutoff.latest_day_time must be HH:MM:SS") from exc
        if parsed.tzinfo is not None or parsed.isoformat(timespec="seconds") != self.latest_day_time:
            raise ValueError("report_cutoff.latest_day_time must be HH:MM:SS")

    def boundary(self, now: object | None = None) -> pd.Timestamp:
        current = pd.Timestamp.now() if now is None else pd.Timestamp(now)
        return pd.Timestamp.combine(current.date(), time.fromisoformat(self.latest_day_time))

    @property
    def signature(self) -> str:
        return f"report-cutoff-v1:{self.boundary().isoformat()}"

    def cap_end(self, end: object, *, now: object | None = None) -> pd.Timestamp:
        """Cap an inclusive display-time SQL bound before any aggregation."""
        return min(pd.Timestamp(end), self.boundary(now))

    def filter_frame(
        self, frame: pd.DataFrame, time_column: str, *, now: object | None = None,
    ) -> pd.DataFrame:
        """Copy report facts; raw snapshots and their coverage remain untouched."""
        if frame.empty:
            return frame.copy()
        timestamps = pd.to_datetime(frame[time_column], errors="coerce")
        return frame.loc[timestamps.le(self.boundary(now))].copy()
