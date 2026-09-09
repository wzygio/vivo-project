"""Persistence contract consumed by daily throughput history orchestration."""

from pathlib import Path
from typing import Protocol

import pandas as pd


class ThroughputHistoryPort(Protocol):
    def update(
        self, scope: str, prod_code: str, facts: pd.DataFrame, *,
        coverage_start: pd.Timestamp, coverage_end: pd.Timestamp,
    ) -> None: ...

    def read(self, scope: str, prod_code: str) -> pd.DataFrame: ...

    def snapshot_path(self, scope: str, prod_code: str) -> Path: ...
