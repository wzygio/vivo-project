"""Read-only data boundary for the large-hole report."""

from typing import Protocol

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery


class IjpHoleDataPort(Protocol):
    def fetch_counts(self, query: IjpQuery) -> pd.DataFrame: ...

    def list_picis(self, query: IjpQuery) -> tuple[str, ...]: ...
