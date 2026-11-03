"""Consumer-owned workbook source contract."""

from typing import Protocol
import pandas as pd


class PointTestSource(Protocol):
    def read_sheets(self) -> dict[str, pd.DataFrame]:
        """Return raw rows including the header, keyed by workbook sheet name."""
        ...
