"""Consumer-owned equipment baseline and real/fabricated snapshot reads."""
from typing import Protocol

import pandas as pd


class PartsDataPort(Protocol):
    def load_baseline(self, baseline_path: str) -> pd.DataFrame:
        """Return a normalized baseline, including numeric lifespan limits."""
        ...

    def load_snapshots(
        self, spec_df: pd.DataFrame, *, as_of: pd.Timestamp, max_age_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return real and fabricated frames separately; preserve source policy."""
        ...

    def refresh(self, baseline_path: str) -> pd.DataFrame:
        """Force the existing source refresh and return its usable snapshot."""
        ...
