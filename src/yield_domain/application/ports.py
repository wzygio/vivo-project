"""Consumer-owned Yield data reads; frames carry native data_health attrs."""
from pathlib import Path
from typing import Protocol, Sequence

import pandas as pd

from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig


class YieldDataPort(Protocol):
    def read_panel(
        self, query: YieldQueryConfig, policy: YieldDataPolicy,
        *, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Return display-time facts with health attrs, including successful empty windows.

        No partial refresh may replace a complete snapshot. Unavailable facts carry
        unavailable health or raise an error; callers must not treat either as empty success.
        """
        ...

    def read_array_times(
        self, lot_ids: Sequence[str], custom_times: dict[str, str],
    ) -> pd.DataFrame:
        """Return display-time input facts; raise on source failure."""
        ...

    def read_rate_overrides(
        self, path: Path | None, sheet_name: str,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """Return Sheet and Lot overrides; absent optional configuration returns None."""
        ...
