from __future__ import annotations

from typing import Callable, Protocol

import pandas as pd

from src.inline_domain.application.spc.ports import SpcDataPort


class MonitorSpcDataPort(SpcDataPort, Protocol):
    def get_scrap_data(self, prod_code: str) -> pd.DataFrame: ...


MonitorSpcRepositoryFactory = Callable[[str], MonitorSpcDataPort]
