from __future__ import annotations

from typing import Protocol

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig


class SpcDataPort(Protocol):
    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame: ...

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame: ...
