"""Repository facade dedicated to the automatic-warning monitor use case."""

from __future__ import annotations

import pandas as pd

from src.inline_domain.application.monitor.ports import MonitorSpcDataPort
from src.inline_domain.application.spc.dtos import SpcQueryConfig


class InlineMonitorRepository:
    """Expose only the data capabilities required by ``MonitorAnalysisService``."""

    def __init__(self, spc_source: MonitorSpcDataPort) -> None:
        self._spc_source = spc_source

    def get_spc_measurements(
        self,
        config: SpcQueryConfig,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        return self._spc_source.get_spc_measurements(config, force_refresh)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return self._spc_source.get_spc_spec_limits(prod_code)

    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        return self._spc_source.get_scrap_data(prod_code)
