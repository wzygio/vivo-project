from __future__ import annotations

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.spc.ports import SpcDataPort


class CtqRepository:
    """Expose CTQ-specific preprocessing over the shared measurement source."""

    def __init__(self, measurement_repository: SpcDataPort) -> None:
        self.measurement_repository = measurement_repository

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        ctq_config = config.model_copy(update={"data_type_filter": "CTQ"})
        return self.measurement_repository.get_spc_measurements(ctq_config, force_refresh)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return self.measurement_repository.get_spc_spec_limits(prod_code)
