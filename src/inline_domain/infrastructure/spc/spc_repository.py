"""Thin SPC projection over the shared measurement preparation port."""

from __future__ import annotations

import pandas as pd

from src.inline_domain.application.ports.measurement_snapshot import (
    MeasurementPreparationPort,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig


class SpcRepository:
    """Expose the SPC data contract while delegating preparation to measurement."""

    def __init__(self, preparation: MeasurementPreparationPort) -> None:
        if preparation is None:
            raise ValueError("SPC repository requires a measurement preparation port")
        self._preparation = preparation

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        """Return the SPC projection prepared by the shared measurement pipeline."""
        return self._preparation.get_prepared_measurements(config, force_refresh)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """Return the product spec limits prepared by the shared measurement pipeline."""
        return self._preparation.get_spec_limits(prod_code)
