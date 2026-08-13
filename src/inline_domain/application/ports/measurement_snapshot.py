"""Port for the shared raw Inline measurement snapshot."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class MeasurementSnapshotPort(Protocol):
    """Provide one product's normalized raw measurement fact set."""

    def get_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool = False,
    ) -> pd.DataFrame: ...


class MeasurementMetadataPort(Protocol):
    """Provide parameter catalog and specification facts."""

    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame: ...

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame: ...


class MainProcessHistoryPort(Protocol):
    """Provide the routed manufacturing histories required by SPC enrichment."""

    def get_main_process_history(
        self,
        routed_measurements: pd.DataFrame,
        history_start: object,
        history_end: object,
    ) -> pd.DataFrame: ...
