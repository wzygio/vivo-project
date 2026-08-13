"""AOI TT adapter that projects report data from the shared raw snapshot."""

from __future__ import annotations

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.ports.measurement_snapshot import (
    MeasurementMetadataPort,
    MeasurementSnapshotPort,
)
TT_DETAIL_COLUMNS = [
    "factory", "prod_code", "start_time", "sheet_id", "lot_id",
    "step_id", "tt_name", "tt_qty",
]


def _project_tt_param_set(specs: pd.DataFrame) -> pd.DataFrame:
    if specs.empty:
        return pd.DataFrame(columns=["step_id", "param_name"])
    return specs[specs["param_type"].isna()][["step_id", "param_name"]].drop_duplicates()


def _project_tt_spec_limits(specs: pd.DataFrame) -> pd.DataFrame:
    if specs.empty:
        return pd.DataFrame(columns=["prod_code", "step_id", "tt_name", "usl", "ucl"])
    return (
        specs[specs["param_type"].isna()][["prod_code", "step_id", "param_name", "usl", "ucl"]]
        .rename(columns={"param_name": "tt_name"})
        .reset_index(drop=True)
    )


class AoiTtRepository:
    """Apply AOI TT parameter and field contracts to shared measurements."""

    def __init__(
        self,
        raw_measurements: MeasurementSnapshotPort,
        metadata: MeasurementMetadataPort,
    ) -> None:
        self.raw_measurements = raw_measurements
        self.metadata = metadata

    def get_tt_details(self, query: AoiTtQueryConfig) -> pd.DataFrame:
        raw = self.raw_measurements.get_measurements(query.prod_code, query.end_date)
        specs = self.metadata.get_parameter_specs(query.prod_code)
        param_set = _project_tt_param_set(specs)
        if raw.empty or param_set.empty:
            return pd.DataFrame(columns=TT_DETAIL_COLUMNS)

        pairs = param_set[["step_id", "param_name"]].drop_duplicates()
        details = raw.merge(pairs, on=["step_id", "param_name"], how="inner")
        if details.empty:
            return pd.DataFrame(columns=TT_DETAIL_COLUMNS)

        details = details.copy()
        details["start_time"] = pd.to_datetime(details["start_time"], errors="coerce")
        details["param_value"] = pd.to_numeric(details["param_value"], errors="coerce").fillna(0)
        start = pd.Timestamp(query.start_date)
        end = pd.Timestamp(query.end_date) + pd.Timedelta(days=1)
        details = details[
            details["start_time"].ge(start) & details["start_time"].lt(end)
        ].copy()

        if query.factory:
            details = details[details["factory"].eq(query.factory.upper())]
        if query.step_id:
            details = details[details["step_id"].eq(query.step_id)]
        if query.tt_name:
            details = details[details["param_name"].eq(query.tt_name)]

        return (
            details.rename(columns={"param_name": "tt_name", "param_value": "tt_qty"})
            .reindex(columns=TT_DETAIL_COLUMNS)
            .dropna(subset=["start_time"])
            .reset_index(drop=True)
        )

    def get_tt_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return _project_tt_spec_limits(self.metadata.get_parameter_specs(prod_code))
