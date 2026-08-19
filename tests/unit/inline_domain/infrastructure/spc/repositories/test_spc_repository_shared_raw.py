from __future__ import annotations

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.shared.measurement_preparation import (
    InlineMeasurementPreparationRepository,
)
from inline_domain.infrastructure.spc.spc_repository import SpcRepository


class FakeRawMeasurements:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool]] = []

    def get_measurements(
        self, prod_code: str, end_date: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        self.calls.append((prod_code, end_date, force_refresh))
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": prod_code,
                    "start_time": "2026-08-02 10:00:00",
                    "sheet_id": "S1",
                    "lot_id": "L1",
                    "step_id": "100",
                    "param_name": "SPC_PARAM",
                    "site_name": "P1",
                    "unit_id": "EQ1",
                    "param_value": 4.2,
                },
                {
                    "factory": "ARRAY",
                    "prod_code": prod_code,
                    "start_time": "2026-08-02 10:00:00",
                    "sheet_id": "S1",
                    "lot_id": "L1",
                    "step_id": "100",
                    "param_name": "CTQ_PARAM",
                    "site_name": "P1",
                    "unit_id": "EQ1",
                    "param_value": 5.1,
                },
            ]
        )


class FakeMetadata:
    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ref_param_name": "SPC_PARAM", "data_type": "SPC"},
                {"ref_param_name": "CTQ_PARAM", "data_type": "CTQ"},
            ]
        )

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame()


class FakeMainProcessHistory:
    def get_main_process_history(self, routed, history_start, history_end) -> pd.DataFrame:
        return pd.DataFrame()


def test_spc_repository_derives_spc_view_from_shared_raw_port(monkeypatch) -> None:
    raw = FakeRawMeasurements()
    monkeypatch.setattr(
        InlineMeasurementPreparationRepository,
        "_apply_outlier_filters",
        lambda _self, measurements, _prod: measurements,
    )
    repository = SpcRepository(
        InlineMeasurementPreparationRepository(
            raw_measurements=raw,
            metadata=FakeMetadata(),
            main_process_history=FakeMainProcessHistory(),
        )
    )

    result = repository.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M678",
            start_date="2026-08-01",
            end_date="2026-08-10",
            data_type_filter="SPC",
        ),
        force_refresh=True,
    )

    assert raw.calls == [("M678", "2026-08-10", True)]
    assert result["param_name"].tolist() == ["SPC_PARAM"]
    assert result["data_type"].tolist() == ["SPC"]
    assert "sheet_start_time" in result.columns
