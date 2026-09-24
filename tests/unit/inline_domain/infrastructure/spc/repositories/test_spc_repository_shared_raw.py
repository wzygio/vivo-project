from __future__ import annotations

import pandas as pd
import pytest

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.ctq.ctq_repository import CtqRepository
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


@pytest.mark.parametrize("scope", ["SPC", "CTQ"])
@pytest.mark.parametrize("time_column", ["start_time", "sheet_start_time"])
def test_shared_projection_keeps_latest_point_by_five_keys(
    monkeypatch, scope: str, time_column: str,
) -> None:
    base = {
        "factory": "ARRAY", "prod_code": "M678", "sheet_id": "S1",
        "lot_id": "L1", "step_id": "100", "param_name": f"{scope}_PARAM",
        "site_name": "P1", "unit_id": "EQ1", "param_value": 1.0,
        time_column: "2026-08-02 09:00:00",
    }
    # Deliberately unordered, including duplicates across factories/devices.
    rows = [
        {**base, time_column: "2026-08-02 11:00:00", "param_value": 3.0},
        base,
        {**base, "factory": "OLED", time_column: "2026-08-02 10:00:00", "param_value": 2.0},
        {**base, "factory": "TP", "unit_id": "EQ2",
         time_column: "2026-08-02 11:00:00", "param_value": 4.0},
    ]
    # Each component of the requested identity must keep distinct points apart.
    for key, value in [("prod_code", "M626"), ("step_id", "200"),
                       ("sheet_id", "S2"), ("site_name", "P2"),
                       ("param_name", f"{scope}_OTHER")]:
        rows.append({**base, key: value})
    source = pd.DataFrame(rows)
    original = source.copy(deep=True)
    raw = FakeRawMeasurements()
    monkeypatch.setattr(raw, "get_measurements", lambda *args: source)
    metadata = FakeMetadata()
    monkeypatch.setattr(metadata, "get_parameter_catalog", lambda prod_code: pd.DataFrame([
        {"ref_param_name": f"{scope}_PARAM", "data_type": scope},
        {"ref_param_name": f"{scope}_OTHER", "data_type": scope},
    ]))
    filtered_inputs = []

    def capture_filter(_self, measurements, _prod):
        filtered_inputs.append(measurements.copy())
        return measurements

    monkeypatch.setattr(InlineMeasurementPreparationRepository, "_apply_outlier_filters", capture_filter)
    repository = SpcRepository(InlineMeasurementPreparationRepository(
        raw_measurements=raw, metadata=metadata,
        main_process_history=FakeMainProcessHistory(),
    ))
    if scope == "CTQ":
        repository = CtqRepository(repository)

    result = repository.get_spc_measurements(SpcQueryConfig(
        prod_code="M678", start_date="2026-08-01", end_date="2026-08-10",
        data_type_filter="SPC",
    ))

    keys = ["prod_code", "step_id", "param_name", "sheet_id", "site_name"]
    for frame in [filtered_inputs[0], result]:
        assert len(frame) == 6
        assert not frame.duplicated(keys).any()
        winner = frame.loc[frame[keys].eq(pd.Series(base)[keys]).all(axis=1)].iloc[0]
        assert winner["sheet_start_time"] == pd.Timestamp("2026-08-02 11:00:00")
        assert winner["param_value"] == 4.0
        assert winner["factory"] == "TP"
        assert winner["unit_id"] == "EQ2"
    assert result["data_type"].eq(scope).all()
    pd.testing.assert_frame_equal(source, original)
