from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.infrastructure.aoi_tt.aoi_tt_repository import AoiTtRepository
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import build_sheet_point_df


class FakeRawMeasurements:
    def get_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        assert (prod_code, end_date, force_refresh) == ("M678", "2026-08-10", False)
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": "M678",
                    "start_time": "2026-08-01 08:00:00",
                    "sheet_id": "SHEET-1",
                    "lot_id": "LOT-1",
                    "step_id": "11620",
                    "param_name": "TDSUM",
                    "site_name": None,
                    "unit_id": "AOI-1",
                    "param_value": 3,
                },
                {
                    "factory": "ARRAY",
                    "prod_code": "M678",
                    "start_time": "2026-08-01 08:00:00",
                    "sheet_id": "SHEET-1",
                    "lot_id": "LOT-1",
                    "step_id": "11620",
                    "param_name": "SE_L1T",
                    "site_name": "P1",
                    "unit_id": "SPC-1",
                    "param_value": 50,
                },
            ]
        )


class FakeMetadata:
    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "11620",
                    "param_name": "TDSUM",
                    "param_type": None,
                    "usl": 5,
                    "ucl": 3,
                }
            ]
        )


def test_aoi_tt_repository_projects_only_step_parameter_pairs_from_shared_raw_data() -> None:
    repository = AoiTtRepository(
        raw_measurements=FakeRawMeasurements(),
        metadata=FakeMetadata(),
    )
    query = AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-08-01",
        end_date="2026-08-10",
    )

    result = repository.get_tt_details(query)

    assert result.to_dict("records") == [
        {
            "factory": "ARRAY",
            "prod_code": "M678",
            "start_time": pd.Timestamp("2026-08-01 08:00:00"),
            "sheet_id": "SHEET-1",
            "lot_id": "LOT-1",
            "step_id": "11620",
            "tt_name": "TDSUM",
            "tt_qty": 3,
        }
    ]


def test_aoi_tt_repository_exposes_injected_particle_size_counts() -> None:
    query = AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-08-01",
        end_date="2026-08-10",
    )
    expected = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "sheet_id": "SHEET-1",
                "step_id": "11620",
                "particle_size": "O",
                "particle_qty": 2,
            }
        ]
    )
    captured: list[AoiTtQueryConfig] = []

    def load_particles(received: AoiTtQueryConfig) -> pd.DataFrame:
        captured.append(received)
        return expected

    repository = AoiTtRepository(
        raw_measurements=FakeRawMeasurements(),
        metadata=FakeMetadata(),
        particle_size_loader=load_particles,
    )

    result = repository.get_particle_size_counts(query)

    assert captured == [query]
    assert result is expected


def _tt_fact(**changes: object) -> dict[str, object]:
    return {
        "prod_code": "Z517", "factory": "ARRAY", "step_id": "11620",
        "param_name": "TDSUM", "sheet_id": "L3Z66900P02", "lot_id": "L3Z66900PAA",
        "start_time": "2026-09-14 08:20:13", "param_value": 354,
        "unit_id": "3AT001-AOI", "site_name": "G", **changes,
    }


def _repository_for(raw: pd.DataFrame) -> AoiTtRepository:
    specs = raw[["prod_code", "step_id", "param_name"]].drop_duplicates().assign(param_type=None)
    return AoiTtRepository(
        raw_measurements=SimpleNamespace(get_measurements=lambda *_args: raw),
        metadata=SimpleNamespace(get_parameter_specs=lambda _product: specs),
    )


def _query() -> AoiTtQueryConfig:
    return AoiTtQueryConfig(prod_code="Z517", start_date="2026-09-01", end_date="2026-09-22")


@pytest.mark.parametrize("reverse", [False, True])
def test_latest_inspection_replaces_older_value_before_sheet_aggregation(reverse: bool) -> None:
    rows = [
        _tt_fact(),
        _tt_fact(start_time="2026-09-15 11:32:35", param_value=309, unit_id="3AT008-AOI"),
    ]
    raw = pd.DataFrame(rows[::-1] if reverse else rows)
    original = raw.copy(deep=True)

    details = _repository_for(raw).get_tt_details(_query())

    assert details.tt_qty.tolist() == [309]
    assert details.start_time.tolist() == [pd.Timestamp("2026-09-15 11:32:35")]
    assert build_sheet_point_df(details).tt_qty.tolist() == [309]
    pd.testing.assert_frame_equal(raw, original)


@pytest.mark.parametrize("dimension,value", [
    ("prod_code", "OTHER"), ("factory", "TP"), ("step_id", "21320"),
    ("param_name", "DSUM"), ("sheet_id", "ANOTHER-SHEET"),
])
def test_latest_inspection_is_scoped_to_all_five_business_dimensions(dimension: str, value: str) -> None:
    raw = pd.DataFrame([
        _tt_fact(),
        _tt_fact(start_time="2026-09-15 11:32:35", param_value=309, **{dimension: value}),
    ])
    details = _repository_for(raw).get_tt_details(_query())
    assert sorted(details.tt_qty.tolist()) == [309, 354]


def test_reinspection_does_not_split_by_lot_or_equipment() -> None:
    raw = pd.DataFrame([
        _tt_fact(),
        _tt_fact(start_time="2026-09-15 11:32:35", param_value=309,
                 lot_id="UPDATED-LOT", unit_id="NEW-UNIT"),
    ])
    details = _repository_for(raw).get_tt_details(_query())
    assert details.tt_qty.tolist() == [309]
    assert details.lot_id.tolist() == ["UPDATED-LOT"]


@pytest.mark.parametrize("site", ["G", None])
def test_array_reinspection_keeps_latest_per_site_before_sheet_aggregation(site) -> None:
    raw = pd.DataFrame([
        _tt_fact(site_name=site, start_time="2026-09-15 11:32:35", param_value=309),
        _tt_fact(site_name=site),
        _tt_fact(site_name="OTHER-SITE", param_value=10),
    ])
    details = _repository_for(raw).get_tt_details(_query())

    assert sorted(details.tt_qty.tolist()) == [10, 309]
    assert build_sheet_point_df(details).tt_qty.tolist() == [319]


@pytest.mark.parametrize("factory", ["OLED", "TP"])
def test_other_factories_preserve_existing_sheet_level_reinspection(factory: str) -> None:
    raw = pd.DataFrame([
        _tt_fact(factory=factory),
        _tt_fact(factory=factory, site_name="OTHER-SITE",
                 start_time="2026-09-15 11:32:35", param_value=309),
    ])
    assert _repository_for(raw).get_tt_details(_query()).tt_qty.tolist() == [309]


def test_invalid_and_out_of_window_dates_do_not_displace_latest_eligible_inspection() -> None:
    raw = pd.DataFrame([
        _tt_fact(),
        _tt_fact(start_time="2026-09-15 11:32:35", param_value=309),
        _tt_fact(start_time="2026-09-23 00:00:00", param_value=999),
        _tt_fact(start_time="invalid", param_value=888),
        _tt_fact(start_time="2026-08-31 23:59:59", param_value=777),
    ])
    assert _repository_for(raw).get_tt_details(_query()).tt_qty.tolist() == [309]


def test_equal_timestamps_keep_last_input_record_without_summing() -> None:
    raw = pd.DataFrame([_tt_fact(), _tt_fact(param_value=309), _tt_fact(param_value=309)])
    assert _repository_for(raw).get_tt_details(_query()).tt_qty.tolist() == [309]
