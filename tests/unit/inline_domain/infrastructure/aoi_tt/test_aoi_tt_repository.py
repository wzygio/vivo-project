from __future__ import annotations

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.infrastructure.aoi_tt.aoi_tt_repository import AoiTtRepository


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
