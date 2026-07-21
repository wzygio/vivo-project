from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.spc_domain.infrastructure.data_loader import SpcQueryConfig
import src.spc_domain.infrastructure.repositories.spc_repository as spc_repository
from src.spc_domain.infrastructure.repositories.spc_repository import SpcRepository


def _measurements() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "step_id": ["10140", "11140", "11620", "11629", "UNKNOWN"],
            "param_name": ["CTQ_ONLY", "MIXED", "AOI_ONLY", "RS_ONLY", "UNMAPPED"],
        }
    )


def test_repository_filters_measurements_by_spc_station_mapping() -> None:
    result = SpcRepository.filter_measurements_by_step_data_type(_measurements(), "SPC")

    assert result["step_id"].tolist() == ["11140"]


def test_repository_keeps_multi_type_station_for_ctq_mapping() -> None:
    result = SpcRepository.filter_measurements_by_step_data_type(_measurements(), "CTQ")

    assert result["step_id"].tolist() == ["10140", "11140"]


def test_repository_does_not_filter_when_all_station_types_are_requested() -> None:
    measurements = _measurements()

    result = SpcRepository.filter_measurements_by_step_data_type(measurements, "ALL")

    pd.testing.assert_frame_equal(result, measurements)


def test_repository_applies_station_type_filter_after_parameter_whitelist_merge(tmp_path: Path) -> None:
    source_measurements = _measurements().assign(
        factory="ARRAY",
        prod_code="M626",
        sheet_start_time=datetime(2026, 6, 1),
        sheet_id=["S1", "S2", "S3", "S4", "S5"],
        site_name="P1",
        unit_id="EQ-01",
        param_value=1.0,
    )
    whitelist = pd.DataFrame(
        {
            "ref_param_name": source_measurements["param_name"].str.upper(),
            "data_type": "CTQ",
        }
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-01",
        data_type_filter="SPC",
    )

    with (
        patch.object(spc_repository, "load_spc_measurements", return_value=source_measurements),
        patch.object(spc_repository, "load_param_whitelist", return_value=whitelist),
        patch.object(SpcRepository, "_apply_outlier_filters", lambda self, df, prod_code: df),
        patch.object(spc_repository, "export_probed_details"),
    ):
        repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=False, db_manager=object())
        result = repo.get_spc_measurements(query)

    assert result["step_id"].tolist() == ["11140"]
