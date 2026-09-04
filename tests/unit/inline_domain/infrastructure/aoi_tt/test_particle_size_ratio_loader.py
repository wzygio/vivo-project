from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.infrastructure.aoi_tt.particle_size_ratio_loader import (
    load_particle_size_ratios,
)


def test_ratio_loader_normalizes_station_size_and_ratio(tmp_path: Path) -> None:
    path = tmp_path / "ratios.xlsx"
    pd.DataFrame(
        [
            {"step_id": 11620, "particle_size": " s ", "分配比例": 0.65},
            {"step_id": 11620, "particle_size": "M", "分配比例": 0.30},
            {"step_id": 11620, "particle_size": "L", "分配比例": 0.03},
            {"step_id": 11620, "particle_size": "H", "分配比例": 0.02},
        ]
    ).to_excel(path, sheet_name="比例规格表", index=False)

    result = load_particle_size_ratios(path)

    assert result.to_dict("records") == [
        {"step_id": "11620", "particle_size": "S", "ratio": 0.65},
        {"step_id": "11620", "particle_size": "M", "ratio": 0.30},
        {"step_id": "11620", "particle_size": "L", "ratio": 0.03},
        {"step_id": "11620", "particle_size": "H", "ratio": 0.02},
    ]


def test_ratio_loader_rejects_incomplete_station_distribution(tmp_path: Path) -> None:
    path = tmp_path / "ratios.xlsx"
    pd.DataFrame(
        [{"step_id": 11620, "particle_size": "S", "分配比例": 0.65}]
    ).to_excel(path, sheet_name="比例规格表", index=False)

    with pytest.raises(ValueError, match="S/M/L/H"):
        load_particle_size_ratios(path)
