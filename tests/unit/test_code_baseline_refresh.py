from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.yield_domain.core.mwd_trend_processor import (
    _ensure_code_baseline_current,
    _load_code_baseline,
)


def _write_baseline(path: Path, generated_at: str, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="Sheet1")
        pd.DataFrame(
            [
                {"key": "generated_at", "value": generated_at},
                {"key": "refresh_reason", "value": "test"},
            ]
        ).to_excel(writer, index=False, sheet_name="_metadata")


def _raw_daily_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 2,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_desc": "CodeA",
                "defect_panel_count": 4,
                "total_panels": 100,
            },
        ]
    )


def test_expired_code_baseline_rebuilds_from_current_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-04-01T00:00:00",
        rows=[{"defect_desc": "CodeA", "baseline_rate": 0.9}],
    )

    refreshed = _ensure_code_baseline_current(
        _raw_daily_rows(),
        "PTEST",
        now=datetime(2026, 5, 2),
        max_age_days=30,
    )
    baseline_map = _load_code_baseline("PTEST")

    assert refreshed.loc[0, "defect_desc"] == "CodeA"
    assert baseline_map["CodeA"] == pytest.approx(0.03)


def test_fresh_code_baseline_rebuilds_when_current_window_has_new_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-05-01T00:00:00",
        rows=[{"defect_desc": "CodeA", "baseline_rate": 0.9}],
    )
    current_rows = pd.concat(
        [
            _raw_daily_rows(),
            pd.DataFrame(
                [
                    {
                        "warehousing_time": pd.Timestamp("2026-05-02"),
                        "defect_desc": "CodeB",
                        "defect_panel_count": 1,
                        "total_panels": 100,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    _ensure_code_baseline_current(
        current_rows,
        "PTEST",
        now=datetime(2026, 5, 10),
        max_age_days=30,
    )
    baseline_map = _load_code_baseline("PTEST")

    assert baseline_map["CodeA"] == pytest.approx(0.03)
    assert baseline_map["CodeB"] == pytest.approx(0.01)
