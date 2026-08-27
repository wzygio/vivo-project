"""Unit tests for the shared decorated-feature pipeline (段2).

Covers:
- scope routing: ``ctq`` reads/writes ``ctq_sheet_oos_decoration.xlsx``,
  ``spc`` uses ``spc_sheet_oos_decoration.xlsx``;
- ``none`` scope skips decoration entirely (preprocess-only features);
- cache key behaviour: (prod_code, scope, start, end, snapshot_signature).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.shared.decorated_features import (
    InMemoryFeaturesSource,
    fetch_decorated_features,
)
from src.shared_kernel.config import ConfigLoader

PROD = "M678"
START_DATE = "2026-08-01"
END_DATE = "2026-08-10"

DECORATION_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "param_name",
    "sheet_id",
    "sheet_start_time",
    "sheet_max",
    "sheet_min",
    "sheet_mean",
    "usl",
    "lsl",
    "oos_type",
    "flag",
]


@pytest.fixture(autouse=True)
def _clear_shared_cache():
    fetch_decorated_features.clear()
    yield
    fetch_decorated_features.clear()


@pytest.fixture
def decoration_root(monkeypatch, tmp_path: Path) -> Path:
    """Redirect the decoration workbooks into tmp_path (existing test pattern)."""
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    resources = tmp_path / "resources" / "inline_domain"
    resources.mkdir(parents=True, exist_ok=True)
    return resources


def _measurements_df() -> pd.DataFrame:
    """One sheet with an in-spec point and an OOS point (usl=60)."""
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": PROD,
                "sheet_start_time": "2026-08-05 09:00:00",
                "sheet_id": "S1",
                "step_id": "100",
                "param_name": "THK",
                "site_name": site_name,
                "param_value": value,
                "data_type": "CTQ",
            }
            for site_name, value in (("P1", 50.0), ("P2", 100.0))
        ]
    )


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": PROD,
                "step_id": "100",
                "param_name": "THK",
                "usl": 60.0,
                "lsl": 40.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            }
        ]
    )


def _source() -> InMemoryFeaturesSource:
    return InMemoryFeaturesSource(_measurements_df(), _spec_df())


def _write_decoration_workbook(
    resources: Path, file_name: str, flag: object
) -> Path:
    path = resources / file_name
    row = {
        "factory": "ARRAY",
        "prod_code": PROD,
        "step_id": "100",
        "param_name": "THK",
        "sheet_id": "S1",
        "sheet_start_time": "2026-08-05 09:00:00",
        "sheet_max": 100.0,
        "sheet_min": 50.0,
        "sheet_mean": 75.0,
        "usl": 60.0,
        "lsl": 40.0,
        "oos_type": "USL",
        "flag": flag,
    }
    pd.DataFrame([row], columns=DECORATION_COLUMNS).to_excel(
        path, sheet_name=PROD, index=False, engine="openpyxl"
    )
    return path


# ---------------------------------------------------------------------------
# scope routing
# ---------------------------------------------------------------------------
def test_ctq_scope_uses_ctq_decoration_workbook(decoration_root: Path) -> None:
    # CTQ workbook deletes the OOS sheet; SPC workbook would keep it (flag=False).
    _write_decoration_workbook(decoration_root, "ctq_sheet_oos_decoration.xlsx", "Delete")
    _write_decoration_workbook(decoration_root, "spc_sheet_oos_decoration.xlsx", False)

    payload = fetch_decorated_features(
        _source(), PROD, "ctq", START_DATE, END_DATE, "route-ctq"
    )

    # The Delete flag from the CTQ workbook removed the only sheet's points.
    assert payload["raw_measurements_df"].empty
    assert payload["sheet_features_df"].empty
    decoration = payload["sheet_oos_decoration"]
    assert decoration["decoration_path"].endswith("ctq_sheet_oos_decoration.xlsx")
    assert decoration["decoration_sheet"] == PROD


def test_spc_scope_uses_spc_decoration_workbook(decoration_root: Path) -> None:
    # Inverse flags: SPC keeps the real OOS value, CTQ would delete it.
    _write_decoration_workbook(decoration_root, "ctq_sheet_oos_decoration.xlsx", "Delete")
    _write_decoration_workbook(decoration_root, "spc_sheet_oos_decoration.xlsx", False)

    payload = fetch_decorated_features(
        _source(), PROD, "spc", START_DATE, END_DATE, "route-spc"
    )

    # flag=False keeps the real OOS point value (no clipping, no deletion).
    assert payload["sheet_features_df"]["sheet_max"].tolist() == [100.0]
    decoration = payload["sheet_oos_decoration"]
    assert decoration["decoration_path"].endswith("spc_sheet_oos_decoration.xlsx")
    assert decoration["decoration_sheet"] == PROD


def test_ctq_scope_missing_sheet_means_empty_decoration(decoration_root: Path) -> None:
    # The CTQ workbook exists but has no sheet for this product:
    # the engine treats it as empty decoration semantics -> default clip (flag=True).
    pd.DataFrame(columns=DECORATION_COLUMNS).to_excel(
        decoration_root / "ctq_sheet_oos_decoration.xlsx",
        sheet_name="OTHER_PROD",
        index=False,
        engine="openpyxl",
    )

    payload = fetch_decorated_features(
        _source(), PROD, "ctq", START_DATE, END_DATE, "route-ctq-missing-sheet"
    )

    # Default flag=True clips the OOS point inside the spec.
    assert not payload["sheet_features_df"].empty
    assert payload["sheet_features_df"]["sheet_max"].iloc[0] < 100.0


def test_none_scope_skips_decoration(decoration_root: Path) -> None:
    payload = fetch_decorated_features(
        _source(), PROD, "none", START_DATE, END_DATE, "route-none"
    )

    # Raw OOS value untouched; no decoration payload; no workbook written.
    assert payload["sheet_features_df"]["sheet_max"].tolist() == [100.0]
    assert payload["sheet_oos_decoration"] is None
    assert payload["spec_empty"] is False
    assert list(decoration_root.glob("*.xlsx")) == []


def test_unknown_scope_raises(decoration_root: Path) -> None:
    with pytest.raises(ValueError, match="unknown decoration scope"):
        fetch_decorated_features(
            _source(), PROD, "banana", START_DATE, END_DATE, "route-invalid"
        )


def test_empty_measurements_return_empty_payload(decoration_root: Path) -> None:
    source = InMemoryFeaturesSource(pd.DataFrame(), _spec_df())

    payload = fetch_decorated_features(
        source, PROD, "spc", START_DATE, END_DATE, "route-empty"
    )

    assert payload["sheet_features_df"].empty
    assert payload["raw_measurements_df"].empty
    assert payload["sheet_oos_decoration"] is None
    assert payload["spec_empty"] is False


# ---------------------------------------------------------------------------
# cache key behaviour
# ---------------------------------------------------------------------------
class _CountingSource(InMemoryFeaturesSource):
    def __init__(self) -> None:
        super().__init__(_measurements_df(), _spec_df())
        self.measure_calls = 0

    def get_spc_measurements(self, config, force_refresh: bool = False) -> pd.DataFrame:
        self.measure_calls += 1
        return super().get_spc_measurements(config, force_refresh)


def test_cache_key_covers_window_scope_product_and_signature(
    decoration_root: Path,
) -> None:
    source = _CountingSource()

    # Same key -> single computation, second call hits the cache.
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k1")
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 1

    # Different window -> separate entry (correctness first).
    fetch_decorated_features(source, PROD, "none", START_DATE, "2026-08-09", "k1")
    assert source.measure_calls == 2

    # Different scope -> separate entry.
    fetch_decorated_features(source, PROD, "spc", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 3

    # Different snapshot signature -> separate entry.
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k2")
    assert source.measure_calls == 4

    # Different product -> separate entry.
    fetch_decorated_features(source, "M626", "none", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 5
