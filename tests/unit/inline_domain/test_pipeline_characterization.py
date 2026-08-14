"""Characterization tests locking the current inline pipeline behaviour.

These tests pin the observable behaviour that the upcoming refactor
(shared preparation logic -> infrastructure/measurement, scrap adapter ->
infrastructure/monitor) must preserve:

- InlineMeasurementPreparationRepository shared preparation pipeline
  (clean/dedup, LOSS exclusion, whitelist merge + data_type injection,
  outlier filtering, time-window/dimension filters, main-process trace),
  surfaced through SpcRepository.get_spc_measurements.
- SpcRepository.get_spc_spec_limits YAML override behaviour.
- InlineScrapRepository.get_scrap_data Excel -> OOC-disguised contract.
- MonitorAnalysisService.fetch_dashboard_data_dict aggregation contract.

Intentional behaviour changes (monitor CTQ decoration calibre, AOI
decoration exemption) are deliberately NOT characterized here.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc import spc_data_decoration
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.measurement.measurement_preparation import (
    InlineMeasurementPreparationRepository,
)
from src.inline_domain.infrastructure.monitor.scrap_repository import (
    InlineScrapRepository,
)
from src.inline_domain.infrastructure.spc.spc_repository import SpcRepository
from src.shared_kernel.config import ConfigLoader

PROD = "M678"
START_DATE = "2026-08-01"
END_DATE = "2026-08-10"


# ---------------------------------------------------------------------------
# Fake ports (in-memory DataFrames), following the existing test style
# ---------------------------------------------------------------------------
class FakeRawMeasurements:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self.calls: list[tuple[str, str, bool]] = []

    def get_measurements(
        self, prod_code: str, end_date: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        self.calls.append((prod_code, end_date, force_refresh))
        return self._df.copy()


class FakeMetadata:
    def __init__(self, catalog: pd.DataFrame | None, specs: pd.DataFrame | None = None) -> None:
        self._catalog = catalog
        self._specs = specs if specs is not None else pd.DataFrame()

    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame | None:
        return self._catalog

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame:
        return self._specs


class FakeMainProcessHistory:
    def __init__(self, history: pd.DataFrame | None = None) -> None:
        self._history = history if history is not None else pd.DataFrame()
        self.calls: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    def get_main_process_history(self, routed, history_start, history_end) -> pd.DataFrame:
        self.calls.append((history_start, history_end))
        return self._history


def _raw_row(
    start_time: str,
    sheet_id: str,
    step_id: str,
    param_name: str,
    site_name: str,
    param_value: object,
    factory: str = "ARRAY",
) -> dict:
    return {
        "factory": factory,
        "prod_code": PROD,
        "start_time": start_time,
        "sheet_id": sheet_id,
        "lot_id": "L1",
        "step_id": step_id,
        "param_name": param_name,
        "site_name": site_name,
        "unit_id": "EQ1",
        "param_value": param_value,
    }


def _build_shared_raw() -> pd.DataFrame:
    """Fixed raw snapshot covering every preparation pipeline stage."""
    return pd.DataFrame(
        [
            # Dedup loser: same key as the next row, earlier timestamp.
            _raw_row("2026-08-02 10:00:00", "S1", "100", "SPC_PARAM", "P1", 4.2),
            # Dedup winner (keep="last" after sorting by sheet_start_time).
            _raw_row("2026-08-03 10:00:00", "S1", "100", "SPC_PARAM", "P1", 9.9),
            _raw_row("2026-08-02 11:00:00", "S1", "100", "CTQ_PARAM", "P1", 5.1),
            _raw_row("2026-08-02 12:00:00", "S1", "100", "AOI_PARAM", "P1", 7.7),
            # LOSS keyword: physically excluded before any whitelist merge.
            _raw_row("2026-08-02 13:00:00", "S1", "100", "LOSS_YIELD", "P1", 1.0),
            # Not present in the parameter catalog: dropped by the inner merge.
            _raw_row("2026-08-02 14:00:00", "S1", "100", "UNLISTED_PARAM", "P1", 3.0),
            # Lowercase param_name: still matches the whitelist (upper merge key).
            _raw_row("2026-08-02 15:00:00", "S4", "100", "spc_param", "P1", 4.8),
            # Outside the query time window (before start_date).
            _raw_row("2026-07-15 10:00:00", "S2", "100", "SPC_PARAM", "P1", 4.4),
            # Different factory + step for dimension filter checks.
            _raw_row("2026-08-04 10:00:00", "S3", "200", "SPC_PARAM", "P1", 4.6, factory="OLED"),
        ]
    )


def _build_catalog() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ref_param_name": "SPC_PARAM", "data_type": "SPC"},
            {"ref_param_name": "CTQ_PARAM", "data_type": "CTQ"},
            # NULL data_type classifies to AOI.
            {"ref_param_name": "AOI_PARAM", "data_type": None},
        ]
    )


def _query(**overrides) -> SpcQueryConfig:
    params = {
        "prod_code": PROD,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "data_type_filter": "ALL",
    }
    params.update(overrides)
    return SpcQueryConfig(**params)


def _build_repository(
    monkeypatch,
    raw_df: pd.DataFrame,
    catalog: pd.DataFrame | None,
    specs: pd.DataFrame | None = None,
    history: pd.DataFrame | None = None,
) -> tuple[SpcRepository, FakeRawMeasurements, FakeMainProcessHistory]:
    """Build a repository with the outlier filter neutralized (existing pattern)."""
    monkeypatch.setattr(
        InlineMeasurementPreparationRepository,
        "_apply_outlier_filters",
        lambda _self, measurements, _prod: measurements,
    )
    raw = FakeRawMeasurements(raw_df)
    main_history = FakeMainProcessHistory(history)
    repository = SpcRepository(
        InlineMeasurementPreparationRepository(
            raw_measurements=raw,
            metadata=FakeMetadata(catalog, specs),
            main_process_history=main_history,
        )
    )
    return repository, raw, main_history


# ---------------------------------------------------------------------------
# get_spc_measurements: shared preparation pipeline
# ---------------------------------------------------------------------------
def test_pipeline_cleans_dedups_merges_whitelist_and_traces(monkeypatch) -> None:
    repository, raw, main_history = _build_repository(
        monkeypatch, _build_shared_raw(), _build_catalog()
    )

    result = repository.get_spc_measurements(_query(), force_refresh=True)

    # Port call contract: raw snapshot keyed by (prod_code, end_date, force_refresh).
    assert raw.calls == [(PROD, END_DATE, True)]

    # Observable row set after the full pipeline.
    assert len(result) == 5
    assert set(result["param_name"]) == {"SPC_PARAM", "spc_param", "CTQ_PARAM", "AOI_PARAM"}

    # start_time renamed to sheet_start_time and coerced to datetime.
    assert "start_time" not in result.columns
    assert pd.api.types.is_datetime64_any_dtype(result["sheet_start_time"])

    # Dedup keeps the latest record per (prod, factory, sheet, step, param, site).
    s1_spc = result[
        (result["sheet_id"] == "S1") & (result["param_name"] == "SPC_PARAM")
    ]
    assert s1_spc["param_value"].tolist() == [9.9]

    # data_type injected from the whitelist; NULL classified as AOI;
    # the lowercase param_name still matches the whitelist.
    data_type_by_param = dict(zip(result["param_name"], result["data_type"]))
    assert data_type_by_param == {
        "SPC_PARAM": "SPC",
        "spc_param": "SPC",
        "CTQ_PARAM": "CTQ",
        "AOI_PARAM": "AOI",
    }

    # Main-process trace: empty specs fall back to step_id/EQP, empty history
    # falls back to the measurement unit.
    assert set(result["main_step_id"]) == {"100", "200"}
    assert set(result["main_eqp_type"]) == {"EQP"}
    assert set(result["main_process_unit_id"]) == {"EQ1"}
    assert set(result["main_process_trace_source"]) == {"measurement_unit_fallback"}
    assert result["main_process_event_time"].isna().all()

    # History window: one month before start up to end_date.
    assert main_history.calls == [
        (pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-10"))
    ]


@pytest.mark.parametrize(
    ("data_type_filter", "expected_params", "expected_rows"),
    [
        ("SPC", {"SPC_PARAM", "spc_param"}, 3),
        ("CTQ", {"CTQ_PARAM"}, 1),
        ("AOI", {"AOI_PARAM"}, 1),
    ],
)
def test_pipeline_data_type_filter_restricts_whitelist_merge(
    monkeypatch, data_type_filter: str, expected_params: set, expected_rows: int
) -> None:
    repository, _, _ = _build_repository(monkeypatch, _build_shared_raw(), _build_catalog())

    result = repository.get_spc_measurements(_query(data_type_filter=data_type_filter))

    assert len(result) == expected_rows
    assert set(result["param_name"]) == expected_params
    assert set(result["data_type"]) == {data_type_filter}


def test_pipeline_time_window_is_start_inclusive_end_exclusive(monkeypatch) -> None:
    repository, _, _ = _build_repository(monkeypatch, _build_shared_raw(), _build_catalog())

    # end_date "2026-08-03" -> upper bound 2026-08-04 (exclusive):
    # the 2026-08-04 OLED row and the 2026-07-15 row are both excluded.
    result = repository.get_spc_measurements(_query(end_date="2026-08-03"))

    assert len(result) == 4
    assert result["sheet_start_time"].min() >= pd.Timestamp("2026-08-03") - pd.Timedelta(
        days=2
    )
    assert "S3" not in set(result["sheet_id"])
    assert "S2" not in set(result["sheet_id"])


def test_pipeline_dimension_filters(monkeypatch) -> None:
    repository, _, _ = _build_repository(monkeypatch, _build_shared_raw(), _build_catalog())

    # factory filter is case-insensitive on the config side (upper-cased).
    array_only = repository.get_spc_measurements(_query(factory="array"))
    assert len(array_only) == 4
    assert set(array_only["factory"]) == {"ARRAY"}

    oled_only = repository.get_spc_measurements(_query(factory="OLED"))
    assert oled_only["sheet_id"].tolist() == ["S3"]

    step_only = repository.get_spc_measurements(_query(step_id="200"))
    assert step_only["sheet_id"].tolist() == ["S3"]

    param_only = repository.get_spc_measurements(_query(param_name="CTQ_PARAM"))
    assert param_only["param_name"].tolist() == ["CTQ_PARAM"]


def test_pipeline_empty_catalog_returns_empty(monkeypatch) -> None:
    repository, _, _ = _build_repository(
        monkeypatch, _build_shared_raw(), pd.DataFrame()
    )

    result = repository.get_spc_measurements(_query())

    assert result.empty


def test_pipeline_missing_catalog_marks_all_rows_unknown(monkeypatch) -> None:
    repository, _, _ = _build_repository(monkeypatch, _build_shared_raw(), None)

    result = repository.get_spc_measurements(_query())

    # No whitelist merge: UNLISTED_PARAM survives; LOSS rows are still excluded.
    assert "UNLISTED_PARAM" in set(result["param_name"])
    assert "LOSS_YIELD" not in set(result["param_name"])
    assert set(result["data_type"]) == {"UNKNOWN"}


def test_pipeline_outlier_filter_drops_out_of_bounds_values(
    monkeypatch, tmp_path: Path
) -> None:
    """Exercise the real _apply_outlier_filters through the CSV fallback rules."""
    rules_dir = tmp_path / "output" / "decrypted_files"
    rules_dir.mkdir(parents=True)
    (rules_dir / "spc_outlier_filters.csv").write_text(
        "prod_col,step_col,param_col,lower_col,upper_col\n"
        "ALL,100,SPC_PARAM,1.0,100.0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )

    raw_df = pd.DataFrame(
        [
            _raw_row("2026-08-02 10:00:00", "S1", "100", "SPC_PARAM", "P1", 4.2),
            _raw_row("2026-08-02 10:00:00", "S2", "100", "SPC_PARAM", "P1", 0.5),
            _raw_row("2026-08-02 10:00:00", "S3", "100", "SPC_PARAM", "P1", 150.0),
        ]
    )
    catalog = pd.DataFrame([{"ref_param_name": "SPC_PARAM", "data_type": "SPC"}])
    repository = SpcRepository(
        InlineMeasurementPreparationRepository(
            raw_measurements=FakeRawMeasurements(raw_df),
            metadata=FakeMetadata(catalog),
            main_process_history=FakeMainProcessHistory(),
        )
    )

    result = repository.get_spc_measurements(_query())

    # value <= lower (0.5) and value >= upper (150.0) are physically removed.
    assert result["sheet_id"].tolist() == ["S1"]
    assert result["param_value"].tolist() == [4.2]


# ---------------------------------------------------------------------------
# get_spc_spec_limits: YAML override behaviour
# ---------------------------------------------------------------------------
def _build_spec_df() -> pd.DataFrame:
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
            },
            {
                "prod_code": PROD,
                "step_id": "200",
                "param_name": "THK",
                "usl": 60.0,
                "lsl": 40.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
        ]
    )


def test_spec_limits_apply_yaml_overrides(monkeypatch, tmp_path: Path) -> None:
    product_config_dir = tmp_path / "config" / "products"
    product_config_dir.mkdir(parents=True)
    (product_config_dir / f"{PROD}.yaml").write_text(
        "spc_spec_override:\n"
        "  - prod_code: M678\n"
        '    step_id: "100"\n'
        "    param_name: THK\n"
        "    ucl: 55.0\n"
        "    lcl: 45.0\n"
        '  - step_id: "200"\n'
        "    param_name: THK\n"
        "    usl: 99.0\n"
        "  - prod_code: M626\n"
        '    step_id: "100"\n'
        "    param_name: THK\n"
        "    ucl: 1.0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    repository = SpcRepository(
        InlineMeasurementPreparationRepository(
            raw_measurements=FakeRawMeasurements(pd.DataFrame()),
            metadata=FakeMetadata(pd.DataFrame(), _build_spec_df()),
            main_process_history=FakeMainProcessHistory(),
        )
    )

    result = repository.get_spc_spec_limits(PROD)

    assert len(result) == 2
    row_100 = result[result["step_id"] == "100"].iloc[0]
    # Matched override (prod_code + step_id + param_name) applies ucl/lcl only.
    assert row_100["ucl"] == 55.0
    assert row_100["lcl"] == 45.0
    assert row_100["usl"] == 60.0
    assert row_100["target"] == 50.0
    # Override without prod_code applies to any product.
    row_200 = result[result["step_id"] == "200"].iloc[0]
    assert row_200["usl"] == 99.0
    assert row_200["ucl"] == 54.0
    # Override for another product is skipped (row_100 ucl stays 55.0, not 1.0).


def test_spec_limits_pass_through_without_product_yaml(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    repository = SpcRepository(
        InlineMeasurementPreparationRepository(
            raw_measurements=FakeRawMeasurements(pd.DataFrame()),
            metadata=FakeMetadata(pd.DataFrame(), _build_spec_df()),
            main_process_history=FakeMainProcessHistory(),
        )
    )

    result = repository.get_spc_spec_limits(PROD)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), _build_spec_df().reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# get_scrap_data: Excel -> OOC-disguised contract
# ---------------------------------------------------------------------------
def _write_scrap_excel(scrap_path: Path) -> None:
    scrap_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"产品型号": "M678", "Sheet_ID": "SH1", "报废时间": "2026-08-01", "报废站点": "21230"},
            {"产品型号": "m678", "Sheet_ID": "SH2", "报废时间": "2026-08-03", "报废站点": "21230"},
            {"产品型号": "M626", "Sheet_ID": "SH3", "报废时间": "2026-08-02", "报废站点": "31230"},
        ]
    ).to_excel(scrap_path, index=False, engine="openpyxl")


def test_scrap_data_transforms_excel_into_ooc_contract(
    monkeypatch, tmp_path: Path
) -> None:
    _write_scrap_excel(tmp_path / "resources" / "scrap_sheets.xlsx")
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "scrap_factory_mapping.yaml").write_text(
        'mappings:\n  "21230": ARRAY\ndefault_prefix_rules:\n  "31": OLED\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    repository = InlineScrapRepository()

    result = repository.get_scrap_data(PROD)

    # prod_code filter is case-insensitive: M678 + m678 rows kept, M626 dropped.
    assert len(result) == 2
    assert set(result["sheet_id"]) == {"SH1", "SH2"}

    # Column-name standardization and type coercion
    # (报废站点 is read back from Excel as an integer).
    assert pd.api.types.is_datetime64_any_dtype(result["sheet_start_time"])
    assert set(result["step_id"].astype(str)) == {"21230"}

    # Factory inferred from the scrap step mapping.
    assert set(result["factory"]) == {"ARRAY"}

    # OOC disguise contract consumed by the aggregation engine.
    assert set(result["is_ooc"]) == {1}
    assert set(result["is_oos"]) == {0}
    assert set(result["is_soos"]) == {0}
    assert set(result["param_name"]) == {"报废"}
    assert set(result["site_name"]) == {"报废"}
    assert set(result["data_type"]) == {"报废"}
    assert set(result["spc_status"]) == {"OOC"}

    # Placeholder columns required by apply_spc_rules-compatible outputs.
    for col in ["sheet_mean", "sheet_max", "sheet_min", "usl", "lsl", "ucl", "lcl"]:
        assert col in result.columns
        assert result[col].isna().all()


def test_scrap_data_returns_empty_when_excel_missing(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    repository = InlineScrapRepository()

    result = repository.get_scrap_data(PROD)

    assert result.empty


# ---------------------------------------------------------------------------
# MonitorAnalysisService.fetch_dashboard_data_dict: aggregation contract
# ---------------------------------------------------------------------------
class _FakeMonitorRepository:
    """In-memory repository producing one OK sheet and one OOC sheet."""

    scrap_calls: list[str] = []

    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        self.scrap_calls.append(prod_code)
        return pd.DataFrame()

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        rows = []
        for sheet_id, start_time, value in (
            ("S1", "2026-08-08 09:00:00", 50.0),  # mean 50.0 inside ucl/lcl -> OK
            ("S2", "2026-08-09 09:00:00", 55.0),  # mean 55.0 > ucl 54, < usl 60 -> OOC
        ):
            for site_name in ("P1", "P2"):
                rows.append(
                    {
                        "factory": "ARRAY",
                        "prod_code": config.prod_code,
                        "sheet_start_time": start_time,
                        "sheet_id": sheet_id,
                        "step_id": "100",
                        "param_name": "THK",
                        "site_name": site_name,
                        "param_value": value,
                        "data_type": "SPC",
                    }
                )
        return pd.DataFrame(rows)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
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


def test_monitor_dashboard_aggregation_contract(monkeypatch, tmp_path: Path) -> None:
    MonitorAnalysisService.fetch_dashboard_data_dict.clear()
    _FakeMonitorRepository.scrap_calls = []
    # Pin the 3-month analysis window so time buckets are deterministic.
    MonitorAnalysisService.set_analysis_end_date(datetime(2026, 8, 10))
    # Keep the sheet-OOS decoration workbook inside tmp_path.
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-08-10",
        data_type_filter="SPC",
    )
    try:
        result = MonitorAnalysisService.fetch_dashboard_data_dict(
            _repository_factory=lambda _prod: _FakeMonitorRepository(),
            query_config_json=query.model_dump_json(),
            time_type="MIXED",
            data_type_filter="SPC",
            snapshot_signature="characterization-monitor-aggregation",
        )
    finally:
        MonitorAnalysisService.set_analysis_end_date(None)

    assert set(result) == {"global_summary_df", "detail_df", "station_detail_df"}
    global_summary_df = result["global_summary_df"]
    detail_df = result["detail_df"]
    station_detail_df = result["station_detail_df"]

    # The scrap branch is only entered for 报废/ALL monitoring types.
    assert _FakeMonitorRepository.scrap_calls == []

    # --- station detail: aggregated before time-bucket expansion (1:1 physical).
    assert len(station_detail_df) == 1
    station = station_detail_df.iloc[0]
    assert (station["prod_code"], station["factory"], station["step_id"]) == (
        "M626",
        "ARRAY",
        "100",
    )
    assert station["data_type"] == "SPC"
    assert station["抽检数"] == 2
    assert station["OOS片数"] == 0
    assert station["OOC片数"] == 1
    assert station["SOOS片数"] == 0
    assert station["OOC"] == pytest.approx(0.5)

    # --- global summary: MIXED scaffolding = 3 months + 3 weeks + 7 days.
    assert "sort_index" not in global_summary_df.columns
    assert set(global_summary_df["time_group"]) == {
        "2026M08", "2026M07", "2026M06",
        "2026W33", "2026W32", "2026W31",
        "20260810", "20260809", "20260808", "20260807",
        "20260806", "20260805", "20260804",
    }
    # Each bucket carries one dummy scaffold row; day buckets add the real sheet.
    day_ooc = global_summary_df.set_index("time_group")
    assert day_ooc.loc["20260809", "抽检数"] == 2
    assert day_ooc.loc["20260809", "OOC片数"] == 1
    assert day_ooc.loc["20260808", "抽检数"] == 2
    assert day_ooc.loc["20260808", "OOC片数"] == 0
    assert day_ooc.loc["2026M08", "抽检数"] == 3
    assert day_ooc.loc["2026M08", "OOC片数"] == 1

    # --- detail: dummy scaffold rows (NULL data_type) are excluded by the
    # data_type groupby, so day buckets hold only the real sheets.
    detail_ooc = detail_df[
        (detail_df["prod_code"] == "M626") & (detail_df["data_type"] == "SPC")
    ].set_index("time_group")
    assert detail_ooc.loc["20260809", "抽检数"] == 1
    assert detail_ooc.loc["20260809", "OOC片数"] == 1
    assert detail_ooc.loc["20260808", "OOC片数"] == 0
