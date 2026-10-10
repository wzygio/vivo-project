"""Report-only date exclusions: aggregates, source ledgers and native caches."""

import importlib
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs import aoi_rs_service, decoration_service as rs_decoration
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.application.aoi_tt import aoi_tt_service, decoration_service as tt_decoration
from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.core.shared.sheet_oos_decoration import merge_detail_with_decoration_flags
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.shared.date_exclusion import apply_inline_date_exclusion


@pytest.fixture
def report_case(monkeypatch, tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "global.yaml").write_text(
        "application:\n  cache_ttl_hours: 4\n", encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: ["OLED"])
    monkeypatch.setattr(ConfigLoader, "get_auto_decoration_param_exemptions", lambda: [])
    monkeypatch.setattr(aoi_tt_service, "_particle_size_runtime_config", lambda: (True, 0.1, "test"))
    policy = {"value": (("OLED",), "2026-10-01", "2026-10-06")}
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: policy["value"])
    ledgers = []

    def persist(_directory, detail, *_args, key_columns, **_kwargs):
        ledgers.append(detail.copy())
        return SimpleNamespace(decoration_df=merge_detail_with_decoration_flags(
            detail, pd.DataFrame(), key_columns,
        ))

    monkeypatch.setattr(rs_decoration, "persist_sheet_oos_decoration_outcome", persist)
    monkeypatch.setattr(tt_decoration, "persist_sheet_oos_decoration_outcome", persist)
    rows = pd.DataFrame({
        "prod_code": ["P"] * 4, "factory": ["OLED", "OLED", "ARRAY", "OLED"],
        "step_id": ["1"] * 4, "sheet_id": ["A", "B", "C", "D"],
        "lot_id": ["L1", "L1", "L2", "L3"],
        "start_time": pd.to_datetime(["2026-09-30", "2026-10-01", "2026-10-02", "2026-10-07"]),
    })
    return policy, ledgers, rows


def _service_case(kind, rows):
    calls = []

    def read(_query):
        calls.append(1)
        return apply_inline_date_exclusion(source)

    if kind == "rs":
        module = aoi_rs_service
        service = module.AoiRsReportService
        config = AoiRsQueryConfig(prod_code="P", start_date="2026-09-01", end_date="2026-10-10")
        source = rows.assign(rs_code="RS", code_qty=[3., 9., 2., 1.])
        specs = pd.DataFrame([
            dict(factory="OLED", step_id="1", rs_code="RS", type_flag=kind, spec=value)
            for kind, value in [("LOT_RATIO", 4.), ("SHEET_ID", 10.)]
        ])
        port = SimpleNamespace(get_rs_details=read, get_pass_through=lambda _q: apply_inline_date_exclusion(rows),
                               get_rs_spec_limits=lambda _p: specs)
    else:
        module = aoi_tt_service
        service = module.AoiTtReportService
        config = AoiTtQueryConfig(prod_code="P", start_date="2026-09-01", end_date="2026-10-10")
        source = rows.assign(tt_name="TT", tt_qty=[3., 9., 2., 1.])
        specs = pd.DataFrame([dict(step_id="1", tt_name="TT", usl=4., ucl=10.)])
        port = SimpleNamespace(get_tt_details=read, get_tt_spec_limits=lambda _p: specs)
    fetch = getattr(service, f"fetch_aoi_{kind}_report_payload")
    facade = getattr(service, f"get_aoi_{kind}_report_data")
    fetch.clear()
    return module, source, port, calls, fetch, lambda: facade(
        _data_port=port, query_config_json=config.model_dump_json(), snapshot_signature="date-filter",
    )


@pytest.mark.parametrize("kind", ["rs", "tt"])
def test_report_filters_before_aggregation_and_config_changes_invalidate_cache(report_case, kind):
    policy, ledgers, rows = report_case
    _module, source, _port, calls, fetch, run = _service_case(kind, rows)
    before = source.copy(deep=True)
    try:
        result = run()
        details = getattr(result, f"{kind}_details_df")
        assert details.sheet_id.tolist() == ["A", "C", "D"]
        assert len(result.indicators_df) == 2
        assert ledgers[0].empty  # Excluded facts never reach OOS decoration or its ledger.
        if kind == "rs":
            lot = result.lot_points_df.set_index("lot_id").loc["L1"]
            assert (lot.rs_qty, lot.sheet_qty, lot.value) == (3., 1, 3.)
            assert result.pass_through_df.sheet_id.tolist() == ["A", "C", "D"]
            assert result.sheet_points_df.sheet_id.tolist() == ["C", "A", "D"]
        assert len(calls) == 1
        run()
        assert len(calls) == 1
        policy["value"] = (("OLED",), "2026-10-01", "2026-10-07")
        assert getattr(run(), f"{kind}_details_df").sheet_id.tolist() == ["A", "C"]
        assert len(calls) == 2
        policy["value"] = None
        assert set(getattr(run(), f"{kind}_details_df").sheet_id) == {"A", "B", "C", "D"}
        assert len(calls) == 3
        pd.testing.assert_frame_equal(source, before)
    finally:
        fetch.clear()


@pytest.mark.parametrize("kind", ["rs", "tt"])
def test_all_filtered_data_has_no_report_points_or_indicators(report_case, kind):
    policy, _ledgers, rows = report_case
    policy["value"] = (("OLED", "ARRAY"), "2026-09-01", "2026-10-10")
    _module, _source, _port, _calls, fetch, run = _service_case(kind, rows)
    try:
        result = run()
        assert getattr(result, f"{kind}_details_df").empty
        assert result.indicators_df.empty
        if kind == "rs":
            assert result.lot_points_df.empty
            assert result.sheet_points_df.empty
            assert result.pass_through_df.empty
    finally:
        fetch.clear()


@pytest.mark.parametrize("kind", ["rs", "tt"])
def test_native_payload_survives_service_reload_during_cache_fill(report_case, kind):
    _policy, _ledgers, rows = report_case
    module, source, port, _calls, fetch, run = _service_case(kind, rows)
    original_symbols = module.__dict__.copy()

    def read(_query):
        importlib.reload(module)
        return apply_inline_date_exclusion(source)

    setattr(port, f"get_{kind}_details", read)
    try:
        result = run()
        assert getattr(result, f"{kind}_details_df").sheet_id.tolist() == ["A", "C", "D"]
    finally:
        fetch.clear()
        getattr(getattr(module, f"Aoi{kind.title()}ReportService"), f"fetch_aoi_{kind}_report_payload").clear()
        module.__dict__.update(original_symbols)
