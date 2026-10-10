"""Excluded display dates never leave source adapters, including cache/fallback."""

from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.fixture
def policy(monkeypatch):
    current = [(("OLED", "TP"), "2026-10-01", "2026-10-06")]
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: current[0])
    monkeypatch.setattr(
        ConfigLoader, "get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=True, offset_days=4),
    )
    return current


def source_rows():
    return pd.DataFrame({
        "factory": ["OLED", "OLED", "TP", "ARRAY", "OLED"],
        "prod_code": ["P"] * 5,
        "start_time": pd.to_datetime([
            "2026-09-26 08:00", "2026-09-27 00:00", "2026-10-02 23:59",
            "2026-09-28 08:00", "2026-10-03 08:00",
        ]),
        "sheet_id": ["BEFORE", "START", "END", "ARRAY", "AFTER"],
        "lot_id": ["L"] * 5, "step_id": ["1"] * 5,
        "param_name": ["CD"] * 5, "site_name": ["P1"] * 5,
        "unit_id": ["EQ"] * 5, "param_value": [5., 100., 100., 6., 7.],
    })


@pytest.mark.parametrize("read_mode", ["new", "cached", "fallback", "refresh"])
def test_measurement_output_excludes_dates_without_rewriting_source(tmp_path, policy, read_mode):
    raw = source_rows()
    before = raw.copy(deep=True)
    calls = []

    def load(*_args):
        calls.append(1)
        return raw

    repository = InlineMeasurementSnapshotRepository(tmp_path, object(), measurement_loader=load)
    repository.get_measurements("P", "2026-10-09")
    snapshot = tmp_path / "inline_measurements_P.parquet"
    source_bytes = snapshot.read_bytes()
    if read_mode == "fallback":
        def fail(*_args):
            raise RuntimeError("database unavailable")
        repository.measurement_loader = fail
        result = repository.get_measurements("P", "2026-10-09", force_refresh=True)
    elif read_mode == "refresh":
        result = repository.refresh_measurements("P", "2026-10-09")
        assert result.refreshed_from_db
        result = result.measurements
    else:
        result = repository.get_measurements("P", "2026-10-09", force_refresh=read_mode == "new")
    assert result.sheet_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
    pd.testing.assert_frame_equal(raw, before)
    assert pd.read_parquet(snapshot).sheet_id.tolist() == raw.sheet_id.tolist()
    if read_mode in {"cached", "fallback"}:
        assert snapshot.read_bytes() == source_bytes
    if read_mode == "cached":
        assert len(calls) == 1
        policy[0] = None
        assert len(repository.get_measurements("P", "2026-10-09")) == 5
        assert len(calls) == 1


@pytest.mark.parametrize("kind", ["details", "throughput"])
def test_rs_source_and_denominator_are_filtered_on_fresh_and_cached_reads(tmp_path, policy, kind):
    raw = source_rows().drop(columns=["param_name", "site_name", "unit_id", "param_value"])
    details = raw.assign(rs_code="RS", code_qty=1)
    repository = AoiRsSnapshotRepository(
        tmp_path, SimpleNamespace(), details_loader=lambda *_args: details,
        pass_through_loader=lambda *_args: raw,
    )
    query = AoiRsQueryConfig(prod_code="P", start_date="2026-09-01", end_date="2026-10-09")
    read = repository.get_rs_details if kind == "details" else repository.get_pass_through
    assert read(query).sheet_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
    assert read(query).sheet_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
    prefix = "aoi_rs_details" if kind == "details" else "aoi_rs_pass_through"
    assert len(pd.read_parquet(tmp_path / f"{prefix}_P.parquet")) == 5
    policy[0] = None
    assert len(read(query)) == 5


def test_exclusion_precedes_latest_point_selection_and_oos_decoration(tmp_path, policy, monkeypatch):
    from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.infrastructure.shared import measurement_preparation as module

    # A bad reinspection during the excluded dates must not replace the older point.
    raw = source_rows().iloc[[0, 1, 3]].copy()
    raw["start_time"] += pd.Timedelta(days=4)
    raw.loc[raw.sheet_id.eq("START"), "sheet_id"] = "BEFORE"
    metadata = SimpleNamespace(get_parameter_catalog=lambda _p: None)
    repository = module.InlineMeasurementPreparationRepository(
        SimpleNamespace(get_measurements=lambda *_a: raw), metadata,
        SimpleNamespace(get_main_process_history=lambda *_a, **_kw: pd.DataFrame()),
    )
    seen = []

    def outlier_input(frame, _product):
        seen.extend(frame.param_value.tolist())
        return frame

    monkeypatch.setattr(repository, "_apply_outlier_filters", outlier_input)
    monkeypatch.setattr(metadata, "get_parameter_specs", lambda _p: pd.DataFrame(), raising=False)
    monkeypatch.setattr(module, "attach_main_process_spec", lambda frame, _specs: frame)
    monkeypatch.setattr(module, "apply_main_process_history", lambda frame, _history: frame)
    query = SpcQueryConfig(prod_code="P", start_date="2026-09-01", end_date="2026-10-09")
    prepared = repository.get_prepared_measurements(query)
    assert prepared.param_value.tolist() == [5., 6.]
    assert seen == [5., 6.]
    specs = pd.DataFrame([dict(prod_code="P", step_id="1", param_name="CD", usl=10., lsl=2., ucl=8., lcl=3.)])
    decorated = prepare_decorated_data(prepared, specs, "P", "spc", product_dir=tmp_path, persist=False)
    assert decorated.sheet_oos_decoration_result.decoration_df.empty
    assert decorated.raw_measurements_df.param_value.tolist() == [5., 6.]
    assert decorated.original_sheet_features_df.sheet_max.tolist() == [6., 5.]


def test_particle_sql_windows_cannot_aggregate_excluded_counts(policy):
    from src.inline_domain.infrastructure.aoi_tt.particle_size_loader import load_particle_size_counts
    from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig

    calls = []

    def load(_db, **kwargs):
        calls.append((kwargs["start_time"], kwargs["end_time"]))
        return pd.DataFrame()

    query = AoiTtQueryConfig(prod_code="P", factory="TP", start_date="2026-09-30", end_date="2026-10-07")
    result = load_particle_size_counts(object(), query, tp_data_loader=load)
    assert result.empty
    # Display [Oct 1, Oct 7) maps to source [Sep 27, Oct 3), and is never queried.
    assert calls == [
        (pd.Timestamp("2026-09-26").to_pydatetime(), pd.Timestamp("2026-09-27").to_pydatetime()),
        (pd.Timestamp("2026-10-03").to_pydatetime(), pd.Timestamp("2026-10-04").to_pydatetime()),
    ]


def test_tt_excluded_reinspection_cannot_replace_eligible_sheet(policy):
    from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
    from src.inline_domain.infrastructure.aoi_tt.aoi_tt_repository import AoiTtRepository

    raw = source_rows().iloc[[0, 1]].copy()
    raw["start_time"] += pd.Timedelta(days=4)
    raw["sheet_id"] = "SAME"
    specs = pd.DataFrame([dict(prod_code="P", step_id="1", param_name="CD", param_type=None)])
    repository = AoiTtRepository(
        raw_measurements=SimpleNamespace(get_measurements=lambda *_a: raw),
        metadata=SimpleNamespace(get_parameter_specs=lambda _p: specs),
    )
    query = AoiTtQueryConfig(prod_code="P", start_date="2026-09-01", end_date="2026-10-09")
    result = repository.get_tt_details(query)
    assert result.tt_qty.tolist() == [5.]
    policy[0] = None
    assert repository.get_tt_details(query).tt_qty.tolist() == [100.]


def test_scrap_adapter_excludes_shifted_dates_without_changing_source(tmp_path, policy, monkeypatch):
    from src.inline_domain.infrastructure.monitor.scrap_repository import InlineScrapRepository

    raw = source_rows()
    path = tmp_path / "scrap.xlsx"
    raw.rename(columns={"start_time": "warehousing_time"}).to_excel(path, index=False)
    before = path.read_bytes()
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *_a: path)
    monkeypatch.setattr(
        InlineScrapRepository, "_infer_factory_from_step", staticmethod(lambda _step: "OLED"),
    )
    result = InlineScrapRepository().get_scrap_data("P")
    assert result.sheet_id.tolist() == ["BEFORE", "AFTER"]
    assert path.read_bytes() == before


@pytest.mark.parametrize("alarm_type", ["oos", "ooc"])
def test_history_reads_and_updates_preserve_excluded_source_rows(tmp_path, policy, alarm_type):
    from src.inline_domain.infrastructure.shared.oos_history_store import OosHistoryStore
    from src.inline_domain.infrastructure.shared.ooc_history_store import OocHistoryStore

    facts = source_rows().rename(columns={"start_time": "sheet_start_time"})
    facts["sheet_start_time"] += pd.Timedelta(days=4)
    facts = facts.assign(sheet_max=11., sheet_min=5., sheet_mean=8., usl=10., lsl=2.,
                         ucl=7., lcl=3., oos_type="USL", ooc_type="UCL")
    store = OosHistoryStore(tmp_path) if alarm_type == "oos" else OocHistoryStore(tmp_path)
    store.update("spc", "P", facts, coverage_start=pd.Timestamp("2026-09-01"),
                 coverage_end=pd.Timestamp("2026-10-10"))
    path = store.snapshot_path("spc", "P")
    before = path.read_bytes()
    assert store.read("spc", "P").frame.sheet_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
    assert path.read_bytes() == before
    store.update("spc", "P", facts.iloc[0:0], coverage_start=pd.Timestamp("2026-10-08"),
                 coverage_end=pd.Timestamp("2026-10-09"))
    assert len(pd.read_parquet(path)) == 5
    policy[0] = None
    assert len(store.read("spc", "P").frame) == 5


def test_throughput_history_filters_output_without_pruning_other_dates_on_update(tmp_path, policy):
    from src.inline_domain.core.shared.throughput_facts import build_daily_throughput_facts
    from src.inline_domain.infrastructure.shared.throughput_history_store import ThroughputHistoryStore

    raw = source_rows()
    raw["start_time"] += pd.Timedelta(days=4)
    facts = build_daily_throughput_facts(raw, scope="spc", prod_code="P",
                                       event_time_column="start_time", item_id_column="sheet_id")
    store = ThroughputHistoryStore(tmp_path)
    result = store.update("spc", "P", facts, coverage_start=pd.Timestamp("2026-09-01"),
                          coverage_end=pd.Timestamp("2026-10-10"))
    assert result.item_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
    path = store.snapshot_path("spc", "P")
    before = path.read_bytes()
    assert len(store.read("spc", "P")) == 3
    assert path.read_bytes() == before
    store.update("spc", "P", facts.iloc[0:0], coverage_start=pd.Timestamp("2026-10-08"),
                 coverage_end=pd.Timestamp("2026-10-09"))
    assert len(pd.read_parquet(path)) == 5


def test_excel_alarm_output_filters_current_policy_without_rewriting_workbook(tmp_path, policy):
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore, clear_excel_alarm_cache
    from src.inline_domain.infrastructure.shared.oos_decision_repository import OosDecisionWorkbookRepository

    facts = source_rows().rename(columns={"start_time": "sheet_start_time"})
    facts["sheet_start_time"] += pd.Timedelta(days=4)
    facts["flag"] = False
    path = tmp_path / "spc_sheet_oos_decoration.xlsx"
    facts.to_excel(path, sheet_name="P", index=False)
    before = path.read_bytes()
    store = ExcelAlarmStore({"spc": path})
    decisions = OosDecisionWorkbookRepository(tmp_path)
    try:
        assert store.read("spc", "P")["frame"].sheet_id.tolist() == ["BEFORE", "ARRAY", "AFTER"]
        assert len(decisions.load_fallback(path.name, "P", ["sheet_id"])) == 3
        policy[0] = None
        assert len(store.read("spc", "P")["frame"]) == 5
        assert len(decisions.load_fallback(path.name, "P", ["sheet_id"])) == 5
        assert path.read_bytes() == before
    finally:
        clear_excel_alarm_cache()


@pytest.mark.parametrize("scope", ["aoi_tt", "aoi_rs"])
def test_configurable_workbook_and_page_cache_follow_policy(tmp_path, policy, monkeypatch, scope):
    from src.inline_domain.infrastructure.shared.oos_decision_repository import OosDecisionWorkbookRepository
    from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import load_sheet_oos_decoration

    facts = source_rows()
    facts["start_time"] += pd.Timedelta(days=4)
    if scope == "aoi_rs":
        facts = facts.rename(columns={"start_time": "sheet_start_time"})
    facts = facts.assign(tt_name="TT", rs_code="RS", chart_kind="sheet", flag=False)
    path = tmp_path / "custom-alarms.xlsx"
    facts.to_excel(path, sheet_name="P", index=False)
    before = path.read_bytes()
    keys = ["prod_code", "sheet_id"]
    assert len(load_sheet_oos_decoration(tmp_path, path.name, "P", key_columns=keys)) == 3
    assert len(OosDecisionWorkbookRepository(tmp_path).load_fallback(path.name, "P", keys)) == 3
    if scope == "aoi_tt":
        from app.sections.inline_domain.aoi_tt import aoi_tt_dashboard as dashboard
        monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *_a: path)
        cache = dashboard._load_aoi_tt_oos_decoration_cached
        read = lambda: dashboard.load_aoi_tt_oos_decoration("P")
    else:
        from app.sections.inline_domain.aoi_rs import aoi_rs_dashboard as dashboard
        stat = path.stat()
        cache = dashboard._load_cached_aoi_rs_sheet_oos_decoration
        read = lambda: dashboard.load_cached_aoi_rs_sheet_oos_decoration(
            stat.st_mtime_ns, stat.st_size, "P", str(tmp_path), path.name,
        )
    cache.clear()
    try:
        assert len(read()) == 3
        policy[0] = None
        assert len(read()) == 5
        policy[0] = (("OLED", "TP"), "2026-09-30", "2026-10-07")
        assert read().sheet_id.tolist() == ["ARRAY"]
        assert path.read_bytes() == before
    finally:
        cache.clear()
