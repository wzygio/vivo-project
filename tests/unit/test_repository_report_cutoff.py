"""Repository output boundaries must not truncate persisted source coverage."""

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy


@pytest.fixture
def report_clock(monkeypatch):
    clock = {"day": "2026-09-11"}
    monkeypatch.setattr(
        ReportCutoffPolicy, "boundary",
        lambda self, now=None: pd.Timestamp(f"{clock['day']} {self.latest_day_time}"),
    )
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(True, 4))
    return clock


def source_times():
    return pd.to_datetime(["2026-09-06 18:00:00", "2026-09-07 12:00:00", "2026-09-07 12:00:01"])


def test_shared_measurements_cut_both_read_and_refresh_but_keep_raw_snapshot(tmp_path, report_clock):
    from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository

    calls = []
    def loader(*args):
        calls.append(args)
        return pd.DataFrame({"start_time": source_times(), "param_value": [1, 2, 3]})

    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    assert repo.get_measurements("M1", "2026-09-11").param_value.tolist() == [1, 2]
    assert len(pd.read_parquet(tmp_path / "inline_measurements_M1.parquet")) == 3
    assert repo.refresh_measurements("M1", "2026-09-11").measurements.param_value.tolist() == [1, 2]
    report_clock["day"] = "2026-09-12"
    assert repo.get_measurements("M1", "2026-09-11").param_value.tolist() == [1, 2, 3]
    assert len(calls) == 2


def test_rs_snapshot_keeps_afternoon_for_tomorrow(tmp_path, report_clock):
    from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
    from src.inline_domain.infrastructure.aoi_rs.data_loader import RS_DETAIL_COLUMNS
    from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository

    source = pd.DataFrame({"start_time": source_times(), "code_qty": [1, 2, 3]}).reindex(columns=RS_DETAIL_COLUMNS)
    repo = AoiRsSnapshotRepository(tmp_path, object(), details_loader=lambda *args: source.copy())
    query = AoiRsQueryConfig(prod_code="M1", start_date="2026-09-01", end_date="2026-09-11")
    assert repo.get_rs_details(query).code_qty.tolist() == [1, 2]
    assert len(pd.read_parquet(tmp_path / "aoi_rs_details_M1.parquet")) == 3
    report_clock["day"] = "2026-09-12"
    assert repo.get_rs_details(query).code_qty.tolist() == [1, 2, 3]


@pytest.mark.parametrize("snapshot", [False, True])
def test_qtime_direct_and_snapshot_cut_report_output(tmp_path, monkeypatch, report_clock, snapshot):
    from src.indicator_domain.application.qtime.dtos import QTimeQuery
    from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository, DETAIL_COLUMNS

    source = pd.DataFrame({
        "timekey": ["20260906180000000000", "20260907120000000000", "20260907120000000001"],
        "lot_id": ["old", "noon", "afternoon"], "step_desc": "A->B", "shop": "ARRAY",
    }).reindex(columns=DETAIL_COLUMNS)
    repo = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path if snapshot else None)
    monkeypatch.setattr(repo, "_read_frame", lambda *args, **kwargs: source.copy())
    query = QTimeQuery(start_time=datetime(2026, 9, 1), end_time=datetime(2026, 9, 12), shop="ARRAY", step_descriptions=("A->B",))
    assert set(repo.fetch_details(query).lot_id) == {"old", "noon"}
    if snapshot:
        assert len(pd.read_parquet(repo._snapshot_store.source_path("ARRAY"))) == 3
    report_clock["day"] = "2026-09-12"
    assert set(repo.fetch_details(query).lot_id) == {"old", "noon", "afternoon"}


@pytest.mark.parametrize("method", ["fetch_details", "fetch_daily_counts", "fetch_glass_ratios"])
def test_ijp_caps_before_sql_aggregation(monkeypatch, report_clock, method):
    from src.indicator_domain.application.ijp.dtos import IjpQuery
    from src.indicator_domain.infrastructure.ijp.repository import IjpRepository

    repo = IjpRepository(SimpleNamespace(engine=object()))
    calls = []
    def read(*args, **kwargs):
        calls.append(kwargs["params"])
        return pd.DataFrame(columns=["print_time", "productcode", "line", "printer", "glass_id", "rs_code", "code_num", "day"])
    monkeypatch.setattr(repo, "_read_frame", read)
    query = IjpQuery(start_time=datetime(2026, 9, 1), end_time=datetime(2026, 9, 11, 23, 59, 59))
    getattr(repo, method)(query)
    assert calls[-1]["end_time"] == "2026-09-07 12:00:00"
    getattr(repo, method)(query.model_copy(update={"end_time": datetime(2026, 9, 4, 23, 59, 59)}))
    assert calls[-1]["end_time"] == "2026-08-31 23:59:59"


def test_yield_uses_actual_ship_time_only_for_latest_business_day(tmp_path, monkeypatch, report_clock):
    from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
    from src.yield_domain.infrastructure.repositories.yield_repository import PanelRepository

    raw = pd.DataFrame({
        "warehousing_time": ["2026-09-06", "2026-09-07", "2026-09-07"],
        "warehousing_event_time": ["20260907180000123456", "20260907120000000000", "20260907120000000001"],
        "panel_id": ["historical", "noon", "afternoon"],
    })
    repo = PanelRepository(tmp_path / "yield.parquet", YieldDataPolicy(work_order_types=("P",)), db_manager=SimpleNamespace(engine=object()))
    calls = []
    def fetch(*args):
        calls.append(args)
        return raw.copy()
    monkeypatch.setattr(repo, "_fetch_from_db_in_chunks", fetch)
    query = YieldQueryConfig(start_date="2026-09-01", end_date="2026-09-11", product_code="M1")
    actual = repo.get_panel_details(query)
    assert actual.panel_id.tolist() == ["historical", "noon"]
    assert "warehousing_event_time" not in actual
    assert len(pd.read_parquet(tmp_path / "yield.parquet")) == 3
    report_clock["day"] = "2026-09-12"
    assert repo.get_panel_details(query).panel_id.tolist() == ["historical", "noon", "afternoon"]
    assert len(calls) == 1


def test_equipment_uses_real_today_without_forwarding(monkeypatch, report_clock):
    from src.equipment_domain.infrastructure import data_loader

    raw = pd.DataFrame({"glass_start_time": pd.to_datetime(["2026-09-10 18:00", "2026-09-11 12:00", "2026-09-11 13:00"])})
    monkeypatch.setattr(data_loader, "load_part_life_snapshot", lambda *args: raw.copy())
    monkeypatch.setattr(data_loader, "load_fabricated_part_life_snapshot", lambda *args, **kwargs: raw.copy())
    real, fabricated = data_loader.load_report_part_life_snapshots(object(), pd.DataFrame(), as_of=pd.Timestamp("2026-09-11 18:00"), max_age_days=3)
    assert len(real) == len(fabricated) == 2
    assert len(raw) == 3


def test_particle_counts_limit_rows_before_min_and_count(report_clock):
    from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
    from src.inline_domain.infrastructure.aoi_tt.particle_size_loader import load_particle_size_counts

    calls = []
    def aggregate(_db, **kwargs):
        calls.append(kwargs)
        rows = source_times()[source_times() < kwargs["end_time"]]
        return pd.DataFrame({"start_time": [rows.min()], "particle_qty": [len(rows)]})
    query = AoiTtQueryConfig(prod_code="M1", factory="ARRAY", start_date="2026-09-01", end_date="2026-09-11")
    result = load_particle_size_counts(object(), query, array_data_loader=aggregate)
    assert calls[0]["end_time"] == datetime(2026, 9, 7, 12, 0, 0, 1)
    assert result.particle_qty.tolist() == [2]


def test_iqc_inspection_uses_inspection_timestamp(tmp_path, monkeypatch, report_clock):
    import json
    from src.iqc_domain.infrastructure import demo_repository

    (tmp_path / "inspection.json").write_text(json.dumps({
        "columns": ["报检日期", "检验时间"],
        "rows": [["2026-09-10", "2026-09-10 18:00:00"], ["2026-09-11", "2026-09-11 13:00:00"]],
    }), encoding="utf-8")
    monkeypatch.setattr(demo_repository, "DEMO_DIR", tmp_path)
    assert len(demo_repository.read_demo_report("inspection")) == 1


def test_yield_legacy_snapshot_rebuilds_for_precise_time(tmp_path, monkeypatch, report_clock):
    from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
    from src.yield_domain.infrastructure.repositories.yield_repository import PanelRepository

    path = tmp_path / "legacy.parquet"
    pd.DataFrame({"warehousing_time": pd.to_datetime(["2026-09-07"]), "panel_id": ["afternoon"]}).to_parquet(path)
    calls = []
    def fetch(*args):
        calls.append(args)
        return pd.DataFrame({"warehousing_time": ["2026-09-07"], "panel_id": ["afternoon"], "warehousing_event_time": ["20260907130000000000"]})
    repo = PanelRepository(path, YieldDataPolicy(work_order_types=("P",)), db_manager=SimpleNamespace(engine=object()))
    monkeypatch.setattr(repo, "_fetch_from_db_in_chunks", fetch)
    assert repo.get_panel_details(YieldQueryConfig(start_date="2026-09-01", end_date="2026-09-11", product_code="M1")).empty
    assert calls[0][:2] == ("2026-08-28", "2026-09-07")
    assert "warehousing_event_time" in pd.read_parquet(path)


def test_yield_empty_window_stays_cached_with_new_source_schema(tmp_path, monkeypatch, report_clock):
    from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
    from src.yield_domain.infrastructure.repositories.yield_repository import PanelRepository

    repo = PanelRepository(tmp_path / "empty.parquet", YieldDataPolicy(work_order_types=("P",)), db_manager=SimpleNamespace(engine=object()))
    calls = []
    def fetch(*args):
        calls.append(args)
        return pd.DataFrame()
    monkeypatch.setattr(repo, "_fetch_from_db_in_chunks", fetch)
    query = YieldQueryConfig(start_date="2026-09-01", end_date="2026-09-11", product_code="M1")
    for _ in range(2):
        actual = repo.get_panel_details(query)
        assert actual.empty and "warehousing_event_time" not in actual
        assert actual.attrs["data_health"]["status"] == "fresh"
    assert len(calls) == 1
