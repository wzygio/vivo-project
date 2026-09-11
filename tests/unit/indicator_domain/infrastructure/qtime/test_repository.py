from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime import repository as qtime_repository
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository
from src.indicator_domain.infrastructure.qtime.snapshot_store import QTimeSnapshotStore
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.fixture(autouse=True)
def explicit_forward_policy(monkeypatch):
    monkeypatch.setattr(
        qtime_repository.ConfigLoader,
        "get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=True, offset_days=4),
    )


@pytest.mark.parametrize("use_snapshot", [False, True])
def test_successful_empty_query_keeps_fresh_health(tmp_path, monkeypatch, use_snapshot):
    calls = []

    def read_sql(*_args, **_kwargs):
        calls.append(True)
        return pd.DataFrame(columns=qtime_repository.DETAIL_COLUMNS)

    monkeypatch.setattr(qtime_repository.pd, "read_sql", read_sql)
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=tmp_path if use_snapshot else None,
    )
    query = _current_query(date(2026, 9, 2))
    first = repository.fetch_details(query)
    second = repository.fetch_details(query)
    assert first.empty and second.empty
    for result in (first, second):
        health = result.attrs["data_health"]
        assert health["status"] == "fresh"
        assert health["refreshed_at"]
        assert health["source_start"] and health["source_end"]
        assert health["error_code"] == ""
    assert len(calls) == (1 if use_snapshot else 2)


def test_v2_empty_snapshot_is_refetched_in_full(tmp_path, monkeypatch):
    store = QTimeSnapshotStore(tmp_path, ttl_hours=12)
    with monkeypatch.context() as old:
        old.setattr(QTimeSnapshotStore, "POLICY_VERSION", "qtime-source-v2")
        store.write(
            "ARRAY", pd.DataFrame(columns=qtime_repository.DETAIL_COLUMNS),
            source_start=pd.Timestamp("2026-07-28"),
            source_end=pd.Timestamp("2026-08-30"),
            refreshed_at=datetime.now().astimezone(),
        )
    calls = []

    def read_sql(_statement, _engine, params):
        calls.append(params)
        return pd.DataFrame([
            _detail_row("A->B", "L001", "M626", "20260729010000519922")
        ])

    monkeypatch.setattr(qtime_repository.pd, "read_sql", read_sql)
    repository = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path)
    result = repository.fetch_details(_current_query(date(2026, 9, 2)))
    assert result["lot_id"].tolist() == ["L001"]
    assert calls[0]["start_time"] == "20260728000000"
    assert store.read("ARRAY", fresh_only=True, normalizer=lambda frame: frame) is not None


def test_list_products_returns_clean_sorted_unique_options(monkeypatch) -> None:
    def fake_read_sql(statement, engine):
        assert "eda.imp_qtime_tzbjx" in str(statement)
        return pd.DataFrame({"productspecname": [" M678 ", "M626", "M626", None]})

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
    repository = QTimeRepository(SimpleNamespace(engine=object()))

    assert repository.list_products() == ("M626", "M678")


def test_list_step_options_include_original_codes_and_are_scoped_to_shop(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            {
                "step_desc": [" B->C ", "A->B", "A->B", None],
                "f_step": [" 21200 ", "21100", "21100", "21100"],
                "t_step": [" 21300 ", "21200", "21200", "21200"],
            }
        )

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
    repository = QTimeRepository(SimpleNamespace(engine=object()))

    assert repository.list_step_options("OLED") == (
        QTimeStepOption(step_desc="A->B", f_step="21100", t_step="21200"),
        QTimeStepOption(step_desc="B->C", f_step="21200", t_step="21300"),
    )
    assert captured["params"] == {"shop": "OLED"}
    assert "WHERE shop = :shop" in captured["statement"]
    assert "WHERE shop = 'OLED'" not in captured["statement"]
    assert "f_step" in captured["statement"]
    assert "t_step" in captured["statement"]


def test_fetch_details_uses_bound_filters_and_returns_the_report_contract(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), engine=engine, params=params)
        return pd.DataFrame(
            [
                {
                    "STEP_DESC": "M3_DE->M3_STR",
                    "LOT_ID": "L001",
                    "PROD_QTY": "1",
                    "SUB_PROD_TYPE": "P",
                    "F_STEP": "15500",
                    "T_STEP": "15600",
                    "Q_SPEC": "2.5",
                    "WAIT_TIME": "0.41",
                    "TIMEKEY": "20260729010000",
                    "SHOP": "ARRAY",
                    "PRODCODE": "M626",
                }
            ]
        )

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
    engine = object()
    repository = QTimeRepository(SimpleNamespace(engine=engine))
    query = QTimeQuery(
        start_time=datetime(2026, 8, 2, 1, 0),
        end_time=datetime(2026, 9, 1, 1, 0),
        shop="ARRAY",
        step_descriptions=("M3_DE->M3_STR", "M4_DE->M4_STR"),
        products=("M626", "M626'); DROP TABLE qtime; --"),
    )

    result = repository.fetch_details(query)

    assert captured["engine"] is engine
    assert captured["params"] == {
        "start_time": "20260729010000",
        "end_time": "20260828010000",
        "shop": "ARRAY",
        "step_descriptions": ("M3_DE->M3_STR", "M4_DE->M4_STR"),
        "products": ("M626", "M626'); DROP TABLE qtime; --"),
    }
    assert "DROP TABLE" not in captured["statement"]
    assert "step_desc IN" in captured["statement"]
    assert list(result.columns) == [
        "step_desc",
        "lot_id",
        "prod_qty",
        "sub_prod_type",
        "f_step",
        "t_step",
        "q_spec",
        "wait_time",
        "timekey",
        "shop",
        "prodcode",
    ]
    assert result.loc[0, "q_spec"] == 2.5
    assert result.loc[0, "wait_time"] == 0.41
    assert result.loc[0, "timekey"] == "20260802010000"


def test_database_failures_are_exposed_as_a_safe_domain_error(monkeypatch) -> None:
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("password=do-not-leak")
        ),
    )
    repository = QTimeRepository(SimpleNamespace(engine=object()))

    with pytest.raises(QTimeDataAccessError) as caught:
        repository.list_products()

    assert str(caught.value) == "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
    assert "password" not in str(caught.value)


def test_shop_snapshot_is_shared_across_query_filters_and_keeps_source_time(
    tmp_path,
    monkeypatch,
) -> None:
    calls = 0
    captured_params: list[dict[str, object]] = []

    def fake_read_sql(_statement, _engine, params):
        nonlocal calls
        calls += 1
        captured_params.append(dict(params))
        return pd.DataFrame(
            [
                _detail_row("A->B", "L001", "M626", "20260729010000"),
                _detail_row("B->C", "L002", "M678", "20260730010000"),
            ]
        )

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=tmp_path,
        snapshot_ttl_hours=12,
    )
    base_query = _current_query(date(2026, 9, 2))
    first_query = base_query.model_copy(
        update={"step_descriptions": ("A->B",), "products": ("M626",)}
    )
    second_query = base_query.model_copy(
        update={"step_descriptions": ("B->C",), "products": ("M678",)}
    )

    first = repository.fetch_details(first_query)
    second = repository.fetch_details(second_query)

    assert calls == 1
    assert captured_params[0] == {
        "start_time": "20260728000000",
        "end_time": "20260830000000",
        "shop": "ARRAY",
    }
    assert first.loc[0, "timekey"] == "20260802010000"
    assert second.loc[0, "timekey"] == "20260803010000"
    snapshot_path = tmp_path / "qtime_source_array.parquet"
    assert pd.read_parquet(snapshot_path).loc[0, "timekey"] == "20260729010000"
    assert not list(tmp_path.glob("qtime_details_*.parquet"))
    assert not list(tmp_path.glob("qtime_*_qtime-source-v1.parquet"))

    snapshot = QTimeSnapshotStore(tmp_path, ttl_hours=12).read(
        "ARRAY", fresh_only=False, normalizer=lambda frame: frame
    )
    assert snapshot is not None
    assert snapshot.metadata.source_start == "2026-07-28T00:00:00"
    assert snapshot.metadata.source_end == "2026-08-30T00:00:00"
    assert first.attrs["data_health"]["source_start"] == "2026-07-28T00:00:00"
    assert second.attrs["data_health"]["source_end"] == "2026-08-30T00:00:00"
    assert not list(tmp_path.glob("qtime_source_*.meta.json"))


def test_detail_snapshot_falls_back_after_database_failure(
    tmp_path,
    monkeypatch,
) -> None:
    source = pd.DataFrame(
        [_detail_row("M3_DE->M3_STR", "L001", "M626", "20260729010000")]
    )
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: source.copy(),
    )
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=tmp_path,
        snapshot_ttl_hours=12,
    )
    query = QTimeQuery(
        start_time=datetime(2026, 8, 2, 1),
        end_time=datetime(2026, 9, 1, 1),
        shop="ARRAY",
        step_descriptions=("M3_DE->M3_STR",),
        products=("M626",),
    )
    first = repository.fetch_details(query)
    assert first.attrs["data_health"]["status"] == "fresh"
    refreshed_at = first.attrs["data_health"]["refreshed_at"]
    monkeypatch.setattr(QTimeSnapshotStore, "_is_fresh", lambda *_args: False)
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    result = repository.fetch_details(query)

    assert result.loc[0, "timekey"] == "20260802010000"
    assert result.attrs["data_health"]["status"] == "stale"
    assert result.attrs["data_health"]["refreshed_at"] == refreshed_at
    assert result.attrs["data_health"]["error_code"] == "QTIME_SOURCE_READ_FAILED"


def test_expired_snapshot_refresh_replaces_overlap_and_prunes_old_month(
    tmp_path,
    monkeypatch,
) -> None:
    from src.shared_kernel.report_cutoff import ReportCutoffPolicy

    # This scenario advances through October; keep its wall clock explicit.
    monkeypatch.setattr(ReportCutoffPolicy, "boundary", lambda self, now=None: pd.Timestamp("2026-10-02 12:00:00"))
    responses = iter(
        [
            pd.DataFrame(
                [
                    _detail_row("A->B", "OLD", "M626", "20260729010000"),
                    _detail_row("A->B", "REPLACE", "M626", "20260829010000", 1.0),
                ]
            ),
            pd.DataFrame(
                [
                    _detail_row("A->B", "REPLACE", "M626", "20260829010000", 9.0),
                    _detail_row("A->B", "NEW", "M626", "20260928010000", 2.0),
                ]
            ),
        ]
    )
    captured_params: list[dict[str, object]] = []

    def fake_read_sql(_statement, _engine, params):
        captured_params.append(dict(params))
        return next(responses)

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=tmp_path,
        snapshot_ttl_hours=12,
    )
    september_query = _current_query(date(2026, 9, 2))
    october_query = _current_query(date(2026, 10, 2))

    repository.fetch_details(september_query)
    snapshot_path = tmp_path / "qtime_source_array.parquet"
    monkeypatch.setattr(QTimeSnapshotStore, "_is_fresh", lambda *_args: False)

    result = repository.fetch_details(october_query)

    assert captured_params[1] == {
        "start_time": "20260828000000",
        "end_time": "20260929000000",
        "shop": "ARRAY",
    }
    assert result["lot_id"].tolist() == ["NEW", "REPLACE"]
    stored = pd.read_parquet(snapshot_path)
    assert stored["lot_id"].tolist() == ["NEW", "REPLACE"]
    assert stored.loc[stored["lot_id"] == "REPLACE", "wait_time"].item() == 9.0
    assert "OLD" not in stored["lot_id"].tolist()


def test_legacy_l1_is_removed_only_after_all_shop_snapshots_exist(tmp_path) -> None:
    legacy_detail = tmp_path / "qtime_details_deadbeef.parquet"
    legacy_options = tmp_path / "qtime_step_options_array_qtime-source-v1.parquet"
    legacy_detail.write_bytes(b"legacy")
    legacy_options.write_bytes(b"legacy")
    store = QTimeSnapshotStore(tmp_path, ttl_hours=12)
    source = pd.DataFrame(
        [_detail_row("A->B", "L001", "M626", "20260829010000")]
    )
    refreshed_at = datetime.now().astimezone()

    for shop in ("ARRAY", "OLED"):
        store.write(
            shop,
            source,
            source_start=pd.Timestamp("2026-07-28"),
            source_end=pd.Timestamp("2026-08-30"),
            refreshed_at=refreshed_at,
        )
        assert legacy_detail.exists()
        assert legacy_options.exists()

    store.write(
        "TP",
        source,
        source_start=pd.Timestamp("2026-07-28"),
        source_end=pd.Timestamp("2026-08-30"),
        refreshed_at=refreshed_at,
    )

    assert not legacy_detail.exists()
    assert not legacy_options.exists()


def test_step_options_use_unified_snapshot_without_creating_option_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    detail_source = pd.DataFrame(
        [
            _detail_row("B->C", "L002", "M626", "20260829010000"),
            _detail_row("A->B", "L001", "M626", "20260829020000"),
        ]
    )
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: detail_source.copy(),
    )
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=tmp_path,
        snapshot_ttl_hours=12,
    )
    repository.fetch_details(_current_query(date(2026, 9, 2)))
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("fresh unified snapshot should satisfy options")
        ),
    )

    assert repository.list_step_options("ARRAY") == (
        QTimeStepOption(step_desc="A->B", f_step="15500", t_step="15600"),
        QTimeStepOption(step_desc="B->C", f_step="15500", t_step="15600"),
    )
    assert not list(tmp_path.glob("qtime_step_options_*.parquet"))


def _detail_row(
    step_desc: str,
    lot_id: str,
    prodcode: str,
    timekey: str,
    wait_time: float = 0.41,
) -> dict[str, object]:
    return {
        "step_desc": step_desc,
        "lot_id": lot_id,
        "prod_qty": 1,
        "sub_prod_type": "P",
        "f_step": "15500",
        "t_step": "15600",
        "q_spec": 2.5,
        "wait_time": wait_time,
        "timekey": timekey,
        "shop": "ARRAY",
        "prodcode": prodcode,
    }


def _current_query(as_of: date) -> QTimeQuery:
    current_month_start = as_of.replace(day=1)
    previous_month_start = pd.Timestamp(current_month_start) - pd.DateOffset(months=1)
    return QTimeQuery(
        start_time=previous_month_start.to_pydatetime(),
        end_time=datetime.combine(as_of + pd.Timedelta(days=1), datetime.min.time()),
        shop="ARRAY",
        step_descriptions=("A->B", "B->C"),
        products=(),
    )
