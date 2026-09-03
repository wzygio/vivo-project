import os
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime import repository as qtime_repository
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository


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


def test_detail_snapshot_persists_source_time_and_reuses_fresh_data(
    tmp_path,
    monkeypatch,
) -> None:
    calls = 0
    captured_params: dict[str, object] = {}

    def fake_read_sql(_statement, _engine, params):
        nonlocal calls
        calls += 1
        captured_params.update(params)
        return pd.DataFrame(
            [
                {
                    "step_desc": "M3_DE->M3_STR",
                    "lot_id": "L001",
                    "prod_qty": 1,
                    "sub_prod_type": "P",
                    "f_step": "15500",
                    "t_step": "15600",
                    "q_spec": 2.5,
                    "wait_time": 0.41,
                    "timekey": "20260729010000",
                    "shop": "ARRAY",
                    "prodcode": "M626",
                }
            ]
        )

    monkeypatch.setattr(qtime_repository.pd, "read_sql", fake_read_sql)
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
    second = repository.fetch_details(query)

    assert calls == 1
    assert captured_params["start_time"] == "20260601000000"
    assert captured_params["end_time"] == "20260901010000"
    assert first.loc[0, "timekey"] == "20260802010000"
    pd.testing.assert_frame_equal(second, first)
    snapshot_path = next(tmp_path.glob("qtime_details_*.parquet"))
    assert pd.read_parquet(snapshot_path).loc[0, "timekey"] == "20260729010000"


def test_detail_snapshot_falls_back_after_database_failure(
    tmp_path,
    monkeypatch,
) -> None:
    source = pd.DataFrame(
        [
            {
                "step_desc": "M3_DE->M3_STR",
                "lot_id": "L001",
                "prod_qty": 1,
                "sub_prod_type": "P",
                "f_step": "15500",
                "t_step": "15600",
                "q_spec": 2.5,
                "wait_time": 0.41,
                "timekey": "20260729010000",
                "shop": "ARRAY",
                "prodcode": "M626",
            }
        ]
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
    repository.fetch_details(query)
    snapshot_path = next(tmp_path.glob("qtime_details_*.parquet"))
    os.utime(snapshot_path, (0, 0))
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    result = repository.fetch_details(query)

    assert result.loc[0, "timekey"] == "20260802010000"


def test_step_option_snapshot_keeps_filter_entry_available_when_database_fails(
    tmp_path,
    monkeypatch,
) -> None:
    source = pd.DataFrame(
        {
            "step_desc": ["A->B"],
            "f_step": ["15500"],
            "t_step": ["15600"],
        }
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
    expected = (QTimeStepOption(step_desc="A->B", f_step="15500", t_step="15600"),)
    assert repository.list_step_options("ARRAY") == expected
    snapshot_path = next(tmp_path.glob("qtime_step_options_*.parquet"))
    os.utime(snapshot_path, (0, 0))
    monkeypatch.setattr(
        qtime_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    assert repository.list_step_options("ARRAY") == expected
