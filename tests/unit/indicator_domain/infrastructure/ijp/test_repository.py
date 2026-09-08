from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.infrastructure.ijp import repository as ijp_repository
from src.indicator_domain.infrastructure.ijp.repository import IjpRepository
from src.shared_kernel.data_forward import DataForwardPolicy


def _query(**overrides) -> IjpQuery:
    return IjpQuery(
        start_time=datetime(2026, 8, 31, 7, 0),
        end_time=datetime(2026, 9, 1, 7, 0),
        **overrides,
    )


def _repository(engine: object) -> IjpRepository:
    repository = IjpRepository(SimpleNamespace(engine=engine))
    repository._data_forward_policy = DataForwardPolicy(  # noqa: SLF001
        enabled=True,
        offset_days=4,
    )
    return repository


def test_fetch_details_uses_bound_parameters_for_every_user_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            [
                {
                    "print_time": "2026-08-27 08:00:00",
                    "productcode": "M678",
                    "glass_id": "G1",
                    "printer": "3CEE01-IK2-PR1",
                    "rs_code": "C3DM1",
                    "image_name": "G1_A01.jpg",
                }
            ]
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = _repository(object())

    repository.fetch_details(
        _query(
            picis=("LOT1'); DROP TABLE eda.spot_eda_oled_view_dft_v; --",),
            codes=("C3DM1",),
            lines=("3CEE01",),
        )
    )

    assert "DROP TABLE" not in captured["statement"]
    assert "LOT1'); DROP TABLE" in captured["params"]["picis"][0]
    assert captured["params"]["equip_whitelist"] == (
        "3CEE01-IK2-PR1",
        "3CEE01-IK2-PR2",
        "3CEE02-IK2-PR1",
        "3CEE02-IK2-PR2",
        "3CEE04-IKT-PRT",
    )
    assert captured["params"]["code_whitelist"][0] == "C3DM0"
    assert "LIMIT 5000" in captured["statement"]


def test_fetch_details_omits_optional_clauses_when_filters_are_empty(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            [
                {
                    "print_time": "2026-08-27 08:00:00",
                    "productcode": "M678",
                    "glass_id": "G1",
                    "printer": "3CEE01-IK2-PR1",
                    "rs_code": "C3DM1",
                    "image_name": "G1_A01.jpg",
                }
            ]
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = _repository(object())

    result = repository.fetch_details(_query())

    assert "work_order_types" not in captured["params"]
    assert "picis" not in captured["params"]
    assert captured["params"]["start_time"] == "2026-08-27 07:00:00"
    assert captured["params"]["end_time"] == "2026-08-28 07:00:00"
    assert list(result.columns) == ijp_repository.DETAIL_COLUMNS
    assert result.loc[0, "print_time"] == pd.Timestamp("2026-08-31 08:00:00")


def test_fetch_daily_ratios_uses_the_query_window_without_lookback(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            {
                "productcode": ["M626", "M626", "M626"],
                "line": ["3CEE01", "3CEE01", "3CEE01"],
                "printer": [
                    "3CEE01-IK2-PR1",
                    "3CEE01-IK2-PR1",
                    "3CEE01-IK2-PR2",
                ],
                "day": ["2026-08-27", "2026-08-27", "2026-08-27"],
                "rs_code": ["C3DM1", "C3RA1", "C3DM1"],
                "code_num": [3, 1, 2],
            }
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = _repository(object())

    ratios = repository.fetch_daily_ratios(_query())

    assert captured["params"]["start_time"] == "2026-08-27 07:00:00"
    assert captured["params"]["end_time"] == "2026-08-28 07:00:00"
    assert ratios["day"].tolist() == ["2026-08-31"] * 3
    assert ratios["ratio"].tolist() == [0.75, 0.25, 1.0]


def test_event_time_cast_only_applies_to_postgresql(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params=None):
        captured.update(statement=str(statement))
        return pd.DataFrame({"pici": ["LOT1"]})

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)

    pg_engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    _repository(pg_engine).list_picis(
        datetime(2026, 8, 31), datetime(2026, 9, 1), ()
    )
    assert "EVENT_TIME::TIMESTAMP" in captured["statement"]
    assert "EVENT_TIME <> 'NaT'" in captured["statement"]

    sqlite_engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    _repository(sqlite_engine).list_picis(
        datetime(2026, 8, 31), datetime(2026, 9, 1), ()
    )
    assert "::TIMESTAMP" not in captured["statement"]


def test_database_failures_are_exposed_as_a_safe_domain_error(monkeypatch) -> None:
    monkeypatch.setattr(
        ijp_repository.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("password=do-not-leak")
        ),
    )
    repository = _repository(object())

    with pytest.raises(IjpDataAccessError) as caught:
        repository.fetch_details(_query())

    assert str(caught.value) == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
    assert "password" not in str(caught.value)
