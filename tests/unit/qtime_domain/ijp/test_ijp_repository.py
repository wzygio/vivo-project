from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.qtime_domain.application.ijp.dtos import IjpQuery
from src.qtime_domain.application.ijp.errors import IjpDataAccessError
from src.qtime_domain.infrastructure import ijp_repository
from src.qtime_domain.infrastructure.ijp_repository import IjpRepository


def _query(**overrides) -> IjpQuery:
    return IjpQuery(
        start_time=datetime(2026, 8, 31, 7, 0),
        end_time=datetime(2026, 9, 1, 7, 0),
        **overrides,
    )


def test_fetch_details_uses_bound_parameters_for_every_user_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            columns=["print_time", "productcode", "glass_id", "printer", "rs_code", "image_name"]
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = IjpRepository(SimpleNamespace(engine=object()))

    repository.fetch_details(
        _query(
            glass_ids=("G1'); DROP TABLE eda.spot_eda_oled_view_dft_v; --",),
            codes=("C3DM1",),
            lines=("3CEE01",),
        )
    )

    assert "DROP TABLE" not in captured["statement"]
    assert "G1'); DROP TABLE" in captured["params"]["glass_ids"][0]
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
            columns=["print_time", "productcode", "glass_id", "printer", "rs_code", "image_name"]
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = IjpRepository(SimpleNamespace(engine=object()))

    result = repository.fetch_details(_query())

    assert "product_names" not in captured["params"]
    assert "picis" not in captured["params"]
    assert list(result.columns) == ijp_repository.DETAIL_COLUMNS


def test_fetch_daily_ratios_expands_the_window_by_seven_days(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params):
        captured.update(statement=str(statement), params=params)
        return pd.DataFrame(
            {
                "day": ["2026-08-31", "2026-08-31", "2026-09-01"],
                "rs_code": ["C3DM1", "C3RA1", "C3DM1"],
                "code_num": [3, 1, 2],
            }
        )

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)
    repository = IjpRepository(SimpleNamespace(engine=object()))

    ratios = repository.fetch_daily_ratios(_query())

    assert captured["params"]["start_time"] == "2026-08-24 07:00:00"
    assert captured["params"]["end_time"] == "2026-09-01 07:00:00"
    assert ratios["ratio"].tolist() == [0.75, 0.25, 1.0]


def test_event_time_cast_only_applies_to_postgresql(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_sql(statement, engine, params=None):
        captured.update(statement=str(statement))
        return pd.DataFrame({"pici": ["LOT1"]})

    monkeypatch.setattr(ijp_repository.pd, "read_sql", fake_read_sql)

    pg_engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    IjpRepository(SimpleNamespace(engine=pg_engine)).list_picis(
        datetime(2026, 8, 31), datetime(2026, 9, 1), ()
    )
    assert "EVENT_TIME::TIMESTAMP" in captured["statement"]
    assert "EVENT_TIME <> 'NaT'" in captured["statement"]

    sqlite_engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    IjpRepository(SimpleNamespace(engine=sqlite_engine)).list_picis(
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
    repository = IjpRepository(SimpleNamespace(engine=object()))

    with pytest.raises(IjpDataAccessError) as caught:
        repository.fetch_details(_query())

    assert str(caught.value) == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
    assert "password" not in str(caught.value)
