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


def test_list_step_options_include_original_codes_and_are_scoped_to_shop(monkeypatch) -> None:
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


def test_fetch_details_uses_bound_filters_and_returns_the_report_contract(monkeypatch) -> None:
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
                    "TIMEKEY": "20260802010000",
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
        "start_time": "20260802010000",
        "end_time": "20260901010000",
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
