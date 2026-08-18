from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.measurement.step_description_loader import (
    STEP_DESCRIPTION_COLUMNS,
    build_step_description_map,
    load_step_descriptions,
)


def _make_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
    return engine


def test_load_step_descriptions_strips_filters_and_deduplicates() -> None:
    engine = _make_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_processoperationspec_v ("
                "oper_code TEXT, description TEXT)"
            )
        )
        connection.execute(text("INSERT INTO mdw.dwr_mes_processoperationspec_v VALUES ('11620', ' 贴膜 ')"))
        connection.execute(text("INSERT INTO mdw.dwr_mes_processoperationspec_v VALUES ('11620', '重复描述')"))
        connection.execute(text("INSERT INTO mdw.dwr_mes_processoperationspec_v VALUES ('11630', NULL)"))
        connection.execute(text("INSERT INTO mdw.dwr_mes_processoperationspec_v VALUES ('11640', '  ')"))

    result = load_step_descriptions(SimpleNamespace(engine=engine))

    # 同一 step_id 取首个非空描述；NULL/空白描述被过滤
    assert result.to_dict("records") == [{"step_id": "11620", "step_desc": "贴膜"}]


def test_load_step_descriptions_empty_table_returns_empty_df() -> None:
    engine = _make_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_processoperationspec_v ("
                "oper_code TEXT, description TEXT)"
            )
        )

    result = load_step_descriptions(SimpleNamespace(engine=engine))

    assert result.empty
    assert list(result.columns) == STEP_DESCRIPTION_COLUMNS


def test_load_step_descriptions_falls_back_to_second_candidate_table() -> None:
    engine = _make_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_processoperationspec ("
                "oper_code TEXT, description TEXT)"
            )
        )
        connection.execute(text("INSERT INTO mdw.dwr_mes_processoperationspec VALUES ('11620', '贴膜')"))

    result = load_step_descriptions(SimpleNamespace(engine=engine))

    assert result.to_dict("records") == [{"step_id": "11620", "step_desc": "贴膜"}]


def test_load_step_descriptions_returns_empty_df_when_all_candidates_missing() -> None:
    engine = _make_engine()

    result = load_step_descriptions(SimpleNamespace(engine=engine))

    assert result.empty
    assert list(result.columns) == STEP_DESCRIPTION_COLUMNS


def test_build_step_description_map() -> None:
    df = pd.DataFrame(
        [
            {"step_id": "11620", "step_desc": "贴膜"},
            {"step_id": "11630", "step_desc": "切割"},
        ]
    )

    assert build_step_description_map(df) == {"11620": "贴膜", "11630": "切割"}
    assert build_step_description_map(pd.DataFrame(columns=STEP_DESCRIPTION_COLUMNS)) == {}
