"""AOI_RS 数据加载器测试：用 sqlite ATTACH 模拟 eda/mdw 双 schema 做集成式验证。"""

from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    AoiRsQueryConfig,
    load_rs_details,
)


def _fake_db_manager() -> SimpleNamespace:
    """构造挂有 eda/mdw 两个 schema 的 sqlite 引擎，内置最小数据集。"""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_rs_array ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, sheet_id TEXT,"
                " sheet_start_time TEXT, rs_code TEXT, code_qty INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_rs_oled ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, glass_id TEXT,"
                " glass_start_time TEXT, rs_code TEXT, code_qty INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_rs_tsp ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, glass_id TEXT,"
                " glass_start_time TEXT, rs_code TEXT, code_qty INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_productspec ("
                "productspecname TEXT, productcode TEXT)"
            )
        )
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M678', 'M678')"))
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M626', 'M626')"))
        # ARRAY: 命中 M678 两行 + 其他产品一行（应被过滤）
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_rs_array VALUES"
                " ('SPEC-M678', '11629', 'LOT-A1', 'SHT-A01', '2026-07-15 08:00:00', 'A1PPS', 3),"
                " ('SPEC-M678', '11629', 'LOT-A1', 'SHT-A02', '2026-07-16 09:00:00', 'A1PPS', 0),"
                " ('SPEC-M626', '11629', 'LOT-A9', 'SHT-A99', '2026-07-16 09:00:00', 'A1PPS', 7)"
            )
        )
        # OLED: glass 列名不同，厂别统一为 OLED
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_rs_oled VALUES"
                " ('SPEC-M678', '21329', 'LOT-O1', 'GLS-O01', '2026-08-01 10:00:00', 'C4BP3', 2)"
            )
        )
        # TP: glass 列名，厂别统一为 TP
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_rs_tsp VALUES"
                " ('SPEC-M678', '43629', 'LOT-T1', 'GLS-T01', '2026-08-02 11:00:00', 'T3DMR', 5)"
            )
        )
    return SimpleNamespace(engine=engine)


def _config() -> AoiRsQueryConfig:
    return AoiRsQueryConfig(
        prod_code="M678",
        start_date="2026-07-01",
        end_date="2026-08-10",
    )


def test_load_rs_details_unifies_three_factories_with_product_join() -> None:
    db_manager = _fake_db_manager()

    df = load_rs_details(db_manager, _config())

    assert set(df["factory"]) == {"ARRAY", "OLED", "TP"}
    # 产品过滤经 product_spec join 字典生效：M626 行被排除
    assert set(df["prod_code"]) == {"M678"}
    assert len(df) == 4
    # glass_id 归一为 sheet_id，时间列归一为 start_time
    row_oled = df[df["factory"] == "OLED"].iloc[0]
    assert row_oled["sheet_id"] == "GLS-O01"
    assert row_oled["lot_id"] == "LOT-O1"
    assert row_oled["step_id"] == "21329"
    assert row_oled["rs_code"] == "C4BP3"
    assert row_oled["code_qty"] == 2
    # code_qty 为数值且保留 0 值行（OLED 存在复判无缺陷记录）
    assert pd.api.types.is_numeric_dtype(df["code_qty"])
    assert (df["code_qty"] == 0).any()
    assert pd.api.types.is_datetime64_any_dtype(df["start_time"])


def test_load_rs_details_applies_time_window() -> None:
    db_manager = _fake_db_manager()
    config = AoiRsQueryConfig(prod_code="M678", start_date="2026-08-01", end_date="2026-08-10")

    df = load_rs_details(db_manager, config)

    assert set(df["factory"]) == {"OLED", "TP"}
