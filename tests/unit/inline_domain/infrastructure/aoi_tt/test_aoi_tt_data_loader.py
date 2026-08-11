"""AOI_TT 数据加载器测试：用 sqlite ATTACH 模拟 eda/mdw 双 schema 做集成式验证。"""

from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.aoi_tt.data_loader import (
    TT_DETAIL_COLUMNS,
    AoiTtQueryConfig,
    load_tt_details,
    load_tt_param_set,
    load_tt_spec_limits,
)


def _fake_db_manager() -> SimpleNamespace:
    """构造挂有 eda/mdw 两个 schema 的 sqlite 引擎，内置最小数据集。

    规格表 mdw.dwd_imp_dv_param_spec 中 param_type IS NULL 的 (step_id, param_name)
    才是 TT 参数；SE_L1T 带 param_type，应被排除。usl/ucl 故意以 TEXT 存入以验证数值化。
    """
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_array ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, sheet_id TEXT,"
                " sheet_start_time TEXT, param_name TEXT, param_value REAL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_oled ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, glass_id TEXT,"
                " glass_start_time TEXT, param_name TEXT, param_value REAL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_tsp ("
                "product_spec TEXT, step_id TEXT, lot_id TEXT, glass_id TEXT,"
                " glass_start_time TEXT, param_name TEXT, param_value REAL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_productspec ("
                "productspecname TEXT, productcode TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE mdw.dwd_imp_dv_param_spec ("
                "prod_code TEXT, step_id TEXT, param_name TEXT, param_type TEXT,"
                " usl TEXT, ucl TEXT)"
            )
        )
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M678', 'M678')"))
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M626', 'M626')"))
        # TT 参数集：param_type IS NULL 的三个组合；SE_L1T 非 TT；M626 属其他产品
        conn.execute(
            text(
                "INSERT INTO mdw.dwd_imp_dv_param_spec VALUES"
                " ('M678', '11620', 'TDSUM', NULL, '5.0', '3.0'),"
                " ('M678', '11620', 'SE_L1T', 'ARRAY_OTHER', NULL, NULL),"
                " ('M678', '21320', 'DSUM_L', NULL, '2.0', '1.0'),"
                " ('M678', '43620', 'TOTAL_O_L', NULL, '8.0', '6.0'),"
                " ('M626', '11620', 'TDSUM', NULL, '9.0', '9.0')"
            )
        )
        # ARRAY: TDSUM 命中两行（含 0 值行）；SE_L1T 非 TT 参数被排除；
        # 99999/TDSUM 组合不在参数集中被排除；M626 行被产品过滤排除
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_array VALUES"
                " ('SPEC-M678', '11620', 'LOT-A1', 'SHT-A01', '2026-07-15 08:00:00', 'TDSUM', 3),"
                " ('SPEC-M678', '11620', 'LOT-A1', 'SHT-A02', '2026-07-16 09:00:00', 'TDSUM', 0),"
                " ('SPEC-M678', '11620', 'LOT-A1', 'SHT-A02', '2026-07-16 09:00:00', 'SE_L1T', 7),"
                " ('SPEC-M678', '99999', 'LOT-A1', 'SHT-A03', '2026-07-16 10:00:00', 'TDSUM', 5),"
                " ('SPEC-M626', '11620', 'LOT-A9', 'SHT-A99', '2026-07-16 09:00:00', 'TDSUM', 7)"
            )
        )
        # OLED: glass 列名不同，厂别统一为 OLED
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_oled VALUES"
                " ('SPEC-M678', '21320', 'LOT-O1', 'GLS-O01', '2026-08-01 10:00:00', 'DSUM_L', 2)"
            )
        )
        # TP: glass 列名，厂别统一为 TP
        conn.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_tsp VALUES"
                " ('SPEC-M678', '43620', 'LOT-T1', 'GLS-T01', '2026-08-02 11:00:00', 'TOTAL_O_L', 5)"
            )
        )
    return SimpleNamespace(engine=engine)


def _config() -> AoiTtQueryConfig:
    return AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-07-01",
        end_date="2026-08-10",
    )


def test_load_tt_param_set_returns_only_null_param_type_pairs() -> None:
    db_manager = _fake_db_manager()

    df = load_tt_param_set(db_manager, "M678")

    pairs = set(zip(df["step_id"], df["param_name"]))
    assert pairs == {("11620", "TDSUM"), ("21320", "DSUM_L"), ("43620", "TOTAL_O_L")}
    # 非 TT 参数（param_type 非空）不得进入参数集
    assert "SE_L1T" not in set(df["param_name"])


def test_load_tt_details_unifies_factories_and_filters_tt_params() -> None:
    db_manager = _fake_db_manager()

    df = load_tt_details(db_manager, _config())

    assert set(df["factory"]) == {"ARRAY", "OLED", "TP"}
    # 产品过滤经 product_spec join 字典生效：M626 行被排除
    assert set(df["prod_code"]) == {"M678"}
    # (step_id, param_name) 组合过滤生效：SE_L1T 与 99999/TDSUM 错配组合均被排除
    assert set(df["tt_name"]) == {"TDSUM", "DSUM_L", "TOTAL_O_L"}
    assert len(df) == 4
    # glass_id 归一为 sheet_id，时间列归一为 start_time
    row_oled = df[df["factory"] == "OLED"].iloc[0]
    assert row_oled["sheet_id"] == "GLS-O01"
    assert row_oled["lot_id"] == "LOT-O1"
    assert row_oled["step_id"] == "21320"
    assert row_oled["tt_name"] == "DSUM_L"
    assert row_oled["tt_qty"] == 2
    # tt_qty 为数值且保留 0 值行（每片必测，无 TT 记 0）
    assert pd.api.types.is_numeric_dtype(df["tt_qty"])
    assert (df["tt_qty"] == 0).any()
    assert pd.api.types.is_datetime64_any_dtype(df["start_time"])


def test_load_tt_details_applies_time_window() -> None:
    db_manager = _fake_db_manager()
    config = AoiTtQueryConfig(prod_code="M678", start_date="2026-08-01", end_date="2026-08-10")

    df = load_tt_details(db_manager, config)

    assert set(df["factory"]) == {"OLED", "TP"}


def test_load_tt_details_returns_empty_when_product_has_no_tt_params() -> None:
    db_manager = _fake_db_manager()
    config = AoiTtQueryConfig(prod_code="M999", start_date="2026-07-01", end_date="2026-08-10")

    df = load_tt_details(db_manager, config)

    assert df.empty
    assert list(df.columns) == TT_DETAIL_COLUMNS


def test_load_tt_spec_limits_filters_null_type_and_coerces_numeric() -> None:
    db_manager = _fake_db_manager()

    df = load_tt_spec_limits(db_manager, "M678")

    assert set(df["prod_code"]) == {"M678"}
    # 只取 param_type IS NULL 的 TT 规格，SE_L1T 不在其中
    assert set(df["tt_name"]) == {"TDSUM", "DSUM_L", "TOTAL_O_L"}
    assert len(df) == 3
    # usl/ucl 由 TEXT 数值化为 float
    assert pd.api.types.is_numeric_dtype(df["usl"])
    assert pd.api.types.is_numeric_dtype(df["ucl"])
    row = df[(df["step_id"] == "11620") & (df["tt_name"] == "TDSUM")].iloc[0]
    assert row["usl"] == 5.0
    assert row["ucl"] == 3.0
