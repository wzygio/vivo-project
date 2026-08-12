"""AOI_RS 过货分母与规格表加载器测试（sqlite ATTACH 模拟 eda/mdw）。"""

from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    AoiRsQueryConfig,
    load_pass_through,
    load_rs_spec_limits,
)


def _fake_db_manager() -> SimpleNamespace:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        for view, id_col, time_col in [
            ("spot_eda_array_view_sht_v", "sheet_id", "sheet_start_time"),
            ("spot_eda_oled_view_gls_v", "glass_id", "glass_start_time"),
            ("spot_eda_tp_view_gls_v", "glass_id", "glass_start_time"),
        ]:
            conn.execute(
                text(
                    f"CREATE TABLE eda.{view} ("
                    f"{id_col} TEXT, {time_col} TEXT, step_id TEXT, product_spec TEXT, lot_id TEXT)"
                )
            )
        conn.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_productspec (productspecname TEXT, productcode TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ("
                "prod_code TEXT, factory TEXT, type_flag TEXT, step_id TEXT,"
                " rs_code TEXT, code_desc TEXT, spec NUMERIC)"
            )
        )
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M678', 'M678')"))
        conn.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M626', 'M626')"))
        conn.execute(
            text(
                "INSERT INTO eda.spot_eda_array_view_sht_v VALUES"
                " ('SHT-A01', '2026-07-15 08:00:00', '11629', 'SPEC-M678', 'LOT-A1'),"
                " ('SHT-A02', '2026-07-15 09:00:00', '11629', 'SPEC-M678', 'LOT-A1'),"
                " ('SHT-A99', '2026-07-15 09:30:00', '11629', 'SPEC-M626', 'LOT-A9')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO eda.spot_eda_tp_view_gls_v VALUES"
                " ('GLS-T01', '2026-08-02 11:00:00', '43629', 'SPEC-M678', 'LOT-T1')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO mdw.dwd_imp_rs_code_xishu_fo_tzsbjx VALUES"
                " ('M678', 'ARRAY', 'MWD_RATIO', '11629', 'A1PPS', 'PHT责M1残留', 0.5),"
                " ('M678', 'ARRAY', 'LOT_RATIO', '11629', 'A1PPS', 'PHT责M1残留', 30),"
                " ('M678', 'ARRAY', 'SHEET_ID', '11629', 'A1PPS', 'PHT责M1残留', 4),"
                " ('M626', 'ARRAY', 'MWD_RATIO', '11629', 'A1PPS', 'PHT责M1残留', 9)"
            )
        )
    return SimpleNamespace(engine=engine)


def test_load_pass_through_uses_tp_view_and_product_join() -> None:
    db_manager = _fake_db_manager()
    config = AoiRsQueryConfig(prod_code="M678", start_date="2026-07-01", end_date="2026-08-10")

    df = load_pass_through(db_manager, config)

    assert set(df["factory"]) == {"ARRAY", "TP"}
    assert set(df["prod_code"]) == {"M678"}
    row_tp = df[df["factory"] == "TP"].iloc[0]
    assert row_tp["sheet_id"] == "GLS-T01"
    assert row_tp["step_id"] == "43629"
    assert pd.api.types.is_datetime64_any_dtype(df["start_time"])
    # By Lot 均值的分母需要按 Lot 统计过货片数
    assert "lot_id" in df.columns
    assert row_tp["lot_id"] == "LOT-T1"


def test_load_rs_spec_limits_filters_product_and_coerces_spec() -> None:
    db_manager = _fake_db_manager()

    df = load_rs_spec_limits(db_manager, "M678")

    assert set(df["prod_code"]) == {"M678"}
    assert set(df["type_flag"]) == {"MWD_RATIO", "LOT_RATIO", "SHEET_ID"}
    assert pd.api.types.is_numeric_dtype(df["spec"])
    row = df[df["type_flag"] == "MWD_RATIO"].iloc[0]
    assert row["code_desc"] == "PHT责M1残留"
    assert row["spec"] == 0.5
