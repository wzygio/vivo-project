"""AOI_RS 报表数据加载器（DAO）。

涉及数据库表（探查确认于 2026-08-10，见 references/domain/aoi_rs/spec-data_source.md）：

RS Code 明细表（分子 / By Lot / By Sheet）：
- eda.spc_tzbjx_rs_array — ARRAY 厂，ID 列 sheet_id，时间列 sheet_start_time
- eda.spc_tzbjx_rs_oled  — OLED 厂，ID 列 glass_id，时间列 glass_start_time
- eda.spc_tzbjx_rs_tsp   — TP 厂，ID 列 glass_id，时间列 glass_start_time
  三表同构，关键列：product_spec / step_id / lot_id / rs_code / code_qty。
  注意：productcode 列在 array 表全空，产品过滤必须走 product_spec join 产品字典。

过货明细视图（月周天趋势分母）：
- eda.spot_eda_array_view_sht_v / eda.spot_eda_oled_view_gls_v / eda.spot_eda_tp_view_gls_v
  （任务文档所写 spot_eda_tsp_view_gls_v 不存在，TP 正确命名为 tp）

规格表：mdw.dwd_imp_rs_code_xishu_fo_tzsbjx
  粒度 prod_code + factory + step_id + rs_code + type_flag；
  type_flag ∈ MWD_RATIO（月周天）/ LOT_RATIO（By Lot）/ SHEET_ID、GLASS_ID（By Sheet）；
  code_desc 为 Code 中文名称，spec 为单边上限值。

产品字典：mdw.dwr_mes_productspec（productspecname → productcode）
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

# 三元组映射：(物理表名, ID 列名, 时间戳列名)，与 SPC data_loader 的 factory_meta 同构
RS_FACTORY_META = {
    "ARRAY": ("spc_tzbjx_rs_array", "sheet_id", "sheet_start_time"),
    "OLED": ("spc_tzbjx_rs_oled", "glass_id", "glass_start_time"),
    "TP": ("spc_tzbjx_rs_tsp", "glass_id", "glass_start_time"),
}

PASS_FACTORY_META = {
    "ARRAY": ("spot_eda_array_view_sht_v", "sheet_id", "sheet_start_time"),
    "OLED": ("spot_eda_oled_view_gls_v", "glass_id", "glass_start_time"),
    "TP": ("spot_eda_tp_view_gls_v", "glass_id", "glass_start_time"),
}

RS_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "start_time",
    "sheet_id",
    "lot_id",
    "step_id",
    "rs_code",
    "code_qty",
]

PASS_THROUGH_COLUMNS = ["factory", "prod_code", "start_time", "sheet_id", "lot_id", "step_id"]


def _read_sql(
    db_manager: "DatabaseManager",
    sql: str,
    error_message: str,
    params: dict[str, object] | None = None,
) -> pd.DataFrame:
    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")
        df = pd.read_sql(text(sql), db_manager.engine, params=params)
        df.columns = df.columns.str.lower()
        return df
    except Exception as exc:  # noqa: BLE001 - DAO 层容错，返回空表由上层降级
        logger.error("%s: %s", error_message, exc, exc_info=True)
        return pd.DataFrame()


def load_rs_details(
    db_manager: "DatabaseManager",
    query_config: AoiRsQueryConfig,
) -> pd.DataFrame:
    """加载三厂 RS Code 明细并抹平列名差异（UNION ALL + 产品字典 join）。"""
    logger.info("==> [DAO] 抽取产品 %s 的 AOI RS 明细...", query_config.prod_code)

    start_time_fmt = f"{query_config.start_date} 00:00:00"
    end_time_fmt = f"{query_config.end_date} 23:59:59"

    sql_queries = []
    for fac, (table_name, id_col, time_col) in RS_FACTORY_META.items():
        q = f"""
        SELECT
            '{fac}' AS factory,
            P.productcode AS prod_code,
            T.{time_col} AS start_time,
            T.{id_col} AS sheet_id,
            T.lot_id,
            T.step_id,
            T.rs_code,
            T.code_qty
        FROM eda.{table_name} T
        JOIN mdw.dwr_mes_productspec P ON T.product_spec = P.productspecname
        WHERE T.{time_col} >= :start_time
          AND T.{time_col} <= :end_time
          AND P.productcode = :prod_code
        """
        sql_queries.append(q)

    final_sql = " UNION ALL ".join(sql_queries)
    df = _read_sql(
        db_manager,
        final_sql,
        "[DAO] 提取 AOI RS 明细失败",
        params={
            "start_time": start_time_fmt,
            "end_time": end_time_fmt,
            "prod_code": query_config.prod_code,
        },
    )
    if df.empty:
        return pd.DataFrame(columns=RS_DETAIL_COLUMNS)

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["code_qty"] = pd.to_numeric(df["code_qty"], errors="coerce").fillna(0)
    df = df.dropna(subset=["start_time"]).reset_index(drop=True)
    logger.info("[DAO] 成功提取 %d 条 AOI RS 明细。", len(df))
    return df


def load_pass_through(
    db_manager: "DatabaseManager",
    query_config: AoiRsQueryConfig,
) -> pd.DataFrame:
    """加载三厂过货明细（月周天趋势图分母），列名抹平后与 RS 明细同构。"""
    logger.info("==> [DAO] 抽取产品 %s 的过货明细（分母）...", query_config.prod_code)

    start_time_fmt = f"{query_config.start_date} 00:00:00"
    end_time_fmt = f"{query_config.end_date} 23:59:59"

    sql_queries = []
    for fac, (view_name, id_col, time_col) in PASS_FACTORY_META.items():
        q = f"""
        SELECT
            '{fac}' AS factory,
            P.productcode AS prod_code,
            T.{time_col} AS start_time,
            T.{id_col} AS sheet_id,
            T.lot_id,
            T.step_id
        FROM eda.{view_name} T
        JOIN mdw.dwr_mes_productspec P ON T.product_spec = P.productspecname
        WHERE T.{time_col} >= :start_time
          AND T.{time_col} <= :end_time
          AND P.productcode = :prod_code
        """
        sql_queries.append(q)

    final_sql = " UNION ALL ".join(sql_queries)
    df = _read_sql(
        db_manager,
        final_sql,
        "[DAO] 提取过货明细失败",
        params={
            "start_time": start_time_fmt,
            "end_time": end_time_fmt,
            "prod_code": query_config.prod_code,
        },
    )
    if df.empty:
        return pd.DataFrame(columns=PASS_THROUGH_COLUMNS)

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df = df.dropna(subset=["start_time"]).reset_index(drop=True)
    logger.info("[DAO] 成功提取 %d 条过货明细。", len(df))
    return df


def load_rs_spec_limits(
    db_manager: "DatabaseManager",
    prod_code: str,
) -> pd.DataFrame:
    """加载 RS Code 规格线（按 type_flag 区分适用图类型，spec 为单边上限）。"""
    sql_query = f"""
    SELECT
        prod_code,
        factory,
        type_flag,
        step_id,
        rs_code,
        code_desc,
        spec
    FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx
    WHERE prod_code = :prod_code
    """
    df = _read_sql(
        db_manager,
        sql_query,
        "[DAO] 提取 RS 规格基准失败",
        params={"prod_code": prod_code},
    )
    if df.empty:
        return pd.DataFrame(
            columns=["prod_code", "factory", "type_flag", "step_id", "rs_code", "code_desc", "spec"]
        )

    df["spec"] = pd.to_numeric(df["spec"], errors="coerce")
    logger.info("[DAO] 成功提取 %d 条 RS 规格规则。", len(df))
    return df
