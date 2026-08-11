"""AOI_TT 报表数据加载器（DAO）。

涉及数据库表（探查确认于 2026-08-10，见 references/domain/aoi_tt/spec-data_source.md）：

TT 明细表（分子 / By Lot / By Sheet / 趋势分母）：
- eda.spc_tzbjx_array — ARRAY 厂，ID 列 sheet_id，时间列 sheet_start_time
- eda.spc_tzbjx_oled  — OLED 厂，ID 列 glass_id，时间列 glass_start_time
- eda.spc_tzbjx_tsp   — TP 厂，ID 列 glass_id，时间列 glass_start_time
  三表同构，关键列：product_spec / step_id / lot_id / param_name / param_value。
  产品过滤必须走 product_spec join 产品字典（同 RS 链路）。

TT 参数识别：mdw.dwd_imp_dv_param_spec 中 param_type IS NULL 的 (step_id, param_name)
  组合即 AOI TT 参数全集（TDSUM / DSUM_L / DSUM_O / TOTAL_O_L）。

趋势分母：测量表自身 distinct sheet/glass（TDSUM/DSUM 每片必测；
  过货视图 spot_eda_*_view_* 不含 AOI 站点 xx620/21320/43620 记录，不可用）。

规格表：mdw.dwd_imp_dv_param_spec
  粒度 prod_code + step_id + param_name；TT 只取 usl/ucl（越小越好型上限）。

产品字典：mdw.dwr_mes_productspec（productspecname → productcode）
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

# 三元组映射：(物理表名, ID 列名, 时间戳列名)，与 AOI_RS data_loader 同构
TT_FACTORY_META = {
    "ARRAY": ("spc_tzbjx_array", "sheet_id", "sheet_start_time"),
    "OLED": ("spc_tzbjx_oled", "glass_id", "glass_start_time"),
    "TP": ("spc_tzbjx_tsp", "glass_id", "glass_start_time"),
}

TT_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "start_time",
    "sheet_id",
    "lot_id",
    "step_id",
    "tt_name",
    "tt_qty",
]

TT_SPEC_COLUMNS = ["prod_code", "step_id", "tt_name", "usl", "ucl"]


class AoiTtQueryConfig(BaseModel):
    """AOI_TT 报表查询的强类型配置（固定窗口：上一自然月 1 日 ~ 当前日期）。"""

    start_date: str = Field(..., description="开始日期, 格式 YYYY-MM-DD")
    end_date: str = Field(..., description="结束日期, 格式 YYYY-MM-DD")
    prod_code: str = Field(..., description="产品代码 (必须精确指定以避免全表扫)")
    factory: Optional[str] = Field(None, description="厂别 (ARRAY/OLED/TP)")
    step_id: Optional[str] = Field(None, description="站点ID")
    tt_name: Optional[str] = Field(None, description="TT 参数名")


def _read_sql(db_manager: "DatabaseManager", sql: str, errMsg: str) -> pd.DataFrame:
    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")
        df = pd.read_sql(text(sql), db_manager.engine)
        df.columns = df.columns.str.lower()
        return df
    except Exception as exc:  # noqa: BLE001 - DAO 层容错，返回空表由上层降级
        logger.error("%s: %s", errMsg, exc, exc_info=True)
        return pd.DataFrame()


def load_tt_param_set(
    db_manager: "DatabaseManager",
    prod_code: str,
) -> pd.DataFrame:
    """加载 TT 参数全集：规格表中 param_type IS NULL 的 (step_id, param_name) 组合。"""
    sql_query = f"""
    SELECT DISTINCT step_id, param_name
    FROM mdw.dwd_imp_dv_param_spec
    WHERE prod_code = '{prod_code}'
      AND param_type IS NULL
    """
    df = _read_sql(db_manager, sql_query, "[DAO] 提取 TT 参数集失败")
    if df.empty:
        return pd.DataFrame(columns=["step_id", "param_name"])
    logger.info("[DAO] 产品 %s 的 TT 参数组合共 %d 个。", prod_code, len(df))
    return df


def load_tt_spec_limits(
    db_manager: "DatabaseManager",
    prod_code: str,
) -> pd.DataFrame:
    """加载 TT 规格线（usl/ucl，粒度 prod+step+param，三张图共用）。"""
    sql_query = f"""
    SELECT
        prod_code,
        step_id,
        param_name AS tt_name,
        usl,
        ucl
    FROM mdw.dwd_imp_dv_param_spec
    WHERE prod_code = '{prod_code}'
      AND param_type IS NULL
    """
    df = _read_sql(db_manager, sql_query, "[DAO] 提取 TT 规格基准失败")
    if df.empty:
        return pd.DataFrame(columns=TT_SPEC_COLUMNS)

    df["usl"] = pd.to_numeric(df["usl"], errors="coerce")
    df["ucl"] = pd.to_numeric(df["ucl"], errors="coerce")
    logger.info("[DAO] 成功提取 %d 条 TT 规格规则。", len(df))
    return df


def load_tt_details(
    db_manager: "DatabaseManager",
    query_config: AoiTtQueryConfig,
) -> pd.DataFrame:
    """加载三厂 TT 明细并抹平列名差异（UNION ALL + 产品字典 join + TT 参数过滤）。"""
    logger.info("==> [DAO] 抽取产品 %s 的 AOI TT 明细...", query_config.prod_code)

    param_set_df = load_tt_param_set(db_manager, query_config.prod_code)
    if param_set_df.empty:
        logger.warning("[DAO] 产品 %s 无 TT 参数规格（param_type IS NULL），返回空明细。", query_config.prod_code)
        return pd.DataFrame(columns=TT_DETAIL_COLUMNS)

    # (step_id, param_name) 组合过滤：跨厂统一拼接，站点全局唯一，错配组合天然不命中
    pair_conditions = " OR ".join(
        f"(T.step_id = '{row.step_id}' AND T.param_name = '{row.param_name}')"
        for row in param_set_df.itertuples(index=False)
    )

    start_time_fmt = f"{query_config.start_date} 00:00:00"
    end_time_fmt = f"{query_config.end_date} 23:59:59"

    sql_queries = []
    for fac, (table_name, id_col, time_col) in TT_FACTORY_META.items():
        q = f"""
        SELECT
            '{fac}' AS factory,
            P.productcode AS prod_code,
            T.{time_col} AS start_time,
            T.{id_col} AS sheet_id,
            T.lot_id,
            T.step_id,
            T.param_name AS tt_name,
            T.param_value AS tt_qty
        FROM eda.{table_name} T
        JOIN mdw.dwr_mes_productspec P ON T.product_spec = P.productspecname
        WHERE T.{time_col} >= '{start_time_fmt}'
          AND T.{time_col} <= '{end_time_fmt}'
          AND P.productcode = '{query_config.prod_code}'
          AND ({pair_conditions})
        """
        sql_queries.append(q)

    final_sql = " UNION ALL ".join(sql_queries)
    df = _read_sql(db_manager, final_sql, "[DAO] 提取 AOI TT 明细失败")
    if df.empty:
        return pd.DataFrame(columns=TT_DETAIL_COLUMNS)

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["tt_qty"] = pd.to_numeric(df["tt_qty"], errors="coerce").fillna(0)
    df = df.dropna(subset=["start_time"]).reset_index(drop=True)
    logger.info("[DAO] 成功提取 %d 条 AOI TT 明细。", len(df))
    return df
