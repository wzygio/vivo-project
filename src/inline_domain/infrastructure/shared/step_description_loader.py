"""Reusable loader for the process operation dictionary (step_id -> 站点描述).

纯展示用途：结果只服务前端站点标签拼接，不进入 application/core 层。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

STEP_DESCRIPTION_COLUMNS = ["step_id", "step_desc"]

# 工序字典候选表名：准确表名无法离线验证，按候选顺序逐个尝试。
STEP_DESCRIPTION_TABLE_CANDIDATES = [
    "mdw.dwr_mes_processoperationspec_v",
    "mdw.dwr_mes_processoperationspec",
]


def load_step_descriptions(db_manager: "DatabaseManager") -> pd.DataFrame:
    """Load step_id -> step_desc pairs from the process operation dictionary.

    按候选表名顺序尝试，捕获数据库异常后回退下一个；全部失败时返回带列空表，
    仅记录 warning，保证页面不因字典缺失而崩。
    """
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized.")

    result: pd.DataFrame | None = None
    last_error: Exception | None = None
    for table in STEP_DESCRIPTION_TABLE_CANDIDATES:
        try:
            result = pd.read_sql(
                text(
                    f"""
                    SELECT DISTINCT
                        oper_code AS step_id,
                        description AS step_desc
                    FROM {table}
                    WHERE oper_code IS NOT NULL
                      AND description IS NOT NULL
                    """
                ),
                db_manager.engine,
            )
            result.columns = result.columns.str.lower()
            break
        except Exception as exc:  # 表不存在/方言差异时回退下一个候选表
            last_error = exc
            logger.warning("读取工序字典表 %s 失败：%s", table, exc)

    if result is None:
        logger.warning("工序字典不可用（最后错误：%s），站点描述降级为空。", last_error)
        return pd.DataFrame(columns=STEP_DESCRIPTION_COLUMNS)
    if result.empty:
        return pd.DataFrame(columns=STEP_DESCRIPTION_COLUMNS)

    normalized = result.reindex(columns=STEP_DESCRIPTION_COLUMNS).copy()
    normalized["step_id"] = normalized["step_id"].astype(str).str.strip()
    normalized["step_desc"] = normalized["step_desc"].astype(str).str.strip()
    normalized = normalized[normalized["step_id"].ne("") & normalized["step_desc"].ne("")]
    # 同一 step_id 多条记录时取首个非空描述
    return normalized.drop_duplicates(subset=["step_id"], keep="first").reset_index(drop=True)


def build_step_description_map(df: pd.DataFrame) -> dict[str, str]:
    """Build a step_id -> step_desc mapping from the loader result."""
    if df.empty:
        return {}
    return dict(zip(df["step_id"].astype(str), df["step_desc"].astype(str)))
