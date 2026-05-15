# src/equipment_domain/infrastructure/data_loader.py
"""
[数据访问层 DAO] 关键备件报表数据加载器。

职责:
1. load_spec_baseline()    — 加载 CSV 规格基线配置表
2. load_latest_part_life() — 从 PostgreSQL 查询每个腔室最新的备件寿命数据
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

# ==============================================================================
#  常量定义
# ==============================================================================

REQUIRED_BASELINE_COLUMNS: List[str] = [
    "厂别", "膜层", "制程", "机台", "腔室",
    "备件类型", "寿命规格", "预警值",
]

PART_LIFE_SQL = """
SELECT step_id, sub_equip_id, param_name, value, glass_start_time
FROM (
    SELECT
        B.step_id,
        B.sub_equip_id,
        B.value,
        B.glass_start_time,
        B.param_name,
        ROW_NUMBER() OVER(
            PARTITION BY B.sub_equip_id
            ORDER BY B.glass_start_time DESC
        ) as rn
    FROM eda.ARRAY_PDS_RESULT_T B
    WHERE
        (param_name LIKE '%TRGTLIFE%_MAX' OR param_name LIKE '%MASKLIFE%_MAX')
        AND B.sub_equip_id LIKE '%PM%'
) T
WHERE T.rn = 1
"""


# ==============================================================================
#  公开函数
# ==============================================================================


def load_spec_baseline(baseline_path: str | Path) -> pd.DataFrame:
    """
    加载 CSV 规格基线配置表。

    [防御性设计]
    - 文件不存在 → 抛出 FileNotFoundError 并提供友好提示
    - 必要列缺失 → 抛出 ValueError 并列出缺失列名
    - 数值列异常 → pd.to_numeric(errors='coerce') 容错

    Args:
        baseline_path: CSV 文件路径

    Returns:
        pd.DataFrame: 包含规格基线数据的 DataFrame

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 缺少必要列
    """
    path = Path(baseline_path)

    # 校验文件存在
    if not path.exists():
        raise FileNotFoundError(
            f"规格基线 CSV 文件不存在: {path.resolve()}\n"
            f"请确认 resources/critical_parts_baseline.csv 已正确放置。"
        )

    logger.info(f"正在加载规格基线 CSV: {path}")
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")

    # 校验必要列
    missing_cols = [col for col in REQUIRED_BASELINE_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"规格基线 CSV 缺少必要列: {missing_cols}\n"
            f"当前列: {list(df.columns)}"
        )

    # 数值列安全转换
    df["寿命规格"] = pd.to_numeric(df["寿命规格"], errors="coerce")
    df["预警值"] = pd.to_numeric(df["预警值"], errors="coerce")

    # 去除全空行
    df = df.dropna(how="all").reset_index(drop=True)

    logger.info(f"成功加载 {len(df)} 条规格基线记录")
    return df


def load_latest_part_life(db_manager: "DatabaseManager") -> pd.DataFrame:
    """
    从 eda.ARRAY_PDS_RESULT_T 查询每个腔室最新的备件寿命值。

    使用 ROW_NUMBER() 窗口函数按腔室分组、按时间倒序，取 rn=1。
    支持 TRGTLIFE 和 MASKLIFE 两类参数，仅筛选 '%PM%' 腔室。

    [防御性设计]
    - engine 未初始化 → 日志警告 + 返回空 DataFrame
    - SQL 执行异常 → try-except 捕获 + 日志记录 + 返回空 DataFrame
    - value 列 → pd.to_numeric(errors='coerce')
    - 列名 → 统一转小写

    Args:
        db_manager: 数据库管理器实例

    Returns:
        pd.DataFrame: 包含 step_id, sub_equip_id, param_name, value, glass_start_time
    """
    if db_manager.engine is None:
        logger.warning("数据库引擎未初始化，无法查询备件寿命数据。")
        return pd.DataFrame()

    try:
        logger.info("执行备件寿命最新值 SQL 查询...")
        df = pd.read_sql(text(PART_LIFE_SQL), db_manager.engine)

        # 统一列名小写
        df.columns = df.columns.str.lower()

        if df.empty:
            logger.info("备件寿命查询结果为空。")
            return df

        # value 列数值化
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

        # glass_start_time 转 datetime
        df["glass_start_time"] = pd.to_datetime(
            df["glass_start_time"], errors="coerce"
        )

        logger.info(f"成功提取 {len(df)} 条备件寿命记录。")
        return df

    except Exception as e:
        logger.error(f"查询备件寿命数据失败: {e}", exc_info=True)
        return pd.DataFrame()
