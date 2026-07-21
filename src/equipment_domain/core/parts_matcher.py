# src/equipment_domain/core/parts_matcher.py
"""
[核心业务逻辑] 关键备件规格与数据库记录的匹配算法。

职责:
1. 对快照数据按 (step_id, sub_equip_id) 预建索引，O(1) 查找
2. 基于 (站点=step_id, 机台号-腔室=sub_equip_id, 参数名称 LIKE pattern) 精确匹配
3. 从匹配结果中选取 glass_start_time 最新的记录
4. 纯业务逻辑，不依赖数据库或文件 I/O
"""

import re
from typing import Optional

import pandas as pd

from src.equipment_domain.core.parts_identity import build_fabricated_param_name


def _build_snapshot_index(
    snapshot_df: pd.DataFrame,
) -> dict:
    """
    对快照 DataFrame 按 (step_id, sub_equip_id) 建哈希索引。

    Args:
        snapshot_df: DB 快照 DataFrame（含 step_id, sub_equip_id, param_name, value, glass_start_time）

    Returns:
        dict: {(step_id, sub_equip_id): DataFrame subset}
    """
    if snapshot_df.empty:
        return {}

    index: dict[tuple, pd.DataFrame] = {}
    for (st, mc), group in snapshot_df.groupby(["step_id", "sub_equip_id"], sort=False):
        index[(st, mc)] = group

    return index


def _compile_like_pattern(like_pattern: str) -> re.Pattern:
    """
    将 SQL LIKE 模式编译为正则表达式。

    SQL LIKE 规则:
    - % → .*  (任意字符序列)
    - _ → .   (任意单个字符)

    Args:
        like_pattern: SQL LIKE 模式，如 '%TARGET_KWH%' 或 '%MASKLIFE%_MAX'

    Returns:
        re.Pattern: 编译后的正则
    """
    # re.escape 不转义 % 和 _（它们不是 Python 正则元字符）
    # 因此直接替换 SQL LIKE 通配符 → 正则等价物
    escaped = re.escape(like_pattern)
    escaped = escaped.replace("%", ".*")
    escaped = escaped.replace("_", ".")
    return re.compile("^" + escaped + "$", re.IGNORECASE)


def find_matching_db_record(
    spec_row: pd.Series,
    snapshot_df: pd.DataFrame,
    snapshot_index: Optional[dict] = None,
) -> Optional[pd.Series]:
    """
    对一条规格行，在 DB 快照结果中查找匹配的最近记录。

    匹配逻辑（AND 条件）：
    1. 站点精确匹配: spec.站点 == db.step_id
    2. 机台号-腔室精确匹配: spec.机台号-腔室 == db.sub_equip_id
    3. 参数名称 LIKE 匹配: db.param_name LIKE spec.参数名称

    Args:
        spec_row: 一条基线规格行 (包含 站点, 机台号-腔室, 参数名称 列)
        snapshot_df: 全部 DB 快照记录 DataFrame
        snapshot_index: 预建索引 dict，若为 None 则自动构建

    Returns:
        匹配到的 DB 记录 (Series, 取 glass_start_time 最新的那条)
        若无匹配返回 None
    """
    if snapshot_df.empty:
        return None

    station = str(spec_row.get("站点", "")).strip()
    machine_chamber = str(spec_row.get("机台号-腔室", "")).strip()
    raw_param_pattern = spec_row.get("参数名称", "")
    param_pattern = (
        ""
        if raw_param_pattern is None or bool(pd.isna(raw_param_pattern))
        else str(raw_param_pattern).strip()
    )

    if not station or not machine_chamber:
        return None

    # 使用预建索引 O(1) 查找
    if snapshot_index is not None:
        subset = snapshot_index.get((station, machine_chamber))
        if subset is None or subset.empty:
            return None
    else:
        subset = snapshot_df[
            (snapshot_df["step_id"] == station)
            & (snapshot_df["sub_equip_id"] == machine_chamber)
        ]
        if subset.empty:
            return None

    # 非空参数沿用 LIKE；空参数只精确匹配仿造数据内部键。
    if param_pattern:
        like_re = _compile_like_pattern(param_pattern)
        param_mask = subset["param_name"].astype(str).str.match(like_re, na=False)
    else:
        fabricated_param_name = build_fabricated_param_name(spec_row)
        param_mask = subset["param_name"].astype(str).eq(fabricated_param_name)
    matched = subset[param_mask]

    if matched.empty:
        return None

    # 取 glass_start_time 最新的记录
    valid_time_mask = matched["glass_start_time"].notna()
    if valid_time_mask.any():
        return matched.loc[matched["glass_start_time"].idxmax()]
    else:
        return matched.iloc[0]


def build_and_match_all(
    spec_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    fallback_snapshot_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    批量匹配：对规格表所有行匹配快照数据，返回合并后的报表 DataFrame。

    先建一次性索引，然后逐行查找，避免重复扫描 2.9M 行。

    Args:
        spec_df: 规格基线 DataFrame
        snapshot_df: 真实 DB 快照 DataFrame
        fallback_snapshot_df: 仅在真实记录缺失时使用的仿造快照 DataFrame

    Returns:
        pd.DataFrame: 合并后的报表明细（含测量值、测量时间、匹配参数名）
    """
    fallback_snapshot = (
        fallback_snapshot_df
        if isinstance(fallback_snapshot_df, pd.DataFrame)
        else pd.DataFrame()
    )
    if snapshot_df.empty and fallback_snapshot.empty:
        # 无数据时仍返回完整规格行（测量值为空）
        result = spec_df.copy()
        result["测量值"] = None
        result["测量时间"] = None
        result["匹配参数名"] = None
        return result

    snapshot_index = _build_snapshot_index(snapshot_df)
    fallback_index = _build_snapshot_index(fallback_snapshot)

    rows = []
    for _, spec_row in spec_df.iterrows():
        matched = find_matching_db_record(spec_row, snapshot_df, snapshot_index)
        if matched is None and not fallback_snapshot.empty:
            matched = find_matching_db_record(
                spec_row,
                fallback_snapshot,
                fallback_index,
            )

        row = {
            "厂别": spec_row.get("厂别"),
            "备件类型": spec_row.get("备件类型"),
            "设备类型": spec_row.get("设备类型"),
            "膜层": spec_row.get("膜层"),
            "制程": spec_row.get("制程"),
            "寿命规格": spec_row.get("寿命规格"),
            "站点": spec_row.get("站点"),
            "机台号-腔室": spec_row.get("机台号-腔室"),
            "参数名称": spec_row.get("参数名称"),
            "测量值": matched.get("value") if matched is not None else None,
            "测量时间": str(matched["glass_start_time"]) if matched is not None and pd.notna(matched.get("glass_start_time")) else None,
            "匹配参数名": matched.get("param_name") if matched is not None else None,
        }
        rows.append(row)

    return pd.DataFrame(rows)
