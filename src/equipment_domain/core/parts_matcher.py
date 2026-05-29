# src/equipment_domain/core/parts_matcher.py
"""
[核心业务逻辑] 关键备件规格与数据库记录的匹配算法。

职责:
1. 基于子串匹配（机台、腔室、备件类型）建立关联
2. 从匹配结果中选取 glass_start_time 最新的记录
3. 纯业务逻辑，不依赖数据库或文件 I/O
"""

from typing import Optional

import pandas as pd


def find_matching_db_record(
    spec_row: pd.Series,
    db_df: pd.DataFrame,
) -> Optional[pd.Series]:
    """
    对一条规格行，在 DB 结果中查找匹配的最近记录。

    匹配逻辑（AND 条件）：
    1. 机台匹配: spec.机台 是 db.sub_equip_id 的子串（大小写不敏感）
    2. 腔室匹配: spec.腔室 是 db.sub_equip_id 的子串
       - 若腔室含 '/' (如 P3/P4)，拆分为 ['P3','P4'] 任一匹配即可
    3. 备件类型匹配: spec.备件类型 是 db.param_name 的子串

    Args:
        spec_row: 一条基线规格行 (包含 机台, 腔室, 备件类型 列)
        db_df: 全部 DB 记录 DataFrame

    Returns:
        匹配到的 DB 记录 (Series, 取 glass_start_time 最新的那条)
        若无匹配返回 None
    """
    if db_df.empty:
        return None

    machine = str(spec_row.get("机台", "")).strip().upper()
    chambers_raw = str(spec_row.get("腔室", "")).strip()
    chambers = [ch.strip().upper() for ch in chambers_raw.split("/") if ch.strip()]
    part_type = str(spec_row.get("备件类型", "")).strip().upper()

    if not machine or not chambers or not part_type:
        return None

    def _matches(row: pd.Series) -> bool:
        sub_id = str(row.get("sub_equip_id", "")).upper()
        p_name = str(row.get("param_name", "")).upper()

        if machine not in sub_id:
            return False
        if not any(ch in sub_id for ch in chambers):
            return False
        if part_type not in p_name:
            return False
        return True

    matched = db_df[db_df.apply(_matches, axis=1)]

    if matched.empty:
        return None

    valid_time_mask = matched["glass_start_time"].notna()
    if valid_time_mask.any():
        return matched.loc[matched["glass_start_time"].idxmax()]
    else:
        return matched.iloc[0]
