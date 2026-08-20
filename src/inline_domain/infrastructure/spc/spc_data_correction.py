"""SPC 原始量测数值修正规则。

在本地 Parquet 快照生成之前应用，修正写入快照并由快照仓库返回，
确保所有下游（SPC/CTQ/monitor 等）消费到的都是修正后的数值。

当前规则：
- prod_code 为 "M673"
- param_name 包含 "PPA"（不区分大小写）
- site_name 在 [99, 114] 闭区间内
→ param_value 减 5
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

M673_PPA_TARGET_PROD = "M673"
M673_PPA_KEYWORD = "PPA"
M673_PPA_SITE_MIN = 99
M673_PPA_SITE_MAX = 114
M673_PPA_VALUE_OFFSET = -5.0


def apply_spc_value_corrections(measurements: pd.DataFrame) -> pd.DataFrame:
    """返回应用 SPC 数值修正后的量测副本；空表或无命中时原样返回。"""
    if measurements is None or measurements.empty:
        return measurements

    corrected = measurements.copy()
    site = pd.to_numeric(corrected["site_name"], errors="coerce")
    mask = (
        corrected["prod_code"].astype(str).str.strip().str.upper().eq(M673_PPA_TARGET_PROD)
        & corrected["param_name"]
        .astype(str)
        .str.contains(M673_PPA_KEYWORD, case=False, na=False)
        & site.between(M673_PPA_SITE_MIN, M673_PPA_SITE_MAX, inclusive="both")
    )

    hit_count = int(mask.sum())
    if hit_count == 0:
        return corrected

    values = pd.to_numeric(corrected["param_value"], errors="coerce")
    corrected.loc[mask, "param_value"] = values[mask] + M673_PPA_VALUE_OFFSET
    logger.info(
        "🛠️ [SpcCorrection] M673 PPA site[%s,%s] 命中 %s 条记录，param_value 修正 %+g",
        M673_PPA_SITE_MIN,
        M673_PPA_SITE_MAX,
        hit_count,
        M673_PPA_VALUE_OFFSET,
    )
    return corrected
