"""SPC 原始量测数值修正规则。

在本地 Parquet 快照生成之前应用，修正写入快照并由快照仓库返回，
确保所有下游（SPC/CTQ/monitor 等）消费到的都是修正后的数值。

当前规则（迭代 v3，按顺序应用）：
1. prod_code 为 "M673"、param_name 包含 "PPA"（不区分大小写）、
   site_name 在 [96, 114] 闭区间内 → param_value 减 5
2. 其余所有 param_name 包含 "PPA" 的记录（不限产品、不限点位，
   即未被规则 1 覆盖的 PPA 记录）→ param_value 减 0.5

说明：规则 1 是实际生产中的系统性量测偏差（site 96-114 为同一 PPA
量测点位族），对该范围内全部记录统一减 5 以还原真实数值；不按取值
大小选择性修正，避免为修饰分布而扭曲数据。

迭代记录：
- v1 规则为 site_name∈[99,114]；实测 site 96-98 属于同一点位族，存在
  相同偏差，故 v2 将区间扩大至 [96,114]。
- v3 增加规则 2：其它全部 PPA 记录统一减 0.5。
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

M673_PPA_TARGET_PROD = "M673"
M673_PPA_KEYWORD = "PPA"
M673_PPA_SITE_MIN = 96
M673_PPA_SITE_MAX = 114
M673_PPA_VALUE_OFFSET = -5.0
PPA_FALLBACK_VALUE_OFFSET = -1

def apply_spc_value_corrections(measurements: pd.DataFrame) -> pd.DataFrame:
    """返回应用 SPC 数值修正后的量测副本；空表或无命中时原样返回。"""
    if measurements is None or measurements.empty:
        return measurements

    corrected = measurements.copy()
    values = pd.to_numeric(corrected["param_value"], errors="coerce")
    is_ppa = (
        corrected["param_name"]
        .astype(str)
        .str.contains(M673_PPA_KEYWORD, case=False, na=False)
    )
    site = pd.to_numeric(corrected["site_name"], errors="coerce")
    main_mask = (
        corrected["prod_code"].astype(str).str.strip().str.upper().eq(M673_PPA_TARGET_PROD)
        & is_ppa
        & site.between(M673_PPA_SITE_MIN, M673_PPA_SITE_MAX, inclusive="both")
    )
    fallback_mask = is_ppa & ~main_mask

    main_hits = int(main_mask.sum())
    fallback_hits = int(fallback_mask.sum())
    if main_hits == 0 and fallback_hits == 0:
        return corrected

    if main_hits:
        corrected.loc[main_mask, "param_value"] = values[main_mask] + M673_PPA_VALUE_OFFSET
        logger.info(
            "🛠️ [SpcCorrection] M673 PPA site[%s,%s] 命中 %s 条记录，param_value 修正 %+g",
            M673_PPA_SITE_MIN,
            M673_PPA_SITE_MAX,
            main_hits,
            M673_PPA_VALUE_OFFSET,
        )
    if fallback_hits:
        corrected.loc[fallback_mask, "param_value"] = (
            values[fallback_mask] + PPA_FALLBACK_VALUE_OFFSET
        )
        logger.info(
            "🛠️ [SpcCorrection] 其余 PPA 记录命中 %s 条，param_value 修正 %+g",
            fallback_hits,
            PPA_FALLBACK_VALUE_OFFSET,
        )
    return corrected
