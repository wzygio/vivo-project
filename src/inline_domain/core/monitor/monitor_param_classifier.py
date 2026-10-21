# -*- coding: utf-8 -*-
"""
SPC 参数类型分类器 (Core Domain)

纯函数，无 I/O 依赖，将 IMP_SPC_TZBJX 表中的原始 data_type 值映射为标准分类标签。
"""

from typing import Optional

PARAM_CLASSIFICATION_SIGNATURE = "inline-param-type-v3-tt-allowlist"


def is_ppa_parameter(param_name: object) -> bool:
    """PPA measurements belong to SPC regardless of missing/stale type metadata."""
    return "PPA" in str(param_name).upper()


def classify_param_type(
    raw_data_type: Optional[str], param_name: object = None,
) -> str:
    """
    将 DB 原始 data_type 值映射为统一分类标签。

    映射规则:
        - 参数名称包含 PPA（不区分大小写）→ 'SPC'，优先于原始类型
        - NULL / 空字符串 / 仅空白 → 'AOI'
        - 其他 → 去空白后转大写 (如 'spc' → 'SPC', 'ctq' → 'CTQ')

    Args:
        raw_data_type: IMP_SPC_TZBJX.data_type 的原始值，可能为 None
        param_name: 参数名称；未提供时保持原始类型映射行为

    Returns:
        'SPC' | 'CTQ' | 'AOI' | 其他大写字符串
    """
    if is_ppa_parameter(param_name):
        return 'SPC'

    if raw_data_type is None:
        return 'AOI'

    stripped = str(raw_data_type).strip()
    if stripped == '':
        return 'AOI'

    return stripped.upper()
