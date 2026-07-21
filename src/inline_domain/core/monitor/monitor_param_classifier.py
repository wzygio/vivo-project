# -*- coding: utf-8 -*-
"""
SPC 参数类型分类器 (Core Domain)

纯函数，无 I/O 依赖，将 IMP_SPC_TZBJX 表中的原始 data_type 值映射为标准分类标签。
"""

from typing import Optional


def classify_param_type(raw_data_type: Optional[str]) -> str:
    """
    将 DB 原始 data_type 值映射为统一分类标签。

    映射规则:
        - NULL / 空字符串 / 仅空白 → 'AOI'
        - 其他 → 去空白后转大写 (如 'spc' → 'SPC', 'ctq' → 'CTQ')

    Args:
        raw_data_type: IMP_SPC_TZBJX.data_type 的原始值，可能为 None

    Returns:
        'SPC' | 'CTQ' | 'AOI' | 其他大写字符串
    """
    if raw_data_type is None:
        return 'AOI'

    stripped = str(raw_data_type).strip()
    if stripped == '':
        return 'AOI'

    return stripped.upper()
