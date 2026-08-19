"""Inline 报表 Sheet 点位图样式决策：折线图或箱线图。

是否绘制折线图由前端配置（参数名包含 token）决定，SPC 与 CTQ 共用同一口径；
匹配不区分大小写，按普通文本而非正则表达式处理。
"""

from __future__ import annotations

from collections.abc import Iterable

CHART_TYPE_BOX = "box"
CHART_TYPE_LINE = "line"


def resolve_chart_type(param_name: object, line_param_name_contains: Iterable[str]) -> str:
    """Resolve the Sheet point chart style from frontend-owned configuration."""
    parameter_name = "" if param_name is None else str(param_name)
    for configured_value in line_param_name_contains:
        token = str(configured_value).strip()
        if token and token.casefold() in parameter_name.casefold():
            return CHART_TYPE_LINE
    return CHART_TYPE_BOX
