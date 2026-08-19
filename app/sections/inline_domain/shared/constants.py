"""Inline 报表 section 共享常量：厂别顺序、调色板与周期标签。"""

from __future__ import annotations

# 四个 inline 报表共用的厂别展示顺序
INLINE_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]

# Sheet 点位箱线图按腔室着色 / AOI 按 Code 分线共用的调色板
SHEET_BOX_PALETTE = ["#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#0f766e", "#dc2626", "#64748b"]
CODE_PALETTE = SHEET_BOX_PALETTE

# SPC/CTQ 月周天分布：周期标签、窗口上限与箱线配色
PERIOD_LABELS = {"month": "月", "week": "周", "day": "日"}
PERIOD_WINDOW_LIMITS = {"month": 2, "week": 3, "day": 7}
PERIOD_COLORS = {"month": "#2563eb", "week": "#16a34a", "day": "#f59e0b"}
PERIOD_FILL_COLORS = {
    "month": "rgba(37, 99, 235, 0.18)",
    "week": "rgba(22, 163, 74, 0.18)",
    "day": "rgba(245, 158, 11, 0.18)",
}

# AOI 月周天趋势：过货量/检测片数柱状配色与周期名
PERIOD_BAR_COLORS = {
    "month": "rgba(37, 99, 235, 0.55)",
    "week": "rgba(22, 163, 74, 0.55)",
    "day": "rgba(245, 158, 11, 0.55)",
}
PERIOD_TYPE_NAMES = {"month": "月", "week": "周", "day": "天"}
# 月/周/天组间留白：零宽空格分隔符，两个间隙用不同数量避免 category 轴合并
PERIOD_SEPARATORS = ["​", "​​"]
