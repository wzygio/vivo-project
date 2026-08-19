"""Inline 报表 section 的共享组装层（spc / ctq / aoi_rs / aoi_tt 共用）。

section 是组装层（对齐后端 application 层）：这里只保留筛选级联与修饰后台等
交互组装逻辑；图表绘制统一位于 ``app/charts/inline/``。
"""

from app.sections.inline_domain.shared.constants import INLINE_FACTORY_OPTIONS
from app.sections.inline_domain.shared.decoration_admin import (
    excel_bytes,
    render_sheet_oos_decoration_admin,
)
from app.sections.inline_domain.shared.filters import (
    apply_report_filter,
    filter_signature,
    get_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory,
    normalise_selection,
    render_cascade_filters,
    unique_sorted,
)

__all__ = [
    "INLINE_FACTORY_OPTIONS",
    "apply_report_filter",
    "excel_bytes",
    "filter_signature",
    "get_available_factories",
    "get_options_for_factory_steps",
    "get_steps_for_factory",
    "normalise_selection",
    "render_cascade_filters",
    "render_sheet_oos_decoration_admin",
    "unique_sorted",
]
