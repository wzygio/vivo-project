"""Inline 报表 section 的公共管线（public pipeline）。

统一承载四个 inline 报表页面（spc / ctq / aoi_rs / aoi_tt）复用的前端处理逻辑：
级联筛选、折线/箱线图决策、规格线绘制、月周天与 Sheet 点位图表、AOI 趋势/点线图。
各 section 只保留业务差异（指标语义、规格口径、文案、session key 前缀）。
"""

from app.sections.inline_domain.shared.aoi_charts import (
    AoiSpecLine,
    add_spec_trace,
    code_color_map,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
)
from app.sections.inline_domain.shared.chart_type import (
    CHART_TYPE_BOX,
    CHART_TYPE_LINE,
    resolve_chart_type,
)
from app.sections.inline_domain.shared.constants import (
    CODE_PALETTE,
    INLINE_FACTORY_OPTIONS,
    PERIOD_BAR_COLORS,
    PERIOD_COLORS,
    PERIOD_FILL_COLORS,
    PERIOD_LABELS,
    PERIOD_SEPARATORS,
    PERIOD_TYPE_NAMES,
    PERIOD_WINDOW_LIMITS,
    SHEET_BOX_PALETTE,
)
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
from app.sections.inline_domain.shared.sheet_charts import (
    create_period_overview_chart,
    create_sheet_points_box_chart,
    create_sheet_points_box_charts,
)
from app.sections.inline_domain.shared.spec_lines import (
    apply_measurement_spec_lines,
    first_measurement_spec_row,
    format_spec_value,
    resolve_cl_value,
    resolve_measurement_y_range,
    resolve_target_value,
)

__all__ = [
    "AoiSpecLine",
    "CHART_TYPE_BOX",
    "CHART_TYPE_LINE",
    "CODE_PALETTE",
    "INLINE_FACTORY_OPTIONS",
    "PERIOD_BAR_COLORS",
    "PERIOD_COLORS",
    "PERIOD_FILL_COLORS",
    "PERIOD_LABELS",
    "PERIOD_SEPARATORS",
    "PERIOD_TYPE_NAMES",
    "PERIOD_WINDOW_LIMITS",
    "SHEET_BOX_PALETTE",
    "add_spec_trace",
    "apply_measurement_spec_lines",
    "apply_report_filter",
    "code_color_map",
    "create_aoi_period_trend_chart",
    "create_aoi_point_chart",
    "create_period_overview_chart",
    "create_sheet_points_box_chart",
    "create_sheet_points_box_charts",
    "excel_bytes",
    "filter_signature",
    "first_measurement_spec_row",
    "format_spec_value",
    "get_available_factories",
    "get_options_for_factory_steps",
    "get_steps_for_factory",
    "normalise_selection",
    "render_cascade_filters",
    "render_sheet_oos_decoration_admin",
    "resolve_chart_type",
    "resolve_cl_value",
    "resolve_measurement_y_range",
    "resolve_target_value",
    "unique_sorted",
]
