"""Inline 报表共享图表层（app/charts/inline/）。

承载 spc / ctq / aoi_rs / aoi_tt 四个报表复用的绘图逻辑：折线/箱线决策、
规格线绘制、月周天与 Sheet 点位图表、AOI 趋势/点线图。section 层只组装，
不直接绘制。
"""

from app.charts.inline_domain.aoi_charts import (
    AoiSpecLine,
    add_spec_trace,
    code_color_map,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
)
from app.charts.inline_domain.chart_type import (
    CHART_TYPE_BOX,
    CHART_TYPE_LINE,
    resolve_chart_type,
)
from app.charts.inline_domain.constants import (
    CODE_PALETTE,
    PERIOD_BAR_COLORS,
    PERIOD_COLORS,
    PERIOD_FILL_COLORS,
    PERIOD_LABELS,
    PERIOD_SEPARATORS,
    PERIOD_TYPE_NAMES,
    PERIOD_WINDOW_LIMITS,
    SHEET_BOX_PALETTE,
)
from app.charts.inline_domain.sheet_charts import (
    create_period_overview_chart,
    create_sheet_points_box_chart,
    create_sheet_points_box_charts,
)
from app.charts.inline_domain.spec_lines import (
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
    "code_color_map",
    "create_aoi_period_trend_chart",
    "create_aoi_point_chart",
    "create_period_overview_chart",
    "create_sheet_points_box_chart",
    "create_sheet_points_box_charts",
    "first_measurement_spec_row",
    "format_spec_value",
    "resolve_chart_type",
    "resolve_cl_value",
    "resolve_measurement_y_range",
    "resolve_target_value",
]
