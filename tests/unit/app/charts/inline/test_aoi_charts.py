import pandas as pd

from app.charts.inline_domain.aoi_charts import (
    AoiSpecLine,
    add_spec_trace,
    code_color_map,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
)
import plotly.graph_objects as go


def _trend_df() -> pd.DataFrame:
    rows = []
    axis = [("month", "2026-07", 101), ("month", "2026-08", 102)]
    axis += [("week", f"2026-W{w:02d}", 200 + i) for i, w in enumerate((30, 31, 32), start=1)]
    axis += [("day", f"2026-08-{d:02d}", 300 + i) for i, d in enumerate(range(4, 11), start=1)]
    for period_type, label, sort in axis:
        rows.append(
            {
                "period_type": period_type,
                "period_label": label,
                "period_sort": sort,
                "factory": "ARRAY",
                "step_id": "11620",
                "code": "C1",
                "value": 0.5,
                "sheet_qty": 10,
            }
        )
    return pd.DataFrame(rows)


def test_trend_chart_has_grouped_axis_bars_line_and_spec_traces() -> None:
    trend_df = _trend_df()
    throughput_df = trend_df[["period_type", "period_label", "period_sort", "sheet_qty"]].drop_duplicates()
    figure = create_aoi_period_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_lines=[AoiSpecLine(0.8, "USL", "#dc2626"), AoiSpecLine(0.7, "UCL", "#f59e0b", "dot")],
        code_name="TDSUM",
        title="t",
        line_value_label="TT/片",
        bar_unit_name="检测片数",
        y_title="平均每片 TT 个数",
    )

    bars = [trace for trace in figure.data if trace.type == "bar"]
    lines = [trace for trace in figure.data if trace.type == "scatter"]
    assert len(bars) == 3  # 月/周/天三组柱
    # 折线 + USL + UCL
    assert len(lines) == 3
    # x 轴含两个零宽分隔位（月→周、周→天），显示标签去年份前缀
    x_labels = list(figure.data[0].x) + []
    full_axis = list(lines[0].x)
    assert "07" in full_axis and "W30" in full_axis and "08-04" in full_axis
    assert figure.layout.yaxis2.title.text == "检测片数（片）"


def test_trend_chart_skips_missing_spec_values() -> None:
    trend_df = _trend_df()
    throughput_df = trend_df[["period_type", "period_label", "period_sort", "sheet_qty"]].drop_duplicates()
    figure = create_aoi_period_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_lines=[AoiSpecLine(None, "USL", "#dc2626")],
        code_name="TDSUM",
        title="t",
        line_value_label="TT/片",
        bar_unit_name="检测片数",
        y_title="平均每片 TT 个数",
    )
    lines = [trace for trace in figure.data if trace.type == "scatter"]
    assert len(lines) == 1  # 仅折线，无规格线


def test_point_chart_draws_one_line_per_code_with_spec() -> None:
    point_df = pd.DataFrame(
        [
            {"lot_id": "L1", "first_start_time": "2026-08-01", "code": "A", "value": 0.1},
            {"lot_id": "L2", "first_start_time": "2026-08-02", "code": "A", "value": 0.2},
            {"lot_id": "L1", "first_start_time": "2026-08-01", "code": "B", "value": 0.3},
        ]
    )
    figure = create_aoi_point_chart(
        point_df=point_df,
        id_col="lot_id",
        code_column="code",
        code_specs={
            "A": [AoiSpecLine(0.5, "规格", "#2563eb")],
            "B": [],
        },
        title="t",
        y_title="平均每片个数",
        y_col="value",
    )
    lines = [trace for trace in figure.data if trace.type == "scatter"]
    assert len(lines) == 3  # A 线 + B 线 + A 的规格线
    # x 按首次过货时间排序
    assert list(lines[0].x) == ["L1", "L2"]


def test_point_chart_uses_code_display_names() -> None:
    point_df = pd.DataFrame(
        [{"lot_id": "L1", "first_start_time": "2026-08-01", "code": "A", "value": 0.1}]
    )
    figure = create_aoi_point_chart(
        point_df=point_df,
        id_col="lot_id",
        code_column="code",
        code_specs={"A": [AoiSpecLine(0.5, "规格", "#2563eb")]},
        title="t",
        y_title="y",
        y_col="value",
        code_names={"A": "A（描述）"},
    )
    names = [trace.name for trace in figure.data]
    assert "A（描述）" in names
    assert "A（描述） 规格" in names


def test_add_spec_trace_spans_first_to_last_x() -> None:
    fig = go.Figure()
    add_spec_trace(fig, ["a", "b", "c"], 1.5, "s", "#000000")
    trace = fig.data[0]
    assert list(trace.x) == ["a", "c"]
    assert list(trace.y) == [1.5, 1.5]


def test_code_color_map_cycles_palette() -> None:
    colors = code_color_map(["A", "B"])
    assert colors["A"] != colors["B"]
    assert colors["A"] == code_color_map(["A", "B"])["A"]
