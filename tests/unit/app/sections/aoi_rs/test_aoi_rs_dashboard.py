"""AOI_RS Dashboard 测试：筛选过滤、图表构建、渲染门控。"""

from contextlib import nullcontext
from datetime import date

import pandas as pd

from app.sections.aoi_rs import aoi_rs_dashboard
from app.sections.aoi_rs.aoi_rs_dashboard import (
    create_aoi_rs_point_chart,
    create_aoi_rs_trend_chart,
    filter_aoi_rs_report,
    get_codes_for_factory_steps,
    get_default_aoi_rs_start_date,
    render_aoi_rs_indicator_sections,
)


def _trend_df() -> pd.DataFrame:
    rows = []
    # 2 月 + 3 周 + 7 天的 period 轴（period_sort 已按 100/200/300 编排）
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
                "step_id": "11629",
                "rs_code": "A1PPS",
                "rs_qty": 5,
                "sheet_qty": 10,
                "value": 0.5,
            }
        )
        rows.append(
            {
                "period_type": period_type,
                "period_label": label,
                "period_sort": sort,
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A2CIP",
                "rs_qty": 2,
                "sheet_qty": 10,
                "value": 0.2,
            }
        )
    return pd.DataFrame(rows)


def test_default_start_date_uses_previous_month_first_day() -> None:
    assert get_default_aoi_rs_start_date(date(2026, 8, 10)) == date(2026, 7, 1)
    assert get_default_aoi_rs_start_date(date(2026, 1, 15)) == date(2025, 12, 1)


def test_filter_options_cascade_factory_step_code() -> None:
    indicator_df = pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "d1"},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "12629", "rs_code": "A2CIP", "code_desc": "d2"},
            {"prod_code": "M678", "factory": "TP", "step_id": "43629", "rs_code": "T3DMR", "code_desc": "d3"},
        ]
    )
    assert get_codes_for_factory_steps(indicator_df, "ARRAY", ["11629"]) == ["A1PPS"]
    assert get_codes_for_factory_steps(indicator_df, "TP", ["43629"]) == ["T3DMR"]
    assert get_codes_for_factory_steps(indicator_df, "ARRAY", []) == []


def test_filter_report_by_factory_codes_steps() -> None:
    df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "v": 1},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A2CIP", "v": 2},
            {"factory": "TP", "step_id": "43629", "rs_code": "T3DMR", "v": 3},
        ]
    )
    out = filter_aoi_rs_report(df, "ARRAY", ["A1PPS"], ["11629"])
    assert list(out["v"]) == [1]


def test_trend_chart_has_bars_line_spec_and_grouped_axis() -> None:
    trend_df = _trend_df()
    trend_df = trend_df[trend_df["rs_code"] == "A1PPS"]
    throughput_df = (
        trend_df[["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]]
        .drop_duplicates()
    )

    figure = create_aoi_rs_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_value=0.8,
        code_name="A1PPS（d1）",
        title="月周天趋势",
    )

    bar_traces = [t for t in figure.data if t.type == "bar"]
    # 月/周/天各一组柱状（分组配色区分粒度）
    assert len(bar_traces) == 3
    assert {t.name for t in bar_traces} == {"过货量（月）", "过货量（周）", "过货量（天）"}
    assert len({t.marker.color for t in bar_traces}) == 3
    # 柱状在次 Y 轴，不与比值线互相压扁
    assert all(t.yaxis == "y2" for t in bar_traces)
    # 只有过货量柱保留图注；折线与规格线的图注被去掉
    assert all(t.showlegend is not False for t in bar_traces)
    # 单 Code 一条比值线 + 一条规格虚线
    lines = [t for t in figure.data if t.type == "scatter" and t.mode == "lines+markers"]
    assert [t.name for t in lines] == ["A1PPS（d1）"]
    spec_traces = [t for t in figure.data if t.type == "scatter" and "规格" in (t.name or "")]
    assert len(spec_traces) == 1
    assert all(not t.showlegend for t in lines + spec_traces)
    # x 轴：2 月 + 分隔 + 3 周 + 分隔 + 7 天 = 14 个类目，组间留白；标签不含年份
    x_labels = list(lines[0].x)
    assert len(x_labels) == 14
    assert x_labels[0] == "07" and x_labels[1] == "08"
    assert x_labels[3] == "W30"
    assert x_labels[-1] == "08-10"
    assert not any("2026" in label for label in x_labels)
    # 分隔位置无线值（断开），但有柱位（2月+sep+3周+sep+7天 → 索引 2 与 6）
    sep_indices = [2, 6]
    assert all(pd.isna(lines[0].y[i]) for i in sep_indices)
    # 月组柱子带全月过货量
    month_bar = [t for t in bar_traces if t.name == "过货量（月）"][0]
    assert list(month_bar.y) == [10, 10]


def test_trend_chart_without_spec_draws_no_spec_line() -> None:
    trend_df = _trend_df()
    trend_df = trend_df[trend_df["rs_code"] == "A1PPS"]
    throughput_df = (
        trend_df[["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]]
        .drop_duplicates()
    )
    figure = create_aoi_rs_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_value=None,
        code_name="A1PPS",
        title="月周天趋势",
    )
    assert not [t for t in figure.data if t.type == "scatter" and "规格" in (t.name or "")]


def test_point_chart_orders_x_by_first_time_and_draws_spec() -> None:
    lot_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "lot_id": "LOT-B", "rs_qty": 5, "first_start_time": pd.Timestamp("2026-08-02")},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "lot_id": "LOT-A", "rs_qty": 1, "first_start_time": pd.Timestamp("2026-08-01")},
        ]
    )
    figure = create_aoi_rs_point_chart(
        point_df=lot_df,
        id_col="lot_id",
        code_specs={"A1PPS": 30},
        code_names={"A1PPS": "A1PPS"},
        title="By Lot",
        y_title="RS个数",
    )

    scatter = [t for t in figure.data if t.mode == "lines+markers"][0]
    assert list(scatter.x) == ["LOT-A", "LOT-B"]
    assert list(scatter.y) == [1, 5]
    assert any("规格" in (t.name or "") for t in figure.data)
    # 图注下移到底部边距内，不遮挡竖排的 ID 标签
    assert figure.layout.legend.y <= -0.45
    assert figure.layout.margin.b >= 180


def test_point_chart_supports_value_column_for_lot_average() -> None:
    lot_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "lot_id": "LOT-B", "rs_qty": 5, "sheet_qty": 4, "value": 1.25, "first_start_time": pd.Timestamp("2026-08-02")},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "lot_id": "LOT-A", "rs_qty": 1, "sheet_qty": 2, "value": 0.5, "first_start_time": pd.Timestamp("2026-08-01")},
        ]
    )
    figure = create_aoi_rs_point_chart(
        point_df=lot_df,
        id_col="lot_id",
        code_specs={"A1PPS": 3},
        code_names={"A1PPS": "A1PPS"},
        title="By Lot",
        y_title="平均每片 RS 个数",
        y_col="value",
    )

    scatter = [t for t in figure.data if t.mode == "lines+markers"][0]
    assert list(scatter.x) == ["LOT-A", "LOT-B"]
    assert list(scatter.y) == [0.5, 1.25]  # 画的是 Lot 内平均每片，而非 Σcode_qty


def test_render_sections_expander_per_code_with_three_side_by_side_charts(monkeypatch) -> None:
    rendered: list[object] = []
    expander_titles: list[str] = []
    expander_expanded: list[bool] = []

    class _FakeExpander:
        def __init__(self, title, expanded):
            self.title = title
            self.expanded = expanded
        def __enter__(self):
            expander_titles.append(self.title)
            expander_expanded.append(self.expanded)
            return self
        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "expander",
        lambda title, expanded=False, **_kw: _FakeExpander(title, expanded),
    )
    monkeypatch.setattr(aoi_rs_dashboard.st, "plotly_chart", lambda fig, **_kw: rendered.append(fig))
    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "columns",
        lambda n, **_kw: [nullcontext() for _ in range(n if isinstance(n, int) else len(n))],
    )
    monkeypatch.setattr(aoi_rs_dashboard.st, "subheader", lambda *_a, **_kw: None)
    monkeypatch.setattr(aoi_rs_dashboard.st, "container", lambda **_kw: nullcontext())
    monkeypatch.setattr(aoi_rs_dashboard.st, "info", lambda *_a, **_kw: None)

    details = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 08:00"), "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 3},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 09:00"), "sheet_id": "S2", "lot_id": "L2", "step_id": "11629", "rs_code": "A2CIP", "code_qty": 1},
            {"factory": "TP", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 10:00"), "sheet_id": "G1", "lot_id": "L9", "step_id": "43629", "rs_code": "T3DMR", "code_qty": 2},
        ]
    )
    pass_through = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 07:00"), "sheet_id": "S1", "step_id": "11629"},
            {"factory": "TP", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 07:00"), "sheet_id": "G1", "step_id": "43629"},
        ]
    )
    indicators = pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "PHT责M1残留"},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11629", "rs_code": "A2CIP", "code_desc": None},
            {"prod_code": "M678", "factory": "TP", "step_id": "43629", "rs_code": "T3DMR", "code_desc": None},
        ]
    )

    render_aoi_rs_indicator_sections(
        rs_details_df=details,
        pass_through_df=pass_through,
        spec_df=pd.DataFrame(),
        indicators_df=indicators,
        end_date=date(2026, 8, 10),
    )

    # 每个（站点+Code）一个默认展开的 Expander：3 个 Code → 3 个 expander × 3 图 = 9 张图
    assert len(expander_titles) == 3
    assert all(expander_expanded)
    assert len(rendered) == 9
    # Expander 标题含站点与 Code（带中文名）
    assert any("11629" in t and "A1PPS" in t and "PHT责M1残留" in t for t in expander_titles)
    assert any("43629" in t and "T3DMR" in t for t in expander_titles)
