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


def test_trend_chart_has_line_per_code_and_spec_lines_ordered_axis() -> None:
    figure = create_aoi_rs_trend_chart(
        trend_df=_trend_df(),
        code_specs={"A1PPS": 0.8, "A2CIP": None},
        code_names={"A1PPS": "A1PPS（d1）", "A2CIP": "A2CIP"},
        title="ARRAY | 11629 | 月周天趋势",
    )

    scatter_traces = [t for t in figure.data if t.type == "scatter" and t.mode == "lines+markers"]
    assert {t.name for t in scatter_traces} == {"A1PPS（d1）", "A2CIP"}
    spec_traces = [t for t in figure.data if t.type == "scatter" and t.name and "规格" in t.name]
    # 仅 A1PPS 有规格线；A2CIP 规格为 None 不画
    assert len(spec_traces) == 1
    assert "A1PPS" in spec_traces[0].name
    # x 轴顺序：2 月 → 3 周 → 7 天
    x_labels = list(figure.data[0].x)
    assert x_labels[0] == "2026-07" and x_labels[1] == "2026-08"
    assert x_labels[2].startswith("2026-W")
    assert x_labels[-1] == "2026-08-10"
    assert len(x_labels) == 12


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


def test_render_sections_renders_three_charts_per_step(monkeypatch) -> None:
    rendered: list[object] = []
    headers: list[str] = []
    monkeypatch.setattr(aoi_rs_dashboard.st, "subheader", lambda text, **_kw: headers.append(text))
    monkeypatch.setattr(aoi_rs_dashboard.st, "plotly_chart", lambda fig, **_kw: rendered.append(fig))
    monkeypatch.setattr(aoi_rs_dashboard.st, "container", lambda **_kw: nullcontext())
    monkeypatch.setattr(aoi_rs_dashboard.st, "info", lambda *_a, **_kw: None)

    details = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 08:00"), "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 3},
            {"factory": "TP", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 09:00"), "sheet_id": "G1", "lot_id": "L9", "step_id": "43629", "rs_code": "T3DMR", "code_qty": 2},
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

    # 两个站点分组 × 三张图
    assert len(rendered) == 6
    assert len(headers) == 2
    assert any("11629" in h for h in headers)
