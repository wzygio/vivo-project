"""AOI_TT Dashboard 测试：筛选级联与门控、过滤函数、图表构建、渲染。"""

from contextlib import nullcontext
from datetime import date

import pandas as pd

from app.sections.inline_domain.aoi_tt import aoi_tt_dashboard
from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
    create_aoi_tt_point_chart,
    create_aoi_tt_trend_chart,
    filter_aoi_tt_report,
    get_available_factories,
    get_codes_for_factory_steps,
    get_default_aoi_tt_start_date,
    get_steps_for_factory,
    render_aoi_tt_filters,
    render_aoi_tt_indicator_sections,
)


def _indicator_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "DSUM_L"},
            {"prod_code": "M678", "factory": "OLED", "step_id": "21320", "tt_name": "DSUM_O"},
            {"prod_code": "M678", "factory": "TP", "step_id": "43620", "tt_name": "TOTAL_O_L"},
        ]
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
                "step_id": "11620",
                "tt_name": "TDSUM",
                "tt_qty": 5,
                "sheet_qty": 10,
                "value": 0.5,
            }
        )
    return pd.DataFrame(rows)


def test_default_start_date_uses_previous_month_first_day() -> None:
    assert get_default_aoi_tt_start_date(date(2026, 8, 10)) == date(2026, 7, 1)
    assert get_default_aoi_tt_start_date(date(2026, 1, 15)) == date(2025, 12, 1)


def test_filter_options_cascade_factory_step_code() -> None:
    indicator_df = _indicator_df()

    assert get_available_factories(indicator_df) == ["ARRAY", "OLED", "TP"]
    assert get_steps_for_factory(indicator_df, "ARRAY") == ["11620"]
    assert get_steps_for_factory(indicator_df, "TP") == ["43620"]
    assert get_codes_for_factory_steps(indicator_df, "ARRAY", ["11620"]) == ["DSUM_L", "TDSUM"]
    assert get_codes_for_factory_steps(indicator_df, "TP", ["43620"]) == ["TOTAL_O_L"]
    assert get_codes_for_factory_steps(indicator_df, "ARRAY", []) == []


def test_filter_report_by_factory_codes_steps() -> None:
    df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "v": 1},
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "DSUM_L", "v": 2},
            {"factory": "TP", "step_id": "43620", "tt_name": "TOTAL_O_L", "v": 3},
        ]
    )
    out = filter_aoi_tt_report(df, "ARRAY", ["TDSUM"], ["11620"])
    assert list(out["v"]) == [1]


def test_filter_report_by_particle_size() -> None:
    df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "particle_size": "Total", "v": 1},
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "particle_size": "S", "v": 2},
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "particle_size": "L", "v": 3},
        ]
    )

    out = filter_aoi_tt_report(
        df,
        "ARRAY",
        ["TDSUM"],
        ["11620"],
        ["S"],
    )

    assert out["particle_size"].tolist() == ["S"]


def _install_fake_widgets(monkeypatch, *, button_clicked: bool) -> dict:
    """把 render_aoi_tt_filters 依赖的 st 控件换成字典会话态的假实现。"""
    session: dict = {}
    captured = {"multiselect": {}, "button_kwargs": None, "column_specs": []}

    def fake_selectbox(_label, *, options, key, **_kw):
        return session.get(key, options[0])

    def fake_multiselect(label, *, options, key, **kw):
        captured["multiselect"][label] = {"options": options, "disabled": kw.get("disabled", False)}
        return session.get(key, kw.get("default", []))

    def fake_button(_label, **kw):
        captured["button_kwargs"] = kw
        return button_clicked

    monkeypatch.setattr(aoi_tt_dashboard.st, "container", lambda **_kw: nullcontext())
    monkeypatch.setattr(aoi_tt_dashboard.st, "markdown", lambda *_a, **_kw: None)
    def fake_columns(spec, **_kw):
        captured["column_specs"].append(spec)
        return [nullcontext() for _ in spec]

    monkeypatch.setattr(aoi_tt_dashboard.st, "columns", fake_columns)
    monkeypatch.setattr(aoi_tt_dashboard.st, "selectbox", fake_selectbox)
    monkeypatch.setattr(aoi_tt_dashboard.st, "multiselect", fake_multiselect)
    monkeypatch.setattr(aoi_tt_dashboard.st, "button", fake_button)
    monkeypatch.setattr(aoi_tt_dashboard.st, "session_state", session)
    captured["session"] = session
    return captured


def test_render_filters_query_button_applies_signature_and_allows_render(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=True)
    # 模拟用户已选好站点（厂别无切换，不触发重置）
    captured["session"].update(
        {
            "aoi_tt_previous_factory_filter": "ARRAY",
            "aoi_tt_step_filter": ["11620"],
        }
    )

    factory, codes, steps, particle_sizes, should_render = render_aoi_tt_filters(
        indicator_df=_indicator_df()
    )

    assert factory == "ARRAY"
    assert steps == ["11620"]
    # 站点确定后 Code 默认全选
    assert codes == ["DSUM_L", "TDSUM"]
    assert particle_sizes == ["Total", "S", "M", "L", "H"]
    assert captured["multiselect"]["Particle Size"]["options"] == ["Total", "S", "M", "L", "H"]
    assert len(captured["column_specs"][0]) == 5
    assert should_render is True
    # 查询签名落在 aoi_tt_ 前缀的会话键下
    assert captured["session"]["aoi_tt_applied_filter_signature"] == (
        "ARRAY",
        ("11620",),
        ("DSUM_L", "TDSUM"),
    )


def test_render_filters_without_query_click_blocks_render(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=False)
    captured["session"].update(
        {
            "aoi_tt_previous_factory_filter": "ARRAY",
            "aoi_tt_step_filter": ["11620"],
        }
    )

    _factory, codes, _steps, particle_sizes, should_render = render_aoi_tt_filters(
        indicator_df=_indicator_df()
    )

    assert codes == ["DSUM_L", "TDSUM"]  # 查询门控不影响筛选回显
    assert particle_sizes == ["Total", "S", "M", "L", "H"]
    assert should_render is False


def test_render_filters_factory_switch_resets_steps_and_disables_query(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=True)
    # 会话中残留上次 TP 厂的筛选；本次默认厂别为 ARRAY → 触发重置
    captured["session"].update(
        {
            "aoi_tt_previous_factory_filter": "TP",
            "aoi_tt_step_filter": ["43620"],
            "aoi_tt_code_filter": ["TOTAL_O_L"],
        }
    )

    factory, codes, steps, _particle_sizes, should_render = render_aoi_tt_filters(
        indicator_df=_indicator_df()
    )

    assert factory == "ARRAY"
    assert steps == []
    assert codes == []
    assert should_render is False
    # 未选站点：Code 下拉禁用，查询按钮禁用
    assert captured["multiselect"]["Code名称"]["disabled"] is True
    assert captured["button_kwargs"]["disabled"] is True


def test_render_filters_oled_only_offers_total_particle_size(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=False)
    captured["session"].update(
        {
            "aoi_tt_factory_filter": "OLED",
            "aoi_tt_previous_factory_filter": "OLED",
            "aoi_tt_step_filter": ["21320"],
        }
    )

    factory, _codes, _steps, particle_sizes, _should_render = render_aoi_tt_filters(
        indicator_df=_indicator_df()
    )

    assert factory == "OLED"
    assert particle_sizes == ["Total"]
    assert captured["multiselect"]["Particle Size"]["options"] == ["Total"]


def test_trend_chart_has_bars_line_and_usl_ucl_spec_traces() -> None:
    trend_df = _trend_df()
    throughput_df = (
        trend_df[["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]]
        .drop_duplicates()
    )

    figure = create_aoi_tt_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        usl=0.8,
        ucl=0.6,
        code_name="TDSUM",
        title="月周天趋势",
    )

    bar_traces = [t for t in figure.data if t.type == "bar"]
    # 月/周/天各一组柱状（分组配色区分粒度）
    assert len(bar_traces) == 3
    assert {t.name for t in bar_traces} == {"检测片数（月）", "检测片数（周）", "检测片数（天）"}
    assert len({t.marker.color for t in bar_traces}) == 3
    # 柱状在次 Y 轴，不与比值线互相压扁
    assert all(t.yaxis == "y2" for t in bar_traces)
    # 只有检测片数柱保留图注；折线与 USL/UCL 的图注被去掉
    assert all(t.showlegend is not False for t in bar_traces)
    # 单 TT 一条比值线
    lines = [t for t in figure.data if t.type == "scatter" and t.mode == "lines+markers"]
    assert [t.name for t in lines] == ["TDSUM"]
    # USL/UCL 两条规格虚线（dash / dot）
    spec_traces = [t for t in figure.data if t.type == "scatter" and t.mode == "lines"]
    assert {t.name for t in spec_traces} == {"TDSUM USL", "TDSUM UCL"}
    dashes = {t.name: t.line.dash for t in spec_traces}
    assert dashes == {"TDSUM USL": "dash", "TDSUM UCL": "dot"}
    assert {tuple(t.y) for t in spec_traces} == {(0.8, 0.8), (0.6, 0.6)}
    assert all(not t.showlegend for t in lines + spec_traces)
    # x 轴：2 月 + 分隔 + 3 周 + 分隔 + 7 天 = 14 个类目，组间留白；标签不含年份
    x_labels = list(lines[0].x)
    assert len(x_labels) == 14
    assert x_labels[0] == "07" and x_labels[1] == "08"
    assert x_labels[3].startswith("W")
    assert x_labels[-1] == "08-10"
    assert not any("2026" in label for label in x_labels)
    # 分隔位置无线值（断开），索引 2 与 6
    assert all(pd.isna(lines[0].y[i]) for i in (2, 6))
    # 月组柱子带全月检测片数
    month_bar = [t for t in bar_traces if t.name == "检测片数（月）"][0]
    assert list(month_bar.y) == [10, 10]


def test_trend_chart_without_specs_draws_no_spec_traces() -> None:
    trend_df = _trend_df()
    throughput_df = (
        trend_df[["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]]
        .drop_duplicates()
    )
    figure = create_aoi_tt_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        usl=None,
        ucl=None,
        code_name="TDSUM",
        title="月周天趋势",
    )
    assert not [t for t in figure.data if t.type == "scatter" and t.mode == "lines"]


def test_trend_chart_with_only_usl_draws_single_spec_trace() -> None:
    trend_df = _trend_df()
    throughput_df = (
        trend_df[["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]]
        .drop_duplicates()
    )
    figure = create_aoi_tt_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        usl=0.8,
        ucl=None,
        code_name="TDSUM",
        title="月周天趋势",
    )
    spec_traces = [t for t in figure.data if t.type == "scatter" and t.mode == "lines"]
    assert [t.name for t in spec_traces] == ["TDSUM USL"]


def test_point_chart_orders_x_by_first_time_and_draws_usl_ucl() -> None:
    lot_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "lot_id": "LOT-B", "tt_qty": 5, "first_start_time": pd.Timestamp("2026-08-02")},
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "lot_id": "LOT-A", "tt_qty": 1, "first_start_time": pd.Timestamp("2026-08-01")},
        ]
    )
    figure = create_aoi_tt_point_chart(
        point_df=lot_df,
        id_col="lot_id",
        code_specs={"TDSUM": (30.0, 10.0)},
        title="By Lot",
        y_title="TT个数",
    )

    scatter = [t for t in figure.data if t.mode == "lines+markers"][0]
    assert list(scatter.x) == ["LOT-A", "LOT-B"]
    assert list(scatter.y) == [1, 5]
    spec_names = {t.name for t in figure.data if t.mode == "lines"}
    assert spec_names == {"TDSUM USL", "TDSUM UCL"}
    # 图注下移到底部边距内，不遮挡竖排的 ID 标签
    assert figure.layout.legend.y <= -0.45
    assert figure.layout.margin.b >= 180


def test_point_chart_supports_value_column_for_lot_average() -> None:
    lot_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "lot_id": "LOT-B", "tt_qty": 5, "sheet_qty": 3, "value": 5 / 3, "first_start_time": pd.Timestamp("2026-08-02")},
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM", "lot_id": "LOT-A", "tt_qty": 1, "sheet_qty": 1, "value": 1.0, "first_start_time": pd.Timestamp("2026-08-01")},
        ]
    )
    figure = create_aoi_tt_point_chart(
        point_df=lot_df,
        id_col="lot_id",
        code_specs={"TDSUM": (30.0, 10.0)},
        title="By Lot",
        y_title="平均每片 TT 个数",
        y_col="value",
    )

    scatter = [t for t in figure.data if t.mode == "lines+markers"][0]
    assert list(scatter.x) == ["LOT-A", "LOT-B"]
    assert list(scatter.y) == [1.0, 5 / 3]  # 画的是 Lot 内平均每片，而非 Σtt_qty


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
        aoi_tt_dashboard.st,
        "expander",
        lambda title, expanded=False, **_kw: _FakeExpander(title, expanded),
    )
    monkeypatch.setattr(aoi_tt_dashboard.st, "plotly_chart", lambda fig, **_kw: rendered.append(fig))
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "columns",
        lambda n, **_kw: [nullcontext() for _ in range(n if isinstance(n, int) else len(n))],
    )
    monkeypatch.setattr(aoi_tt_dashboard.st, "subheader", lambda *_a, **_kw: None)
    monkeypatch.setattr(aoi_tt_dashboard.st, "container", lambda **_kw: nullcontext())
    monkeypatch.setattr(aoi_tt_dashboard.st, "info", lambda *_a, **_kw: None)

    details = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 08:00"), "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 3},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 09:00"), "sheet_id": "S2", "lot_id": "L2", "step_id": "11620", "tt_name": "DSUM_L", "tt_qty": 1},
            {"factory": "TP", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 10:00"), "sheet_id": "G1", "lot_id": "L9", "step_id": "43620", "tt_name": "TOTAL_O_L", "tt_qty": 2},
        ]
    )
    indicators = pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "DSUM_L"},
            {"prod_code": "M678", "factory": "TP", "step_id": "43620", "tt_name": "TOTAL_O_L"},
        ]
    )

    render_aoi_tt_indicator_sections(
        tt_details_df=details,
        spec_df=pd.DataFrame(),
        indicators_df=indicators,
        end_date=date(2026, 8, 10),
    )

    # 每个（站点+TT）一个默认展开的 Expander：3 个 TT → 3 个 expander × 3 图 = 9 张图
    assert len(expander_titles) == 3
    assert all(expander_expanded)
    assert len(rendered) == 9
    # Expander 标题含站点与 TT 参数名
    assert any("11620" in t and "TDSUM" in t for t in expander_titles)
    assert any("43620" in t and "TOTAL_O_L" in t for t in expander_titles)


def test_render_sections_keep_particle_sizes_in_one_expander(monkeypatch) -> None:
    rendered: list[object] = []
    expander_titles: list[str] = []
    particle_labels: list[str] = []

    class _FakeExpander:
        def __init__(self, title, expanded):
            self.title = title
            self.expanded = expanded

        def __enter__(self):
            expander_titles.append(self.title)
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "expander",
        lambda title, expanded=False, **_kw: _FakeExpander(title, expanded),
    )
    monkeypatch.setattr(aoi_tt_dashboard.st, "plotly_chart", lambda fig, **_kw: rendered.append(fig))
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "columns",
        lambda n, **_kw: [nullcontext() for _ in range(n if isinstance(n, int) else len(n))],
    )
    monkeypatch.setattr(aoi_tt_dashboard.st, "subheader", lambda *_a, **_kw: None)
    monkeypatch.setattr(aoi_tt_dashboard.st, "info", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "markdown",
        lambda text, **_kw: particle_labels.append(text) if "Particle Size" in text else None,
    )

    details = _details_with_particle_sizes()
    indicators = pd.DataFrame(
        [{"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"}]
    )

    render_aoi_tt_indicator_sections(
        tt_details_df=details,
        spec_df=pd.DataFrame(),
        indicators_df=indicators,
        end_date=date(2026, 8, 10),
    )

    assert len(expander_titles) == 1
    assert len(rendered) == 15
    assert particle_labels == [
        "**Particle Size：Total**",
        "**Particle Size：S**",
        "**Particle Size：M**",
        "**Particle Size：L**",
        "**Particle Size：H**",
    ]


def _details_with_particle_sizes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-09 08:00"),
                "sheet_id": "S1",
                "lot_id": "L1",
                "step_id": "11620",
                "tt_name": "TDSUM",
                "particle_size": particle_size,
                "tt_qty": quantity,
            }
            for particle_size, quantity in (("Total", 10), ("S", 6), ("M", 3), ("L", 0.7), ("H", 0.3))
        ]
    )
