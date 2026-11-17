import pandas as pd
import pytest

from app.charts.inline_domain.capability_trend_chart import create_capability_trend_chart


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_counts_and_rates_have_separate_axes_and_period_type_lines(metric):
    summary = pd.DataFrame({
        "指标": [f"{metric.upper()}总项目数", "达标项目数", "预警项目数", f"{metric.capitalize()}≥1.33达标率"],
        "Y26": [100, 90, 10, "90.00%"], "Q4": [30, 24, 6, ".8"],
        "M9": [10, 9, 1, "90%"], "M10": [20, 15, 5, "75%"], "M11": [0, 0, 0, "0%"],
        "W44": [5, 4, 1, ".8"], "W45": ["—", "—", "—", "—"],
        "W46": [6, 3, 3, "50%"], "W47": [0, 0, 0, "100%"],
    })
    figure = create_capability_trend_chart(summary, metric=metric)
    bars = [trace for trace in figure.data if trace.type == "bar"]
    lines = [trace for trace in figure.data if trace.type == "scatter"]
    assert len(bars) == len(lines) == 4
    assert len({trace.marker.color for trace in bars}) == 4
    assert all(trace.yaxis == "y" for trace in bars)
    assert all(trace.yaxis == "y2" for trace in lines)
    assert [list(trace.x) for trace in lines] == [["Y26"], ["Q4"], ["M9", "M10", "M11"], ["W44", "W45", "W46", "W47"]]
    assert list(lines[2].y) == [.9, .75, 1]
    assert list(lines[3].y) == [.8, None, .5, 1]
    assert all(trace.connectgaps is False for trace in lines)
    assert list(figure.layout.xaxis.categoryarray) == list(summary.columns[1:])


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_product_charts_render_after_summary_with_unique_keys(monkeypatch, metric):
    from app.sections.inline_domain.monitor import cpk_monitor_dashboard as ui
    from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel

    calls, headings = [], []
    monkeypatch.setattr(ui.st, "markdown", headings.append)
    monkeypatch.setattr(ui.st, "dataframe", lambda *args, **kwargs: None)
    monkeypatch.setattr(ui.st, "query_params", {})
    monkeypatch.setattr(ui.st, "plotly_chart", lambda figure, **kwargs: calls.append((figure, kwargs)))
    monkeypatch.setattr(ui, "get_product_label_formatter", lambda: lambda product: product)
    def summary(total):
        return pd.DataFrame({"指标": [f"{metric.upper()}总项目数", "达标项目数", "预警项目数", f"{metric.capitalize()}≥1.33达标率"],
                             "W46": [total, total, 0, "100%"]})
    view = CpkMonitorViewModel(summary(30), pd.DataFrame(),
                               product_summary_dfs={"M626": summary(10), "M678": summary(20)})
    ui.render_capability_monitor_results(view, metric=metric)
    assert len(calls) == 3
    assert len({options["key"] for _, options in calls}) == 3
    assert [list(figure.data[0].y) for figure, _ in calls] == [[30], [10], [20]]
    assert all(options["config"]["scrollZoom"] is False for _, options in calls)
    assert headings[-2:] == [f"#### M626 {metric.upper()} 达标率趋势图", f"#### M678 {metric.upper()} 达标率趋势图"]

