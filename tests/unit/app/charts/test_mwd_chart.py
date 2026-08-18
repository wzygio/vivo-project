import pandas as pd

from app.charts.mwd_chart import create_code_trend_chart


def test_code_trend_chart_adds_throughput_line_on_secondary_axis() -> None:
    trend_df = pd.DataFrame(
        {
            "time_period": ["2026-06", "2026-07"],
            "defect_group": ["GROUP-A", "GROUP-A"],
            "defect_desc": ["CODE-A", "CODE-A"],
            "defect_rate": [0.001, 0.002],
            "total_panels": [12_000, 15_000],
        }
    )

    figure = create_code_trend_chart(
        trend_df,
        title="月度",
        y_range=[0, 0.003],
        warning_line_value=0.0025,
    )

    assert figure is not None
    throughput_trace = next(trace for trace in figure.data if trace.name == "过货数")
    assert list(throughput_trace.y) == [12_000, 15_000]
    assert list(throughput_trace.text) == [12_000, 15_000]
    assert throughput_trace.yaxis == "y2"
    assert figure.layout.yaxis2.overlaying == "y"
    assert figure.layout.yaxis2.side == "right"


def test_code_trend_chart_keeps_working_without_throughput_column() -> None:
    trend_df = pd.DataFrame(
        {
            "time_period": ["2026-07"],
            "defect_group": ["GROUP-A"],
            "defect_desc": ["CODE-A"],
            "defect_rate": [0.002],
        }
    )

    figure = create_code_trend_chart(
        trend_df,
        title="月度",
        y_range=[0, 0.003],
    )

    assert figure is not None
    assert all(trace.name != "过货数" for trace in figure.data)
    assert getattr(figure.layout, "yaxis2", None) is None
