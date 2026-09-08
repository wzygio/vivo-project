from types import SimpleNamespace

import pandas as pd

from app.sections.inline_domain.monitor import oos_monitor_dashboard as dashboard


def test_trend_and_top10_are_vertical_bars_with_ten_slots(monkeypatch):
    charts = []
    trends = []
    monkeypatch.setattr(dashboard.st, "altair_chart", lambda chart, **kwargs: trends.append(chart))
    monkeypatch.setattr(dashboard.st, "bar_chart", lambda data, **kwargs: charts.append((data, kwargs)))
    monkeypatch.setattr(dashboard.st, "line_chart", lambda *a, **k: (_ for _ in ()).throw(AssertionError("line chart")))
    monkeypatch.setattr(dashboard.st, "markdown", lambda *a, **k: None)
    monkeypatch.setattr(dashboard.st, "dataframe", lambda *a, **k: None)
    monkeypatch.setattr(dashboard.st, "columns", lambda n: [SimpleNamespace(metric=lambda *a: None)] * n)
    detail = pd.DataFrame([dict(alarm_type="OOS", scope="spc", factory="ARRAY", prod_code="M626", step_id="21200", metric_name="x", item_id="s", event_time="2026-09-08", observed_value=3, upper_limit=2, lower_limit=0, limit_type="USL")])
    station = pd.DataFrame([dict(step_id="21200", scope="spc", alarm_type="OOS", oos_count=45)])
    summary = pd.DataFrame({"报警类型": ["OOS报警片数", "OOC报警片数", "SOOS报警片数"], "Y26": [100, 3, 0], "Q1": ["—", "—", "—"], "W37": ["—", 1, 0]})
    view = SimpleNamespace(detail_df=detail, period_summary_df=summary, trend_df=pd.DataFrame([dict(event_date="2026-09-08", scope="spc", alarm_type="OOS", oos_count=45)]), station_df=station)
    dashboard.render_oos_monitor_results(view)
    assert len(charts) == 1 and len(trends) == 1
    assert all(options["horizontal"] is False for _, options in charts)
    trend = trends[0].data.pivot(index="周期", columns="报警类型", values="数量").reindex(["Y26", "Q1", "W37"])
    assert trend.index.tolist() == ["Y26", "Q1", "W37"]
    assert trend.loc["Y26", "OOS报警片数"] == 100
    assert trend.loc["W37", "OOS报警片数"] == 0
    assert trend.loc["Q1"].eq(0).all()
    assert summary.loc[0, "Q1"] == "—"
    spec = trends[0].to_dict()
    assert spec["mark"]["type"] == "bar"
    assert spec["encoding"]["color"]["scale"]["domain"] == ["年", "季", "月", "周"]
    assert len(set(spec["encoding"]["color"]["scale"]["range"])) == 4
    top, options = charts[0]
    assert len(top) == 10 and top.index[0] == "21200"
    assert top.iloc[0].sum() == 45 and top.iloc[1:].sum().sum() == 0
    assert all(not name.strip() for name in top.index[1:])
    assert options["sort"] is False
    assert len(station) == 1


def test_period_axis_reserves_four_weeks_even_when_all_history_missing():
    from src.inline_domain.core.monitor.period_summary import MONITOR_SUMMARY_COLUMNS, build_period_summary_from_records

    summary = build_period_summary_from_records(pd.DataFrame(columns=MONITOR_SUMMARY_COLUMNS), end_date=pd.Timestamp("2026-09-08"))
    chart = dashboard.build_period_trend_chart(summary)
    spec = chart.to_dict()
    assert spec["encoding"]["x"]["scale"]["domain"] == ["Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W34", "W35", "W36", "W37"]
    assert chart.data["数量"].eq(0).all()
