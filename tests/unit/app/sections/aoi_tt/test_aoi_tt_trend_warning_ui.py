"""TT warnings evaluate Total and retain the existing chart breakdown."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

FIXTURE = Path(__file__).resolve().parents[4] / "e2e/fixtures/aoi_tt_trend_app.py"


@pytest.mark.parametrize("scenario,charts,success,info,error", [
    ("有预警", 6, False, False, False), ("跨日预警", 6, False, False, False),
    ("无预警", 0, True, False, False), ("数据不足", 0, False, True, False),
    ("仅粒径预警", 0, True, False, False),
    ("加载失败", 0, False, False, True),
])
def test_compact_tt_warning_states_and_particle_chart_scope(scenario, charts, success, info, error):
    app = AppTest.from_file(str(FIXTURE)).run()
    app.selectbox[0].select(scenario).run()
    assert not app.exception
    assert len(app.get("plotly_chart")) == charts
    assert bool(app.success) == success and bool(app.info) == info and bool(app.error) == error
    assert not app.caption
    if charts:
        table = app.dataframe[0].value
        assert table.columns.tolist() == ["厂别", "站点", "TT名称", "当日", "当日密度",
            "T-1有效日", "T-1密度", "T-2有效日", "T-2密度", "基线5日均值", "预警原因"]
        assert table["TT名称"].tolist() == ["TDSUM"]
        assert table["当日密度"].tolist() == [30]
        assert table["预警原因"].tolist() == ["当日超规"]
        if scenario == "跨日预警":
            assert table["T-1有效日"].tolist() == ["2026-10-16"]
            assert table["T-2有效日"].tolist() == ["2026-10-13"]
        assert not any("OTHER" in expander.label for expander in app.expander)
        sizes = [item.value for item in app.markdown if "Particle Size" in item.value]
        assert sizes == ["**Particle Size：Total**", "**Particle Size：S**"]


def test_same_tt_particle_can_render_in_all_three_areas_without_duplicate_chart_ids():
    app = AppTest.from_file(str(FIXTURE)).run()
    app.checkbox[0].check().run()
    assert not app.exception
    charts = app.get("plotly_chart")
    assert len(charts) == 15
    ids = [chart.proto.id for chart in charts]
    assert len(set(ids)) == 15
    assert sum("aoi_tt_trend_alert" in key for key in ids) == 6
    assert sum("aoi_tt_oos_alert" in key for key in ids) == 6


def test_automatic_charts_preserve_station_denominator_with_different_tt_sheet_sets(monkeypatch):
    from contextlib import nullcontext
    from datetime import date
    import pandas as pd
    from app.sections.inline_domain.aoi_tt import aoi_tt_dashboard as dashboard
    from app.sections.inline_domain.aoi_tt import trend_alerts as ui
    from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportViewModel
    from src.inline_domain.core.aoi_tt.aoi_tt_calculator import build_period_trend_df
    from src.inline_domain.core.aoi_tt.trend_alerts import INDICATOR_KEYS, evaluate_aoi_tt_trends

    rows = []
    for day, other_sheets in zip([17, 18, 19], [3, 2, 1]):
        common = dict(prod_code="M678", factory="OLED", step_id="21320", particle_size="Total",
                      start_time=pd.Timestamp(2026, 10, day), lot_id=f"L{day}")
        rows.append({**common, "tt_name": "DSUM_L", "sheet_id": f"A{day}", "tt_qty": 10})
        rows.extend({**common, "tt_name": "DSUM_O", "sheet_id": f"B{day}-{n}", "tt_qty": 0}
                    for n in range(other_sheets))
    details = pd.DataFrame(rows)
    candidates = details[["prod_code", *INDICATOR_KEYS]].drop_duplicates()
    reference = date(2026, 10, 20)
    evaluation = evaluate_aoi_tt_trends(build_period_trend_df(details, reference), candidates,
                                      reference_date=reference)
    alerts = evaluation.loc[evaluation.is_alert]
    assert alerts.tt_name.tolist() == ["DSUM_L"]
    assert alerts.target_density.tolist() == [5]
    monkeypatch.setattr(ui, "render_density_trend_alert_center", lambda *a, **kw: alerts)
    monkeypatch.setattr(dashboard.st, "expander", lambda *a, **kw: nullcontext())
    monkeypatch.setattr(dashboard.st, "subheader", lambda *a, **kw: None)
    rendered = []
    monkeypatch.setattr(dashboard, "_render_particle_size_charts", lambda **kw: rendered.append(kw))
    ui.render_aoi_tt_trend_alerts(AoiTtReportViewModel(
        details, pd.DataFrame(), candidates, evaluation), reference_date=reference)
    assert len(rendered) == 1
    daily = rendered[0]["trend_df"].query("period_type == 'day' and tt_name == 'DSUM_L'")
    assert daily.sheet_qty.tolist() == [4, 3, 2]
    assert daily.value.tolist() == pytest.approx([2.5, 10 / 3, 5])
