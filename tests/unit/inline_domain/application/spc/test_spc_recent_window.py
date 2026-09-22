from datetime import date
import pandas as pd
import pytest

from src.inline_domain.application.shared import decorated_data
from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features
from src.inline_domain.core.shared import sheet_oos_decoration as rules
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report


def measurements() -> pd.DataFrame:
    return pd.DataFrame([
        dict(prod_code="TEST", factory="ARRAY", step_id="100", param_name="THK",
             sheet_id=sheet, sheet_start_time=pd.Timestamp(day), site_name=site,
             param_value=value, unit_id="EQ1", data_type="SPC")
        for sheet, day in [("old", "2026-08-20"), ("previous", "2026-09-14"),
                           ("previous2", "2026-09-20"), ("current", "2026-09-21")]
        for site, value in [("P1", 50.0), ("P2", 80.0)]
    ])


def specs() -> pd.DataFrame:
    return pd.DataFrame([dict(prod_code="TEST", step_id="100", param_name="THK",
                              usl=60.0, lsl=40.0, ucl=55.0, lcl=45.0, target=50.0)])


@pytest.mark.parametrize("database_target, expected_target", [("22.5", 22.5), (None, 28.5)])
def test_database_target_reaches_points_features_capability_and_all_charts(
    monkeypatch, tmp_path, database_target, expected_target,
):
    from app.sections.inline_domain.spc import spc_dashboard
    from src.inline_domain.infrastructure.shared import measurement_metadata_loader as metadata

    class Decisions:
        def load_sheet_oos_decisions(self, *args):
            return pd.DataFrame(columns=[*rules.OOS_KEY_COLUMNS, "flag"])

    database_specs = pd.DataFrame([dict(
        prod_code="M678", step_id="1D450", param_name="CD1",
        usl=36.0, lsl=21.0, ucl=None, lcl=21.3, target=database_target,
    )])
    monkeypatch.setattr(metadata, "_read_sql", lambda *args: database_specs.copy())
    limits = metadata.load_parameter_specs(None, "M678")
    points = measurements().assign(prod_code="M678", step_id="1D450", param_name="CD1")
    points["param_value"] = points["site_name"].map({"P1": 22.0, "P2": 23.0})
    result = decorated_data.prepare_decorated_data(
        points, limits, "M678", "spc", product_dir=tmp_path,
        persist=False, decoration_port=Decisions(), sheet_features_start_date="2026-09-14",
    )
    for frame in [result.raw_measurements_df, result.sheet_features_df]:
        if database_target is None:
            assert frame["target"].isna().all()
        else:
            assert frame["target"].eq(expected_target).all()

    capability = build_period_capability_report(
        result.sheet_features_df, date(2026, 9, 21), result.raw_measurements_df,
        "point_value", previous_week_only=True,
    )
    assert capability["target"].tolist() == [expected_target]
    point_std = pd.Series([22.0, 23.0, 22.0, 23.0]).std()
    expected_cpm = 15.0 / (6.0 * (point_std ** 2 + (22.5 - expected_target) ** 2) ** 0.5)
    assert capability.iloc[0]["cpm"] == pytest.approx(expected_cpm)

    payload = spc_dashboard._build_indicator_render_payload(
        "CD1", "box", result.sheet_features_df, capability,
        result.raw_measurements_df, "point_value", reference_date=date(2026, 9, 21),
    )
    for key in ["fig1", "chamber_fig", "time_fig"]:
        figure = payload[key]
        targets = [a.text for a in figure.layout.annotations if a.text.startswith("Target:")]
        if database_target is None:
            assert targets == []
            assert not any(shape.y0 == shape.y1 == expected_target for shape in figure.layout.shapes)
        else:
            assert targets == [f"Target: {expected_target}"]
            assert any(shape.y0 == shape.y1 == expected_target for shape in figure.layout.shapes)


@pytest.mark.parametrize("flag", [True, False, None, "Delete"])
def test_spc_point_decoration_needs_no_sheet_statistics(flag):
    points = measurements()
    decisions = points[rules.OOS_KEY_COLUMNS].drop_duplicates().assign(flag=flag)
    expected_decisions = decisions.assign(flag=False) if flag == "Delete" else decisions
    expected = rules.apply_sheet_oos_decoration(
        points, preprocess_sheet_features(points, specs()), expected_decisions,
    )
    actual = rules.apply_spc_point_decoration(points, specs(), decisions)
    pd.testing.assert_frame_equal(actual[points.columns], expected[points.columns])
    assert len(actual) == len(points)


def test_spc_builds_features_only_for_recent_points(monkeypatch, tmp_path):
    class Decisions:
        def load_sheet_oos_decisions(self, *args):
            return pd.DataFrame(columns=[*rules.OOS_KEY_COLUMNS, "flag"])

    seen = []
    original = decorated_data._preprocess_sheet_features_by_type

    def spy(points, limits):
        seen.append(set(points["sheet_id"]))
        return original(points, limits)

    monkeypatch.setattr(decorated_data, "_preprocess_sheet_features_by_type", spy)
    result = decorated_data.prepare_decorated_data(
        measurements(), specs(), "TEST", "spc", product_dir=tmp_path,
        persist=False, decoration_port=Decisions(), sheet_features_start_date="2026-09-14",
    )
    assert seen == [{"previous", "previous2", "current"}] * 2
    assert set(result.raw_measurements_df.sheet_id) == {"old", "previous", "previous2", "current"}
    assert set(result.sheet_features_df.sheet_id) == {"previous", "previous2", "current"}
    assert result.raw_measurements_df.param_value.max() < 60


@pytest.mark.parametrize("reference", [date(2026, 9, 21), date(2026, 9, 27)])
def test_capability_only_aggregates_previous_complete_week(reference):
    points = measurements()
    features = preprocess_sheet_features(points, specs())
    result = build_period_capability_report(
        features, reference, points, "point_value", previous_week_only=True,
    )
    assert result.period_type.tolist() == ["week"]
    assert result.period_label.tolist() == ["2026-W38"]
    assert result.sample_count.tolist() == [2]
    assert result.point_count.tolist() == [4]


def test_previous_week_does_not_fall_back_to_older_data():
    points = measurements().query("sheet_id == 'old'")
    result = build_period_capability_report(
        preprocess_sheet_features(points, specs()), date(2026, 9, 21),
        points, "point_value", previous_week_only=True,
    )
    assert result.empty


def test_spc_charts_keep_historical_overview_and_recent_detail(monkeypatch):
    from app.sections.inline_domain.spc import spc_dashboard

    points = measurements()
    features = preprocess_sheet_features(points.query("sheet_id != 'old'"), specs())
    captured = {}

    def capture(**kwargs):
        captured.update(kwargs)
        return None, None

    monkeypatch.setattr(spc_dashboard, "_create_sheet_points_box_charts", capture)
    payload = spc_dashboard._build_indicator_render_payload(
        "THK", "box", features, pd.DataFrame(), points, "point_value",
        reference_date=date(2026, 9, 21),
    )
    assert set(captured["raw_measurements_df"].sheet_id) == {"previous", "previous2", "current"}
    assert captured["date_only"] is True
    assert any("2026-08" in trace.name for trace in payload["fig1"].data)
    assert any("2026-09-21" in trace.name for trace in payload["fig1"].data)


def test_spc_upload_rejects_delete_but_ctq_still_accepts_it():
    from app.sections.inline_domain.shared.sheet_oos_admin import validate_decision_upload

    decisions = measurements()[rules.OOS_KEY_COLUMNS].drop_duplicates().assign(flag="Delete")
    assert not validate_decision_upload(decisions, allow_delete=False)[0]
    assert validate_decision_upload(decisions)[0]


def test_cutoff_is_inclusive_and_feature_window_does_not_trim_overview(monkeypatch, tmp_path):
    from src.inline_domain.application.shared.decorated_features import InMemoryFeaturesSource, fetch_decorated_features
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.report_cutoff import ReportCutoffPolicy

    monkeypatch.setattr(ConfigLoader, "get_project_root", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(ReportCutoffPolicy, "boundary", lambda self, now=None: pd.Timestamp("2026-09-21 12:00"))
    extra = measurements().iloc[[0]].copy()
    points = pd.concat([
        measurements(),
        extra.assign(sheet_id="noon", sheet_start_time=pd.Timestamp("2026-09-21 12:00")),
        extra.assign(sheet_id="after", sheet_start_time=pd.Timestamp("2026-09-21 12:00:00.000001")),
    ], ignore_index=True)
    fetch_decorated_features.clear()
    result = fetch_decorated_features(
        InMemoryFeaturesSource(points, specs()), "TEST", "spc", "2026-08-01", "2026-09-21",
        sheet_features_start_date="2026-09-14",
    )
    assert set(result["sheet_features_df"].sheet_id) == {"previous", "previous2", "current", "noon"}
    assert "old" in set(result["raw_measurements_df"].sheet_id)
    assert "after" not in set(result["raw_measurements_df"].sheet_id)
    fetch_decorated_features.clear()


def test_recent_feature_coverage_does_not_claim_historical_days(monkeypatch, tmp_path):
    from src.inline_domain.application.shared import decorated_features as pipeline
    from src.shared_kernel.config import ConfigLoader

    monkeypatch.setattr(ConfigLoader, "get_project_root", classmethod(lambda cls: tmp_path))
    source = pipeline.InMemoryFeaturesSource(measurements(), specs())
    source.supports_shared_history_persistence = True
    writes = []
    monkeypatch.setattr(pipeline, "persist_ooc_facts", lambda **kwargs: writes.append(kwargs))
    monkeypatch.setattr(pipeline, "persist_throughput_facts", lambda **kwargs: writes.append(kwargs))
    pipeline.fetch_decorated_features.clear()
    pipeline.fetch_decorated_features(
        source, "TEST", "spc", "2026-08-01", "2026-09-21", sheet_features_start_date="2026-09-14",
    )
    assert len(writes) == 2
    assert all(write["coverage_start"] == pd.Timestamp("2026-09-14") for write in writes)
    assert "old" not in set(writes[1]["details"].sheet_id)
    pipeline.fetch_decorated_features.clear()


def test_service_keeps_overview_for_historical_only_indicator(monkeypatch, tmp_path):
    from src.inline_domain.application.shared.decorated_features import InMemoryFeaturesSource, fetch_decorated_features
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.shared_kernel.config import ConfigLoader

    monkeypatch.setattr(ConfigLoader, "get_project_root", classmethod(lambda cls: tmp_path))
    fetch_decorated_features.clear()
    SpcReportService.fetch_spc_report_payload.clear()
    report = SpcReportService.get_spc_report_data(
        InMemoryFeaturesSource(measurements().query("sheet_id == 'old'"), specs()),
        SpcQueryConfig(prod_code="TEST", start_date="2026-08-01", end_date="2026-09-21").model_dump_json(),
    )
    assert report.sheet_features_df.empty
    assert report.period_capability_df.empty
    assert len(report.raw_measurements_df) == 2
    assert report.indicators_df.param_name.tolist() == ["THK"]
    fetch_decorated_features.clear()
    SpcReportService.fetch_spc_report_payload.clear()


def test_previous_week_crosses_iso_year_boundary():
    points = measurements().iloc[:4].copy()
    points["sheet_start_time"] = [pd.Timestamp("2025-12-29")] * 2 + [pd.Timestamp("2026-01-04")] * 2
    report = build_period_capability_report(
        preprocess_sheet_features(points, specs()), date(2026, 1, 5), points,
        "point_value", previous_week_only=True,
    )
    assert report.period_label.tolist() == ["2026-W01"]
    assert report.point_count.tolist() == [4]


@pytest.mark.parametrize("state", ["normal", "historical_only", "empty"])
def test_spc_public_chart_rendering(state):
    from streamlit.testing.v1 import AppTest

    script = '''
from datetime import date
import pandas as pd
from app.sections.inline_domain.spc.spc_dashboard import render_spc_indicator_sections
from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report
points = pd.DataFrame([
    dict(prod_code="TEST", factory="ARRAY", step_id="100", param_name="THK",
         sheet_id=str(i), site_name="P1", sheet_start_time=pd.Timestamp(day),
         param_value=value, usl=60., lsl=40., ucl=55., lcl=45., main_process_unit_id="EQ1")
    for i, (day, value) in enumerate([("2026-08-20", 49.), ("2026-09-14", 50.), ("2026-09-20", 51.), ("2026-09-21", 52.)])
])
if STATE == "historical_only":
    points = points.iloc[:1]
if STATE == "empty":
    points = points.iloc[:0]
limits = pd.DataFrame([dict(prod_code="TEST", step_id="100", param_name="THK", usl=60., lsl=40.)])
recent = points.loc[points.sheet_start_time.ge("2026-09-14")]
features = preprocess_sheet_features(recent, limits)
capability = build_period_capability_report(features, date(2026, 9, 21), previous_week_only=True)
render_spc_indicator_sections(capability, features, points, reference_date=date(2026, 9, 21))
'''
    app = AppTest.from_string(f"STATE = {state!r}\n" + script).run(timeout=20)
    assert not app.exception
    assert len(app.get("plotly_chart")) == (0 if state == "empty" else 3)
