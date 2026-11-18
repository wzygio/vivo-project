"""Ended ISO-week statistics must not depend on newer same-Sheet measurements."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.shared.measurement_preparation import InlineMeasurementPreparationRepository


def points():
    return pd.DataFrame([
        dict(prod_code="M678", factory="ARRAY", step_id="S", param_name="THK", sheet_id=sheet,
             site_name=site, start_time=timestamp, param_value=value)
        for sheet, timestamp in [("A", "2026-11-09"), ("B", "2026-11-11")]
        for site, value in [("1", 9.), ("2", 11.)]
    ])


def specs():
    return pd.DataFrame([dict(prod_code="M678", step_id="S", param_name="THK", usl=19., lsl=1.)])


@pytest.mark.parametrize("sigma_source", ["point_value", "sheet_mean"])
@pytest.mark.parametrize("remeasure_all_sites", [False, True])
def test_previous_week_is_unchanged_after_newer_measurements_of_same_sheet(sigma_source, remeasure_all_sites):
    original = points().rename(columns={"start_time": "sheet_start_time"})
    current = original.iloc[:2 if remeasure_all_sites else 1].assign(
        sheet_start_time="2026-11-16", param_value=17.)
    later = pd.concat([original, current], ignore_index=True)
    def calculate(frame, reference):
        return build_period_capability_report(preprocess_sheet_features(frame, specs()), reference,
            frame, sigma_source, previous_week_only=True)
    monday = calculate(original, date(2026, 11, 16))
    tuesday = calculate(later, date(2026, 11, 17))
    pd.testing.assert_frame_equal(monday, tuesday)
    assert tuesday["mean_value"].tolist() == [10.]
    assert tuesday["sample_count"].tolist() == [2]


@pytest.mark.parametrize("scope,expected_count", [("SPC", 5), ("CTQ", 4)])
def test_spc_latest_point_preparation_preserves_each_week(monkeypatch, scope, expected_count):
    from src.inline_domain.infrastructure.shared import measurement_preparation as module

    original = points()
    later = pd.concat([original,
        original.iloc[[0]].assign(start_time="2026-11-09 12:00:00", param_value=10.),
        original.iloc[[0]].assign(start_time="2026-11-16", param_value=17.)], ignore_index=True)
    later["start_time"] = pd.to_datetime(later["start_time"], format="mixed")
    monkeypatch.setattr(module, "apply_inline_date_exclusion", lambda frame, **kwargs: frame.copy())
    monkeypatch.setattr(module, "attach_main_process_spec", lambda frame, specifications: frame.copy())
    monkeypatch.setattr(module, "apply_main_process_history", lambda frame, history: frame.copy())
    metadata = SimpleNamespace(
        get_parameter_catalog=lambda product: pd.DataFrame([{"ref_param_name": "THK", "data_type": scope}]),
        get_parameter_specs=lambda product: specs(),
    )
    history = SimpleNamespace(get_main_process_history=lambda *args, **kwargs: pd.DataFrame())
    repository = InlineMeasurementPreparationRepository(object(), metadata, history)
    monkeypatch.setattr(repository, "_apply_outlier_filters", lambda frame, product: frame.copy())
    config = SpcQueryConfig(prod_code="M678", start_date="2026-11-01", end_date="2026-11-17", data_type_filter=scope)
    result = repository._prepare_shared_measurements(later, config)
    assert len(result) == expected_count
    if scope == "SPC":
        previous = result.loc[result.sheet_start_time.lt("2026-11-16")]
        assert len(previous) == 4
        assert sorted(previous.param_value.tolist()) == [9., 10., 11., 11.]
    assert not any(column.startswith("_spc_") for column in result)
    pd.testing.assert_frame_equal(original, points())


def test_fixed_point_decisions_and_weekly_membership_ignore_later_reinspections(tmp_path):
    from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
    from src.inline_domain.application.spc.sheet_membership import publish_spc_sheet_memberships

    original = points().rename(columns={"start_time": "sheet_start_time"}).assign(data_type="SPC", unit_id="EQ1")
    original.loc[original.site_name.eq("2"), "param_value"] = 30.
    later = pd.concat([original, original.iloc[:2].assign(sheet_start_time="2026-11-16", param_value=80.)], ignore_index=True)
    decisions = pd.DataFrame([
        dict(prod_code="M678", step_id="S", param_name="THK", sheet_id="A", flag=False),
        dict(prod_code="M678", step_id="S", param_name="THK", sheet_id="B", flag=True),
    ])
    port = SimpleNamespace(load_sheet_oos_decisions=lambda *args: decisions.copy())
    def prepare(frame, reference):
        return prepare_decorated_data(frame, specs(), "M678", "spc", product_dir=tmp_path,
            persist=False, decoration_port=port, sheet_features_start_date="2026-11-09",
            spc_point_policy=(.5, "2026-10-05", reference.isoformat()))
    monday = prepare(original, date(2026, 11, 16))
    tuesday = prepare(later, date(2026, 11, 17))
    old = tuesday.raw_measurements_df.loc[tuesday.raw_measurements_df.sheet_start_time.lt("2026-11-16")]
    pd.testing.assert_frame_equal(monday.raw_measurements_df.reset_index(drop=True), old.reset_index(drop=True))
    expected = build_period_capability_report(monday.sheet_features_df, date(2026, 11, 16),
        monday.raw_measurements_df, "point_value", previous_week_only=True)
    actual = build_period_capability_report(tuesday.sheet_features_df, date(2026, 11, 17),
        tuesday.raw_measurements_df, "point_value", previous_week_only=True)
    pd.testing.assert_frame_equal(expected, actual)
    snapshots = []
    store = SimpleNamespace(publish=snapshots.append)
    for prepared, reference in [(monday, date(2026, 11, 16)), (tuesday, date(2026, 11, 17))]:
        publish_spc_sheet_memberships(prepared.raw_measurements_df, store=store, product="M678",
            start_date="2026-11-09", end_date=reference, reference_date=reference, policy_signature="fixed")
    assert snapshots[0]["groups"] == snapshots[1]["groups"]
    assert snapshots[0]["source_generation"] == snapshots[1]["source_generation"]
