"""The weekly sample-span gate suppresses alerts, never capability calculations."""
from datetime import date

import pandas as pd
import pytest

from app.sections.inline_domain.spc.spc_dashboard import (
    build_weekly_cpk_alerts,
    build_weekly_cpm_alerts,
)
from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration
from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk
from src.inline_domain.core.spc.cpk_decoration import build_capability_anomaly_detail
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore
from src.shared_kernel.config import ConfigLoader


REFERENCE = date(2026, 8, 17)
START = pd.Timestamp("2026-08-10 05:13:57")


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("minimum, hours, expected", [
    (24, 23.999, 0), (24, 24, 1), (24, 30, 1), (72, 48, 0),
    (72, 72, 1), (0, 0, 1),
])
def test_configured_span_applies_to_ledger_reader_page_and_matrix(
    monkeypatch, tmp_path, metric, minimum, hours, expected,
):
    from app.sections.inline_domain.monitor.alert_matrix_service import build_latest_cpk_alerts

    current = [minimum]
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_alert_min_span_hours", lambda: current[0])
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    frame = capability(metric, START + pd.Timedelta(hours=hours))
    assert len(build_capability_anomaly_detail(
        frame, metric, REFERENCE, min_span_hours=minimum,
    )) == expected
    path = tmp_path / "capability.xlsx"
    sheet = "M626" if metric == "cpk" else "M626_cpm"
    frame.rename(columns={metric: f"{metric}_corrected"}).assign(flag=False).to_excel(
        path, sheet_name=sheet, index=False,
    )
    reader = (CpkLatestExcelStore if metric == "cpk" else CpmLatestExcelStore)(path)
    before = reader.source_signature()
    saved = reader.read_product("M626")
    assert saved["status"].tolist() == ["预警" if expected else "数据跨度不足"]
    build_alerts = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    assert len(build_alerts(saved, reference_date=REFERENCE)) == expected
    if metric == "cpk":
        assert len(build_latest_cpk_alerts(saved, REFERENCE)) == expected
    result = prepare_capability_decoration(
        frame, tmp_path / "new", sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    assert len(result.decoration_df) == expected
    assert result.period_capability_df[metric].tolist() == [0.51]
    current[0] = hours + 1
    assert reader.source_signature() != before
    assert reader.read_product("M626")["status"].tolist() == ["数据跨度不足"]


def capability(metric, end, **changes):
    return pd.DataFrame([{
        "prod_code": "M626", "factory": "ARRAY", "step_id": "1B990",
        "param_name": "TFT_5_IOFF1", "period_type": "week",
        "period_label": "2026-W33", "period_sort": 33,
        "period_start": START, "period_end": end, metric: 0.51,
        **changes,
    }])


def saved_detail(frame, metric):
    return normalize_latest_cpk(frame.rename(columns={metric: f"{metric}_corrected"}).assign(
        flag=False,
    ), "M626", metric=metric)


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("hours, expected", [(0, 0), (47.999, 0), (48, 1), (49, 1)])
def test_span_boundary_matches_ledger_and_spc_page(metric, hours, expected):
    frame = capability(metric, START + pd.Timedelta(hours=hours))
    original = frame.copy(deep=True)
    assert len(build_capability_anomaly_detail(frame, metric, REFERENCE)) == expected
    build_alerts = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    assert len(build_alerts(saved_detail(frame, metric), reference_date=REFERENCE)) == expected
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("end", [None, "bad-date", START - pd.Timedelta(hours=60)])
def test_unverifiable_span_is_not_an_alert(metric, end):
    frame = capability(metric, end)
    assert build_capability_anomaly_detail(frame, metric, REFERENCE).empty
    build_alerts = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    assert build_alerts(saved_detail(frame, metric), reference_date=REFERENCE).empty


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_missing_span_columns_are_not_assumed_to_cover_a_full_week(metric):
    frame = capability(metric, START).drop(columns=["period_start", "period_end"])
    assert build_capability_anomaly_detail(frame, metric, REFERENCE).empty
    build_alerts = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    assert build_alerts(saved_detail(frame, metric), reference_date=REFERENCE).empty


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("hours, status", [(18, "数据跨度不足"), (48, "预警")])
def test_monitor_reader_suppresses_old_short_span_records(metric, hours, status):
    frame = capability(metric, START + pd.Timedelta(hours=hours)).rename(
        columns={metric: f"{metric}_corrected"},
    ).assign(flag=False)
    normalized = normalize_latest_cpk(frame, "M626", metric=metric)
    assert normalized["status"].tolist() == [status]
    assert normalized[metric].tolist() == [0.51]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_short_span_does_not_append_anomaly_rows_and_keeps_capability(metric, tmp_path):
    sheet = "M626" if metric == "cpk" else "M626_cpm"
    result = prepare_capability_decoration(
        capability(metric, START + pd.Timedelta(hours=18)), tmp_path,
        sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    assert result.decoration_df.empty
    assert result.period_capability_df[metric].tolist() == [0.51]
    assert not result.decoration_path.exists()
    # More same-week data may make this project eligible on a later refresh.
    result = prepare_capability_decoration(
        capability(metric, START + pd.Timedelta(hours=48)), tmp_path,
        sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    stored = pd.read_excel(result.decoration_path, sheet_name=sheet)
    assert len(stored) == 1 and stored["flag"].tolist() == [False]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_refreshes_span_with_value_and_preserves_historical_decisions(metric, tmp_path):
    sheet = "M626" if metric == "cpk" else "M626_cpm"
    initial = capability(metric, START + pd.Timedelta(hours=48))
    result = prepare_capability_decoration(
        initial, tmp_path, sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    corrected = f"{metric}_corrected"
    replacement = f"{metric}_replacement"
    history = result.decoration_df.assign(period_label="2026-W32", flag=True)
    saved = result.decoration_df.assign(**{replacement: 1.372})
    pd.concat([saved, history], ignore_index=True).to_excel(
        result.decoration_path, sheet_name=sheet, index=False,
    )
    updated = capability(metric, START + pd.Timedelta(hours=18), **{metric: 0.6})
    prepare_capability_decoration(
        updated, tmp_path, sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    stored = pd.read_excel(result.decoration_path, sheet_name=sheet)
    assert stored[corrected].tolist() == [0.6, 0.51]
    assert pd.Timestamp(stored.loc[0, "period_end"]) == START + pd.Timedelta(hours=18)
    assert pd.Timestamp(stored.loc[1, "period_end"]) == START + pd.Timedelta(hours=48)
    assert stored["flag"].tolist() == [False, True]
    assert stored.loc[0, replacement] == 1.372
    assert normalize_latest_cpk(stored, "M626", metric=metric)["status"].tolist() == [
        "数据跨度不足", "已修饰或达标",
    ]
    before = result.decoration_path.read_bytes()
    prepare_capability_decoration(
        updated, tmp_path, sheet_name=sheet, metric=metric, reference_date=REFERENCE,
    )
    assert result.decoration_path.read_bytes() == before


def test_single_sheet_point_sigma_still_computes_both_capabilities(tmp_path):
    common = dict(prod_code="M626", factory="ARRAY", step_id="1B990",
                  param_name="TFT_5_IOFF1", sheet_id="S1", usl=16., lsl=4.)
    features = pd.DataFrame([{**common, "sheet_start_time": START, "sheet_mean": 10.}])
    points = pd.DataFrame([
        {**common, "sheet_start_time": START + pd.Timedelta(minutes=i), "param_value": value}
        for i, value in enumerate([4., 10., 16.])
    ])
    report = build_period_capability_report(
        features, REFERENCE, points, sigma_source="point_value", previous_week_only=True,
    )
    assert report["sample_count"].tolist() == [1]
    assert report["cpk"].iloc[0] == pytest.approx(1 / 3)
    assert report["cpm"].iloc[0] == pytest.approx(1 / 3)
    for metric in ["cpk", "cpm"]:
        result = prepare_capability_decoration(
            report, tmp_path, sheet_name=f"M626_{metric}", metric=metric, reference_date=REFERENCE,
        )
        assert result.decoration_df.empty
        assert result.period_capability_df[metric].iloc[0] == pytest.approx(1 / 3)
        assert not result.decoration_path.exists()
