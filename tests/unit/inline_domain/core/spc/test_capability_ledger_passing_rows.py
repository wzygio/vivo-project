"""Existing ledger rows are immutable; enabled replacement retains its original behavior."""
from datetime import date

import pandas as pd
import pytest

from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration
from src.inline_domain.core.spc.cpk_decoration import apply_capability_decoration, resolve_capability_decoration_sheet


def capability(metric, value):
    return pd.DataFrame([{
        "prod_code": "M678", "factory": "ARRAY", "step_id": "12140", "param_name": "TEST",
        "period_type": "week", "period_label": "2026-W46", "period_sort": 46,
        "period_start": "2026-11-09", "period_end": "2026-11-12", metric: value,
    }])


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("passing_value", [1.33, 1.6, float("inf")])
def test_recomputed_passing_value_cannot_update_or_remove_existing_ledger_row(tmp_path, metric, passing_value):
    sheet = resolve_capability_decoration_sheet("M678", metric)
    def prepare(frame):
        return prepare_capability_decoration(frame, tmp_path, sheet_name=sheet, metric=metric,
            reference_date=date(2026, 11, 17))

    initial = prepare(capability(metric, 0.8))
    initial.decoration_df.assign(flag=True, **{f"{metric}_replacement": 1.372}).to_excel(
        initial.decoration_path, sheet_name=sheet, index=False)
    before = initial.decoration_path.read_bytes()
    result = prepare(capability(metric, passing_value))
    stored = pd.read_excel(initial.decoration_path, sheet_name=sheet)
    assert stored[f"{metric}_corrected"].tolist() == [0.8]
    assert result.decoration_df[f"{metric}_corrected"].tolist() == [0.8]
    assert result.period_capability_df[metric].tolist() == [1.372]
    assert result.period_capability_df[f"{metric}_decorated"].tolist() == [True]
    assert initial.decoration_path.read_bytes() == before
    prepare(capability(metric, passing_value))
    assert initial.decoration_path.read_bytes() == before


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_reading_existing_key_still_appends_new_failure(tmp_path, metric):
    sheet = resolve_capability_decoration_sheet("M678", metric)
    initial = prepare_capability_decoration(capability(metric, 0.8), tmp_path,
        sheet_name=sheet, metric=metric, reference_date=date(2026, 11, 17))
    rows = pd.concat([capability(metric, 1.6), capability(metric, 0.9).assign(param_name="NEW")], ignore_index=True)
    result = prepare_capability_decoration(rows, tmp_path,
        sheet_name=sheet, metric=metric, reference_date=date(2026, 11, 17))
    stored = pd.read_excel(initial.decoration_path, sheet_name=sheet)
    assert stored["param_name"].tolist() == ["TEST", "NEW"]
    assert stored[f"{metric}_corrected"].tolist() == [0.8, 0.9]
    assert stored["flag"].tolist() == [False, False]
    assert result.period_capability_df[metric].tolist() == [1.6, 0.9]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_enabled_decision_keeps_original_replacement_behavior(metric):
    rows = capability(metric, 1.6)
    legacy = rows.rename(columns={metric: f"{metric}_corrected"}).assign(
        flag=True, **{f"{metric}_replacement": 1.372})
    result = apply_capability_decoration(rows, legacy, metric)
    assert result[metric].tolist() == [1.372]
    assert result[f"{metric}_decorated"].tolist() == [True]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_uncovered_failures_and_unknown_values_keep_their_decisions(tmp_path, metric):
    sheet = resolve_capability_decoration_sheet("M678", metric)
    path = tmp_path / "spc_cpk_cpm_decoration.xlsx"
    existing = pd.concat([
        capability(metric, 0.8).assign(param_name="HISTORY"),
        capability(metric, float("nan")).assign(param_name="UNKNOWN"),
        capability(metric, 1.6).assign(param_name="LEGACY_PASS"),
    ], ignore_index=True).rename(columns={metric: f"{metric}_corrected"}).assign(
        flag="TURE", **{f"{metric}_replacement": 1.372})
    other = pd.DataFrame({"note": ["preserve this sheet"]})
    with pd.ExcelWriter(path) as writer:
        existing.to_excel(writer, sheet_name=sheet, index=False)
        other.to_excel(writer, sheet_name="NOTES", index=False)
    prepare_capability_decoration(capability(metric, 0.7).assign(param_name="NEW"), tmp_path,
        sheet_name=sheet, metric=metric, reference_date=date(2026, 11, 17))
    stored = pd.read_excel(path, sheet_name=sheet)
    assert stored["param_name"].tolist() == ["HISTORY", "UNKNOWN", "LEGACY_PASS", "NEW"]
    assert stored["flag"].iloc[:3].tolist() == ["TURE", "TURE", "TURE"]
    assert stored[f"{metric}_replacement"].iloc[:3].tolist() == [1.372, 1.372, 1.372]
    assert pd.isna(stored[f"{metric}_corrected"].iloc[1])
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="NOTES"), other)
