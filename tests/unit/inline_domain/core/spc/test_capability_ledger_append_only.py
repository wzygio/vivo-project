"""History and user decisions survive previous-week reconciliation."""
from datetime import date
import pandas as pd
import pytest

from src.inline_domain.core.spc import cpk_decoration as core
from src.inline_domain.infrastructure.spc.capability_decoration_repository import persist_capability_decoration


def detail(metric, value=0.9, name="EXISTING"):
    return pd.DataFrame([{
        "prod_code": "M678", "factory": "ARRAY", "step_id": "12140", "param_name": name,
        "period_type": "week", "period_label": "2026-W46", "period_sort": 46,
        "period_start": "2026-11-09", "period_end": "2026-11-12",
        f"{metric}_corrected": value,
    }])


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("new_value", [0.7, 1.6, None])
def test_out_of_scope_recalculation_never_edits_saved_cells(tmp_path, monkeypatch, metric, new_value):
    path = tmp_path / "capability.xlsx"
    sheet = core.resolve_capability_decoration_sheet("M678", metric)
    existing = detail(metric, 0.9).assign(flag="TURE", **{f"{metric}_replacement": 1.372, "note": "keep"})
    existing.to_excel(path, sheet_name=sheet, index=False)
    before = path.read_bytes()
    candidate = detail(metric, new_value).assign(period_start="2026-11-10", period_end="2026-11-15")
    monkeypatch.setattr(core, "randint", lambda *_: pytest.fail("existing key regenerated a target"))
    persist_capability_decoration(tmp_path, candidate, sheet_name=sheet, metric=metric,
        computed_detail_df=candidate, reference_date=date(2026, 11, 23), workbook_path=path, raise_on_error=True)
    assert path.read_bytes() == before


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("saved_target", [None, 0.8, 1.4])
def test_reading_invalid_saved_target_does_not_draw_or_change_input(monkeypatch, metric, saved_target):
    ledger = detail(metric).assign(flag=True, **{f"{metric}_replacement": saved_target})
    original = ledger.copy(deep=True)
    computed = ledger.drop(columns=["flag", f"{metric}_replacement"]).rename(columns={f"{metric}_corrected": metric})
    monkeypatch.setattr(core, "randint", lambda *_: pytest.fail("reading regenerated a saved target"))
    result = core.apply_capability_decoration(computed, ledger, metric)
    assert result[metric].tolist() == [0.9]
    assert result[f"{metric}_decorated"].tolist() == [False]
    pd.testing.assert_frame_equal(ledger, original)


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("saved_target", [None, 0.8])
def test_existing_legacy_target_is_not_repaired_automatically(tmp_path, monkeypatch, metric, saved_target):
    path = tmp_path / "capability.xlsx"
    sheet = core.resolve_capability_decoration_sheet("M678", metric)
    existing = detail(metric).assign(flag=True, **{f"{metric}_replacement": saved_target})
    existing.to_excel(path, sheet_name=sheet, index=False)
    before = path.read_bytes()
    monkeypatch.setattr(core, "randint", lambda *_: pytest.fail("existing target was repaired"))
    persist_capability_decoration(tmp_path, detail(metric), sheet_name=sheet, metric=metric,
        workbook_path=path, raise_on_error=True)
    assert path.read_bytes() == before


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_refresh_and_append_preserve_legacy_rows_unknowns_and_extra_columns(tmp_path, monkeypatch, metric):
    path = tmp_path / "capability.xlsx"
    sheet = core.resolve_capability_decoration_sheet("M678", metric)
    existing = pd.DataFrame([
        detail(metric, value, name).iloc[0].to_dict()
        for value, name in [(1.6, "LEGACY_PASS"), (None, "UNKNOWN"), (0.9, "EXISTING")]
    ]).assign(flag="TURE", **{f"{metric}_replacement": 1.372, "note": "keep"})
    other = pd.DataFrame({"note": ["unrelated sheet"]})
    with pd.ExcelWriter(path) as writer:
        existing.to_excel(writer, sheet_name=sheet, index=False)
        other.to_excel(writer, sheet_name="NOTES", index=False)
    before = pd.read_excel(path, sheet_name=sheet)
    draws = []
    def draw(low, high):
        draws.append((low, high))
        return 1337
    monkeypatch.setattr(core, "randint", draw)
    candidates = pd.concat([detail(metric, 0.7), detail(metric, 0.8, "NEW")], ignore_index=True)
    persist_capability_decoration(tmp_path, candidates, sheet_name=sheet, metric=metric,
        computed_detail_df=candidates, reference_date=date(2026, 11, 17), workbook_path=path, raise_on_error=True)
    after = pd.read_excel(path, sheet_name=sheet)
    before.loc[0, f"{metric}_corrected"] = float("nan")
    before.loc[2, f"{metric}_corrected"] = 0.7
    pd.testing.assert_frame_equal(after.iloc[:len(before)].reset_index(drop=True), before)
    assert after["param_name"].tolist() == ["LEGACY_PASS", "UNKNOWN", "EXISTING", "NEW"]
    assert after.iloc[-1]["flag"] == False
    assert after.iloc[-1][f"{metric}_replacement"] == 1.337
    assert pd.isna(after.iloc[-1]["note"])
    assert draws == [(1331, 1399)]
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="NOTES"), other)
