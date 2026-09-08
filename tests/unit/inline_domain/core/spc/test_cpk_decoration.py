from pathlib import Path
from datetime import date
from functools import partial
from zipfile import BadZipFile

import pandas as pd
import pytest

from src.inline_domain.infrastructure.spc import (
    capability_decoration_repository as cpk_decoration,
)
from src.inline_domain.application.spc.capability_decoration_service import (
    prepare_capability_decoration,
    prepare_cpk_decoration,
)
from src.inline_domain.core.spc.cpk_decoration import (
    apply_capability_decoration,
    apply_cpk_decoration,
    resolve_capability_decoration_sheet,
)
from src.inline_domain.infrastructure.spc.capability_decoration_repository import (
    load_cpk_decoration,
)


# Keep legacy fixture periods deterministic; individual boundary tests override this.
prepare_capability_decoration = partial(prepare_capability_decoration, reference_date=date(2026, 7, 27))
prepare_cpk_decoration = partial(prepare_cpk_decoration, reference_date=date(2026, 7, 27))


def _capability_frame(cpk: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "period_type": "week",
                "period_label": "2026-W30",
                "period_sort": 320,
                "period_start": "2026-07-20",
                "period_end": "2026-07-20",
                "cpk": cpk,
            }
        ]
    )


def test_cpk_decoration_defaults_to_computed_cpk_until_an_admin_enables_a_row(tmp_path: Path) -> None:
    computed_df = _capability_frame(1.26)

    result = prepare_cpk_decoration(
        period_capability_df=computed_df,
        product_dir=tmp_path,
        persist_files=False,
        sheet_name="M678",
    )

    assert result.decoration_sheet == "M678"
    assert result.decoration_df["flag"].tolist() == [False]
    assert result.decoration_df["cpk_corrected"].tolist() == [1.26]
    assert result.period_capability_df["cpk"].tolist() == [1.26]
    assert result.period_capability_df["cpk_decorated"].tolist() == [False]

    enabled_df = result.decoration_df.assign(cpk_corrected=1.72, cpk_replacement=1.372, flag=True)
    decorated_df = apply_cpk_decoration(computed_df, enabled_df)

    assert decorated_df["cpk"].tolist() == [1.372]
    assert decorated_df["cpk_decorated"].tolist() == [True]


def test_cpk_decoration_accepts_existing_ture_flag_typo() -> None:
    computed_df = _capability_frame(1.26)
    decoration_df = prepare_cpk_decoration(
        period_capability_df=computed_df,
        product_dir=Path("."),
        persist_files=False,
    ).decoration_df.assign(cpk_corrected=1.72, cpk_replacement=1.372, flag="TURE")

    decorated_df = apply_cpk_decoration(computed_df, decoration_df)

    assert decorated_df["cpk"].tolist() == [1.372]
    assert decorated_df["cpk_decorated"].tolist() == [True]


def test_cpk_decoration_matches_excel_numeric_step_id_to_runtime_string() -> None:
    computed_df = _capability_frame(1.26)
    decoration_df = prepare_cpk_decoration(
        period_capability_df=computed_df,
        product_dir=Path("."),
        persist_files=False,
    ).decoration_df.assign(step_id=12140.0, cpk_corrected=pd.NA, flag=True)

    decorated_df = apply_cpk_decoration(computed_df, decoration_df)

    assert 1.33 < decorated_df["cpk"].iloc[0] < 1.4
    assert decorated_df["cpk_decorated"].tolist() == [True]


def test_cpm_decoration_uses_cpm_columns_and_decorated_flag() -> None:
    computed_df = _capability_frame(1.26).rename(columns={"cpk": "cpm"})

    result = prepare_capability_decoration(
        period_capability_df=computed_df,
        product_dir=Path("."),
        persist_files=False,
        sheet_name="M678_cpm",
        metric="cpm",
    )

    assert result.decoration_sheet == "M678_cpm"
    assert result.decoration_df["flag"].tolist() == [False]
    assert result.decoration_df["cpm_corrected"].tolist() == [1.26]
    assert result.period_capability_df["cpm"].tolist() == [1.26]
    assert result.period_capability_df["cpm_decorated"].tolist() == [False]

    enabled_df = result.decoration_df.assign(cpm_corrected=1.72, cpm_replacement=1.372, flag=True)
    decorated_df = apply_capability_decoration(computed_df, enabled_df, metric="cpm")

    assert decorated_df["cpm"].tolist() == [1.372]
    assert decorated_df["cpm_decorated"].tolist() == [True]


def test_cpk_and_cpm_decoration_sheets_coexist_in_one_workbook(tmp_path: Path) -> None:
    computed_df = _capability_frame(1.26)

    cpk_result = prepare_capability_decoration(
        period_capability_df=computed_df,
        product_dir=tmp_path,
        sheet_name=resolve_capability_decoration_sheet("M678", "cpk"),
        metric="cpk",
    )
    cpm_result = prepare_capability_decoration(
        period_capability_df=computed_df.rename(columns={"cpk": "cpm"}),
        product_dir=tmp_path,
        sheet_name=resolve_capability_decoration_sheet("M678", "cpm"),
        metric="cpm",
    )

    assert cpk_result.decoration_sheet == "M678"
    assert cpm_result.decoration_sheet == "M678_cpm"
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    assert pd.read_excel(decoration_path, sheet_name="M678")["cpk_corrected"].tolist() == [1.26]
    assert pd.read_excel(decoration_path, sheet_name="M678_cpm")["cpm_corrected"].tolist() == [1.26]


def test_load_cpk_decoration_falls_back_to_excel_com_for_enterprise_encrypted_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    product_dir = tmp_path
    decoration_path = product_dir / cpk_decoration.CPK_DECORATION_FILE_NAME
    expected_df = prepare_cpk_decoration(
        period_capability_df=_capability_frame(0.82),
        product_dir=tmp_path,
        persist_files=False,
    ).decoration_df.assign(flag="TURE")
    decoration_path.write_bytes(b"\x00\x00\x00\x00enterprise-encrypted")

    monkeypatch.setattr(
        cpk_decoration.pd,
        "read_excel",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("Excel file format cannot be determined")
        ),
    )
    monkeypatch.setattr(
        cpk_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda path, sheet_name=None: expected_df if path == decoration_path else pd.DataFrame(),
    )

    loaded_df = load_cpk_decoration(product_dir, sheet_name="M678")

    assert loaded_df.equals(expected_df)


def test_prepare_cpk_decoration_never_rewrites_an_existing_user_sheet(tmp_path: Path) -> None:
    computed_df = _capability_frame(1.26)
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    existing_df = prepare_cpk_decoration(
        period_capability_df=computed_df,
        product_dir=tmp_path,
        persist_files=False,
    ).decoration_df.assign(cpk_corrected=1.72, cpk_replacement=1.372, flag="TURE")
    other_sheet_df = pd.DataFrame([{"prod_code": "OTHER", "note": "keep-me"}])
    with pd.ExcelWriter(decoration_path, engine="openpyxl") as writer:
        existing_df.to_excel(writer, index=False, sheet_name="M678")
        other_sheet_df.to_excel(writer, index=False, sheet_name="OTHER")
    original_bytes = decoration_path.read_bytes()

    result = prepare_cpk_decoration(
        period_capability_df=computed_df,
        product_dir=tmp_path,
        sheet_name="M678",
    )

    assert decoration_path.read_bytes() == original_bytes
    assert not (tmp_path / "spc_cpk_detail.xlsx").exists()
    assert result.decoration_sheet == "M678"
    assert result.period_capability_df["cpk"].tolist() == [1.372]
    assert result.period_capability_df["cpk_decorated"].tolist() == [True]
    # 共享工作簿中的其他 sheet 不受影响
    other_loaded = pd.read_excel(decoration_path, sheet_name="OTHER")
    assert other_loaded["note"].tolist() == ["keep-me"]


def test_prepare_cpk_decoration_appends_new_periods_to_an_existing_user_sheet(
    tmp_path: Path,
) -> None:
    existing_capability_df = _capability_frame(1.26)
    new_capability_df = _capability_frame(0.919).assign(
        period_type="week",
        period_label="2026-W35",
        period_sort=208,
        period_start="2026-08-24",
        period_end="2026-08-30",
    )
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    existing_df = prepare_cpk_decoration(
        period_capability_df=existing_capability_df,
        product_dir=tmp_path,
        persist_files=False,
    ).decoration_df.assign(cpk_corrected=1.72, cpk_replacement=1.372, flag=True)
    other_sheet_df = pd.DataFrame([{"prod_code": "OTHER", "note": "keep-me"}])
    with pd.ExcelWriter(decoration_path, engine="openpyxl") as writer:
        existing_df.to_excel(writer, index=False, sheet_name="M678")
        other_sheet_df.to_excel(writer, index=False, sheet_name="OTHER")

    result = prepare_cpk_decoration(
        period_capability_df=pd.concat(
            [existing_capability_df, new_capability_df],
            ignore_index=True,
        ),
        product_dir=tmp_path,
        sheet_name="M678",
        reference_date=date(2026, 8, 31),
    )

    persisted_df = pd.read_excel(decoration_path, sheet_name="M678")
    assert persisted_df["period_label"].tolist() == ["2026-W30", "2026-W35"]
    assert persisted_df["cpk_corrected"].tolist() == [1.72, 0.919]
    assert persisted_df["flag"].tolist() == [True, False]
    assert result.period_capability_df["cpk_decorated"].tolist() == [True, False]
    other_loaded = pd.read_excel(decoration_path, sheet_name="OTHER")
    assert other_loaded["note"].tolist() == ["keep-me"]


def test_prepare_cpk_decoration_preserves_an_unreadable_existing_user_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    encrypted_bytes = b"\x00\x00\x00\x00enterprise-encrypted"
    decoration_path.write_bytes(encrypted_bytes)
    monkeypatch.setattr(
        cpk_decoration.pd,
        "read_excel",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(BadZipFile("File is not a zip file")),
    )
    monkeypatch.setattr(
        cpk_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("Excel COM unavailable")),
    )

    result = prepare_cpk_decoration(
        period_capability_df=_capability_frame(0.82),
        product_dir=tmp_path,
        sheet_name="M678",
    )

    assert decoration_path.read_bytes() == encrypted_bytes
    assert result.period_capability_df["cpk"].tolist() == [0.82]
    assert result.period_capability_df["cpk_decorated"].tolist() == [False]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_prepare_capability_populates_existing_header_only_sheet(tmp_path: Path, metric: str) -> None:
    computed = _capability_frame(1.084).rename(columns={"cpk": metric}).assign(
        prod_code="Z517", period_type="week", period_label="2026-W36",
    )
    sheet = resolve_capability_decoration_sheet("Z517", metric)
    path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    empty = prepare_capability_decoration(
        computed.iloc[:0], tmp_path, persist_files=False, metric=metric,
    ).decoration_df
    other = pd.DataFrame([{"note": "keep user decision", "flag": True}])
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        empty.to_excel(writer, sheet_name=sheet, index=False)
        other.to_excel(writer, sheet_name="OTHER", index=False)

    prepare_capability_decoration(computed, tmp_path, sheet_name=sheet, metric=metric, reference_date=date(2026, 9, 8))

    stored = pd.read_excel(path, sheet_name=sheet)
    assert stored["prod_code"].tolist() == ["Z517"]
    assert stored["period_label"].tolist() == ["2026-W36"]
    assert stored[f"{metric}_corrected"].tolist() == [1.084]
    assert stored["flag"].tolist() == [False]
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="OTHER"), other)
    original_bytes = path.read_bytes()
    prepare_capability_decoration(computed, tmp_path, sheet_name=sheet, metric=metric, reference_date=date(2026, 9, 8))
    assert path.read_bytes() == original_bytes


def test_failed_sheet_read_is_not_treated_as_header_only(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    pd.DataFrame([{"flag": True, "cpk_corrected": 1.72}]).to_excel(
        path, sheet_name="Z517", index=False,
    )
    original_bytes = path.read_bytes()

    def fail_read(*args, **kwargs):
        raise OSError("Sheet read unavailable")

    monkeypatch.setattr(cpk_decoration.pd, "read_excel", fail_read)
    monkeypatch.setattr(cpk_decoration, "_read_encrypted_xlsx_via_com", fail_read)
    monkeypatch.setattr(cpk_decoration, "list_workbook_sheet_names", lambda path: ["Z517"])
    prepare_capability_decoration(
        _capability_frame(1.084).assign(prod_code="Z517"), tmp_path, sheet_name="Z517",
    )
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_daily_capability_is_not_written_or_manually_enabled(tmp_path: Path, metric: str) -> None:
    daily = _capability_frame(0.8).rename(columns={"cpk": metric}).assign(
        period_type="day", period_label="2026-07-20",
    )
    weekly = daily.assign(period_type="week", period_label="2026-W30")
    computed = pd.concat([daily, weekly], ignore_index=True)
    result = prepare_capability_decoration(computed, tmp_path, metric=metric)
    stored = pd.read_excel(result.decoration_path)
    assert stored["period_type"].tolist() == ["week"]
    enabled = result.decoration_df.assign(period_type="day", period_label="2026-07-20", flag=True)
    applied = apply_capability_decoration(computed, enabled, metric)
    assert not applied.loc[applied["period_type"].eq("day"), f"{metric}_decorated"].any()


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("reference", [date(2026, 9, 7), date(2026, 9, 13)])
def test_only_previous_week_failures_enter_ledger(tmp_path: Path, metric: str, reference: date) -> None:
    base = _capability_frame(0.8).rename(columns={"cpk": metric})
    rows = pd.concat([
        base.assign(param_name="failed", period_label="2026-W36"),
        base.assign(param_name="boundary", period_label="2026-W36", **{metric: 1.33}),
        base.assign(param_name="passed", period_label="2026-W36", **{metric: 2.0}),
        base.assign(param_name="missing", period_label="2026-W36", **{metric: float("nan")}),
        base.assign(param_name="current", period_label="2026-W37"),
        base.assign(param_name="old", period_label="2026-W35"),
        base.assign(param_name="monthly", period_type="month", period_label="2026-08"),
    ], ignore_index=True)
    result = prepare_capability_decoration(rows, tmp_path, metric=metric, reference_date=reference)
    stored = pd.read_excel(result.decoration_path)
    assert stored["param_name"].tolist() == ["failed"]
    assert stored["flag"].tolist() == [False]
    assert len(result.period_capability_df) == len(rows)


def test_previous_week_uses_iso_year(tmp_path: Path) -> None:
    rows = _capability_frame(0.8).assign(period_label="2025-W52")
    result = prepare_capability_decoration(rows, tmp_path, reference_date=date(2026, 1, 1))
    assert result.decoration_df["period_label"].tolist() == ["2025-W52"]


def test_existing_decision_survives_when_no_new_anomalies(tmp_path: Path) -> None:
    rows = _capability_frame(0.8)
    first = prepare_capability_decoration(rows, tmp_path, sheet_name="M678")
    first.decoration_df.assign(cpk_corrected=1.72, cpk_replacement=1.372, flag=True).to_excel(
        first.decoration_path, sheet_name="M678", index=False,
    )
    before = first.decoration_path.read_bytes()
    result = prepare_capability_decoration(
        rows, tmp_path, sheet_name="M678", reference_date=date(2026, 8, 10),
    )
    assert first.decoration_path.read_bytes() == before
    assert result.period_capability_df["cpk"].tolist() == [1.372]
    assert result.period_capability_df["cpk_decorated"].tolist() == [True]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_enabled_legacy_anomaly_gets_persisted_passing_replacement(tmp_path: Path, metric: str) -> None:
    rows = _capability_frame(1.084).rename(columns={"cpk": metric})
    first = prepare_capability_decoration(rows, tmp_path, sheet_name="M678", metric=metric)
    ledger = first.decoration_df.drop(columns=[f"{metric}_replacement"], errors="ignore").assign(flag=True)
    ledger.to_excel(first.decoration_path, sheet_name="M678", index=False)
    result = prepare_capability_decoration(rows, tmp_path, sheet_name="M678", metric=metric)
    stored = pd.read_excel(first.decoration_path, sheet_name="M678")
    assert stored[f"{metric}_corrected"].tolist() == [1.084]
    assert stored[f"{metric}_replacement"].iloc[0] > 1.33
    assert result.period_capability_df[metric].iloc[0] == stored[f"{metric}_replacement"].iloc[0]
    assert result.period_capability_df[f"{metric}_decorated"].tolist() == [True]
    before = first.decoration_path.read_bytes()
    prepare_capability_decoration(rows, tmp_path, sheet_name="M678", metric=metric)
    assert first.decoration_path.read_bytes() == before


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
@pytest.mark.parametrize("target", [None, 0.8, 1.33, 1.4, float("inf"), "invalid", 1.88, 1.372])
def test_replacement_is_passing_and_only_applied_when_enabled(metric: str, target: object) -> None:
    rows = _capability_frame(1.084).rename(columns={"cpk": metric})
    ledger = rows.rename(columns={metric: f"{metric}_corrected"}).assign(
        flag=True, **{f"{metric}_replacement": target},
    )
    result = apply_capability_decoration(rows, ledger, metric)
    assert 1.33 < result[metric].iloc[0] < 1.4
    if target == 1.372:
        assert result[metric].iloc[0] == 1.372
    disabled = apply_capability_decoration(rows, ledger.assign(flag=False), metric)
    assert disabled[metric].iloc[0] == 1.084


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_legacy_value_outside_new_range_gets_random_replacement(metric: str) -> None:
    rows = _capability_frame(1.084).rename(columns={"cpk": metric})
    ledger = rows.rename(columns={metric: f"{metric}_corrected"}).assign(
        flag=True, **{f"{metric}_corrected": 1.72},
    )
    result = apply_capability_decoration(rows, ledger, metric)
    assert 1.33 < result[metric].iloc[0] < 1.4
    assert ledger[f"{metric}_corrected"].iloc[0] == 1.72


def test_random_replacements_exclude_endpoints_and_are_reused(monkeypatch) -> None:
    from src.inline_domain.core.spc import cpk_decoration as core

    draws = iter([1331, 1399])
    monkeypatch.setattr(core, "randint", lambda low, high: next(draws))
    ledger = pd.concat([_capability_frame(0.8)] * 2, ignore_index=True).rename(
        columns={"cpk": "cpk_corrected"},
    ).assign(flag=True)
    generated = core.ensure_capability_replacements(ledger, "cpk")
    assert generated["cpk_replacement"].tolist() == [1.331, 1.399]
    # No further draws are available: valid persisted values must be reused.
    pd.testing.assert_frame_equal(core.ensure_capability_replacements(generated, "cpk"), generated)
