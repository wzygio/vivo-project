from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.core.spc import spc_sheet_oos_decoration
from src.inline_domain.core.spc.spc_sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    OOS_DETAIL_FILE_NAME,
    apply_sheet_oos_decoration,
    build_sheet_oos_detail,
    load_sheet_oos_decoration,
    persist_sheet_oos_files,
)


def _sheet_features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_max": 7.5,
                "sheet_min": -1.0,
                "sheet_mean": 0.5,
                "usl": 6.0,
                "lsl": -6.0,
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S2",
                "sheet_start_time": "2026-07-01 09:00:00",
                "sheet_max": 2.0,
                "sheet_min": -7.5,
                "sheet_mean": -0.5,
                "usl": 6.0,
                "lsl": -6.0,
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S3",
                "sheet_start_time": "2026-07-01 10:00:00",
                "sheet_max": 2.0,
                "sheet_min": -1.0,
                "sheet_mean": 0.2,
                "usl": 6.0,
                "lsl": -6.0,
            },
        ]
    )


def _raw_measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 7.5,
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "site_name": "P2",
                "unit_id": "3CEE01-PPA",
                "param_value": 1.0,
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S2",
                "sheet_start_time": "2026-07-01 09:00:00",
                "site_name": "P1",
                "unit_id": "3CEE02-PPA",
                "param_value": -7.5,
            },
        ]
    )


def test_build_sheet_oos_detail_detects_sheet_extreme_oos() -> None:
    detail = build_sheet_oos_detail(_sheet_features())

    assert detail["sheet_id"].tolist() == ["S1", "S2"]
    assert detail["oos_type"].tolist() == ["USL", "LSL"]
    assert set(["factory", "prod_code", "step_id", "param_name", "sheet_id", "sheet_max", "sheet_min", "sheet_mean"]).issubset(
        detail.columns
    )


def test_apply_sheet_oos_decoration_clips_flagged_points_inside_specs_deterministically() -> None:
    decorated_a = apply_sheet_oos_decoration(_raw_measurements(), _sheet_features())
    decorated_b = apply_sheet_oos_decoration(_raw_measurements(), _sheet_features())

    s1_value = decorated_a.loc[decorated_a["sheet_id"] == "S1", "param_value"].iloc[0]
    s2_value = decorated_a.loc[decorated_a["sheet_id"] == "S2", "param_value"].iloc[0]

    assert s1_value < 6.0
    assert s1_value > -6.0
    assert s2_value > -6.0
    assert s2_value < 6.0
    assert decorated_a["param_value"].tolist() == decorated_b["param_value"].tolist()


def test_apply_sheet_oos_decoration_uses_configured_ppa_expanded_clip_bounds() -> None:
    raw_measurements = _raw_measurements()
    raw_measurements.loc[raw_measurements["sheet_id"] == "S1", "param_value"] = [7.5, 6.2]

    decorated = apply_sheet_oos_decoration(
        raw_measurements,
        _sheet_features(),
        clip_rules=[
            {
                "param_name_contains": "PPA",
                "lower_offset": -0.5,
                "upper_offset": 0.5,
            }
        ],
    )

    s1_values = decorated.loc[decorated["sheet_id"] == "S1", "param_value"].tolist()
    assert s1_values[0] < 6.5
    assert s1_values[0] > -6.5
    assert s1_values[1] == 6.2


def test_apply_sheet_oos_decoration_keeps_real_values_when_flag_false() -> None:
    decoration_df = build_sheet_oos_detail(_sheet_features())
    decoration_df["flag"] = [False, True]

    decorated = apply_sheet_oos_decoration(_raw_measurements(), _sheet_features(), decoration_df)

    assert decorated.loc[decorated["sheet_id"] == "S1", "param_value"].iloc[0] == 7.5
    assert decorated.loc[decorated["sheet_id"] == "S2", "param_value"].iloc[0] > -6.0


def test_apply_sheet_oos_decoration_excludes_delete_flagged_sheet_points() -> None:
    decoration_df = build_sheet_oos_detail(_sheet_features())
    decoration_df["flag"] = [" Delete ", True]
    s1_point = _raw_measurements().iloc[[0]]
    raw_measurements = pd.concat(
        [
            _raw_measurements(),
            s1_point.assign(param_name="OTHER_PARAM"),
            s1_point.assign(step_id="OTHER_STEP"),
            s1_point.assign(prod_code="OTHER_PRODUCT"),
        ],
        ignore_index=True,
    )

    decorated = apply_sheet_oos_decoration(
        raw_measurements,
        _sheet_features(),
        decoration_df,
    )

    assert set(decorated["sheet_id"]) == {"S1", "S2"}
    s1_rows = decorated.loc[decorated["sheet_id"] == "S1"]
    assert len(s1_rows) == 3
    assert "OTHER_PARAM" in set(s1_rows["param_name"])
    assert "OTHER_STEP" in set(s1_rows["step_id"])
    assert "OTHER_PRODUCT" in set(s1_rows["prod_code"])
    assert decorated.loc[decorated["sheet_id"] == "S2", "param_value"].iloc[0] > -6.0


def test_merge_detail_preserves_delete_action_but_ignores_edited_statistics() -> None:
    detail = build_sheet_oos_detail(_sheet_features())
    existing = detail.copy()
    existing["flag"] = [" delete ", False]
    existing.loc[existing["sheet_id"] == "S1", ["sheet_min", "sheet_max", "sheet_mean"]] = [
        -999.0,
        999.0,
        123.0,
    ]

    merged = spc_sheet_oos_decoration.merge_detail_with_decoration_flags(
        detail,
        existing,
    )
    s1 = merged.loc[merged["sheet_id"] == "S1"].iloc[0]
    original_s1 = detail.loc[detail["sheet_id"] == "S1"].iloc[0]

    assert s1["flag"] == "Delete"
    assert s1[["sheet_min", "sheet_max", "sheet_mean"]].to_dict() == original_s1[
        ["sheet_min", "sheet_max", "sheet_mean"]
    ].to_dict()


def test_load_sheet_oos_decoration_falls_back_to_excel_com_for_encrypted_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    product_dir = tmp_path / "resources" / "M678"
    product_dir.mkdir(parents=True)
    decoration_path = product_dir / OOS_DECORATION_FILE_NAME
    decoration_path.write_bytes(b"\x00\x00\x00\x00enterprise-encrypted")
    expected = build_sheet_oos_detail(_sheet_features()).assign(flag=["Delete", False])

    monkeypatch.setattr(
        spc_sheet_oos_decoration.pd,
        "read_excel",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("not a zip file")),
    )
    monkeypatch.setattr(
        spc_sheet_oos_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda path: expected if path == decoration_path else pd.DataFrame(),
        raising=False,
    )

    loaded = load_sheet_oos_decoration(product_dir)

    assert loaded["flag"].tolist() == ["Delete", False]


def test_persist_sheet_oos_files_does_not_overwrite_unreadable_existing_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    product_dir = tmp_path / "resources" / "M678"
    product_dir.mkdir(parents=True)
    decoration_path = product_dir / OOS_DECORATION_FILE_NAME
    original_bytes = b"\x00\x00\x00\x00enterprise-encrypted"
    decoration_path.write_bytes(original_bytes)

    monkeypatch.setattr(
        spc_sheet_oos_decoration.pd,
        "read_excel",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("not a zip file")),
    )
    monkeypatch.setattr(
        spc_sheet_oos_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda path: (_ for _ in ()).throw(RuntimeError("Excel unavailable")),
    )

    with pytest.raises(spc_sheet_oos_decoration.SheetOosDecorationReadError):
        persist_sheet_oos_files(product_dir, build_sheet_oos_detail(_sheet_features()))

    assert decoration_path.read_bytes() == original_bytes


def test_persist_sheet_oos_files_writes_product_scoped_detail_and_preserves_flags(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources" / "Z571"
    detail = build_sheet_oos_detail(_sheet_features())

    decoration = persist_sheet_oos_files(product_dir, detail)
    assert (product_dir / OOS_DETAIL_FILE_NAME).exists()
    assert (product_dir / OOS_DECORATION_FILE_NAME).exists()
    assert decoration["flag"].tolist() == [True, True]

    decoration.loc[decoration["sheet_id"] == "S1", "flag"] = False
    decoration.to_excel(product_dir / OOS_DECORATION_FILE_NAME, index=False)

    updated = persist_sheet_oos_files(product_dir, detail)
    loaded = load_sheet_oos_decoration(product_dir)

    assert bool(updated.loc[updated["sheet_id"] == "S1", "flag"].iloc[0]) is False
    assert bool(loaded.loc[loaded["sheet_id"] == "S1", "flag"].iloc[0]) is False
