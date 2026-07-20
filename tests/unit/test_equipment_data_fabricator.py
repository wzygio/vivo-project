from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.equipment_domain.core.parts_matcher import _compile_like_pattern
from src.equipment_domain.core.parts_identity import build_fabricated_param_name
from src.equipment_domain.infrastructure.fake_data import (
    FabricationPolicy,
    fabricate_current_snapshot,
    materialize_param_name,
    write_fabricated_snapshot,
)


def _policy() -> FabricationPolicy:
    return FabricationPolicy(
        random_seed=20260715,
        normal_share=0.60,
        warning_share=0.25,
        over_share=0.15,
        normal_ratio_range=(0.35, 0.85),
        warning_ratio_range=(0.91, 0.99),
        over_ratio_range=(1.01, 1.15),
    )


def _spec_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["Array", "Target", "PVD", "MO", "DEPO", 100.0, "S1", "EQ1-PM3", "%TRGTLIFE%_G_MAX"],
            ["Array", "Target", "PVD", "MO", "DEPO", 100.0, "S1", "EQ1-PM3", "%TRGTLIFE%_G_MAX"],
            ["Array", "Mask", "PVD", "MO", "DEPO", 200.0, "S2", "EQ2-PM4", "%MASKLIFE%_G_MAX"],
            ["Array", "Target", "PVD", "MO", "DEPO", 300.0, "S3", "EQ3-PM5", "%PRE_SPRT_KWH%"],
            ["TP", "陶瓷", "ETCH", "ITO", "ETCH", 400.0, "S4", "EQ4-PM6", ""],
        ],
        columns=[
            "厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格",
            "站点", "机台号-腔室", "参数名称",
        ],
    )


def test_materialized_param_names_match_their_like_patterns() -> None:
    cases = [
        ("%TRGTLIFE%_G_MAX", "3AFS03-SPU-PM3"),
        ("%MASKLIFE%_G_MAX", "3AFS04-SPU-PM4"),
        ("%PRE_SPRT_KWH%", "3AFS05-SPU-PM5"),
    ]

    for pattern, machine in cases:
        param_name = materialize_param_name(pattern, machine)
        assert _compile_like_pattern(pattern).fullmatch(param_name)
        assert "PM" in param_name or param_name.startswith("P")


def test_blank_parameter_identity_is_stable_and_uses_business_fields() -> None:
    spec_row = _spec_rows().iloc[-1].copy()

    first = build_fabricated_param_name(spec_row)
    reordered = build_fabricated_param_name(spec_row.reindex(reversed(spec_row.index)))
    equivalent = spec_row.copy()
    equivalent["寿命规格"] = "400.000"
    changed = spec_row.copy()
    changed["备件类型"] = "Different Part"

    assert first.startswith("__FABRICATED_PART__")
    assert reordered == first
    assert build_fabricated_param_name(equivalent) == first
    assert build_fabricated_param_name(changed) != first


def test_fabrication_is_deterministic_and_emits_one_current_row_per_unique_key() -> None:
    as_of = pd.Timestamp("2026-07-15 08:30:00")

    first = fabricate_current_snapshot(_spec_rows(), _policy(), as_of=as_of)
    second = fabricate_current_snapshot(_spec_rows(), _policy(), as_of=as_of)

    pd.testing.assert_frame_equal(first.snapshot_df, second.snapshot_df)
    assert list(first.snapshot_df.columns) == [
        "step_id", "sub_equip_id", "param_name", "value", "glass_start_time"
    ]
    assert len(first.snapshot_df) == 4
    assert first.snapshot_df.dtypes.astype(str).to_dict() == {
        "step_id": "object",
        "sub_equip_id": "object",
        "param_name": "object",
        "value": "float64",
        "glass_start_time": "datetime64[ns]",
    }
    assert not first.snapshot_df.isna().any().any()
    assert first.snapshot_df["glass_start_time"].eq(as_of).all()
    assert np.isfinite(first.snapshot_df["value"]).all()
    assert first.summary["source_rows"] == 5
    assert first.summary["monitorable_rows"] == 5
    assert first.summary["generated_rows"] == 4
    assert first.summary["synthetic_param_rows"] == 1
    assert first.summary["skipped_blank_param_rows"] == 0
    synthetic_rows = first.snapshot_df[
        first.snapshot_df["param_name"].str.startswith("__FABRICATED_PART__")
    ]
    assert len(synthetic_rows) == 1
    assert set(first.summary["raw_status_counts"]) == {"normal", "warning", "over"}


def test_conflicting_specs_for_one_bottom_key_are_rejected() -> None:
    specs = _spec_rows()
    conflict = specs.iloc[[0]].copy()
    conflict["寿命规格"] = 999.0
    specs = pd.concat([specs, conflict], ignore_index=True)

    with pytest.raises(ValueError, match="conflicting life specifications"):
        fabricate_current_snapshot(specs, _policy(), as_of=pd.Timestamp("2026-07-15"))


def test_snapshot_writer_uses_production_signature_and_refuses_overwrite(tmp_path: Path) -> None:
    specs = _spec_rows()
    result = fabricate_current_snapshot(
        specs,
        _policy(),
        as_of=pd.Timestamp("2026-07-15 08:30:00"),
    )

    output_path = write_fabricated_snapshot(
        result.snapshot_df,
        specs,
        output_dir=tmp_path,
    )

    assert output_path.parent == tmp_path
    assert output_path.name.startswith("part_life_snapshot_")
    assert output_path.suffix == ".parquet"
    pd.testing.assert_frame_equal(pd.read_parquet(output_path), result.snapshot_df)
    with pytest.raises(FileExistsError):
        write_fabricated_snapshot(result.snapshot_df, specs, output_dir=tmp_path)


def test_fabrication_policy_rejects_invalid_distribution() -> None:
    with pytest.raises(ValueError, match="sum to 1"):
        replace(_policy(), normal_share=0.70)
