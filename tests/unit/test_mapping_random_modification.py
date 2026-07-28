import pandas as pd
import numpy as np
import pytest

from yield_domain.core.mapping import mapping_processor as mapping_module
from yield_domain.core.mapping.mapping_processor import apply_hotspot_modification_to_matrix


def _concentrated_matrix() -> pd.DataFrame:
    matrix = pd.DataFrame(0, index=range(10), columns=range(19))
    matrix.loc[9, :] = 10
    matrix.loc[9, 18] = 24
    matrix.loc[2, 2] = 3
    return matrix


def test_random_mapping_modification_targets_product_code_and_batch() -> None:
    matrix = _concentrated_matrix()
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "26/04/29蒸镀批",
            "mode": "random",
            "random_method": "poisson",
            "random_variation": 0.15,
            "random_seed": 2026,
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="26/04/29蒸镀批",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert int(modified.to_numpy().sum()) == int(matrix.to_numpy().sum())
    assert modified.loc[9].sum() < matrix.loc[9].sum() * 0.35

    values = modified.to_numpy().ravel()
    assert values.std() > 0.7
    assert np.unique(values).size >= 4


def test_random_mapping_modification_does_not_apply_to_other_product() -> None:
    matrix = _concentrated_matrix()
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "26/04/29蒸镀批",
            "mode": "random",
            "random_seed": 2026,
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="26/04/29蒸镀批",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M678",
    )

    pd.testing.assert_frame_equal(modified, matrix)


def test_random_mapping_modification_does_not_apply_to_other_batch() -> None:
    matrix = _concentrated_matrix()
    scripts = [
        {
            "enable": True,
            "target_product": "ALL",
            "target_code": "彩斑Mura",
            "target_batch": "26/04/29蒸镀批",
            "mode": "random",
            "random_seed": 2026,
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="26/05/14蒸镀批",
        code_desc="彩斑Mura",
        batch_position=3,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    pd.testing.assert_frame_equal(modified, matrix)


def test_mapping_original_mode_returns_unmodified_matrix() -> None:
    matrix = _concentrated_matrix()
    scripts = [
        {
            "enable": True,
            "target_product": "ALL",
            "target_code": "ALL",
            "target_batch": "0429",
            "mode": "original",
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="26/04/29蒸镀批",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    pd.testing.assert_frame_equal(modified, matrix)


def test_specific_batch_rule_precedes_all_batch_rule_for_overlapping_hotspot() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "ALL",
            "mode": "multiplicative",
            "hotspot_multiplier": 2,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 3,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 30
    assert modified.loc[0, 1] == 10


def test_specific_batch_rule_selects_mode_before_all_batch_rule() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "ALL",
            "mode": "additive",
            "hotspot_adder": 5,
            "normal_multiplier_in_add_mode": 0,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 3,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 30
    assert modified.loc[0, 1] == 10


def test_specific_batch_plan_does_not_inherit_all_batch_hotspots() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "ALL",
            "mode": "additive",
            "hotspot_adder": 5,
            "normal_multiplier_in_add_mode": 0,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "additive",
            "hotspot_adder": 8,
            "normal_multiplier_in_add_mode": 0,
            "hotspot_rules": [{"type": "position", "value": [["1A", "B0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 10
    assert modified.loc[0, 1] == 18


def test_specific_product_rule_precedes_all_product_rule() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    common_script = {
        "enable": True,
        "target_code": "CodeA",
        "target_batch": "2026/04/29",
        "mode": "multiplicative",
        "normal_multiplier": 1,
        "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
    }
    scripts = [
        {
            **common_script,
            "target_product": "ALL",
            "hotspot_multiplier": 2,
        },
        {
            **common_script,
            "target_product": "M626",
            "hotspot_multiplier": 3,
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 30


def test_all_batch_rule_remains_fallback_for_other_batches() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "ALL",
            "mode": "multiplicative",
            "hotspot_multiplier": 2,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 3,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/30",
        code_desc="CodeA",
        batch_position=3,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 20
    assert modified.loc[0, 1] == 10


def test_equal_priority_rules_keep_configuration_order() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    common_script = {
        "enable": True,
        "target_product": "M626",
        "target_code": "CodeA",
        "target_batch": "2026/04/29",
        "mode": "multiplicative",
        "normal_multiplier": 1,
        "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
    }
    scripts = [
        {**common_script, "hotspot_multiplier": 2},
        {**common_script, "hotspot_multiplier": 3},
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 20


def test_specific_batch_index_rule_precedes_all_index_rule() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    common_script = {
        "enable": True,
        "target_product": "M626",
        "target_code": "CodeA",
        "target_batch": "ALL",
        "mode": "multiplicative",
        "normal_multiplier": 1,
        "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
    }
    scripts = [
        {
            **common_script,
            "target_batch_index": "all",
            "hotspot_multiplier": 2,
        },
        {
            **common_script,
            "target_batch_index": "latest",
            "hotspot_multiplier": 3,
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=4,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 30


def test_mapping_modification_matches_two_digit_and_four_digit_batch_dates() -> None:
    matrix = _concentrated_matrix()
    scripts = [
        {
            "enable": True,
            "target_product": "ALL",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 0,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "row", "value": ["2E"]}],
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="26/04/29蒸镀批",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[9].sum() == 0


def test_additive_mode_adds_fixed_values_and_supports_negative_normal_add() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "ALL",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "addtive",
            "hotspot_adder": 5,
            "normal_multiplier_in_add_mode": -3,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        }
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 15
    assert modified.loc[0, 1] == 7


def test_additive_mode_applies_multiple_hotspot_rules_but_first_normal_add_once() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "additive",
            "hotspot_adder": 5,
            "normal_multiplier_in_add_mode": -2,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "additive",
            "hotspot_adder": 8,
            "normal_multiplier_in_add_mode": -99,
            "hotspot_rules": [{"type": "position", "value": [["1A", "B0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 15
    assert modified.loc[0, 1] == 18
    assert modified.loc[0, 2] == 8


def test_additive_row_hotspot_adds_reproducible_per_position_perturbation() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "additive",
            "hotspot_adder": 5,
            "normal_multiplier_in_add_mode": 0,
            "random_seed": 17,
            "hotspot_rules": [{"type": "row", "value": ["1A"]}],
        }
    ]

    first = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )
    second = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    pd.testing.assert_frame_equal(first, second)
    assert set(first.loc[0]).issubset({15, 16, 17})
    assert first.loc[0].nunique() > 1
    pd.testing.assert_frame_equal(first.iloc[1:], matrix.iloc[1:])


def test_multiplicative_mode_applies_multiple_hotspot_rules_but_first_normal_multiplier_once() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 2,
            "normal_multiplier": 0.5,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"]]}],
        },
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 3,
            "normal_multiplier": 0.1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "B0"]]}],
        },
    ]

    modified = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="彩斑Mura",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    assert modified.loc[0, 0] == 20
    assert modified.loc[0, 1] == 30
    assert modified.loc[0, 2] == 5


def test_multiplicative_col_hotspot_adds_reproducible_per_position_perturbation() -> None:
    matrix = pd.DataFrame(10, index=range(10), columns=range(19))
    scripts = [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "CodeA",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 2,
            "normal_multiplier": 1,
            "random_seed": 17,
            "hotspot_rules": [{"type": "col", "value": ["A0"]}],
        }
    ]

    first = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )
    second = apply_hotspot_modification_to_matrix(
        heatmap_matrix=matrix,
        batch_no="2026/04/29",
        code_desc="CodeA",
        batch_position=2,
        total_batches=5,
        script_config_list=scripts,
        product_code="M626",
    )

    pd.testing.assert_frame_equal(first, second)
    assert set(first.loc[:, 0]).issubset({20, 21, 22})
    assert first.loc[:, 0].nunique() > 1
    pd.testing.assert_frame_equal(first.iloc[:, 1:], matrix.iloc[:, 1:])


def test_mapping_rate_cascade_keeps_existing_theoretical_ceiling(monkeypatch) -> None:
    panel_rows = []
    batches = ["2026/01/01", "2026/01/02", "2026/01/03"]
    for batch_no in batches:
        for panel_number in range(100):
            panel_rows.append(
                {
                    "batch_no": batch_no,
                    "panel_id": f"{batch_no}-P{panel_number:03d}",
                    "defect_desc": "CodeA" if panel_number < 20 else None,
                }
            )
    monkeypatch.setattr(
        mapping_module,
        "_get_deterministically_modified_panel_id",
        lambda panel_id, batch_no: panel_id,
    )

    result = mapping_module.prepare_mapping_data(
        pd.DataFrame(panel_rows),
        scaling_factor=0.5,
    )

    assert result.groupby("batch_no").size().reindex(batches).tolist() == [10, 9, 9]
    assert result.groupby("batch_no")["batch_total_input"].first().tolist() == [
        100,
        100,
        100,
    ]


def test_prepare_mapping_data_original_mode_preserves_matching_code_positions(
    monkeypatch,
) -> None:
    original_panel_id = "SHEET0000011AA0"
    default_panel_id = "SHEET0000011AB0"
    shifted_panel_id = "SHEET0000012ES0"
    panel_df = pd.DataFrame(
        [
            {
                "batch_no": "2026/06/29",
                "panel_id": original_panel_id,
                "defect_desc": "CodeOriginal",
            },
            {
                "batch_no": "2026/06/29",
                "panel_id": default_panel_id,
                "defect_desc": "CodeDefault",
            },
        ]
    )
    monkeypatch.setattr(
        mapping_module,
        "_get_deterministically_modified_panel_id",
        lambda panel_id, batch_no: shifted_panel_id,
    )
    scripts = [
        {
            "enable": True,
            "target_product": "Z571",
            "target_code": "CodeOriginal",
            "target_batch": "2026/06/29",
            "mode": "original",
        }
    ]

    result = mapping_module.prepare_mapping_data(
        panel_details_df=panel_df,
        scaling_factor=1,
        hotspot_scripts=scripts,
        product_code="Z571",
    )

    panel_ids_by_code = result.set_index("defect_desc")["panel_id"].to_dict()
    assert panel_ids_by_code["CodeOriginal"] == original_panel_id
    assert panel_ids_by_code["CodeDefault"] == shifted_panel_id


@pytest.mark.parametrize(
    ("mode", "mode_config"),
    [
        ("random", {"random_seed": 17}),
        (
            "additive",
            {
                "hotspot_adder": 5,
                "normal_multiplier_in_add_mode": 0,
                "hotspot_rules": [
                    {"type": "position", "value": [["1A", "A0"]]}
                ],
            },
        ),
        (
            "multiplicative",
            {
                "hotspot_multiplier": 2,
                "normal_multiplier": 1,
                "hotspot_rules": [
                    {"type": "position", "value": [["1A", "A0"]]}
                ],
            },
        ),
    ],
)
def test_prepare_mapping_data_explicit_matrix_modes_bypass_default_position_shift(
    monkeypatch,
    mode: str,
    mode_config: dict,
) -> None:
    original_panel_id = "SHEET0000011AA0"
    panel_df = pd.DataFrame(
        [
            {
                "batch_no": "2026/06/29",
                "panel_id": original_panel_id,
                "defect_desc": "CodeA",
            }
        ]
    )
    monkeypatch.setattr(
        mapping_module,
        "_get_deterministically_modified_panel_id",
        lambda panel_id, batch_no: "SHEET0000012ES0",
    )
    scripts = [
        {
            "enable": True,
            "target_product": "Z571",
            "target_code": "CodeA",
            "target_batch": "2026/06/29",
            "mode": mode,
            **mode_config,
        }
    ]

    result = mapping_module.prepare_mapping_data(
        panel_details_df=panel_df,
        scaling_factor=1,
        hotspot_scripts=scripts,
        product_code="Z571",
    )

    assert result["panel_id"].tolist() == [original_panel_id]


def test_specific_deterministic_position_plan_overrides_global_random(
    monkeypatch,
) -> None:
    original_panel_id = "SHEET0000011AA0"
    shifted_panel_id = "SHEET0000012ES0"
    panel_df = pd.DataFrame(
        [
            {
                "batch_no": "2026/06/29",
                "panel_id": original_panel_id,
                "defect_desc": "CodeA",
            }
        ]
    )
    monkeypatch.setattr(
        mapping_module,
        "_get_deterministically_modified_panel_id",
        lambda panel_id, batch_no: shifted_panel_id,
    )
    scripts = [
        {
            "enable": True,
            "target_product": "ALL",
            "target_code": "CodeA",
            "target_batch": "ALL",
            "mode": "random",
        },
        {
            "enable": True,
            "target_product": "Z571",
            "target_code": "CodeA",
            "target_batch": "2026/06/29",
            "mode": "deterministic_position",
        },
    ]

    result = mapping_module.prepare_mapping_data(
        panel_details_df=panel_df,
        scaling_factor=1,
        hotspot_scripts=scripts,
        product_code="Z571",
    )

    assert result["panel_id"].tolist() == [shifted_panel_id]
