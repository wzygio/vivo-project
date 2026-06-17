import pandas as pd
import numpy as np

from src.yield_domain.core.mapping_processor import apply_hotspot_modification_to_matrix


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
