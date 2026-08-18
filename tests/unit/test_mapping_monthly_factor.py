# tests/unit/test_mapping_monthly_factor.py
"""Mapping 月度缩放倍数（批次所属月份 × Code）行为测试。"""
import pandas as pd
import pytest

from yield_domain.core.mapping.mapping_processor import prepare_mapping_data


def _panel_rows(batch_no: str, code: str | None, count: int, start: int = 0):
    return [
        {
            "batch_no": batch_no,
            "panel_id": f"{batch_no}-P{i + start:04d}",
            "defect_desc": code,
            "defect_group": "Array_Pixel" if code else None,
        }
        for i in range(count)
    ]


def _single_batch_df() -> pd.DataFrame:
    rows = _panel_rows("2026/07/05-LOT1", "CodeX", 10)
    rows += _panel_rows("2026/07/05-LOT1", None, 90, start=100)
    return pd.DataFrame(rows)


def _count(df: pd.DataFrame, code: str) -> int:
    return int((df["defect_desc"] == code).sum())


class TestMappingMonthlyFactor:
    def test_no_factors_keeps_counts_unchanged(self):
        result = prepare_mapping_data(_single_batch_df(), scaling_factor=1.0)
        assert _count(result, "CodeX") == 10

    def test_downscale_samples_deterministically(self):
        factors = {("CodeX", "2026-07"): 0.5}

        first = prepare_mapping_data(
            _single_batch_df(), scaling_factor=1.0, monthly_factors=factors
        )
        second = prepare_mapping_data(
            _single_batch_df(), scaling_factor=1.0, monthly_factors=factors
        )

        assert _count(first, "CodeX") == 5
        pd.testing.assert_frame_equal(first, second)

    def test_upscale_duplicates_with_collision_safe_ids(self):
        factors = {("CodeX", "2026-07"): 2.0}

        result = prepare_mapping_data(
            _single_batch_df(), scaling_factor=1.0, monthly_factors=factors
        )

        assert _count(result, "CodeX") == 20
        code_rows = result[result["defect_desc"] == "CodeX"]
        assert code_rows["panel_id"].is_unique  # _SIM_M 后缀防碰撞

    def test_factor_applies_only_to_matching_batch_month(self):
        # 7 月批次命中因子 0.5；6 月批次无因子 → 不变
        rows = _panel_rows("2026/07/05-LOT1", "CodeX", 10)
        rows += _panel_rows("2026/06/05-LOT0", "CodeX", 8)
        rows += _panel_rows("2026/07/05-LOT1", None, 90, start=100)
        rows += _panel_rows("2026/06/05-LOT0", None, 90, start=200)
        df = pd.DataFrame(rows)
        factors = {("CodeX", "2026-07"): 0.5}

        result = prepare_mapping_data(df, scaling_factor=1.0, monthly_factors=factors)

        july = result[result["batch_no"] == "2026/07/05-LOT1"]
        assert _count(july, "CodeX") == 5
        june = result[result["batch_no"] == "2026/06/05-LOT0"]
        # 6 月批次未被月度因子缩放（级联衰减可能再降，但不会因为 0.5 因子恰好减半）
        assert _count(june, "CodeX") >= 5
