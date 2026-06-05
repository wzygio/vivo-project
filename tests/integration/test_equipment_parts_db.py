"""
集成测试 - 关键备件报表数据库/快照链路。

测试范围:
1. 规格基线文件可加载
2. DB/Parquet 快照链路返回稳定结构
3. 规格匹配、超规修饰、进度状态计算完整执行
"""

from pathlib import Path

import pandas as pd
import pytest

from src.equipment_domain.core.parts_calculator import (
    apply_over_spec_alert_and_decoration,
    batch_calculate_progress_and_status,
)
from src.equipment_domain.core.parts_matcher import (
    build_and_match_all,
    find_matching_db_record,
)
from src.equipment_domain.infrastructure.data_loader import (
    load_part_life_snapshot,
    load_spec_baseline,
)
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


@pytest.fixture(scope="module")
def db_manager() -> DatabaseManager:
    """全局数据库管理器实例（模块级复用）。"""
    return DatabaseManager()


@pytest.fixture
def baseline_path() -> Path:
    """指向项目实际的 CSV 基线文件。"""
    return Path("resources/critical_parts_baseline.csv")


@pytest.fixture
def spec_df(baseline_path: Path) -> pd.DataFrame:
    """加载实际规格基线。"""
    return load_spec_baseline(baseline_path)


class TestSpecBaseline:
    """测试实际规格基线。"""

    def test_spec_baseline_loaded(
        self,
        baseline_path: Path,
        spec_df: pd.DataFrame,
    ) -> None:
        """CSV 基线存在、非空且包含当前关键字段。"""
        assert baseline_path.exists(), f"基线 CSV 不存在: {baseline_path.resolve()}"
        assert len(spec_df) > 0
        assert {"Target", "Mask"}.issubset(set(spec_df["备件类型"].dropna()))
        assert (spec_df["寿命规格"].dropna() > 0).all()


class TestPartLifeSnapshot:
    """测试 DB/Parquet 快照链路。"""

    def test_snapshot_returns_expected_columns(
        self,
        db_manager: DatabaseManager,
        spec_df: pd.DataFrame,
    ) -> None:
        """快照链路执行成功，并在有数据时返回预期列。"""
        df = load_part_life_snapshot(db_manager, spec_df)

        assert isinstance(df, pd.DataFrame)
        if df.empty:
            pytest.skip("数据库/快照返回空结果")

        expected_columns = {
            "step_id", "sub_equip_id", "param_name", "value", "glass_start_time",
        }
        assert expected_columns.issubset(set(df.columns))

    def test_snapshot_value_and_time_types(
        self,
        db_manager: DatabaseManager,
        spec_df: pd.DataFrame,
    ) -> None:
        """有数据时，value 为数值，glass_start_time 为时间类型。"""
        df = load_part_life_snapshot(db_manager, spec_df)

        if df.empty:
            pytest.skip("数据库/快照返回空结果")

        assert pd.api.types.is_numeric_dtype(df["value"])
        assert pd.api.types.is_datetime64_any_dtype(df["glass_start_time"])


class TestPartsPipeline:
    """测试完整匹配与计算管道。"""

    def test_find_matching_record_does_not_raise(
        self,
        db_manager: DatabaseManager,
        spec_df: pd.DataFrame,
    ) -> None:
        """规格行逐条匹配快照数据时不抛异常。"""
        snapshot_df = load_part_life_snapshot(db_manager, spec_df)
        if snapshot_df.empty:
            pytest.skip("数据库/快照返回空结果")

        match_count = 0
        for _, spec_row in spec_df.head(20).iterrows():
            matched = find_matching_db_record(spec_row, snapshot_df)
            if matched is not None:
                match_count += 1

        assert isinstance(match_count, int)
        assert match_count >= 0

    def test_full_pipeline_run(
        self,
        db_manager: DatabaseManager,
        spec_df: pd.DataFrame,
    ) -> None:
        """规格加载 -> 快照 -> 匹配 -> 修饰 -> 计算完整执行。"""
        snapshot_df = load_part_life_snapshot(db_manager, spec_df)
        report_df = build_and_match_all(spec_df, snapshot_df)
        report_df = apply_over_spec_alert_and_decoration(
            report_df,
            group_cols=["厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格"],
        )
        report_df = batch_calculate_progress_and_status(report_df)

        assert len(report_df) == len(spec_df)
        assert {"测量值", "使用进度", "预警状态", "是否超规"}.issubset(
            set(report_df.columns)
        )
