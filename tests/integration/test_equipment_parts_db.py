# tests/integration/test_equipment_parts_db.py
"""
集成测试 — 关键备件报表数据库层。

测试范围:
1. SQL 查询正确执行，返回预期列
2. ROW_NUMBER 去重逻辑生效（每个 sub_equip_id 至多一条记录）
3. 完整 DAO → Service 管道
"""

import pytest
import pandas as pd
from pathlib import Path

from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.equipment_domain.infrastructure.data_loader import (
    load_latest_part_life,
    load_spec_baseline,
)
from src.equipment_domain.application.parts_service import (
    _find_matching_db_record,
)


# ==============================================================================
#  Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def db_manager():
    """全局数据库管理器实例（模块级复用）"""
    return DatabaseManager()


@pytest.fixture
def baseline_path() -> Path:
    """指向项目实际的 CSV 基线文件"""
    return Path("resources/critical_parts_baseline.csv")


# ==============================================================================
#  测试: load_latest_part_life()
# ==============================================================================


class TestLoadLatestPartLife:

    def test_db_query_returns_expected_columns(self, db_manager):
        """
        SQL 执行成功，返回包含预期列的非空 DataFrame。
        """
        df = load_latest_part_life(db_manager)

        # 是否返回了 DataFrame
        assert isinstance(df, pd.DataFrame)

        # 如果数据库无数据（测试环境可能没有这张表），跳过不报错
        if df.empty:
            pytest.skip("数据库返回空结果（可能是测试环境无数据）")

        # 验证预期列存在
        expected_columns = {
            "step_id", "sub_equip_id", "param_name", "value", "glass_start_time"
        }
        actual_columns = set(df.columns)
        missing = expected_columns - actual_columns
        assert not missing, f"缺少列: {missing}"

    def test_db_row_number_unique_per_sub_equip(self, db_manager):
        """
        验证 ROW_NUMBER 窗口函数生效：
        每个 sub_equip_id 在结果中最多出现一次。
        """
        df = load_latest_part_life(db_manager)

        if df.empty:
            pytest.skip("数据库返回空结果")

        # 检查 sub_equip_id 是否有重复
        duplicates = df["sub_equip_id"].duplicated().sum()
        assert duplicates == 0, (
            f"ROW_NUMBER 窗口函数未正确去重: "
            f"发现 {duplicates} 个重复 sub_equip_id"
        )

    def test_db_value_is_numeric(self, db_manager):
        """
        验证 value 列已正确转换为数值类型。
        """
        df = load_latest_part_life(db_manager)

        if df.empty:
            pytest.skip("数据库返回空结果")

        # value 列应为数值类型
        assert df["value"].dtype in (
            "float64", "int64", "Float64", "Int64"
        ), f"value 列不是数值类型: {df['value'].dtype}"

    def test_db_glass_start_time_is_datetime(self, db_manager):
        """
        验证 glass_start_time 列已正确转换为 datetime。
        """
        df = load_latest_part_life(db_manager)

        if df.empty:
            pytest.skip("数据库返回空结果")

        assert pd.api.types.is_datetime64_any_dtype(
            df["glass_start_time"]
        ), "glass_start_time 不是 datetime 类型"


# ==============================================================================
#  测试: 完整 DAO 管道
# ==============================================================================


class TestPartsPipeline:

    def test_spec_baseline_loaded(self, baseline_path: Path):
        """
        验证 CSV 基线文件存在且可正常加载。
        """
        assert baseline_path.exists(), (
            f"基线 CSV 文件不存在: {baseline_path.resolve()}"
        )
        df = load_spec_baseline(baseline_path)
        assert len(df) > 0, "基线 CSV 为空"
        # 验证关键数据
        assert "TRGTLIFE_R" in df["备件类型"].values
        assert "MASKLIFE_R" in df["备件类型"].values
        assert 840 in df["寿命规格"].values

    def test_full_pipeline_run(self, db_manager, baseline_path: Path):
        """
        完整管道执行：加载 CSV → 查询 DB → 匹配 → 计算。
        不验证具体数值，只验证执行不抛异常且返回合理结构。
        """
        # 1. 加载基线
        spec_df = load_spec_baseline(baseline_path)
        assert len(spec_df) > 0

        # 2. 查询 DB
        latest_df = load_latest_part_life(db_manager)

        # 3. 执行匹配（不报异常）
        match_count = 0
        for _, spec_row in spec_df.iterrows():
            matched = _find_matching_db_record(spec_row, latest_df)
            if matched is not None:
                match_count += 1

        # 只要执行不抛异常就算通过
        assert isinstance(match_count, int)
        assert match_count >= 0
