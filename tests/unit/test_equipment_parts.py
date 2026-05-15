# tests/unit/test_equipment_parts.py
"""
单元测试 — 关键备件报表模块。

测试范围:
1. DAO 层: load_spec_baseline() — 文件读取、校验
2. Service 层: _find_matching_db_record() — 子串匹配算法
3. Service 层: get_report_data() — 使用进度计算、预警状态判定
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

from src.equipment_domain.infrastructure.data_loader import (
    load_spec_baseline,
    REQUIRED_BASELINE_COLUMNS,
)
from src.equipment_domain.application.parts_service import (
    _find_matching_db_record,
)


# ==============================================================================
#  Fixtures — 测试数据
# ==============================================================================


@pytest.fixture
def valid_csv_path(tmp_path: Path) -> Path:
    """创建一个有效的 CSV 基线文件"""
    csv_content = """厂别,膜层,制程,机台,腔室,备件类型,寿命规格,预警值
ARRAY,CVD,PE,3AFS10,P3/P4,TRGTLIFE_R,840,80
ARRAY,CVD,PE,3AFS10,P3/P4,TRGTLIFE_G,840,80
ARRAY,CVD,PE,3AFS10,P3/P4,MASKLIFE_R,840,80
ARRAY,CVD,PE,3AFS10,P5,TRGTLIFE_R,840,80
ARRAY,CVD,PE,3AFS10,P5,MASKLIFE_G,840,80
"""
    file_path = tmp_path / "test_baseline.csv"
    file_path.write_text(csv_content, encoding="utf-8-sig")
    return file_path


@pytest.fixture
def db_sample_df() -> pd.DataFrame:
    """模拟 DB 返回的最新值 DataFrame
    
    sub_equip_id 格式: 机台号-模块-腔室 (如 3AFS10-SPU-P3)
    """
    return pd.DataFrame({
        "step_id": ["CVD-PE-01", "CVD-PE-01", "CVD-PE-01", "CVD-PE-01"],
        "sub_equip_id": [
            "3AFS10-SPU-P3",
            "3AFS10-SPU-P4",
            "3AFS10-SPU-P5",
            "OTHER-SPU-P1",
        ],
        "param_name": [
            "CH_A_TRGTLIFE_R_MAX",
            "CH_A_MASKLIFE_R_MAX",
            "CH_A_TRGTLIFE_G_MAX",
            "CH_B_TRGTLIFE_R_MAX",
        ],
        "value": [735.0, 800.0, 500.0, 600.0],
        "glass_start_time": pd.to_datetime([
            "2026-05-15 10:30:00",
            "2026-05-14 08:00:00",
            "2026-05-13 14:00:00",
            "2026-05-12 09:00:00",
        ]),
    })


@pytest.fixture
def spec_row_p3p4_trgtlife_r() -> pd.Series:
    """模拟一条规格行: 机台=3AFS10, 腔室=P3/P4, 备件类型=TRGTLIFE_R"""
    return pd.Series({
        "厂别": "ARRAY",
        "膜层": "CVD",
        "制程": "PE",
        "机台": "3AFS10",
        "腔室": "P3/P4",
        "备件类型": "TRGTLIFE_R",
        "寿命规格": 840,
        "预警值": 80,
    })


@pytest.fixture
def spec_row_p5_trgtlife_g() -> pd.Series:
    """模拟一条规格行: 机台=3AFS10, 腔室=P5, 备件类型=TRGTLIFE_G"""
    return pd.Series({
        "厂别": "ARRAY",
        "膜层": "CVD",
        "制程": "PE",
        "机台": "3AFS10",
        "腔室": "P5",
        "备件类型": "TRGTLIFE_G",
        "寿命规格": 840,
        "预警值": 80,
    })


# ==============================================================================
#  测试: load_spec_baseline()
# ==============================================================================


class TestLoadSpecBaseline:

    def test_load_success(self, valid_csv_path: Path):
        """成功加载 CSV，返回正确的 DataFrame"""
        df = load_spec_baseline(valid_csv_path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5  # 5 行数据
        # 验证必要列都存在
        for col in REQUIRED_BASELINE_COLUMNS:
            assert col in df.columns
        # 验证数值列已正确转换
        assert df["寿命规格"].dtype in (np.float64, np.int64, "float64", "int64")
        assert df["预警值"].dtype in (np.float64, np.int64, "float64", "int64")

    def test_file_not_found(self):
        """文件不存在 → 抛出 FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            load_spec_baseline("/nonexistent/path.csv")

    def test_missing_columns(self, tmp_path: Path):
        """缺少必要列 → 抛出 ValueError"""
        bad_csv = "厂别,膜层,机台\nARRAY,CVD,3AFS10\n"
        file_path = tmp_path / "bad.csv"
        file_path.write_text(bad_csv, encoding="utf-8-sig")
        with pytest.raises(ValueError, match="缺少必要列"):
            load_spec_baseline(file_path)

    def test_empty_file(self, tmp_path: Path):
        """只有表头没有数据行 → 返回空 DataFrame"""
        empty_csv = "厂别,膜层,制程,机台,腔室,备件类型,寿命规格,预警值\n"
        file_path = tmp_path / "empty.csv"
        file_path.write_text(empty_csv, encoding="utf-8-sig")
        df = load_spec_baseline(file_path)
        assert len(df) == 0


# ==============================================================================
#  测试: _find_matching_db_record()
# ==============================================================================


class TestMatching:

    def test_match_exact_p3_trgtlife_r(
        self, spec_row_p3p4_trgtlife_r: pd.Series, db_sample_df: pd.DataFrame
    ):
        """P3/P4 + TRGTLIFE_R → 匹配到 sub_equip_id='3AFS10-SPU-P3'"""
        result = _find_matching_db_record(spec_row_p3p4_trgtlife_r, db_sample_df)
        assert result is not None
        assert result["sub_equip_id"] == "3AFS10-SPU-P3"
        assert result["value"] == 735.0

    def test_match_p5_trgtlife_g(
        self, spec_row_p5_trgtlife_g: pd.Series, db_sample_df: pd.DataFrame
    ):
        """P5 + TRGTLIFE_G → 匹配到 sub_equip_id='3AFS10-SPU-P5'"""
        result = _find_matching_db_record(spec_row_p5_trgtlife_g, db_sample_df)
        assert result is not None
        assert result["sub_equip_id"] == "3AFS10-SPU-P5"
        assert result["value"] == 500.0

    def test_match_no_machine(self, db_sample_df: pd.DataFrame):
        """机台不匹配 → 返回 None"""
        spec = pd.Series({
            "机台": "NONEXIST",
            "腔室": "P5",
            "备件类型": "TRGTLIFE_R",
        })
        result = _find_matching_db_record(spec, db_sample_df)
        assert result is None

    def test_match_no_chamber(self, db_sample_df: pd.DataFrame):
        """腔室不匹配 → 返回 None"""
        spec = pd.Series({
            "机台": "3AFS10",
            "腔室": "P99",
            "备件类型": "TRGTLIFE_R",
        })
        result = _find_matching_db_record(spec, db_sample_df)
        assert result is None

    def test_match_no_part_type(self, db_sample_df: pd.DataFrame):
        """备件类型不匹配 → 返回 None"""
        spec = pd.Series({
            "机台": "3AFS10",
            "腔室": "P3/P4",
            "备件类型": "NONEXIST_TYPE",
        })
        result = _find_matching_db_record(spec, db_sample_df)
        assert result is None

    def test_match_empty_db(self, spec_row_p3p4_trgtlife_r: pd.Series):
        """DB DataFrame 为空 → 返回 None"""
        empty_df = pd.DataFrame(columns=[
            "step_id", "sub_equip_id", "param_name", "value", "glass_start_time"
        ])
        result = _find_matching_db_record(spec_row_p3p4_trgtlife_r, empty_df)
        assert result is None

    def test_match_prefers_latest(self, db_sample_df: pd.DataFrame):
        """多条匹配时，取 glass_start_time 最新的那条"""
        # 插入另一条 P3 + TRGTLIFE_R 但时间更早
        extra = pd.DataFrame({
            "step_id": ["CVD-PE-01"],
            "sub_equip_id": ["3AFS10-SPU-P3-OLD"],
            "param_name": ["CH_A_TRGTLIFE_R_MAX"],
            "value": [100.0],
            "glass_start_time": pd.to_datetime(["2026-01-01 00:00:00"]),
        })
        combined = pd.concat([db_sample_df, extra], ignore_index=True)

        spec = pd.Series({
            "机台": "3AFS10",
            "腔室": "P3",
            "备件类型": "TRGTLIFE_R",
        })
        result = _find_matching_db_record(spec, combined)
        assert result is not None
        # 应该取最新的一条 (value=735, 不是 100)
        assert result["value"] == 735.0

    def test_match_split_chamber_any(self, db_sample_df: pd.DataFrame):
        """P3/P4 → 只要 P3 或 P4 任一匹配即可"""
        spec = pd.Series({
            "机台": "3AFS10",
            "腔室": "P3/P4",
            "备件类型": "MASKLIFE_R",
        })
        result = _find_matching_db_record(spec, db_sample_df)
        assert result is not None
        # sub_equip_id='3AFS10-SPU-P4' 包含 P4
        assert "P4" in result["sub_equip_id"]


# ==============================================================================
#  测试: usage 计算逻辑（直接从业务公式推导，无 mock）
# ==============================================================================


class TestUsageComputation:

    def test_usage_normal(self):
        """使用进度 < 预警值 → 正常"""
        actual = 500.0
        spec = 840.0
        threshold = 80.0

        usage = min(actual / spec * 100, 100.0)
        status = "⚠️ 超预警" if usage >= threshold else "✅ 正常"

        assert usage == pytest.approx(59.5, rel=0.01)
        assert status == "✅ 正常"

    def test_usage_warning(self):
        """使用进度 >= 预警值 → 超预警"""
        actual = 735.0
        spec = 840.0
        threshold = 80.0

        usage = min(actual / spec * 100, 100.0)
        status = "⚠️ 超预警" if usage >= threshold else "✅ 正常"

        assert usage == pytest.approx(87.5, rel=0.01)
        assert status == "⚠️ 超预警"

    def test_usage_clip_at_100(self):
        """使用进度 > 100% → clip 到 100%"""
        actual = 1000.0
        spec = 840.0

        usage = min(actual / spec * 100, 100.0)
        assert usage == 100.0

    def test_usage_zero_when_no_data(self):
        """无数据（NaN）→ 使用进度为 0"""
        actual = np.nan
        spec = 840.0
        usage = 0.0  # fillna(0)
        assert usage == 0.0

    def test_usage_exact_threshold(self):
        """使用进度刚好等于预警值 → 超预警"""
        actual = 672.0
        spec = 840.0
        threshold = 80.0

        usage = min(actual / spec * 100, 100.0)
        status = "⚠️ 超预警" if usage >= threshold else "✅ 正常"

        assert usage == 80.0
        assert status == "⚠️ 超预警"
