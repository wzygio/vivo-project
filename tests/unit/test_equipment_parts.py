"""
单元测试 - 关键备件报表模块。

覆盖范围:
1. 规格基线 CSV 加载
2. 规格行与快照数据匹配
3. 使用进度、预警状态、超规数据修饰
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.equipment_domain.core.parts_calculator import (
    DECORATION_MAX_RATIO,
    DECORATION_MIN_RATIO,
    DISPLAY_PROGRESS_MAX_RATIO,
    OVER_SPEC_COLUMN,
    RAW_VALUE_COLUMN,
    STATUS_NORMAL,
    STATUS_OVER,
    STATUS_WARNING,
    apply_over_spec_alert_and_decoration,
    batch_calculate_progress_and_status,
    calculate_decorated_over_spec_value,
    calculate_usage_progress,
    calculate_warning_status,
    is_over_spec,
)
from src.equipment_domain.core.parts_matcher import (
    build_and_match_all,
    find_matching_db_record,
)
from src.equipment_domain.infrastructure.data_loader import (
    REQUIRED_BASELINE_COLUMNS,
    load_spec_baseline,
)


@pytest.fixture
def valid_csv_path(tmp_path: Path) -> Path:
    """创建一个有效的 CSV 基线文件。"""
    csv_content = """厂别,备件类型,设备类型,膜层,制程,寿命规格,站点,机台号-腔室,参数名称
Array,Target,PVD,MO,Mo DEPO,41000KWH,1K200,3AFS01-SPU-PM5,%TRGTLIFE%_G_MAX
Array,Mask,PVD,MO,Mo DEPO,21000KWH,1K200,3AFS01-SPU-PM5,%MASKLIFE%_G_MAX
Array,Target,PVD,ITO,ITO DEPO,840HR,12200,3AFS10-SPU-PM3,%TARGET_KWH%
"""
    file_path = tmp_path / "critical_parts_baseline.csv"
    file_path.write_text(csv_content, encoding="utf-8-sig")
    return file_path


@pytest.fixture
def spec_df() -> pd.DataFrame:
    """当前关键备件规格表结构。"""
    return pd.DataFrame({
        "厂别": ["Array", "Array", "Array"],
        "备件类型": ["Target", "Mask", "Target"],
        "设备类型": ["PVD", "PVD", "PVD"],
        "膜层": ["MO", "MO", "ITO"],
        "制程": ["Mo DEPO", "Mo DEPO", "ITO DEPO"],
        "寿命规格": [41000.0, 21000.0, 840.0],
        "站点": ["1K200", "1K200", "12200"],
        "机台号-腔室": ["3AFS01-SPU-PM5", "3AFS01-SPU-PM5", "3AFS10-SPU-PM3"],
        "参数名称": ["%TRGTLIFE%_G_MAX", "%MASKLIFE%_G_MAX", "%TARGET_KWH%"],
    })


@pytest.fixture
def snapshot_df() -> pd.DataFrame:
    """模拟 DB/Parquet 快照返回的备件寿命记录。"""
    return pd.DataFrame({
        "step_id": ["1K200", "1K200", "1K200", "12200", "12200"],
        "sub_equip_id": [
            "3AFS01-SPU-PM5",
            "3AFS01-SPU-PM5",
            "3AFS01-SPU-PM5",
            "3AFS10-SPU-PM3",
            "3AFS10-SPU-PM3",
        ],
        "param_name": [
            "CH_A_TRGTLIFE_X_G_MAX",
            "CH_A_TRGTLIFE_X_G_MAX",
            "CH_A_MASKLIFE_X_G_MAX",
            "A_TARGET_KWH_TOTAL",
            "A_OTHER_PARAM",
        ],
        "value": [30000.0, 32000.0, 22000.0, 900.0, 100.0],
        "glass_start_time": pd.to_datetime([
            "2026-05-10 10:30:00",
            "2026-05-12 08:00:00",
            "2026-05-11 14:00:00",
            "2026-05-13 09:00:00",
            "2026-05-13 10:00:00",
        ]),
    })


class TestLoadSpecBaseline:
    """测试规格基线加载。"""

    def test_load_success(self, valid_csv_path: Path) -> None:
        """成功加载 CSV，必要列齐全且寿命规格会转为数值。"""
        df = load_spec_baseline(valid_csv_path)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        for col in REQUIRED_BASELINE_COLUMNS:
            assert col in df.columns
        assert df["寿命规格"].tolist() == [41000.0, 21000.0, 840.0]

    def test_file_not_found_without_excel_fallback(self) -> None:
        """原始 Excel 也不存在时，加载缺失 CSV 会抛出 FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            load_spec_baseline("/nonexistent/path.csv")

    def test_missing_columns(self, tmp_path: Path) -> None:
        """缺少必要列时抛出 ValueError。"""
        bad_csv = "厂别,膜层,站点\nArray,MO,1K200\n"
        file_path = tmp_path / "bad.csv"
        file_path.write_text(bad_csv, encoding="utf-8-sig")

        with pytest.raises(ValueError, match="Missing columns"):
            load_spec_baseline(file_path)


class TestPartsMatcher:
    """测试规格与快照记录匹配。"""

    def test_match_exact_pair_and_like_pattern(
        self,
        spec_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
    ) -> None:
        """站点、机台号-腔室、参数 LIKE 全匹配时返回最新记录。"""
        result = find_matching_db_record(spec_df.iloc[0], snapshot_df)

        assert result is not None
        assert result["value"] == 32000.0
        assert result["glass_start_time"] == pd.Timestamp("2026-05-12 08:00:00")

    def test_match_returns_none_when_station_mismatch(
        self,
        spec_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
    ) -> None:
        """站点不匹配时返回 None。"""
        spec_row = spec_df.iloc[0].copy()
        spec_row["站点"] = "NOT_EXISTS"

        assert find_matching_db_record(spec_row, snapshot_df) is None

    def test_build_and_match_all_keeps_spec_rows(
        self,
        spec_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
    ) -> None:
        """批量匹配会保留所有规格行并补齐测量字段。"""
        result = build_and_match_all(spec_df, snapshot_df)

        assert len(result) == 3
        assert result.loc[0, "测量值"] == 32000.0
        assert result.loc[1, "测量值"] == 22000.0
        assert result.loc[2, "测量值"] == 900.0


class TestPartsCalculator:
    """测试进度、状态、修饰逻辑。"""

    def test_usage_status_thresholds(self) -> None:
        """使用进度按 90/100 阈值判定正常、预警、超规。"""
        assert calculate_usage_progress(80.0, 100.0) == 80.0
        assert calculate_warning_status(80.0) == STATUS_NORMAL
        assert calculate_warning_status(95.0) == STATUS_WARNING
        assert calculate_warning_status(101.0) == STATUS_OVER

    def test_is_over_spec_uses_raw_value(self) -> None:
        """超规预警器直接比较原始测量值和规格线。"""
        assert is_over_spec(101.0, 100.0) is True
        assert is_over_spec(100.0, 100.0) is False
        assert is_over_spec(np.nan, 100.0) is False
        assert is_over_spec(101.0, 0.0) is False

    def test_decorate_over_spec_value_uses_previous_and_spec_range(self) -> None:
        """超规值修饰为 max(上一个值*1.01, [0.9, 0.95]*规格线)。"""
        decorated = calculate_decorated_over_spec_value(
            actual_value=150.0,
            spec_limit=100.0,
            previous_value=94.0,
            spec_ratio=0.93,
        )

        assert decorated == pytest.approx(94.0 * 1.01)

    def test_decorate_over_spec_value_without_previous_uses_spec_range(self) -> None:
        """没有上一个值时，超规值落在规格线 90%-95% 区间。"""
        decorated = calculate_decorated_over_spec_value(
            actual_value=150.0,
            spec_limit=100.0,
            previous_value=None,
            spec_ratio=0.99,
        )

        assert decorated == pytest.approx(DECORATION_MAX_RATIO * 100.0)
        assert DECORATION_MIN_RATIO * 100.0 <= decorated <= DECORATION_MAX_RATIO * 100.0

    def test_frontend_status_uses_decorated_measurement_source(self) -> None:
        """前端状态、进度、测量值必须来自同一份修饰后数据。"""
        df = pd.DataFrame({
            "厂别": ["Array", "Array"],
            "备件类型": ["Target", "Target"],
            "设备类型": ["PVD", "PVD"],
            "膜层": ["MO", "MO"],
            "制程": ["Mo DEPO", "Mo DEPO"],
            "寿命规格": [100.0, 100.0],
            "测量值": [95.0, 105.0],
        })

        decorated = apply_over_spec_alert_and_decoration(
            df,
            group_cols=["厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格"],
        )
        calculated = batch_calculate_progress_and_status(decorated)

        assert decorated[OVER_SPEC_COLUMN].tolist() == [False, True]
        assert decorated.loc[1, RAW_VALUE_COLUMN] == 105.0
        assert decorated.loc[1, "测量值"] == pytest.approx(95.0 * 1.01)
        assert calculated.loc[1, "使用进度"] == pytest.approx(95.95)
        assert calculated.loc[1, "预警状态"] == STATUS_WARNING

    def test_frontend_progress_never_exceeds_96_percent(self) -> None:
        """接近规格线或超规修饰后的进度不能超过 96%。"""
        df = pd.DataFrame({
            "厂别": ["Array", "Array"],
            "备件类型": ["Mask", "Mask"],
            "设备类型": ["PVD", "PVD"],
            "膜层": ["TI", "TI"],
            "制程": ["M3 DEPO", "M3 DEPO"],
            "寿命规格": [100.0, 100.0],
            "测量值": [99.8, 150.0],
        })

        decorated = apply_over_spec_alert_and_decoration(
            df,
            group_cols=["厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格"],
        )
        calculated = batch_calculate_progress_and_status(decorated)

        assert calculated["使用进度"].max() == pytest.approx(
            DISPLAY_PROGRESS_MAX_RATIO * 100.0
        )
        assert calculated["使用进度"].max() <= 96.0
        rendered_progress = [round(progress) for progress in calculated["使用进度"]]
        assert max(rendered_progress) <= 96
        assert calculated["预警状态"].tolist() == [STATUS_WARNING, STATUS_WARNING]
