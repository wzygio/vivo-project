"""
单元测试 - 关键备件报表模块。

覆盖范围:
1. 规格基线 CSV 加载
2. 规格行与快照数据匹配
3. 使用进度、预警状态、超规数据修饰
"""

from pathlib import Path
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from src.equipment_domain.core.parts_calculator import (
    OVER_SPEC_COLUMN,
    PartsAlertPolicy,
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
from src.equipment_domain.core.parts_identity import build_fabricated_param_name
from src.equipment_domain.infrastructure.data_loader import (
    REQUIRED_BASELINE_COLUMNS,
    _expand_baseline_rows_from_sheets,
    _generate_baseline_csv_from_excel,
    filter_recent_part_life_measurements,
    load_spec_baseline,
)
from src.equipment_domain.infrastructure import data_loader
from src.shared_kernel.config import ConfigLoader


def _alert_policy() -> PartsAlertPolicy:
    """Load the same alert policy used by the production application."""
    from src.equipment_domain.config import get_equipment_runtime_config

    return get_equipment_runtime_config().alert_policy


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

    def test_file_not_found_without_excel_fallback(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """原始 Excel 也不存在时，加载缺失 CSV 会抛出 FileNotFoundError。"""
        from src.equipment_domain.config import get_equipment_runtime_config

        missing_source_config = replace(
            get_equipment_runtime_config(),
            source_excel_path=tmp_path / "missing-source.xlsx",
        )
        monkeypatch.setattr(
            data_loader,
            "get_equipment_runtime_config",
            lambda: missing_source_config,
        )
        with pytest.raises(FileNotFoundError):
            load_spec_baseline(tmp_path / "missing-baseline.csv")

    def test_missing_columns(self, tmp_path: Path) -> None:
        """缺少必要列时抛出 ValueError。"""
        bad_csv = "厂别,膜层,站点\nArray,MO,1K200\n"
        file_path = tmp_path / "bad.csv"
        file_path.write_text(bad_csv, encoding="utf-8-sig")

        with pytest.raises(ValueError, match="Missing columns"):
            load_spec_baseline(file_path)

    def test_encrypted_csv_is_normalized_before_retrying_read(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        encrypted_path = tmp_path / "critical_parts_baseline.csv"
        encrypted_path.write_bytes(b"\x00\x00\x00\x00encrypted")
        expected = pd.DataFrame({
            "厂别": ["Array"],
            "备件类型": ["Target"],
            "设备类型": ["PVD"],
            "膜层": ["MO"],
            "制程": ["DEPO"],
            "寿命规格": ["100KWH"],
            "站点": ["S1"],
            "机台号-腔室": ["EQ1-PM3"],
            "参数名称": ["%TRGTLIFE%_G_MAX"],
        })
        decode_error = UnicodeDecodeError("utf-8", b"\xb5", 0, 1, "invalid")
        read_results = iter([decode_error, expected])
        normalized: list[Path] = []

        def fake_read_csv(*_args, **_kwargs):
            result = next(read_results)
            if isinstance(result, Exception):
                raise result
            return result.copy()

        monkeypatch.setattr(data_loader.pd, "read_csv", fake_read_csv)
        monkeypatch.setattr(
            data_loader,
            "_normalize_encrypted_baseline_csv",
            lambda path, **_kwargs: normalized.append(path),
        )

        result = load_spec_baseline(encrypted_path)

        assert normalized == [encrypted_path]
        assert result["寿命规格"].tolist() == [100.0]

    def test_expands_and_merges_rows_from_multiple_configured_sheets(self) -> None:
        """多个规格 Sheet 的行会展开并合并为同一份基线表。"""
        common_prefix = ["Array", "Target", "PVD", "MO", "Mo DEPO", "41000KWH"]
        rows = _expand_baseline_rows_from_sheets(
            {
                "Array规格表1": [
                    common_prefix + ["1K200\n1K201", "PM1", "%LIFE_A%"],
                ],
                "Array规格表2": [
                    common_prefix + ["1K300", "PM2\nPM3", "%LIFE_B%"],
                ],
            }
        )

        assert len(rows) == 4
        assert {row["站点"] for row in rows} == {"1K200", "1K201", "1K300"}
        assert {row["机台号-腔室"] for row in rows} == {"PM1", "PM2", "PM3"}

    def test_generate_baseline_csv_merges_all_configured_sheets(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CSV 生成入口会读取配置中的每个 Sheet 并写入同一份规格表。"""
        from src.equipment_domain.config import get_equipment_runtime_config

        source_excel = tmp_path / "source.xlsx"
        source_excel.touch()
        runtime_config = replace(
            get_equipment_runtime_config(),
            source_excel_path=source_excel,
            source_sheet_names=("规格表A", "规格表B"),
        )
        monkeypatch.setattr(
            data_loader,
            "get_equipment_runtime_config",
            lambda: runtime_config,
        )
        def fake_sheet_reader(excel_path: Path, sheet_names: tuple[str, ...]) -> dict[str, list[list[str]]]:
            assert excel_path == source_excel
            assert sheet_names == ("规格表A", "规格表B")
            return {
                "规格表A": [["Array", "Target", "PVD", "MO", "DEPO", "100", "S1", "M1", "%A%"]],
                "规格表B": [["Array", "Mask", "PVD", "MO", "DEPO", "200", "S2", "M2", "%B%"]],
            }

        monkeypatch.setattr(data_loader, "_read_baseline_sheets_from_excel", fake_sheet_reader)
        output_path = tmp_path / "critical_parts_baseline.csv"

        _generate_baseline_csv_from_excel(output_path)

        result = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig")
        assert result["参数名称"].tolist() == ["%A%", "%B%"]

    def test_equipment_config_is_loaded_from_project_config(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """关键备件运行参数统一从 equipment_config.yaml 读取。"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "equipment_config.yaml").write_text(
            """
equipment:
  baseline:
    source_excel_path: resources/source.xlsx
    source_sheet_names: [规格表A, 规格表B]
  snapshot:
    directory: data/custom-equipment
    ttl_hours: 12
  query:
    lookback_days: 30
    source_table: eda.ARRAY_PDS_RESULT_T
  alert:
    warning_threshold: 85
    over_threshold: 110
    decoration_growth_ratio: 1.02
    decoration_min_ratio: 0.88
    decoration_max_ratio: 0.94
    display_progress_max_ratio: 0.95
  fabrication:
    random_seed: 7
    initial_value_ratio_range: [0.0, 1.0]
    initial_lookback_days: 2
    update_increment_ratio: 0.3
    reset_ratio_range: [0.0, 0.3]
    snapshot_ttl_hours: 24
""".strip(),
            encoding="utf-8",
        )
        monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

        config = ConfigLoader.get_equipment_config()

        assert config["baseline"]["source_sheet_names"] == ["规格表A", "规格表B"]
        assert config["snapshot"]["ttl_hours"] == 12
        assert config["alert"]["warning_threshold"] == 85.0
        from src.equipment_domain.config import get_equipment_runtime_config

        runtime = get_equipment_runtime_config()
        assert runtime.fabrication_policy.snapshot_ttl_hours == 24
        assert runtime.fabrication_policy.update_increment_ratio == 0.3


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

    @pytest.mark.parametrize("blank_param", ["", " ", None, np.nan, pd.NA])
    def test_blank_parameter_matches_only_its_internal_fabricated_key(
        self,
        blank_param: object,
    ) -> None:
        """空参数规格只精确命中自己的内部合成键。"""
        spec_row = pd.Series({
            "厂别": "TP",
            "备件类型": "陶瓷",
            "设备类型": "ETCH",
            "膜层": "ITO",
            "制程": "ETCH",
            "寿命规格": 400.0,
            "站点": "S4",
            "机台号-腔室": "EQ4-PM6",
            "参数名称": blank_param,
        })
        synthetic_key = build_fabricated_param_name(spec_row)
        snapshot = pd.DataFrame({
            "step_id": ["S4", "S4"],
            "sub_equip_id": ["EQ4-PM6", "EQ4-PM6"],
            "param_name": ["UNRELATED_REAL_PARAM", synthetic_key],
            "value": [9999.0, 250.0],
            "glass_start_time": pd.to_datetime([
                "2026-07-15 09:00:00",
                "2026-07-15 08:30:00",
            ]),
        })

        matched = find_matching_db_record(spec_row, snapshot)

        assert matched is not None
        assert matched["param_name"] == synthetic_key
        assert matched["value"] == 250.0

    def test_blank_parameter_does_not_fallback_to_station_machine_match(self) -> None:
        """真实快照没有内部键时，空参数规格不能任意取同机台记录。"""
        spec_row = pd.Series({
            "厂别": "TP",
            "备件类型": "陶瓷",
            "设备类型": "ETCH",
            "膜层": "ITO",
            "制程": "ETCH",
            "寿命规格": 400.0,
            "站点": "S4",
            "机台号-腔室": "EQ4-PM6",
            "参数名称": "",
        })
        snapshot = pd.DataFrame({
            "step_id": ["S4"],
            "sub_equip_id": ["EQ4-PM6"],
            "param_name": ["REAL_DATABASE_PARAM"],
            "value": [9999.0],
            "glass_start_time": pd.to_datetime(["2026-07-15 09:00:00"]),
        })

        assert find_matching_db_record(spec_row, snapshot) is None

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

    def test_batch_match_prefers_real_records_and_uses_fabricated_only_for_gaps(
        self,
        spec_df: pd.DataFrame,
    ) -> None:
        """真实记录优先；仅真实缺失的规格才使用仿造记录。"""
        real_snapshot = pd.DataFrame({
            "step_id": ["1K200"],
            "sub_equip_id": ["3AFS01-SPU-PM5"],
            "param_name": ["P5_TRGTLIFE_G_MAX"],
            "value": [111.0],
            "glass_start_time": pd.to_datetime(["2026-07-20 08:00:00"]),
        })
        fabricated_snapshot = pd.DataFrame({
            "step_id": ["1K200", "1K200"],
            "sub_equip_id": ["3AFS01-SPU-PM5", "3AFS01-SPU-PM5"],
            "param_name": ["P5_TRGTLIFE_G_MAX", "P5_MASKLIFE_G_MAX"],
            "value": [999.0, 222.0],
            "glass_start_time": pd.to_datetime([
                "2026-07-21 08:00:00",
                "2026-07-21 09:00:00",
            ]),
        })

        result = build_and_match_all(
            spec_df,
            real_snapshot,
            fallback_snapshot_df=fabricated_snapshot,
        )

        assert result["测量值"].tolist()[:2] == [111.0, 222.0]
        assert pd.isna(result.loc[2, "测量值"])

    def test_stale_real_measurements_are_excluded_before_fabricated_fallback(self) -> None:
        snapshot = pd.DataFrame({
            "value": [10.0, 20.0, 30.0, 40.0],
            "glass_start_time": pd.to_datetime([
                "2026-08-09 11:59:59",
                "2026-08-09 12:00:00",
                "2026-08-12 12:00:00",
                "2026-08-12 12:00:01",
            ]),
        })

        recent = filter_recent_part_life_measurements(
            snapshot,
            as_of=pd.Timestamp("2026-08-12 12:00:00"),
            max_age_days=3,
        )

        assert recent["value"].tolist() == [20.0, 30.0]


class TestPartsCalculator:
    """测试进度、状态、修饰逻辑。"""

    def test_usage_status_thresholds(self) -> None:
        """使用进度按 90/100 阈值判定正常、预警、超规。"""
        assert calculate_usage_progress(80.0, 100.0) == 80.0
        policy = _alert_policy()
        assert calculate_warning_status(80.0, policy) == STATUS_NORMAL
        assert calculate_warning_status(95.0, policy) == STATUS_WARNING
        assert calculate_warning_status(101.0, policy) == STATUS_OVER

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
            policy=_alert_policy(),
            spec_ratio=0.93,
        )

        assert decorated == pytest.approx(94.0 * 1.01)

    def test_decorate_over_spec_value_without_previous_uses_spec_range(self) -> None:
        """没有上一个值时，超规值落在规格线 90%-95% 区间。"""
        decorated = calculate_decorated_over_spec_value(
            actual_value=150.0,
            spec_limit=100.0,
            previous_value=None,
            policy=_alert_policy(),
            spec_ratio=0.99,
        )

        policy = _alert_policy()
        assert decorated == pytest.approx(policy.decoration_max_ratio * 100.0)
        assert policy.decoration_min_ratio * 100.0 <= decorated <= policy.decoration_max_ratio * 100.0

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
            policy=_alert_policy(),
        )
        calculated = batch_calculate_progress_and_status(decorated, policy=_alert_policy())

        assert decorated[OVER_SPEC_COLUMN].tolist() == [False, True]
        assert decorated.loc[1, RAW_VALUE_COLUMN] == 105.0
        assert decorated.loc[1, "测量值"] == pytest.approx(
            95.0 * _alert_policy().decoration_growth_ratio
        )
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
            policy=_alert_policy(),
        )
        calculated = batch_calculate_progress_and_status(decorated, policy=_alert_policy())

        assert calculated["使用进度"].max() == pytest.approx(
            _alert_policy().display_progress_max_ratio * 100.0
        )
        assert calculated["使用进度"].max() <= 96.0
        rendered_progress = [round(progress) for progress in calculated["使用进度"]]
        assert max(rendered_progress) <= 96
        assert calculated["预警状态"].tolist() == [STATUS_WARNING, STATUS_WARNING]
