# tests/unit/test_yield_service_modifier_wiring.py
"""YieldAnalysisService 与入库良率修饰表的接线测试。"""
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.config_model import AppConfig
from src.yield_domain.application import yield_service as yield_service_module
from src.yield_domain.application.yield_service import YieldAnalysisService


def _config(paths=None) -> AppConfig:
    return AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "M999"},
            "paths": paths or {},
            "processing": {},
        }
    )


def _panel_details() -> pd.DataFrame:
    rows = []
    for day in ("20260701", "20260702"):
        for number in range(10):
            is_defect = day == "20260701" and number == 0
            rows.append(
                {
                    "warehousing_time": day,
                    "panel_id": f"{day}-P{number:02d}",
                    "defect_group": "Array_Pixel" if is_defect else None,
                    "defect_desc": "CodeA" if is_defect else None,
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def locked_window():
    """锁定分析时间窗口到 2026-07-31，并在测试后复位。"""
    YieldAnalysisService.set_analysis_end_date(datetime(2026, 7, 31))
    yield
    YieldAnalysisService._custom_end_date = None


class TestResolveModifierTablePath:
    def test_uses_configured_file_name(self, tmp_path):
        product_dir = tmp_path / "M999"
        config = _config({"yield_modifier_config": {"file_name": "custom.xlsx"}})

        path = YieldAnalysisService.resolve_modifier_table_path(config, product_dir)

        assert path == tmp_path / "custom.xlsx"

    def test_defaults_to_standard_file_name(self, tmp_path):
        path = YieldAnalysisService.resolve_modifier_table_path(
            _config(), tmp_path / "M999"
        )
        assert path.name == "入库良率修饰表.xlsx"


class TestBuildModifierContext:
    """cache miss 时同步修饰表并产出 targets / factors / signature。"""

    def test_context_contains_targets_factors_and_signature(
        self, tmp_path, locked_window
    ):
        config = _config()
        context = YieldAnalysisService._build_modifier_context(
            config, tmp_path / "M999", _panel_details()
        )

        assert set(context) >= {"targets", "group_targets", "factors", "signature"}
        # 表文件被同步创建，CodeA 当月良损 = 1/20
        table_path = tmp_path / "入库良率修饰表.xlsx"
        assert table_path.exists()
        code_df = pd.read_excel(table_path, sheet_name="M999_Code级")
        row = code_df[code_df["不良类型"] == "CodeA"].iloc[0]
        assert row["时间标签"] == "2026-07"
        assert row["当月良损"] == pytest.approx(1 / 20)
        # 未指定 → 目标回落原始良损，倍数 1.0
        assert context["targets"]["CodeA"]["2026-07"] == pytest.approx(1 / 20)
        assert context["group_targets"]["Array_Pixel"]["2026-07"] == pytest.approx(
            1 / 20
        )
        assert context["factors"][("CodeA", "2026-07")] == 1.0

    def test_specified_rate_flows_into_targets_and_factors(
        self, tmp_path, locked_window
    ):
        table_path = tmp_path / "入库良率修饰表.xlsx"
        df = pd.DataFrame(
            [
                {
                    "不良类型": "CodeA",
                    "周期类型": "月度",
                    "时间标签": "2026-07",
                    "当月良损": 0.05,
                    "指定良损": 0.10,
                    "缩放倍数": None,
                }
            ]
        )
        with pd.ExcelWriter(table_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="M999_Code级")

        context = YieldAnalysisService._build_modifier_context(
            _config(), tmp_path / "M999", _panel_details()
        )

        assert context["targets"]["CodeA"]["2026-07"] == pytest.approx(0.10)
        # 倍数 = 0.10 / 0.05 = 2.0
        assert context["factors"][("CodeA", "2026-07")] == pytest.approx(2.0)

    def test_missing_table_file_yields_empty_context(self, tmp_path, locked_window):
        # 不创建文件且不给 panel 数据缺陷 → 空表语义
        empty_panels = _panel_details().assign(defect_group=None, defect_desc=None)
        context = YieldAnalysisService._build_modifier_context(
            _config(), tmp_path / "M999", empty_panels
        )
        assert context["targets"] == {}
        assert context["group_targets"] == {}
        assert context["factors"] == {}


def test_modifier_context_is_shared_across_consumers(
    monkeypatch,
    tmp_path,
) -> None:
    """相同快照/修饰签名下，页面各消费者只触发一次上下文构建。"""
    panel_df = _panel_details()
    expected = {
        "targets": {},
        "group_targets": {},
        "factors": {},
        "signature": "stable",
    }
    calls = 0

    monkeypatch.setattr(
        YieldAnalysisService,
        "get_modified_panel_details",
        staticmethod(lambda *args, **kwargs: panel_df),
    )

    def build_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        return expected

    monkeypatch.setattr(
        YieldAnalysisService,
        "_build_modifier_context",
        staticmethod(build_once),
    )
    YieldAnalysisService.get_modifier_context.clear()

    first = YieldAnalysisService.get_modifier_context(
        _config(),
        tmp_path / "M999",
        snapshot_signature="same-panel",
        modifier_signature="same-modifier",
    )
    second = YieldAnalysisService.get_modifier_context(
        _config(),
        tmp_path / "M999",
        snapshot_signature="same-panel",
        modifier_signature="same-modifier",
    )
    third = YieldAnalysisService.get_modifier_context(
        _config(),
        tmp_path / "M999",
        snapshot_signature="same-panel",
        modifier_signature="changed-modifier",
    )

    assert calls == 2
    assert first == second == third == expected


def test_group_service_passes_code_daily_source_to_group_processor(
    monkeypatch,
    tmp_path,
) -> None:
    panel_df = _panel_details()
    code_results = {"daily_full": pd.DataFrame({"time_period": ["2026-07-01"]})}
    group_targets = {"Array_Pixel": {"2026-07": 0.1}}
    captured = {}

    monkeypatch.setattr(
        YieldAnalysisService,
        "get_modified_panel_details",
        staticmethod(lambda *args, **kwargs: panel_df),
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "get_code_level_trend_data",
        staticmethod(lambda *args, **kwargs: code_results),
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "get_modifier_context",
        staticmethod(
            lambda *args, **kwargs: {
                "group_targets": group_targets,
            }
        ),
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "get_time_window",
        classmethod(
            lambda cls: (datetime(2026, 5, 1), datetime(2026, 7, 31))
        ),
    )

    def fake_create_group(**kwargs):
        captured.update(kwargs)
        return {"daily_full": pd.DataFrame({"ok": [True]})}

    monkeypatch.setattr(
        yield_service_module.MWDTrendProcessor,
        "create_mwd_trend_data",
        staticmethod(fake_create_group),
    )
    YieldAnalysisService.get_mwd_trend_data.clear()

    result = YieldAnalysisService.get_mwd_trend_data(
        _config(),
        tmp_path / "M999",
        snapshot_signature="group-from-code-test",
        modifier_signature="group-from-code-test",
    )

    assert result is not None
    assert captured["mwd_code_data"] is code_results
    assert captured["modifier_targets"] == group_targets
