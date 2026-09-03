# tests/unit/test_yield_service_readonly.py
"""YieldAnalysisService read_only 开关穿透测试（矩阵只读消费，PRD §3.1-4）。

get_mwd_trend_data / get_code_level_trend_data / get_lot_defect_rates 的
read_only=True 必须一路穿透到 get_modifier_context / _build_modifier_context，
使修饰表同步绝不写盘；默认 False 保持既有行为。
"""

from __future__ import annotations

import pandas as pd
import pytest

from yield_domain.application import yield_service
from yield_domain.application.yield_service import YieldAnalysisService


@pytest.fixture(autouse=True)
def _clear_yield_caches():
    for func in (
        YieldAnalysisService.get_mwd_trend_data,
        YieldAnalysisService.get_code_level_trend_data,
        YieldAnalysisService.get_lot_defect_rates,
        YieldAnalysisService.get_modifier_context,
        YieldAnalysisService.get_modified_panel_details,
    ):
        func.clear()
    yield


@pytest.fixture
def captured_context_calls(monkeypatch):
    """拦截 get_modifier_context，记录 read_only 透传值。"""
    calls = []

    def fake_modifier_context(config, product_dir, _db_manager=None, snapshot_signature="", modifier_signature="", read_only=False):
        calls.append(read_only)
        return {"targets": {}, "group_targets": {}, "factors": {}, "signature": "s"}

    monkeypatch.setattr(
        YieldAnalysisService, "get_modifier_context", staticmethod(fake_modifier_context)
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "get_modified_panel_details",
        staticmethod(lambda *args, **kwargs: pd.DataFrame({"lot_id": ["L1"]})),
    )
    return calls


def test_code_level_trend_threads_read_only(captured_context_calls, mock_config, tmp_path, monkeypatch):
    monkeypatch.setattr(
        yield_service.MWDTrendProcessor,
        "create_code_level_mwd_trend_data",
        staticmethod(lambda **kwargs: {"monthly": pd.DataFrame()}),
    )

    YieldAnalysisService.get_code_level_trend_data(
        mock_config, tmp_path, read_only=True
    )
    YieldAnalysisService.get_code_level_trend_data(
        mock_config, tmp_path, snapshot_signature="v2"
    )

    assert captured_context_calls == [True, False]


def test_mwd_trend_threads_read_only(captured_context_calls, mock_config, tmp_path, monkeypatch):
    monkeypatch.setattr(
        yield_service.MWDTrendProcessor,
        "create_mwd_trend_data",
        staticmethod(lambda **kwargs: {"monthly": pd.DataFrame()}),
    )
    monkeypatch.setattr(
        yield_service.MWDTrendProcessor,
        "create_code_level_mwd_trend_data",
        staticmethod(lambda **kwargs: {"monthly": pd.DataFrame()}),
    )

    YieldAnalysisService.get_mwd_trend_data(mock_config, tmp_path, read_only=True)

    assert captured_context_calls and all(flag is True for flag in captured_context_calls)


def test_lot_defect_rates_threads_read_only(captured_context_calls, mock_config, tmp_path, monkeypatch):
    monkeypatch.setattr(
        YieldAnalysisService,
        "_get_array_times",
        staticmethod(lambda *args, **kwargs: pd.DataFrame()),
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "load_static_warning_lines",
        staticmethod(lambda *args, **kwargs: {}),
    )
    monkeypatch.setattr(
        yield_service.MWDTrendProcessor,
        "create_code_level_mwd_trend_data",
        staticmethod(lambda **kwargs: {"monthly": pd.DataFrame()}),
    )
    monkeypatch.setattr(
        yield_service,
        "calculate_lot_defect_rates",
        lambda **kwargs: {"code_level_details": {}},
    )

    YieldAnalysisService.get_lot_defect_rates(mock_config, tmp_path, read_only=True)

    assert captured_context_calls == [True]
