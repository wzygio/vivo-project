import sys
import types

import pandas as pd


streamlit_echarts_stub = types.ModuleType("streamlit_echarts")
streamlit_echarts_stub.st_echarts = lambda *args, **kwargs: None
streamlit_echarts_stub.JsCode = lambda code: code
sys.modules.setdefault("streamlit_echarts", streamlit_echarts_stub)

st_aggrid_stub = types.ModuleType("st_aggrid")
st_aggrid_stub.AgGrid = lambda *args, **kwargs: {}
st_aggrid_stub.GridOptionsBuilder = object
st_aggrid_stub.GridUpdateMode = types.SimpleNamespace(SELECTION_CHANGED="SELECTION_CHANGED")
st_aggrid_stub.DataReturnMode = types.SimpleNamespace()
st_aggrid_stub.JsCode = lambda code: code
sys.modules.setdefault("st_aggrid", st_aggrid_stub)

from app.sections.inline_domain.monitor.monitor_dashboard import (
    _stable_cache_key_fragment,
    get_cached_alarm_detail_tables,
)
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.shared_kernel.config import ConfigLoader


def _call_cached_detail_tables(
    revision_signature: str = "",
    decision_signature: str = "",
) -> None:
    get_cached_alarm_detail_tables(
        object(),
        "{}",
        "MIXED",
        "snapshot-sig",
        "compliance-sig",
        revision_signature,
        decision_signature,
    )


def test_stable_cache_key_fragment_sorts_and_nests() -> None:
    left = _stable_cache_key_fragment({"P2": "r2", "P1": {"ctq": "d2", "spc": "d1"}})
    right = _stable_cache_key_fragment({"P1": {"spc": "d1", "ctq": "d2"}, "P2": "r2"})

    assert left == right
    assert _stable_cache_key_fragment(None) == ""
    assert _stable_cache_key_fragment({}) == ""
    assert _stable_cache_key_fragment({"P1": "r1"}) != _stable_cache_key_fragment(
        {"P1": "r2"}
    )


def test_alarm_detail_cache_key_includes_revision_and_decision_signatures(
    monkeypatch,
) -> None:
    build_calls = {"count": 0}

    def fake_get_details(*args, **kwargs) -> pd.DataFrame:
        build_calls["count"] += 1
        return pd.DataFrame()

    monkeypatch.setattr(
        MonitorAnalysisService,
        "get_monitor_defect_details",
        staticmethod(fake_get_details),
    )
    get_cached_alarm_detail_tables.clear()

    _call_cached_detail_tables(revision_signature="P1=r1", decision_signature="P1=spc:d1")
    first_count = build_calls["count"]
    assert first_count > 0

    # 相同键：命中缓存，不再重建
    _call_cached_detail_tables(revision_signature="P1=r1", decision_signature="P1=spc:d1")
    assert build_calls["count"] == first_count

    # 产品 revision 变化：新的缓存条目
    _call_cached_detail_tables(revision_signature="P1=r2", decision_signature="P1=spc:d1")
    assert build_calls["count"] > first_count

    # 决策签名变化：新的缓存条目
    second_count = build_calls["count"]
    _call_cached_detail_tables(revision_signature="P1=r2", decision_signature="P1=spc:d2")
    assert build_calls["count"] > second_count


def test_alarm_detail_cache_ttl_defaults_to_12_hours() -> None:
    ttl_seconds = ConfigLoader.get_service_cache_ttl_seconds(
        "inline_monitor_alarm_details", default_hours=12
    )
    assert ttl_seconds == 12 * 3600
