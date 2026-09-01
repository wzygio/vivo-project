"""外层 payload 缓存必须配置 ttl（不得为 None），且不超过统一配置的 12h 上限。

外层 ``st.cache_data`` 若无 TTL 会永久遮挡共享层
``fetch_decorated_features`` 的生成判定。统一机制：周期 TTL 上限 12h（随
master 的 ``service_cache.ttl_hours`` 体系配置），手动刷新与决策上传经
缓存键中的产品 revision / 决策签名即时失效，不受周期 TTL 影响。
通过 ``CachedFunc._info.ttl``（秒）读取 streamlit 1.60 缓存配置做结构断言。
"""

import sys
import types

import pytest

# monitor_dashboard 间接导入 streamlit_echarts，bare 模式下组件注册会抛错；
# 参照 tests/unit/app/sections/monitor/ 既有做法先打 stub 再导入。
_streamlit_echarts_stub = types.ModuleType("streamlit_echarts")
_streamlit_echarts_stub.st_echarts = lambda *args, **kwargs: None
_streamlit_echarts_stub.JsCode = lambda code: code
sys.modules.setdefault("streamlit_echarts", _streamlit_echarts_stub)

from app.sections.inline_domain.monitor.monitor_dashboard import (
    get_cached_alarm_detail_tables,
)
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.spc_service import SpcReportService

MAX_OUTER_CACHE_TTL_SECONDS = 12 * 60 * 60


@pytest.mark.parametrize(
    "label,cached_func",
    [
        ("aoi_rs.fetch_aoi_rs_report_payload", AoiRsReportService.fetch_aoi_rs_report_payload),
        ("aoi_tt.fetch_aoi_tt_report_payload", AoiTtReportService.fetch_aoi_tt_report_payload),
        ("ctq.fetch_ctq_report_payload", CtqReportService.fetch_ctq_report_payload),
        ("monitor.fetch_dashboard_data_dict", MonitorAnalysisService.fetch_dashboard_data_dict),
        ("monitor_dashboard.get_cached_alarm_detail_tables", get_cached_alarm_detail_tables),
        ("spc.fetch_spc_report_payload", SpcReportService.fetch_spc_report_payload),
    ],
)
def test_outer_payload_cache_ttl_configured(label: str, cached_func: object) -> None:
    info = getattr(cached_func, "_info", None)
    assert info is not None, f"{label} 不是 st.cache_data 缓存函数"
    ttl = info.ttl
    assert ttl is not None, f"{label} 外层缓存未配置 ttl，会永久遮挡共享层生成判定"
    assert ttl <= MAX_OUTER_CACHE_TTL_SECONDS, (
        f"{label} 外层缓存 ttl={ttl}s 超过 12h（{MAX_OUTER_CACHE_TTL_SECONDS}s）上限"
    )
