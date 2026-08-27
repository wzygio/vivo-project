"""PRD §5.8：外层 payload 缓存必须配置 ttl 且 ≤ 4h。

外层 ``st.cache_data`` 若无 TTL 会永久遮挡共享层
``fetch_decorated_features``（ttl=4h）的 4h 生成判定。
通过 ``CachedFunc._info.ttl``（秒）读取 streamlit 1.60 缓存配置做结构断言。
"""

import pytest

from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.spc_service import SpcReportService

MAX_OUTER_CACHE_TTL_SECONDS = 4 * 60 * 60


@pytest.mark.parametrize(
    "label,cached_func",
    [
        ("ctq.fetch_ctq_report_payload", CtqReportService.fetch_ctq_report_payload),
        ("monitor.fetch_dashboard_data_dict", MonitorAnalysisService.fetch_dashboard_data_dict),
        ("spc.fetch_spc_report_payload", SpcReportService.fetch_spc_report_payload),
    ],
)
def test_outer_payload_cache_ttl_within_4h(label: str, cached_func: object) -> None:
    info = getattr(cached_func, "_info", None)
    assert info is not None, f"{label} 不是 st.cache_data 缓存函数"
    ttl = info.ttl
    assert ttl is not None, f"{label} 外层缓存未配置 ttl，会永久遮挡 4h 生成判定"
    assert ttl <= MAX_OUTER_CACHE_TTL_SECONDS, (
        f"{label} 外层缓存 ttl={ttl}s 超过 4h（{MAX_OUTER_CACHE_TTL_SECONDS}s）"
    )
