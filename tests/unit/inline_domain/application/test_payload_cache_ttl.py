"""外层 payload 缓存必须配置 ttl（不得为 None），且不超过统一配置的 12h 上限。

外层 ``st.cache_data`` 若无 TTL 会永久遮挡共享层
``fetch_decorated_features`` 的生成判定。2026-08-28 合并裁定：周期 TTL 上限
随 master 的 ``service_cache.ttl_hours`` 体系统一为 12h（手动刷新与决策上传经
缓存键中的产品 revision / 决策签名即时生效，不受周期 TTL 影响）。
通过 ``CachedFunc._info.ttl``（秒）读取 streamlit 1.60 缓存配置做结构断言。
"""

import pytest

from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.spc_service import SpcReportService

MAX_OUTER_CACHE_TTL_SECONDS = 12 * 60 * 60


@pytest.mark.parametrize(
    "label,cached_func",
    [
        ("ctq.fetch_ctq_report_payload", CtqReportService.fetch_ctq_report_payload),
        ("monitor.fetch_dashboard_data_dict", MonitorAnalysisService.fetch_dashboard_data_dict),
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
