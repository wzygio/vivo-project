"""Q-Time 当前监控结果的 L2 缓存（PRD §4.3 / ADR-0001 缓存边界）。

缓存键 = (shop, step_descriptions, products, as_of, 决策工作簿 file_stat, 计算版本)，
TTL 读 ``config/global.yaml`` 的 ``application.cache_ttl_hours``。
决策签名沿用 inline ``decision_signature.py``
的 file_stat 门控思路：页面每次 rerun 廉价 stat 一次
``qtime_oos_decoration.xlsx``，用户上传决策 → mtime 变化 → 新缓存条目，
无需显式 clear。

``as_of`` 允许 None：归一为当天 ``date`` 再进键，避免同天内 None 与显式
``date.today()`` 产生不同缓存条目（归一必须发生在缓存键计算之前，因此拆成
公开包装 + 私有缓存函数两层）。
"""

from __future__ import annotations

from datetime import date
from dataclasses import replace
import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from src.indicator_domain.application.qtime.dtos import Shop
from src.indicator_domain.application.qtime.alert_results import QTIME_SHOPS
from src.indicator_domain.application.qtime.ports import QTimeSnapshotRefresh
from src.indicator_domain.application.qtime.service import (
    QTimeMonitoringResult,
    QTimeReportService,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_health import make_data_health

# 决策工作簿不存在（按需创建，用户从未上传过决策）时的确定性哨兵。
MISSING_DECISION_FILE_STAT: tuple[int, int] = (-1, -1)
# Invalidate results computed before support for microsecond source timekeys.
MONITORING_CACHE_VERSION = 5
logger = logging.getLogger(__name__)


def get_qtime_decision_file_stat(
    decoration_path: Path | str | None,
) -> tuple[int, int] | None:
    """廉价 file_stat 探针；无路径或工作簿不存在返回 None（页面侧用哨兵进键）。"""
    if decoration_path is None:
        return None
    try:
        stat = Path(decoration_path).stat()
    except OSError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_size))


def get_cached_monitoring(
    _service: QTimeReportService,
    *,
    shop: Shop,
    step_descriptions: tuple[str, ...],
    products: tuple[str, ...],
    as_of: date | None,
    decision_mtime_ns: int,
    decision_size: int,
) -> QTimeMonitoringResult:
    """``QTimeReportService.get_current_monitoring`` 的缓存入口。

    ``_service`` 下划线前缀排除哈希（ADR-0001 先例），其余键全部为原生
    可哈希类型；``as_of=None`` 在此归一为当天 date 后再进键。
    """
    normalized_as_of = as_of if as_of is not None else date.today()
    payload = _cached_monitoring(
        _service,
        shop=shop,
        step_descriptions=tuple(step_descriptions),
        products=tuple(products),
        as_of=normalized_as_of,
        decision_mtime_ns=int(decision_mtime_ns),
        decision_size=int(decision_size),
        cache_version=MONITORING_CACHE_VERSION,
        source_signature=getattr(_service, "cache_signature", lambda _shop: ())(shop),
    )
    # Resolve after the cache call: the service module can reload during a miss.
    from src.indicator_domain.application.qtime.service import QTimeMonitoringResult as CurrentResult

    return CurrentResult(
        details=payload["details"], alerts=payload["alerts"],
        decoration=payload["decoration"], decisions=payload["decisions"],
        decoration_path=Path(payload["decoration_path"]) if payload["decoration_path"] else None,
        data_health=dict(payload["data_health"]),
    )


@st.cache_data(
    show_spinner=False,
    max_entries=32,
    ttl=ConfigLoader.get_cache_ttl_seconds(),
)
def _cached_monitoring(
    _service: QTimeReportService,
    *,
    shop: Shop,
    step_descriptions: tuple[str, ...],
    products: tuple[str, ...],
    as_of: date,
    decision_mtime_ns: int,
    decision_size: int,
    cache_version: int,
    source_signature: tuple[object, ...],
) -> dict:
    """Cache only primitive payloads; reconstruct the current ViewModel outside."""
    result = _service.get_current_monitoring(
        shop=shop,
        step_descriptions=step_descriptions,
        products=products,
        as_of=as_of,
    )
    return {
        "details": result.details, "alerts": result.alerts,
        "decoration": result.decoration, "decisions": result.decisions,
        "decoration_path": str(result.decoration_path) if result.decoration_path else None,
        "data_health": dict(result.data_health),
    }


def get_cached_shop_monitoring(
    _service: QTimeReportService,
    *,
    shop: Shop,
    as_of: date | None,
    decision_mtime_ns: int,
    decision_size: int,
) -> QTimeMonitoringResult:
    """厂别级公共入口：该厂别全部站点 + 全产品（products=()）的一次计算。

    生产流程命中 ``_cached_monitoring`` 同一组 (shop, 全站点, 全产品)
    缓存条目，并将完整厂别投影发布为独立 Excel 结果；页面和矩阵的预警
    消费只读已发布文件，不因缓存缺失触发本函数。无站点
    （QTimeQuery min_length=1）或取站点失败时错误上抛，由生产调用方降级。
    """
    options = _service.get_filter_options(shop)
    step_descriptions = tuple(
        option.step_desc for option in options["step_options"]
    )
    result = get_cached_monitoring(
        _service,
        shop=shop,
        step_descriptions=step_descriptions,
        products=(),
        as_of=as_of,
        decision_mtime_ns=decision_mtime_ns,
        decision_size=decision_size,
    )
    publisher = getattr(_service, "publish_alert_results", None)
    if publisher is not None:
        try:
            publisher(result, shop, as_of or date.today())
        except Exception:
            logger.exception("Q-Time saved alert publication failed for shop=%s", shop)
            return replace(result, alert_publication_error="Q-Time 预警结果更新失败，请稍后重试。")
    return result


def get_qtime_cached_funcs() -> list:
    """页头「刷新缓存」需清理的 qtime L2 缓存函数清单（矩阵清单同款模式）。"""
    return [_cached_monitoring]


def publish_refreshed_qtime_alerts(
    service: QTimeReportService, refreshes: tuple[QTimeSnapshotRefresh, ...], *,
    as_of: date | None = None,
) -> bool:
    """Publish all shops after a successful explicit source-snapshot refresh."""
    if (len(refreshes) != len(QTIME_SHOPS)
            or {item.shop for item in refreshes} != set(QTIME_SHOPS)
            or not all(item.refreshed_from_database for item in refreshes)):
        logger.error("Q-Time alert publication requires all shops freshly refreshed")
        return False
    reference_date = as_of or date.today()
    decision_stat = (
        get_qtime_decision_file_stat(service.decoration_path)
        or MISSING_DECISION_FILE_STAT
    )
    succeeded = True
    for refresh in refreshes:
        shop = refresh.shop
        try:
            if refresh.row_count == 0:
                # A successful empty source has no station options. Publish its
                # actual refresh evidence without inventing a query station.
                empty = QTimeMonitoringResult(
                    pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                    service.decoration_path,
                    make_data_health(
                        "fresh", refreshed_at=refresh.refreshed_at,
                        source_start=refresh.source_start, source_end=refresh.source_end,
                    ),
                )
                service.publish_alert_results(empty, shop, reference_date)
                continue
            result = get_cached_shop_monitoring(
                service, shop=shop, as_of=reference_date,
                decision_mtime_ns=decision_stat[0], decision_size=decision_stat[1],
            )
            if result.alert_publication_error:
                succeeded = False
        except Exception:
            logger.exception("Q-Time refreshed alert publication failed for shop=%s", shop)
            succeeded = False
    return succeeded
