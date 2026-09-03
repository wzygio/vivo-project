"""Q-Time 当前监控结果的 L2 缓存（PRD §4.3 / ADR-0001 缓存边界）。

缓存键 = (shop, step_descriptions, products, as_of, 决策工作簿 file_stat)，
TTL 读 ``config/global.yaml`` 的 ``service_cache.ttl_hours.qtime_monitoring``
（默认 12h，与 inline 对齐）。决策签名沿用 inline ``decision_signature.py``
的 file_stat 门控思路：页面每次 rerun 廉价 stat 一次
``qtime_oos_decoration.xlsx``，用户上传决策 → mtime 变化 → 新缓存条目，
无需显式 clear。

``as_of`` 允许 None：归一为当天 ``date`` 再进键，避免同天内 None 与显式
``date.today()`` 产生不同缓存条目（归一必须发生在缓存键计算之前，因此拆成
公开包装 + 私有缓存函数两层）。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import streamlit as st

from src.indicator_domain.application.qtime.dtos import Shop
from src.indicator_domain.application.qtime.service import (
    QTimeMonitoringResult,
    QTimeReportService,
)
from src.shared_kernel.config import ConfigLoader

# 决策工作簿不存在（按需创建，用户从未上传过决策）时的确定性哨兵。
MISSING_DECISION_FILE_STAT: tuple[int, int] = (-1, -1)


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
    return _cached_monitoring(
        _service,
        shop=shop,
        step_descriptions=tuple(step_descriptions),
        products=tuple(products),
        as_of=normalized_as_of,
        decision_mtime_ns=int(decision_mtime_ns),
        decision_size=int(decision_size),
    )


@st.cache_data(
    show_spinner=False,
    max_entries=32,
    ttl=ConfigLoader.get_service_cache_ttl_seconds(
        "qtime_monitoring", default_hours=12
    ),
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
) -> QTimeMonitoringResult:
    """实际缓存层：仅转发，不做任何判定逻辑修改。"""
    return _service.get_current_monitoring(
        shop=shop,
        step_descriptions=step_descriptions,
        products=products,
        as_of=as_of,
    )


def get_cached_shop_monitoring(
    _service: QTimeReportService,
    *,
    shop: Shop,
    as_of: date | None,
    decision_mtime_ns: int,
    decision_size: int,
) -> QTimeMonitoringResult:
    """厂别级公共入口：该厂别全部站点 + 全产品（products=()）的一次计算。

    矩阵（全产品聚合）与 Q-Time 页面（页内内存过滤）共用此入口，命中
    ``_cached_monitoring`` 同一组 (shop, 全站点, 全产品) 缓存条目，
    不新增缓存键维度。无站点（QTimeQuery min_length=1）或取站点失败时
    错误上抛，由调用方降级（页面 error / 矩阵单元格 ⬜）。
    """
    options = _service.get_filter_options(shop)
    step_descriptions = tuple(
        option.step_desc for option in options["step_options"]
    )
    return get_cached_monitoring(
        _service,
        shop=shop,
        step_descriptions=step_descriptions,
        products=(),
        as_of=as_of,
        decision_mtime_ns=decision_mtime_ns,
        decision_size=decision_size,
    )


def get_qtime_cached_funcs() -> list:
    """页头「刷新缓存」需清理的 qtime L2 缓存函数清单（矩阵清单同款模式）。"""
    return [_cached_monitoring]
