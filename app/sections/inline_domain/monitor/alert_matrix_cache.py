"""自动预警矩阵的缓存入口与生产装配（PRD §4.1/§3.2，ADR-0001 缓存边界）。

与纯计算层 ``alert_matrix_service.py`` 分离：

- ``get_cached_alert_matrix``：``@st.cache_data`` 包装，TTL 读
  ``config/global.yaml`` 的 ``service_cache.ttl_hours.alert_matrix_payload``；
  键 = (products, 参考周周一, 签名)，签名由
  ``alert_matrix_service.build_alert_matrix_signature`` 对
  ``build_default_signature_components`` 的分量做确定性摘要；
- ``build_default_signature_components``：集中采集逐产品 revision、
  逐 (prod, scope) 决策签名（file_stat 门控）、qtime 决策工作簿 stat——
  签名组装只此一处（PRD §6 风险缓解）；
- ``build_default_matrix_context``：生产依赖装配（inline 资源目录、SPC CPK
  payload、yield 只读入口、qtime 全产品监控）。所有服务对象经下划线前缀
  参数或闭包进入缓存函数，不参与哈希。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from app.components.page_header import (
    build_product_cache_signature,
    get_product_cache_revision,
)
from app.sections.inline_domain.monitor.alert_matrix_service import (
    AlertMatrixContext,
    build_alert_matrix_payload,
    build_alert_matrix_signature,
)
from app.sections.inline_domain.spc.spc_dashboard import get_default_spc_start_date
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_monitoring,
    get_qtime_decision_file_stat,
)
from src.indicator_domain.composition import build_qtime_service
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared.decision_signature import (
    get_scope_decision_signature,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.composition import build_spc_repository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

# 矩阵内 sheet OOS 行对应的 inline scope
MATRIX_INLINE_SCOPES: tuple[str, ...] = ("spc", "ctq", "aoi_tt", "aoi_rs")
QTIME_SHOPS: tuple[str, ...] = ("ARRAY", "OLED", "TP")
# 与 yield 看板共享同一缓存基签名，复用其 L2 条目（revision 变化同步失效）
YIELD_SNAPSHOT_SIGNATURE_BASE = "yield_dashboard_manual_refresh_v1"
MATRIX_CACHE_BASE_SIGNATURE = "alert_matrix_board_v1"


def get_alert_matrix_week_start(reference_date: date | None = None) -> date:
    """把参考日归一到所在周周一：同一 ISO 周内命中同一缓存条目。"""
    reference = pd.Timestamp(reference_date or date.today()).normalize()
    return (reference - pd.Timedelta(days=reference.weekday())).date()


def build_default_signature_components(products: Sequence[str]) -> dict[str, Any]:
    """采集签名分量：逐产品 revision + 逐 (prod, scope) 决策签名 + qtime 决策 stat。

    分量采集失败降级为确定性 "unavailable" 标记：对应域的数据加载大概率同样
    失败并落入 error 单元格；缓存键保持确定性，不产生每次 rerun 都变化的脏键。
    """
    revisions: dict[str, str] = {}
    decisions: dict[str, str] = {}
    for prod_code in products:
        try:
            revisions[prod_code] = get_product_cache_revision(prod_code)
        except Exception as exc:  # noqa: BLE001 - 降级为确定性标记，见 docstring
            logger.warning("[alert-matrix] 产品 %s revision 读取失败: %s", prod_code, exc)
            revisions[prod_code] = "unavailable"
        for scope in MATRIX_INLINE_SCOPES:
            key = f"{scope}|{prod_code}"
            try:
                decisions[key] = get_scope_decision_signature(scope, prod_code)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[alert-matrix] 决策签名读取失败 (%s): %s", key, exc
                )
                decisions[key] = "unavailable"

    try:
        qtime_path = ConfigLoader.get_domain_resource_path(
            "indicator_domain", "qtime_oos_decoration", "qtime_oos_decoration.xlsx"
        )
        qtime_stat = get_qtime_decision_file_stat(qtime_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[alert-matrix] qtime 决策文件 stat 失败: %s", exc)
        qtime_stat = None
    return {
        "product_revisions": revisions,
        "scope_decision_signatures": decisions,
        "qtime_decision_file_stat": list(qtime_stat or MISSING_DECISION_FILE_STAT),
    }


def load_all_product_qtime_monitoring(
    db_manager: DatabaseManager,
    reference_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """全产品 Q-Time (details, alerts)：3 个 shop 各查一次（全站点）后 union。

    矩阵 payload（按 prodcode 拆分）与点击详情（按产品过滤）共用此入口，
    均命中 ``get_cached_monitoring`` 的 L2 缓存，不会重复打库。
    """
    service = build_qtime_service(db_manager)
    stat = get_qtime_decision_file_stat(service.decoration_path)
    mtime_ns, size = stat if stat is not None else MISSING_DECISION_FILE_STAT
    details_frames: list[pd.DataFrame] = []
    alerts_frames: list[pd.DataFrame] = []
    for shop in QTIME_SHOPS:
        options = service.get_filter_options(shop)  # type: ignore[arg-type]
        step_descriptions = tuple(
            option.step_desc for option in options["step_options"]
        )
        if not step_descriptions:
            continue
        result = get_cached_monitoring(
            service,
            shop=shop,  # type: ignore[arg-type]
            step_descriptions=step_descriptions,
            products=(),
            as_of=reference_date,
            decision_mtime_ns=mtime_ns,
            decision_size=size,
        )
        details_frames.append(result.details)
        alerts_frames.append(result.alerts)
    details_df = (
        pd.concat(details_frames, ignore_index=True) if details_frames else pd.DataFrame()
    )
    alerts_df = (
        pd.concat(alerts_frames, ignore_index=True) if alerts_frames else pd.DataFrame()
    )
    return details_df, alerts_df


def build_default_matrix_context(
    products: Sequence[str],
    *,
    reference_date: date,
) -> AlertMatrixContext:
    """生产装配：构建带真实数据源的 AlertMatrixContext（在缓存 miss 时调用一次）。"""
    from yield_domain.application.yield_service import YieldAnalysisService

    db_manager = DatabaseManager()
    inline_resource_dir = ConfigLoader.get_domain_resource_dir("inline_domain")

    _, default_end_dt = MonitorAnalysisService.get_time_window()
    spc_start_date = get_default_spc_start_date(default_end_dt.date())
    spc_end_date = default_end_dt.strftime("%Y-%m-%d")

    def spc_cpk_loader(prod_code: str) -> pd.DataFrame | None:
        """复用 SPC 页装配：fetch_spc_report_payload（L2 缓存）→ period_capability_df。"""
        query_config = SpcQueryConfig(
            prod_code=prod_code,
            start_date=spc_start_date.strftime("%Y-%m-%d"),
            end_date=spc_end_date,
            data_type_filter="SPC",
        )
        payload = SpcReportService.fetch_spc_report_payload(
            _data_port=build_spc_repository(db_manager, prod_code),
            query_config_json=query_config.model_dump_json(),
            snapshot_signature=build_product_cache_signature(
                MATRIX_CACHE_BASE_SIGNATURE, prod_code
            ),
            period_sigma_source=ConfigLoader.get_spc_period_sigma_source(),
            product_revision=get_product_cache_revision(prod_code),
            decision_signature=get_scope_decision_signature("spc", prod_code),
        )
        capability_df = payload.get("period_capability_df")
        if not isinstance(capability_df, pd.DataFrame) or capability_df.empty:
            return None
        return capability_df

    def _yield_product_resources(prod_code: str):
        config = ConfigLoader.load_config(prod_code)
        product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / prod_code
        snapshot_signature = build_product_cache_signature(
            YIELD_SNAPSHOT_SIGNATURE_BASE, prod_code
        )
        return config, product_dir, snapshot_signature

    def yield_lot_loader(prod_code: str):
        """read_only=True：矩阵只读消费，不触发良损修饰表回写。"""
        config, product_dir, snapshot_signature = _yield_product_resources(prod_code)
        lot_data = YieldAnalysisService.get_lot_defect_rates(
            config,
            product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        if not lot_data:
            return None
        warning_lines = YieldAnalysisService.load_static_warning_lines(
            config, product_dir, snapshot_signature
        )
        return lot_data, warning_lines

    def yield_trend_loader(prod_code: str):
        """read_only=True：矩阵只读消费，不触发良损修饰表回写。"""
        config, product_dir, snapshot_signature = _yield_product_resources(prod_code)
        mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
            config,
            product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
            config,
            product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        if not mwd_group_data and not mwd_code_data:
            return None
        return mwd_group_data, mwd_code_data

    def qtime_monitoring_loader() -> tuple[pd.DataFrame, pd.DataFrame]:
        """全产品 Q-Time 监控（共享入口，与点击详情同一 L2 缓存）。"""
        return load_all_product_qtime_monitoring(db_manager, reference_date)

    return AlertMatrixContext(
        reference_date=reference_date,
        inline_resource_dir=inline_resource_dir,
        spc_cpk_loader=spc_cpk_loader,
        yield_lot_loader=yield_lot_loader,
        yield_trend_loader=yield_trend_loader,
        qtime_monitoring_loader=qtime_monitoring_loader,
    )


@st.cache_data(
    show_spinner=False,
    max_entries=8,
    ttl=ConfigLoader.get_service_cache_ttl_seconds("alert_matrix_payload", default_hours=12),
)
def _cached_alert_matrix_payload(
    products: tuple[str, ...],
    week_start: str,
    signature: str,
    _context_factory: Callable[[], AlertMatrixContext],
) -> dict[str, Any]:
    """缓存边界：键全部原生可哈希；context 工厂下划线前缀排除哈希（ADR-0001）。"""
    context = _context_factory()
    return build_alert_matrix_payload(
        context=context,
        products=list(products),
        signature=signature,
    )


def get_cached_alert_matrix(
    *,
    reference_date: date | None = None,
    products: Sequence[str] | None = None,
    _context_factory: Callable[[], AlertMatrixContext] | None = None,
) -> dict[str, Any]:
    """矩阵 payload 的缓存入口（普通 rerun 命中缓存，签名/周变化才重建）。"""
    product_tuple = (
        tuple(products)
        if products is not None
        else tuple(ConfigLoader.get_enabled_products())
    )
    week_start = get_alert_matrix_week_start(reference_date)
    signature = build_alert_matrix_signature(products=product_tuple)
    factory = _context_factory or (
        lambda: build_default_matrix_context(product_tuple, reference_date=week_start)
    )
    return _cached_alert_matrix_payload(
        product_tuple,
        week_start.isoformat(),
        signature,
        factory,
    )


def get_alert_matrix_cached_funcs() -> list:
    """页头「刷新缓存」需一并清理的矩阵相关 L2 缓存函数清单。

    矩阵页为全产品聚合页（无 product_cache_scope），刷新时不会推进产品
    revision，因此矩阵 payload、点击详情数据包、qtime 监控以及详情懒加载
    链路复用的各域报表 payload 缓存都必须显式 clear，才能保证"刷新缓存后
    矩阵正确重建"（PRD §3.2-5）。
    """
    from app.components.page_header import extract_cached_funcs
    from app.sections.inline_domain.monitor.alert_matrix_detail import (
        _cached_matrix_detail_bundle,
    )
    from src.indicator_domain.application.qtime.cached_monitoring import (
        _cached_monitoring,
    )
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
    from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
    from src.inline_domain.application.ctq.ctq_service import CtqReportService
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from yield_domain.application.yield_service import YieldAnalysisService

    return [
        _cached_alert_matrix_payload,
        _cached_matrix_detail_bundle,
        _cached_monitoring,
        *extract_cached_funcs(
            SpcReportService,
            CtqReportService,
            AoiTtReportService,
            AoiRsReportService,
            YieldAnalysisService,
        ),
    ]
