"""自动预警矩阵的缓存入口与生产装配（PRD §4.1/§3.2，ADR-0001 缓存边界）。

与纯计算层 ``alert_matrix_service.py`` 分离：

- ``get_cached_alert_matrix``：轻量拼装，各单元格 ``@st.cache_data``，TTL 读
  ``config/global.yaml`` 的 ``application.cache_ttl_hours``；
  键 = (指标, 产品, 参考周, 输入签名, 独立 revision)，签名由
  ``alert_matrix_service.build_alert_matrix_signature`` 对
  ``build_default_signature_components`` 的分量做确定性摘要；
- ``build_cell_source_signature``：仅采集本指标输入，不混入其他指标 revision；
- ``build_default_matrix_context``：生产依赖装配（inline 资源目录、SPC CPK
  Excel、yield 只读入口、qtime 全产品监控）。所有服务对象经下划线前缀
  参数或闭包进入缓存函数，不参与哈希。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from pydantic import ValidationError

from app.components.indicator_cache import (
    build_indicator_product_cache_signature,
    get_indicator_product_revision,
)
from app.sections.inline_domain.monitor.alert_matrix_service import (
    MATRIX_ROWS,
    AlertMatrixContext,
    AlertMatrixRow,
    build_alert_matrix_payload,
    build_alert_matrix_signature,
)
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_shop_monitoring,
    get_qtime_decision_file_stat,
)
from src.indicator_domain.composition import build_qtime_service
from src.inline_domain.application.shared.decorated_data import SCOPE_DECORATION_FILE_NAME
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.shared.resource_paths import scope_resource_dir
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_health import attach_data_health, make_data_health
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

# 矩阵内 sheet OOS 行对应的 inline scope
MATRIX_INLINE_SCOPES: tuple[str, ...] = ("spc", "ctq", "aoi_tt", "aoi_rs")
QTIME_SHOPS: tuple[str, ...] = ("ARRAY", "OLED", "TP")
# 与 yield 看板共享同一缓存基签名，复用其 L2 条目（revision 变化同步失效）
YIELD_SNAPSHOT_SIGNATURE_BASE = "yield_dashboard_manual_refresh_v1"
MATRIX_CACHE_BASE_SIGNATURE = "alert_matrix_board_v2_health"


def get_alert_matrix_week_start(reference_date: date | None = None) -> date:
    """把参考日归一到所在周周一：同一 ISO 周内命中同一缓存条目。"""
    reference = pd.Timestamp(reference_date or date.today()).normalize()
    return (reference - pd.Timedelta(days=reference.weekday())).date()


def build_default_signature_components(products: Sequence[str]) -> dict[str, Any]:
    """诊断用逐单元格输入签名映射；不再用作整板缓存键。"""
    return {
        f"{row.row_key}|{product}": build_cell_source_signature(row.row_key, product)
        for row in MATRIX_ROWS for product in products
    }


def cpk_latest_store() -> CpkLatestExcelStore:
    return CpkLatestExcelStore(
        ConfigLoader.get_domain_resource_dir("inline_domain")
        / "spc" / "spc_cpk_cpm_decoration.xlsx"
    )


def _file_signature(path: Path) -> str:
    try:
        stat = path.stat()
        return f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
    except FileNotFoundError:
        return f"{path.resolve()}:missing"


def build_cell_source_signature(row_key: str, product: str) -> str:
    """Only the selected indicator's real input signatures; no board/global revision."""
    if row_key == "spc_cpk_trend":
        return cpk_latest_store().source_signature()
    if row_key.endswith("_sheet_oos") and row_key != "qtime_sheet_oos":
        scope = row_key.removesuffix("_sheet_oos")
        return _file_signature(scope_resource_dir(scope) / SCOPE_DECORATION_FILE_NAME[scope])
    if row_key.startswith("yield_"):
        from yield_domain.application.yield_service import YieldAnalysisService

        config = ConfigLoader.load_config(product)
        directory = ConfigLoader.get_domain_resource_dir("yield_domain") / product
        context = YieldAnalysisService.build_cache_context(config, directory)
        if row_key == "yield_trend_fluctuation":
            context = {key: value for key, value in context.items() if key != "warning_signature"}
        return build_alert_matrix_signature(products=(product,), components=context)
    path = ConfigLoader.get_domain_resource_path("indicator_domain", "qtime_oos_decoration", "qtime_oos_decoration.xlsx")
    source_dir = ConfigLoader.get_project_root() / "data" / "indicator_domain" / "qtime"
    return build_alert_matrix_signature(products=(product,), components={
        "decision": _file_signature(Path(path)),
        "sources": [_file_signature(source_dir / f"qtime_source_{shop.lower()}.parquet") for shop in QTIME_SHOPS],
        "settings": ConfigLoader.load_domain_config("indicator_domain").get("qtime", {}),
        "data_policy": ConfigLoader.get_data_forward_policy().signature,
    })


def load_all_product_qtime_monitoring(
    db_manager: DatabaseManager,
    reference_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """全产品 Q-Time (details, alerts)：3 个 shop 各查一次（全站点）后 union。

    统一走 ``get_cached_shop_monitoring`` 公共入口：矩阵 payload（按 prodcode
    拆分）、点击详情（按产品过滤）与 Q-Time 页面（页内内存过滤）命中同一组
    (shop, 全站点, 全产品) L2 缓存条目，不会重复打库。厂别无站点时
    ``QTimeQuery`` 校验上抛 ValidationError，维持既有 skip 语义。

    alerts 帧在 union 时按 shop 打标（``assign`` 返回新帧，不污染共享缓存
    对象）：ARRAY/OLED/TP 本身即厂别，供矩阵单元格 ``alert_factories`` 提取。
    """
    service = build_qtime_service(db_manager)
    stat = get_qtime_decision_file_stat(service.decoration_path)
    mtime_ns, size = stat if stat is not None else MISSING_DECISION_FILE_STAT
    details_frames: list[pd.DataFrame] = []
    alerts_frames: list[pd.DataFrame] = []
    health_by_shop: dict[str, dict] = {}
    for shop in QTIME_SHOPS:
        try:
            result = get_cached_shop_monitoring(
                service,
                shop=shop,  # type: ignore[arg-type]
                as_of=reference_date,
                decision_mtime_ns=mtime_ns,
                decision_size=size,
            )
        except ValidationError:
            health_by_shop[shop] = make_data_health("unknown")
            continue  # 厂别无站点：与既有 skip 行为一致
        health_by_shop[shop] = dict(result.data_health)
        details_frames.append(result.details)
        alerts_frames.append(result.alerts.assign(shop=shop))
    details_df = (
        pd.concat(details_frames, ignore_index=True) if details_frames else pd.DataFrame()
    )
    alerts_df = (
        pd.concat(alerts_frames, ignore_index=True) if alerts_frames else pd.DataFrame()
    )
    # A missing/stale shop must not disappear during concat and produce green cells.
    states = [item["status"] for item in health_by_shop.values()]
    status = "fresh" if states and all(item == "fresh" for item in states) else "unknown"
    if "stale" in states:
        status = "stale"
    for frame in (details_df, alerts_df):
        attach_data_health(frame, make_data_health(status))
        frame.attrs["data_health_by_shop"] = health_by_shop
    return details_df, alerts_df


def build_default_matrix_context(
    products: Sequence[str],
    *,
    reference_date: date,
) -> AlertMatrixContext:
    """生产装配：构建带真实数据源的 AlertMatrixContext（在缓存 miss 时调用一次）。"""
    from yield_domain.application.yield_service import YieldAnalysisService

    inline_resource_dir = ConfigLoader.get_domain_resource_dir("inline_domain")

    def spc_cpk_loader(prod_code: str) -> pd.DataFrame | None:
        return cpk_latest_store().read_product(prod_code)

    def _yield_product_resources(prod_code: str, indicator_key: str):
        config = ConfigLoader.load_config(prod_code)
        product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / prod_code
        snapshot_signature = build_indicator_product_cache_signature(
            YIELD_SNAPSHOT_SIGNATURE_BASE, prod_code, (indicator_key,)
        )
        cache_context = YieldAnalysisService.build_cache_context(config, product_dir)
        return config, product_dir, snapshot_signature, cache_context

    def yield_lot_loader(prod_code: str):
        """read_only=True：矩阵只读消费，不触发良损修饰表回写。"""
        config, product_dir, snapshot_signature, cache_context = _yield_product_resources(prod_code, "yield_lot_oos")
        lot_data = YieldAnalysisService.get_lot_defect_rates(
            config,
            product_dir,
            _db_manager=DatabaseManager(),
            snapshot_signature=snapshot_signature,
            read_only=True,
            **cache_context,
        )
        if not lot_data:
            return None
        warning_lines = YieldAnalysisService.load_static_warning_lines(
            config,
            product_dir,
            snapshot_signature,
            warning_signature=cache_context["warning_signature"],
        )
        return lot_data, warning_lines

    def yield_health_loader(prod_code: str, indicator_key: str):
        config, _, snapshot_signature, cache_context = _yield_product_resources(prod_code, indicator_key)
        return YieldAnalysisService.get_data_health(
            config, _db_manager=DatabaseManager(), snapshot_signature=snapshot_signature,
            analysis_start_date=cache_context["analysis_start_date"],
            analysis_end_date=cache_context["analysis_end_date"],
        )

    def yield_trend_loader(prod_code: str):
        """read_only=True：矩阵只读消费，不触发良损修饰表回写。"""
        config, product_dir, snapshot_signature, cache_context = _yield_product_resources(prod_code, "yield_trend_fluctuation")
        mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
            config,
            product_dir,
            _db_manager=DatabaseManager(),
            snapshot_signature=snapshot_signature,
            read_only=True,
            analysis_start_date=cache_context["analysis_start_date"],
            analysis_end_date=cache_context["analysis_end_date"],
            modifier_signature=cache_context["modifier_signature"],
        )
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
            config,
            product_dir,
            _db_manager=DatabaseManager(),
            snapshot_signature=snapshot_signature,
            read_only=True,
            analysis_start_date=cache_context["analysis_start_date"],
            analysis_end_date=cache_context["analysis_end_date"],
            modifier_signature=cache_context["modifier_signature"],
        )
        if not mwd_group_data and not mwd_code_data:
            return None
        return mwd_group_data, mwd_code_data

    def qtime_monitoring_loader() -> tuple[pd.DataFrame, pd.DataFrame]:
        """全产品 Q-Time 监控（共享入口，与点击详情同一 L2 缓存）。"""
        return load_all_product_qtime_monitoring(DatabaseManager(), reference_date)

    return AlertMatrixContext(
        reference_date=reference_date,
        inline_resource_dir=inline_resource_dir,
        spc_cpk_loader=spc_cpk_loader,
        yield_lot_loader=yield_lot_loader,
        yield_trend_loader=yield_trend_loader,
        yield_health_loader=yield_health_loader,
        qtime_monitoring_loader=qtime_monitoring_loader,
    )


@st.cache_data(
    show_spinner=False,
    max_entries=1024,
    ttl=ConfigLoader.get_cache_ttl_seconds(),
)
def _cached_alert_matrix_cell(
    row_key: str,
    product: str,
    week_start: str,
    signature: str,
    revision: str,
    _loader: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Independent indicator/product result; loader executes only for this cell's miss."""
    return {
        **_loader(), "cache_signature": signature, "cache_revision": revision,
        "computed_at": datetime.now().isoformat(timespec="microseconds"),
    }


def get_cached_alert_matrix(
    *,
    reference_date: date | None = None,
    products: Sequence[str] | None = None,
    _context_factory: Callable[[], AlertMatrixContext] | None = None,
    _signature_provider: Callable[[str, str], str] | None = None,
    _revision_provider: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    """矩阵 payload 的缓存入口（普通 rerun 命中缓存，签名/周变化才重建）。"""
    product_tuple = (
        tuple(products)
        if products is not None
        else tuple(ConfigLoader.get_enabled_products())
    )
    week_start = get_alert_matrix_week_start(reference_date)
    reference = reference_date or date.today()
    factory = _context_factory or (
        lambda: build_default_matrix_context(product_tuple, reference_date=reference)
    )
    source_signature = _signature_provider or build_cell_source_signature
    revision_provider = _revision_provider or get_indicator_product_revision
    context = factory()  # construction is lazy: no DB/SPC/Yield calculation here

    def load_cell(
        row: AlertMatrixRow, product: str, active_context: AlertMatrixContext,
    ) -> dict[str, Any]:
        try:
            revision = revision_provider(row.row_key, product)
            signature = build_alert_matrix_signature(products=(product,), components={
                "indicator": row.row_key, "source": source_signature(row.row_key, product),
                "revision": revision, "week": week_start.isoformat(),
                "as_of": reference.isoformat() if row.row_key == "qtime_sheet_oos" else None,
            })
        except Exception as exc:
            logger.exception("[alert-matrix] cell signature failed: %s/%s", row.row_key, product)
            return {
                "state": "error", "detail_key": f"{row.row_key}|{product}",
                "message": str(exc)[:200], "alert_factories": [],
            }
        return _cached_alert_matrix_cell(
            row.row_key, product, week_start.isoformat(), signature, revision,
            lambda: row.evaluator(product, active_context),
        )

    return build_alert_matrix_payload(
        context=context, products=product_tuple, cell_loader=load_cell,
    )


def get_alert_matrix_cached_funcs() -> list:
    """兼容入口：整板不应全局清理任何指标缓存，改用定向 revision。"""
    # The aggregate page must never clear shared application caches globally.
    # Refresh is driven exclusively by the selected indicator/product revision.
    return []
