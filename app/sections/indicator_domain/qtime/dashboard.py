"""Presentation helpers and sections for the Q-Time report page."""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
from pydantic import ValidationError
import streamlit as st

from app.charts.indicator_domain.qtime.chart import build_qtime_figure
from app.manager.session_manager import SessionManager
from app.sections.indicator_domain.qtime.alert_center import render_qtime_alert_center
from app.sections.indicator_domain.qtime.decoration_admin import (
    render_qtime_decoration_admin,
)
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_shop_monitoring,
    get_qtime_decision_file_stat,
)
from src.indicator_domain.application.qtime.dtos import QTimeStepOption, Shop
from src.indicator_domain.application.qtime.errors import (
    QTimeDataAccessError,
    QTimeDecorationAccessError,
)
from src.indicator_domain.application.qtime.service import (
    QTimeMonitoringResult,
    QTimeReportService,
)


RESULT_STATE_KEY = "qtime_report_result"
SIGNATURE_STATE_KEY = "qtime_report_signature"


def render_qtime_dashboard(service: QTimeReportService) -> None:
    """Render filters, gated query results, and explicit operational states.

    数据侧按厂别全量取数（``get_cached_shop_monitoring``，与预警矩阵共享
    同一组 L2 缓存条目），产品/站点筛选为内存过滤；本页不参与 Header
    单产品筛选（产品多选内聚在筛选区首列）。
    """
    st.subheader("北极星QTime监控", anchor=False, text_alignment="center")

    with st.container(border=True):
        product_column, shop_column, path_column, search_column = st.columns(
            [1.4, 0.8, 2.4, 0.6],
            vertical_alignment="bottom",
        )
        with product_column:
            product_options = list(SessionManager.AVAILABLE_PRODUCTS)
            selected_products = st.multiselect(
                "产品",
                options=product_options,
                default=product_options,
                placeholder="请选择一个或多个产品",
                key="qtime_products",
            )
        with shop_column:
            shop = st.selectbox(
                "厂别",
                options=("ARRAY", "OLED", "TP"),
                key="qtime_shop",
            )

        try:
            options = service.get_filter_options(shop)
        except QTimeDataAccessError as exc:
            st.error(str(exc))
            return

        with path_column:
            step_options = st.multiselect(
                "站点",
                options=options["step_options"],
                default=list(options["step_options"][:1]),
                format_func=lambda option: option.label,
                placeholder="请选择一个或多个路径",
                key="qtime_step_descriptions",
            )
        with search_column:
            should_query = st.button(
                "查询",
                type="primary",
                width="stretch",
                disabled=not (step_options and selected_products),
                key="qtime_search",
            )

    signature = _filter_signature(shop, step_options, selected_products)
    if should_query:
        _run_query(
            service,
            shop=shop,
            step_options=step_options,
            selected_products=selected_products,
            signature=signature,
        )

    stored_signature = st.session_state.get(SIGNATURE_STATE_KEY)
    monitoring = st.session_state.get(RESULT_STATE_KEY)
    if stored_signature != signature or monitoring is None:
        st.info("请选择筛选条件并点击“查询”。")
        return
    details = monitoring.details
    if details.empty:
        st.info("当前筛选条件下暂无 Q-Time 数据。")
        return

    if render_qtime_decoration_admin(service, monitoring):
        _run_query(
            service,
            shop=shop,
            step_options=step_options,
            selected_products=selected_products,
            signature=signature,
        )
        st.rerun()

    render_qtime_alert_center(
        monitoring.alerts,
        total_lots=details["lot_id"].nunique(),
    )

    for index, step_option in enumerate(step_options):
        step_details = details.loc[details["step_desc"] == step_option.step_desc]
        with st.container(border=True):
            if step_details.empty:
                st.info(f"{step_option.label} 当前筛选条件下暂无 Q-Time 数据。")
                continue
            st.plotly_chart(
                build_qtime_figure(
                    step_details,
                    title=f"北极星QTime监控｜{step_option.label}",
                ),
                width="stretch",
                key=f"qtime_lot_chart_{index}",
            )


def _filter_monitoring_result(
    result: QTimeMonitoringResult,
    *,
    products: tuple[str, ...],
    step_descriptions: tuple[str, ...],
) -> QTimeMonitoringResult:
    """厂别全量结果 → 选中产品 × 站点的内存过滤（details/alerts/decoration 三帧）。

    ``decisions`` 为决策台账全量（修饰后台上传/下载语义不变），不参与过滤。
    """

    def _subset(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        mask = pd.Series(True, index=frame.index)
        if "prodcode" in frame.columns:
            mask &= frame["prodcode"].isin(products)
        if "step_desc" in frame.columns:
            mask &= frame["step_desc"].isin(step_descriptions)
        return frame.loc[mask].reset_index(drop=True)

    return replace(
        result,
        details=_subset(result.details),
        alerts=_subset(result.alerts),
        decoration=_subset(result.decoration),
    )


def _run_query(
    service: QTimeReportService,
    *,
    shop: Shop,
    step_options: list[QTimeStepOption],
    selected_products: list[str],
    signature: tuple[object, ...],
) -> None:
    try:
        file_stat = get_qtime_decision_file_stat(service.decoration_path)
        decision_mtime_ns, decision_size = (
            file_stat if file_stat is not None else MISSING_DECISION_FILE_STAT
        )
        monitoring = get_cached_shop_monitoring(
            service,
            shop=shop,
            as_of=None,
            decision_mtime_ns=decision_mtime_ns,
            decision_size=decision_size,
        )
    except ValidationError as exc:
        message = next(iter(exc.errors()), {}).get("msg", "筛选条件无效")
        st.error(str(message).removeprefix("Value error, "))
        return
    except (QTimeDataAccessError, QTimeDecorationAccessError) as exc:
        st.error(str(exc))
        return

    st.session_state[RESULT_STATE_KEY] = _filter_monitoring_result(
        monitoring,
        products=tuple(selected_products),
        step_descriptions=tuple(option.step_desc for option in step_options),
    )
    st.session_state[SIGNATURE_STATE_KEY] = signature


def _filter_signature(
    shop: Shop,
    step_options: list[QTimeStepOption],
    selected_products: list[str],
) -> tuple[object, ...]:
    return (shop, tuple(step_options), tuple(selected_products))
