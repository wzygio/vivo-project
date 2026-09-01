"""Presentation helpers and sections for the Q-Time report page."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pydantic import ValidationError
import streamlit as st

from app.charts.qtime_domain.qtime_chart import build_qtime_figure
from src.qtime_domain.application.dtos import QTimeQuery, Shop
from src.qtime_domain.application.errors import QTimeDataAccessError
from src.qtime_domain.application.qtime_service import QTimeReportService


TABLE_COLUMN_MAP = {
    "step_desc": "QTime监控",
    "lot_id": "LotID\n批次号",
    "prod_qty": "ProductQTY\n产品数量",
    "sub_prod_type": "ProductionType\n产品类型",
    "f_step": "FromOperation\nFrom站点",
    "t_step": "ToOperation\nTo站点",
    "q_spec": "T_TimeMeasure\nQ_Time标准",
    "wait_time": "WaitTime\n等待时长",
}
RESULT_STATE_KEY = "qtime_report_result"
SIGNATURE_STATE_KEY = "qtime_report_signature"


def build_qtime_table(details: pd.DataFrame) -> pd.DataFrame:
    """Return the stable bilingual detail table used by the report."""
    source_columns = list(TABLE_COLUMN_MAP)
    table = details.reindex(columns=source_columns).rename(columns=TABLE_COLUMN_MAP).copy()
    table.insert(0, "No\n序号", range(1, len(table) + 1))
    return table


def render_qtime_dashboard(service: QTimeReportService) -> None:
    """Render filters, gated query results, and explicit operational states."""
    st.subheader("北极星QTime监控", anchor=False, text_alignment="center")
    default_end = datetime.now().replace(second=0, microsecond=0)
    default_start = default_end - timedelta(days=30)

    with st.container(border=True):
        shop_column, start_column, end_column = st.columns([0.8, 1.2, 1.2])
        with shop_column:
            shop = st.selectbox(
                "厂别",
                options=("ARRAY", "OLED", "TP"),
                key="qtime_shop",
            )
        with start_column:
            start_time = st.datetime_input(
                "开始时间",
                value=default_start,
                key="qtime_start_time",
                width="stretch",
            )
        with end_column:
            end_time = st.datetime_input(
                "结束时间（排他）",
                value=default_end,
                key="qtime_end_time",
                width="stretch",
            )

        try:
            options = service.get_filter_options(shop)
        except QTimeDataAccessError as exc:
            st.error(str(exc))
            return

        product_column, path_column, search_column = st.columns([1.4, 2.2, 0.8])
        with product_column:
            products = st.multiselect(
                "产品（留空表示全部）",
                options=options["products"],
                key="qtime_products",
            )
        with path_column:
            step_desc = st.selectbox(
                "站点 From-To",
                options=options["step_descriptions"],
                index=0 if options["step_descriptions"] else None,
                placeholder="当前厂别暂无路径",
                key="qtime_step_desc",
            )
        with search_column:
            st.write("")
            st.write("")
            should_query = st.button(
                "查询",
                type="primary",
                width="stretch",
                disabled=not bool(step_desc),
                key="qtime_search",
            )

    signature = _filter_signature(start_time, end_time, shop, step_desc, products)
    if should_query:
        _run_query(
            service,
            start_time=start_time,
            end_time=end_time,
            shop=shop,
            step_desc=step_desc,
            products=products,
            signature=signature,
        )

    stored_signature = st.session_state.get(SIGNATURE_STATE_KEY)
    details = st.session_state.get(RESULT_STATE_KEY)
    if stored_signature != signature or details is None:
        st.info("请选择筛选条件并点击“查询”。")
        return
    if details.empty:
        st.info("当前筛选条件下暂无 Q-Time 数据。")
        return

    with st.container(border=True):
        st.plotly_chart(
            build_qtime_figure(details),
            width="stretch",
            key="qtime_lot_chart",
        )

    table = build_qtime_table(details)
    styled_table = table.style.set_properties(**{"text-align": "center"}).apply(
        _zebra_row,
        axis=1,
    )
    st.dataframe(
        styled_table,
        width="stretch",
        hide_index=True,
        height=min(620, 42 + len(table) * 35),
    )


def _run_query(
    service: QTimeReportService,
    *,
    start_time: datetime,
    end_time: datetime,
    shop: Shop,
    step_desc: str | None,
    products: list[str],
    signature: tuple[object, ...],
) -> None:
    try:
        query = QTimeQuery(
            start_time=start_time,
            end_time=end_time,
            shop=shop,
            step_desc=step_desc or "",
            products=tuple(products),
        )
        details = service.get_report(query)
    except ValidationError as exc:
        message = next(iter(exc.errors()), {}).get("msg", "筛选条件无效")
        st.error(str(message).removeprefix("Value error, "))
        return
    except QTimeDataAccessError as exc:
        st.error(str(exc))
        return

    st.session_state[RESULT_STATE_KEY] = details
    st.session_state[SIGNATURE_STATE_KEY] = signature


def _filter_signature(
    start_time: datetime,
    end_time: datetime,
    shop: Shop,
    step_desc: str | None,
    products: list[str],
) -> tuple[object, ...]:
    return (start_time, end_time, shop, step_desc, tuple(products))


def _zebra_row(row: pd.Series) -> list[str]:
    color = "#dbeeff" if row.name % 2 else "#f7fbff"
    return [f"background-color: {color}" for _ in row]
