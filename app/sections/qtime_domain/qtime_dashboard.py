"""Presentation helpers and sections for the Q-Time report page."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas as pd
from pydantic import ValidationError
import streamlit as st

from app.charts.qtime_domain.qtime_chart import build_qtime_figure
from src.qtime_domain.application.dtos import QTimeQuery, Shop
from src.qtime_domain.application.errors import QTimeDataAccessError
from src.qtime_domain.application.qtime_service import QTimeReportService


TABLE_COLUMN_MAP = {
    "step_desc": "QTime / 监控",
    "lot_id": "LotID / 批次号",
    "prod_qty": "ProductQTY / 产品数量",
    "sub_prod_type": "ProductionType / 产品类型",
    "f_step": "FromOperation / From站点",
    "t_step": "ToOperation / To站点",
    "q_spec": "T_TimeMeasure / Q_Time标准 (H)",
    "wait_time": "WaitTime / 等待时长 (H)",
}
RESULT_STATE_KEY = "qtime_report_result"
SIGNATURE_STATE_KEY = "qtime_report_signature"


def default_date_range(today: date) -> tuple[date, date]:
    """Return the inclusive date range shown when the report first opens."""
    return today - timedelta(days=30), today


def build_date_window(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    """Convert inclusive UI dates to the repository's half-open time window."""
    return (
        datetime.combine(start_date, time.min),
        datetime.combine(end_date + timedelta(days=1), time.min),
    )


def build_qtime_table(details: pd.DataFrame) -> pd.DataFrame:
    """Return the stable bilingual detail table used by the report."""
    source_columns = list(TABLE_COLUMN_MAP)
    table = details.reindex(columns=source_columns).rename(columns=TABLE_COLUMN_MAP).copy()
    for column in (
        "T_TimeMeasure / Q_Time标准 (H)",
        "WaitTime / 等待时长 (H)",
    ):
        numeric_values = pd.to_numeric(table[column], errors="coerce")
        table[column] = numeric_values.round().astype("Int64")
    table.insert(0, "No / 序号", range(1, len(table) + 1))
    return table


def render_qtime_dashboard(service: QTimeReportService) -> None:
    """Render filters, gated query results, and explicit operational states."""
    st.subheader("北极星QTime监控", anchor=False, text_alignment="center")
    default_start, default_end = default_date_range(date.today())

    with st.container(border=True):
        shop_column, start_column, end_column = st.columns([0.8, 1.2, 1.2])
        with shop_column:
            shop = st.selectbox(
                "厂别",
                options=("ARRAY", "OLED", "TP"),
                key="qtime_shop",
            )
        with start_column:
            start_date = st.date_input(
                "开始日期",
                value=default_start,
                key="qtime_start_date",
                width="stretch",
            )
        with end_column:
            end_date = st.date_input(
                "结束日期",
                value=default_end,
                key="qtime_end_date",
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
            step_descriptions = st.multiselect(
                "站点 From-To",
                options=options["step_descriptions"],
                default=list(options["step_descriptions"][:1]),
                placeholder="请选择一个或多个路径",
                key="qtime_step_descriptions",
            )
        with search_column:
            st.write("")
            st.write("")
            should_query = st.button(
                "查询",
                type="primary",
                width="stretch",
                disabled=not bool(step_descriptions),
                key="qtime_search",
            )

    signature = _filter_signature(
        start_date,
        end_date,
        shop,
        step_descriptions,
        products,
    )
    if should_query:
        _run_query(
            service,
            start_date=start_date,
            end_date=end_date,
            shop=shop,
            step_descriptions=step_descriptions,
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

    for index, step_description in enumerate(step_descriptions):
        step_details = details.loc[details["step_desc"] == step_description]
        with st.container(border=True):
            if step_details.empty:
                st.info(f"{step_description} 当前筛选条件下暂无 Q-Time 数据。")
                continue
            st.plotly_chart(
                build_qtime_figure(
                    step_details,
                    title=f"北极星QTime监控｜{step_description}",
                ),
                width="stretch",
                key=f"qtime_lot_chart_{index}",
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
    start_date: date,
    end_date: date,
    shop: Shop,
    step_descriptions: list[str],
    products: list[str],
    signature: tuple[object, ...],
) -> None:
    try:
        start_time, end_time = build_date_window(start_date, end_date)
        query = QTimeQuery(
            start_time=start_time,
            end_time=end_time,
            shop=shop,
            step_descriptions=tuple(step_descriptions),
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
    start_date: date,
    end_date: date,
    shop: Shop,
    step_descriptions: list[str],
    products: list[str],
) -> tuple[object, ...]:
    return (start_date, end_date, shop, tuple(step_descriptions), tuple(products))


def _zebra_row(row: pd.Series) -> list[str]:
    color = "#dbeeff" if row.name % 2 else "#f7fbff"
    return [f"background-color: {color}" for _ in row]
