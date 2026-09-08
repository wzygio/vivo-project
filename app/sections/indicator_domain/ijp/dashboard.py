"""Presentation helpers and sections for the IJP overflow report page."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from pydantic import ValidationError
import streamlit as st

from app.charts.indicator_domain.ijp.chart import build_ijp_daily_figure
from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.application.ijp.service import IjpReportService

TABLE_COLUMN_MAP = {
    "print_time": "Print Time",
    "productcode": "ProductCode",
    "glass_id": "Glass ID",
    "printer": "Printer",
    "panel_id": "Panel ID",
    "image_url": "原图",
    "panel_location": "Panel Location",
    "code_ratio": "CODE_RATIO",
}
RESULT_STATE_KEY = "ijp_report_result"
DETAIL_LIMIT = 5000
CHARTS_PER_ROW = 3


def build_ijp_table(details: pd.DataFrame) -> pd.DataFrame:
    """Return the detail table used by the report, with a Total code_ratio row."""
    table = (
        details.reindex(columns=list(TABLE_COLUMN_MAP))
        .rename(columns=TABLE_COLUMN_MAP)
        .copy()
    )
    if table.empty:
        table.insert(0, "No\n序号", pd.Series(dtype="object"))
        return table
    table.insert(0, "No\n序号", range(1, len(table) + 1))
    total = {column: "" for column in table.columns}
    total["No\n序号"] = "Total"
    total["CODE_RATIO"] = round(
        pd.to_numeric(table["CODE_RATIO"], errors="coerce").sum(), 3
    )
    return pd.concat([table, pd.DataFrame([total])], ignore_index=True)


def render_ijp_dashboard(service: IjpReportService) -> None:
    """Render filters, gated query results, and explicit operational states."""
    st.subheader("OLED IJP 溢流监控", anchor=False, text_alignment="center")
    start_time, end_time = service.get_reporting_window()

    with st.container(border=True):
        st.caption(
            f"数据范围：{start_time:%Y/%m/%d} 至 {end_time:%Y/%m/%d}"
            "（固定为上月 1 日至今天）"
        )

        try:
            options = service.get_filter_options(
                tuple(st.session_state.get("ijp_product_codes", [])),
            )
        except IjpDataAccessError as exc:
            st.error(str(exc))
            return

        product_column, line_column, code_column, pici_column = st.columns(4)
        with product_column:
            _retain_available_multiselect_values(
                "ijp_product_codes",
                options["product_codes"],
            )
            product_codes = st.multiselect(
                "产品型号", options=options["product_codes"], key="ijp_product_codes"
            )
        with line_column:
            lines = st.multiselect("线体", options=options["lines"], key="ijp_lines")
        with code_column:
            codes = st.multiselect("CODE", options=options["codes"], key="ijp_codes")

        with pici_column:
            picis = st.multiselect("批次", options=options["picis"], key="ijp_picis")
        should_query = st.button(
            "查询",
            type="primary",
            width="stretch",
            key="ijp_search",
        )

    signature = _filter_signature(
        start_time,
        end_time,
        product_codes,
        lines,
        codes,
        picis,
    )
    if should_query:
        _run_query(
            service,
            start_time=start_time,
            end_time=end_time,
            product_codes=product_codes,
            lines=lines,
            codes=codes,
            picis=picis,
            signature=signature,
        )

    stored = st.session_state.get(RESULT_STATE_KEY)
    if stored is None or stored["signature"] != signature:
        st.info("请选择筛选条件并点击“查询”。")
        return
    details = stored["details"]
    if details.empty:
        st.info("当前筛选条件下暂无 IJP 溢流数据。")
        return

    ratios = stored["ratios"]
    if not ratios.empty:
        _render_grouped_daily_charts(ratios)

    if len(details) >= stored["limit"]:
        st.caption(f"明细仅展示前 {stored['limit']} 行（已截断），请缩小筛选范围。")

    table = build_ijp_table(details)
    styled_table = table.style.set_properties(**{"text-align": "center"}).apply(
        _zebra_row,
        axis=1,
    )
    st.dataframe(
        styled_table,
        width="stretch",
        hide_index=True,
        height=min(620, 42 + len(table) * 35),
        column_config={"原图": st.column_config.LinkColumn("原图", display_text="原图")},
    )


def _run_query(
    service: IjpReportService,
    *,
    start_time: datetime,
    end_time: datetime,
    product_codes: list[str],
    lines: list[str],
    codes: list[str],
    picis: list[str],
    signature: tuple[object, ...],
) -> None:
    try:
        query = IjpQuery(
            start_time=start_time,
            end_time=end_time,
            product_codes=tuple(product_codes),
            lines=tuple(lines),
            codes=tuple(codes),
            picis=tuple(picis),
            detail_limit=DETAIL_LIMIT,
        )
        ratios = service.get_daily_ratios(query)
        details = service.get_details(query)
    except ValidationError as exc:
        message = next(iter(exc.errors()), {}).get("msg", "筛选条件无效")
        st.error(str(message).removeprefix("Value error, "))
        return
    except IjpDataAccessError as exc:
        st.error(str(exc))
        return

    st.session_state[RESULT_STATE_KEY] = {
        "signature": signature,
        "details": details,
        "ratios": ratios,
        "limit": query.detail_limit,
    }


def _filter_signature(*values: object) -> tuple[object, ...]:
    return tuple(tuple(value) if isinstance(value, list) else value for value in values)


def _render_grouped_daily_charts(ratios: pd.DataFrame) -> None:
    required = {"productcode", "line", "printer"}
    grouped = ratios.dropna(subset=list(required))
    for product_index, (product, product_frame) in enumerate(
        grouped.groupby("productcode", sort=True)
    ):
        with st.expander(f"产品：{product}", expanded=True):
            for line_index, (line, line_frame) in enumerate(
                product_frame.groupby("line", sort=True)
            ):
                with st.expander(f"线体：{line}", expanded=True):
                    printers = tuple(sorted(line_frame["printer"].unique()))
                    for row_index, printer_row in enumerate(
                        _chunked(printers, CHARTS_PER_ROW)
                    ):
                        columns = st.columns(len(printer_row))
                        for column, printer_index, printer in zip(
                            columns,
                            range(len(printer_row)),
                            printer_row,
                            strict=False,
                        ):
                            printer_frame = line_frame[
                                line_frame["printer"] == printer
                            ]
                            with column:
                                st.plotly_chart(
                                    build_ijp_daily_figure(
                                        printer_frame,
                                        title=str(printer),
                                    ),
                                    width="stretch",
                                    key=(
                                        "ijp_daily_chart_"
                                        f"{product_index}_{line_index}_"
                                        f"{row_index}_{printer_index}"
                                    ),
                                )


def _chunked(values: tuple[str, ...], size: int) -> tuple[tuple[str, ...], ...]:
    return tuple(
        values[index : index + size] for index in range(0, len(values), size)
    )


def _retain_available_multiselect_values(
    key: str,
    options: tuple[str, ...],
) -> None:
    """Drop stale widget values that are no longer present in its option set."""
    current = tuple(st.session_state.get(key, ()))
    available = set(options)
    retained = [value for value in current if value in available]
    if list(current) != retained:
        st.session_state[key] = retained


def _zebra_row(row: pd.Series) -> list[str]:
    color = "#dbeeff" if row.name % 2 else "#f7fbff"
    return [f"background-color: {color}" for _ in row]
