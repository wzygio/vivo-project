"""Presentation helpers and sections for the IJP overflow report page."""

from __future__ import annotations

from datetime import datetime, timedelta

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
    today_seven = datetime.now().replace(hour=7, minute=0, second=0, microsecond=0)
    default_end = today_seven
    default_start = today_seven - timedelta(days=1)

    with st.container(border=True):
        start_column, end_column, target_column, search_column = st.columns(
            [1.2, 1.2, 0.8, 0.6]
        )
        with start_column:
            start_time = st.datetime_input(
                "开始时间",
                value=default_start,
                key="ijp_start_time",
                width="stretch",
            )
        with end_column:
            end_time = st.datetime_input(
                "结束时间",
                value=default_end,
                key="ijp_end_time",
                width="stretch",
            )
        with target_column:
            target = st.number_input(
                "Target值（%）",
                value=None,
                min_value=0.0,
                max_value=100.0,
                placeholder="不填不画",
                key="ijp_target",
                width="stretch",
            )
        with search_column:
            st.write("")
            st.write("")
            should_query = st.button(
                "查询",
                type="primary",
                width="stretch",
                key="ijp_search",
            )

        try:
            options = service.get_filter_options(
                start_time,
                end_time,
                tuple(st.session_state.get("ijp_product_codes", [])),
                tuple(st.session_state.get("ijp_picis", [])),
            )
        except IjpDataAccessError as exc:
            st.error(str(exc))
            return

        code_column, name_column, type_column, glass_column = st.columns(4)
        with code_column:
            product_codes = st.multiselect(
                "产品型号", options=options["product_codes"], key="ijp_product_codes"
            )
        with name_column:
            product_names = st.multiselect(
                "产品名称", options=options["product_names"], key="ijp_product_names"
            )
        with type_column:
            sub_prod_types = st.multiselect(
                "工单类型", options=options["sub_prod_types"], key="ijp_sub_prod_types"
            )
        with glass_column:
            glass_ids = st.text_input(
                "GlassID（多个用逗号分隔）", key="ijp_glass_ids"
            )

        line_column, equip_column, rs_column, border_column = st.columns(4)
        with line_column:
            lines = st.multiselect("线体", options=options["lines"], key="ijp_lines")
        with equip_column:
            equipments = st.multiselect(
                "设备", options=options["equipments"], key="ijp_equipments"
            )
        with rs_column:
            codes = st.multiselect("CODE", options=options["codes"], key="ijp_codes")
        with border_column:
            panel_locations = st.multiselect(
                "边框", options=options["panel_locations"], key="ijp_panel_locations"
            )

        pici_column, cycle_column, _pad = st.columns([1, 1, 2])
        with pici_column:
            picis = st.multiselect("批次", options=options["picis"], key="ijp_picis")
        with cycle_column:
            cycles = st.multiselect(
                "Cycle", options=options["cycles"], key="ijp_cycles"
            )

    signature = _filter_signature(
        start_time,
        end_time,
        product_codes,
        product_names,
        sub_prod_types,
        glass_ids,
        lines,
        equipments,
        codes,
        panel_locations,
        picis,
        cycles,
        target,
    )
    if should_query:
        _run_query(
            service,
            start_time=start_time,
            end_time=end_time,
            product_codes=product_codes,
            product_names=product_names,
            sub_prod_types=sub_prod_types,
            glass_ids=glass_ids,
            lines=lines,
            equipments=equipments,
            codes=codes,
            panel_locations=panel_locations,
            picis=picis,
            cycles=cycles,
            target=target,
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
        with st.container(border=True):
            st.plotly_chart(
                build_ijp_daily_figure(ratios, stored["target"]),
                width="stretch",
                key="ijp_daily_chart",
            )

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
    product_names: list[str],
    sub_prod_types: list[str],
    glass_ids: str,
    lines: list[str],
    equipments: list[str],
    codes: list[str],
    panel_locations: list[str],
    picis: list[str],
    cycles: list[str],
    target: float | None,
    signature: tuple[object, ...],
) -> None:
    try:
        query = IjpQuery(
            start_time=start_time,
            end_time=end_time,
            product_codes=tuple(product_codes),
            product_names=tuple(product_names),
            sub_prod_types=tuple(sub_prod_types),
            glass_ids=glass_ids,
            lines=tuple(lines),
            equipments=tuple(equipments),
            codes=tuple(codes),
            panel_locations=tuple(panel_locations),
            picis=tuple(picis),
            cycles=tuple(cycles),
            target=target,
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
        "target": target,
        "limit": query.detail_limit,
    }


def _filter_signature(*values: object) -> tuple[object, ...]:
    return tuple(tuple(value) if isinstance(value, list) else value for value in values)


def _zebra_row(row: pd.Series) -> list[str]:
    color = "#dbeeff" if row.name % 2 else "#f7fbff"
    return [f"background-color: {color}" for _ in row]
