"""Interactive Code analysis isolated from the dashboard's data and alerts."""

import pandas as pd
import streamlit as st

from app.components.code_selector import DEFAULT_MONTHLY_RATE_THRESHOLD, create_group_batch_selection_ui
from app.sections.yield_domain.yield_dashboard import render_code_compact_expanders


@st.fragment
def render_code_analysis_section(
    master_df: pd.DataFrame,
    *,
    warning_lines: dict | None,
    mwd_code_data: dict,
    lot_data: dict,
    sheet_data: dict,
    mapping_data: pd.DataFrame | None,
    hotspot_scripts: list,
    product_code: str,
    mapping_layout: dict | None = None,
    rate_threshold: float = DEFAULT_MONTHLY_RATE_THRESHOLD,
    detail_loader=None,
    data_signature: str = '',
) -> None:
    """Filter and render the payload supplied by the latest full dashboard run.

    Widget interactions rerun only this section. Product changes and header
    refreshes remain full app runs and supply updated data to the fragment.
    """
    st.subheader("2️⃣ 入库不良率分析 (Code Level)")
    selection = create_group_batch_selection_ui(
        source_data=master_df,
        key_prefix="unified_focus",
        rate_threshold=rate_threshold,
    )
    selected_groups = selection.get("groups", [])
    codes_by_group = selection.get("codes_by_group", {})
    if not selected_groups or not codes_by_group:
        st.info("请至少选择一个 Defect Group 和 Defect Code。")
        return
    if not selection.get("should_render", False):
        st.info("当前筛选条件尚未查询。")
        return

    if detail_loader is not None:
        from app.sections.yield_domain.lazy_details import render_lazy_code_groups
        pairs = [(str(group), str(code)) for group in selected_groups for code in codes_by_group.get(group, [])]
        render_lazy_code_groups(
            pairs=pairs, warning_lines=warning_lines or {}, mwd_code_data=mwd_code_data,
            lot_data=lot_data, detail_loader=detail_loader, hotspot_scripts=hotspot_scripts,
            product_code=product_code, mapping_layout=mapping_layout,
            data_signature=data_signature, key_prefix='yield_manual', first_expanded=True,
        )
        return

    render_code_compact_expanders(
        selected_groups=selected_groups,
        codes_by_group=codes_by_group,
        warning_lines=warning_lines,
        mwd_code_data=mwd_code_data,
        lot_data=lot_data,
        sheet_data=sheet_data,
        mapping_data=mapping_data,
        hotspot_scripts=hotspot_scripts,
        product_code=product_code,
        mapping_layout=mapping_layout,
    )
