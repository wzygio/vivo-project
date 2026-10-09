"""Independent data-refresh actions; cache refresh belongs to the page header."""

import logging
from collections.abc import Callable, Sequence

import streamlit as st

from src.inline_domain.infrastructure.monitor.excel_alarm_store import clear_alarm_workbook_cache
from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
from src.shared_kernel.config import ConfigLoader


def clear_oos_source_cache(scopes: Sequence[str]) -> None:
    for scope in scopes:
        for alarm_type in ("oos", "ooc"):
            clear_alarm_workbook_cache(scope_decoration_path(scope, alarm_type))


def clear_cpk_source_cache() -> None:
    clear_alarm_workbook_cache(
        ConfigLoader.get_domain_resource_path("inline_domain", "spc_cpk_cpm_decoration", "spc_cpk_cpm_decoration.xlsx")
    )


def render_board_refresh_controls(
    *, key: str, clear_cache: Callable[[], None], query_state_key: str,
    selection_valid: bool = True,
) -> bool:
    """Return a new query request only after successful local invalidation."""
    if st.query_params.get("admin") != "true":
        return False
    with st.container(horizontal=True):
        refresh_data = st.button(
            "刷新数据", key=f"{key}_refresh_data", disabled=not selection_valid,
            help="重新读取当前源 Excel，按本看板筛选条件更新结果。",
        )
    if not refresh_data:
        return False
    try:
        clear_cache()
    except Exception:
        logging.exception("Warning dashboard cache refresh failed: %s", key)
        st.error("刷新失败，请检查源文件是否可访问后重试。")
        return False
    st.session_state.pop(query_state_key, None)
    return True
