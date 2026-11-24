"""Q-Time decoration workbook download and upload UI."""

from __future__ import annotations

import streamlit as st
import pandas as pd
from collections.abc import Callable

from app.utils.admin_mode import is_admin_mode

from src.indicator_domain.application.qtime.decoration_service import (
    build_qtime_decoration_workbook,
)
from src.indicator_domain.application.qtime.errors import QTimeDecorationAccessError
from src.indicator_domain.application.qtime.service import (
    QTimeMonitoringResult,
    QTimeReportService,
)
from src.indicator_domain.core.qtime.decoration import DELETE_ACTION


def render_qtime_decoration_admin(
    service: QTimeReportService,
    result: QTimeMonitoringResult | None,
    *, on_saved: Callable[[], None],
) -> None:
    """Update the decision before the fragment reloads its chart and alert data."""
    feedback_key = "qtime_decision_feedback"

    def save(payload: bytes) -> None:
        if not is_admin_mode():
            return
        try:
            outcome = service.update_decisions(payload)
        except (OSError, QTimeDecorationAccessError):
            st.session_state[feedback_key] = ("error", "Q-Time 决策台账保存失败。")
            return
        st.session_state[feedback_key] = (outcome.status, outcome.message)
        if outcome.status == "success":
            on_saved()

    with st.expander("开发者后台：Q-Time 超规数据修饰", expanded=False):
        feedback = st.session_state.pop(feedback_key, None)
        if feedback:
            if feedback[0] == "success":
                st.toast(feedback[1], icon=":material/check_circle:")
            else:
                st.error(feedback[1])
        st.caption(
            f"flag 支持 True（修饰）、False（保留真实超规预警）、{DELETE_ACTION}（删除记录）。"
            "修改下载文件中的“决策台账”后上传，并保留版本工作表。未提交的业务键保留原决定。"
            "旧版表修改已有决定时请只提交一个业务键。"
        )
        try:
            decisions = service.load_decisions() if hasattr(service, 'load_decisions') else (
                result.decisions if result is not None else pd.DataFrame())
        except QTimeDecorationAccessError:
            st.error("Q-Time 决策台账暂时无法读取。")
            return
        download_column, upload_column = st.columns([1, 1.2])
        with download_column:
            st.markdown("#### 下载修饰表")
            st.download_button(
                "下载修饰表",
                data=build_qtime_decoration_workbook(
                    result.decoration if result is not None else pd.DataFrame(),
                    decisions,
                ),
                file_name="qtime_oos_decoration.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="qtime_oos_decoration_download",
                width="stretch",
                on_click="ignore",
            )
        with upload_column:
            st.markdown("#### 上传修饰表")
            uploaded = st.file_uploader(
                "上传包含决策台账的 Excel",
                type=["xlsx"],
                key="qtime_oos_decoration_upload",
                label_visibility="collapsed",
            )
            if uploaded is not None:
                st.button(
                    "保存决策并刷新", type="primary", key="qtime_oos_decoration_upload_btn",
                    width="stretch", on_click=save, args=(uploaded.getvalue(),),
                )
