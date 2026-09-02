"""Q-Time decoration workbook download and upload UI."""

from __future__ import annotations

import streamlit as st

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
    result: QTimeMonitoringResult,
) -> bool:
    """Render admin controls and return True after a successful decision update."""
    with st.expander("开发者后台：Q-Time 超规数据修饰", expanded=False):
        st.caption(
            f"flag 支持 True（修饰）、False（保留原值并预警）、{DELETE_ACTION}（删除记录）。"
            "修改下载文件中的“决策台账”后上传。"
        )
        if result.decoration_path is not None:
            st.caption(f"修饰工作簿：{result.decoration_path}")
        download_column, upload_column = st.columns([1, 1.2])
        with download_column:
            st.markdown("#### 下载修饰表")
            st.download_button(
                "下载修饰表",
                data=build_qtime_decoration_workbook(
                    result.decoration,
                    result.decisions,
                ),
                file_name="qtime_oos_decoration.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="qtime_oos_decoration_download",
                width="stretch",
            )
        with upload_column:
            st.markdown("#### 上传修饰表")
            uploaded = st.file_uploader(
                "上传包含决策台账的 Excel",
                type=["xlsx"],
                key="qtime_oos_decoration_upload",
                label_visibility="collapsed",
            )
            if uploaded is not None and st.button(
                "确认覆盖并刷新",
                type="primary",
                key="qtime_oos_decoration_upload_btn",
                width="stretch",
            ):
                try:
                    outcome = service.update_decisions(uploaded.getvalue())
                except QTimeDecorationAccessError as exc:
                    st.error(str(exc))
                    return False
                if outcome.status == "success":
                    st.toast(outcome.message, icon=":material/check_circle:")
                    return True
                st.error(outcome.message)
    return False
