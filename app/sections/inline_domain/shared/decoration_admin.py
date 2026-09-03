"""Sheet OOS 修饰后台 UI（SPC/CTQ 共用）。

通过 ``key_prefix`` 与 ``report_name`` 参数化，session key 与各页面历史保持一致。
下载含“当前明细 + 决策台账”两个 sheet；上传只覆盖 ``<产品sheet>__flags`` 决策 sheet，
绝不触碰产品当前明细 sheet，纯逻辑见 ``sheet_oos_admin``，本模块保持薄壳。
"""

from __future__ import annotations

from contextlib import nullcontext
from io import BytesIO

import pandas as pd
import streamlit as st

from app.sections.inline_domain.shared.sheet_oos_admin import (
    build_decision_download_sheets,
    handle_decision_upload,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    DELETE_ACTION,
    get_decision_sheet_name,
)
from src.inline_domain.application.shared.sheet_oos_decoration_service import (
    SheetOosDecorationResult,
)


def excel_bytes(sheet_map: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def render_sheet_oos_decoration_admin(
    decoration_result: SheetOosDecorationResult,
    *,
    show_expander: bool = True,
    report_name: str = "SPC",
    key_prefix: str = "spc",
) -> None:
    """Render the Sheet OOS decorator, optionally inside a parent admin panel."""
    decision_sheet = decoration_result.decision_sheet or get_decision_sheet_name(
        decoration_result.decoration_sheet
    )

    container = (
        st.expander(f"开发者后台：{report_name} 超规片数据修饰", expanded=False)
        if show_expander
        else nullcontext()
    )
    with container:
        st.caption(
            f"flag 支持 True（修饰）、False（保留原值）、{DELETE_ACTION}"
            "（不显示该 Sheet 的当前参数记录）；修改“决策台账”sheet 后上传并确认，"
            "或点击页头“刷新缓存”。"
        )
        st.caption(f"修饰工作簿：{decoration_result.decoration_path}")
        st.caption(
            f"产品 sheet：{decoration_result.decoration_sheet}；决策 sheet：{decision_sheet}"
        )
        if decoration_result.refresh_reason:
            st.caption(f"本次载荷重建原因：{decoration_result.refresh_reason}")
        c_decoration, c_upload = st.columns([1, 1.2])

        with c_decoration:
            st.markdown("#### 下载修饰表")
            st.download_button(
                label="下载修饰表",
                data=excel_bytes(build_decision_download_sheets(decoration_result)),
                file_name=f"{decoration_result.decoration_sheet}_{decoration_result.decoration_path.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_oos_decoration_download",
                use_container_width=True,
            )

        with c_upload:
            st.markdown("#### 上传修饰表")
            uploaded_file = st.file_uploader(
                "上传包含决策台账 sheet 的 Excel",
                type=["xlsx"],
                key=f"{key_prefix}_oos_decoration_upload",
                label_visibility="collapsed",
            )
            if uploaded_file is not None:
                if st.button(
                    "确认覆盖并刷新",
                    type="primary",
                    key=f"{key_prefix}_oos_decoration_upload_btn",
                    use_container_width=True,
                ):
                    outcome = handle_decision_upload(
                        decoration_result, uploaded_file.getbuffer()
                    )
                    # 决策签名变化会自然触发 L2 miss，无需手动清缓存
                    if outcome.status == "success":
                        st.success(outcome.message)
                        st.rerun()
                    elif outcome.status == "unchanged":
                        st.info(outcome.message)
                    else:
                        st.error(outcome.message)
