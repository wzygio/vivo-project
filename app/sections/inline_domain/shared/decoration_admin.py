"""Sheet OOS 修饰后台 UI（SPC/CTQ 共用）。

通过 ``key_prefix`` 与 ``report_name`` 参数化，session key 与各页面历史保持一致。
"""

from __future__ import annotations

from contextlib import nullcontext
from io import BytesIO

import pandas as pd
import streamlit as st

from src.inline_domain.core.shared.sheet_oos_decoration import (
    DELETE_ACTION,
    OOS_DECORATION_COLUMNS,
    OOS_KEY_COLUMNS,
    SheetOosDecorationResult,
)
from src.shared_kernel.utils.excel_tools import replace_workbook_sheet


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
    decoration_df = decoration_result.decoration_df
    decoration_download_df = decoration_df if not decoration_df.empty else pd.DataFrame(columns=OOS_DECORATION_COLUMNS)

    container = (
        st.expander(f"开发者后台：{report_name} 超规片数据修饰", expanded=False)
        if show_expander
        else nullcontext()
    )
    with container:
        st.caption(
            f"flag 支持 True（修饰）、False（保留原值）、{DELETE_ACTION}"
            "（不显示该 Sheet 的当前参数记录）；修改后请上传并确认刷新，"
            "或点击页头“刷新缓存”。"
        )
        st.caption(f"修饰文件：{decoration_result.decoration_path}")
        c_decoration, c_upload = st.columns([1, 1.2])

        with c_decoration:
            st.markdown("#### 下载修饰表")
            st.download_button(
                label="下载修饰表",
                data=excel_bytes({"修饰表": decoration_download_df}),
                file_name=f"{decoration_result.decoration_sheet}_{decoration_result.decoration_path.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_oos_decoration_download",
                use_container_width=True,
            )

        with c_upload:
            st.markdown("#### 上传修饰表")
            uploaded_file = st.file_uploader(
                "上传包含 flag 字段的 Excel",
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
                    try:
                        uploaded_df = pd.read_excel(BytesIO(uploaded_file.getbuffer()))
                        required_columns = {*OOS_KEY_COLUMNS, "flag"}
                        missing_columns = required_columns - set(uploaded_df.columns)
                        if missing_columns:
                            st.error(f"修饰表缺少必要字段：{', '.join(sorted(missing_columns))}")
                            return

                        replace_workbook_sheet(
                            decoration_result.decoration_path,
                            decoration_result.decoration_sheet,
                            uploaded_df,
                        )
                        st.success("修饰表已覆盖，正在刷新缓存。")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"保存修饰表失败：{exc}")
