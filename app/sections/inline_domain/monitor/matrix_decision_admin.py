"""Edit one indicator/product visibility decision from the matrix warning center."""

import logging

import streamlit as st

from app.manager import compliance_manager
from app.utils.admin_mode import is_admin_mode
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.infrastructure.matrix_decisions import update_matrix_decisions


def render_matrix_decision_admin() -> None:
    if not is_admin_mode(st.query_params):
        return
    from app.sections.inline_domain.monitor.alert_matrix_service import MATRIX_ROWS

    with st.expander("预警中心 · 矩阵显示台账", expanded=False):
        if st.session_state.pop("matrix_decision_saved", False):
            st.success("矩阵决定已保存。")
        names = [row.display_name for row in MATRIX_ROWS]
        products = ConfigLoader.get_enabled_products()
        if not products:
            st.caption("当前没有启用的产品。")
            return
        path = compliance_manager.CONFIG_PATH or ConfigLoader.get_compliance_config_path()
        try:
            decisions = compliance_manager.load_matrix_config(path) if path.exists() else {}
        except Exception:
            logging.exception("Matrix decision ledger unavailable")
            st.error("矩阵显示台账读取失败。")
            return
        name = st.selectbox("监控指标", names, key="matrix_decision_metric")
        product = st.selectbox("产品", products, key="matrix_decision_product")
        options = ["False", "True", "Delete"]
        current = str(decisions.get((name, product), False))
        action = st.selectbox(
            "Flag", options, index=options.index(current),
            key=f"matrix_decision_flag_{name}_{product}_{current}",
            help="False 按当前预警显示，True 显示达标，Delete 隐去该单元格。",
        )
        if st.button("保存矩阵决定", key="save_matrix_decision"):
            try:
                update_matrix_decisions(
                    path, names, products, edit=(name, product, parse_decoration_flag(action)),
                    expected=parse_decoration_flag(current),
                )
            except ValueError as exc:
                st.error(str(exc))
            except Exception:
                logging.exception("Matrix decision could not be saved")
                st.error("保存失败，请检查台账是否可写后重试。")
            else:
                st.session_state["matrix_decision_saved"] = True
                st.rerun()
