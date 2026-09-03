"""自动预警矩阵 UI 的隔离 Streamlit harness（AppTest 用，不触 DB/文件）。

- 直接构造含四态的假 payload（2 产品 × 8 行）；
- 详情 loader 由测试经 monkeypatch 替换
  ``alert_matrix_detail.build_default_detail_loaders`` 注入；
- ``st.session_state["fixture_nonce"]`` 进入矩阵签名，隔离 st.cache_data
  的跨用例命中（同用例内多次 run 保持同 nonce 以验证缓存命中）。
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

project_root = Path(__file__).resolve().parents[6]
for import_path in (project_root, project_root / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from app.sections.inline_domain.monitor.alert_matrix import render_alert_matrix_section
from app.sections.inline_domain.monitor.alert_matrix_detail import (
    render_alert_matrix_detail,
)
from app.sections.inline_domain.monitor.alert_matrix_service import (
    CELL_STATE_ALERT,
    CELL_STATE_ERROR,
    CELL_STATE_NO_DATA,
    CELL_STATE_OK,
    MATRIX_ROWS,
)

st.set_page_config(page_title="alert-matrix-fixture", layout="wide")

PRODUCTS = ["M678", "Z571"]

# 行级状态覆盖：默认全 🟢，以下单元格覆盖为其他三态；
# alert 单元格附带 alert_factories（厂别细分切片用），yield 行不支持厂别细分。
ALERT_CELLS = [
    ("qtime_sheet_oos", "M678", ["OLED"]),
    ("spc_sheet_oos", "M678", ["ARRAY", "TP"]),
    ("spc_cpk_trend", "M678", ["TP"]),
    ("yield_trend_fluctuation", "M678", []),
]
NO_DATA_CELL = ("ctq_sheet_oos", "M678")
ERROR_CELL = ("yield_lot_oos", "Z571")
ERROR_MESSAGE = "修饰工作簿读取失败，请确认文件可正常打开且未被锁定"
NO_DATA_MESSAGE = "修饰工作簿不存在"


def _cell(
    row_key: str,
    prod: str,
    state: str,
    message: str = "",
    alert_factories: list[str] | None = None,
) -> dict:
    return {
        "state": state,
        "detail_key": f"{row_key}|{prod}",
        "message": message,
        "alert_factories": list(alert_factories or []),
    }


cells: dict[tuple[str, str], dict] = {}
for row in MATRIX_ROWS:
    for prod in PRODUCTS:
        cells[(row.row_key, prod)] = _cell(row.row_key, prod, CELL_STATE_OK)
for row_key, prod, factories in ALERT_CELLS:
    cells[(row_key, prod)] = _cell(
        row_key, prod, CELL_STATE_ALERT, alert_factories=factories
    )
cells[NO_DATA_CELL] = _cell(*NO_DATA_CELL, CELL_STATE_NO_DATA, NO_DATA_MESSAGE)
cells[ERROR_CELL] = _cell(*ERROR_CELL, CELL_STATE_ERROR, ERROR_MESSAGE)

nonce = st.session_state.get("fixture_nonce", "default")
payload = {
    "products": PRODUCTS,
    "rows": [
        {
            "row_key": row.row_key,
            "display_name": row.display_name,
            "module_group": row.module_group,
            "time_scope": row.time_scope,
            "factory_filter_supported": row.supports_factory_filter,
        }
        for row in MATRIX_ROWS
    ],
    "cells": cells,
    "signature": f"fixture-sig-{nonce}",
    "generated_at": "2026-09-02T10:00:00",
    "reference_week": {"label": "2026-W35", "start": "2026-08-24", "end": "2026-08-31"},
}

if st.session_state.get("fixture_mode") == "board":
    # 整板入口模式：验证 payload 加载失败时的 info 降级
    from app.sections.inline_domain.monitor.alert_matrix import (
        render_alert_matrix_board,
    )

    render_alert_matrix_board()
else:
    render_alert_matrix_section(payload)
    render_alert_matrix_detail(payload, step_desc_map=None)
