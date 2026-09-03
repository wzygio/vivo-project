"""自动预警矩阵 E2E 的隔离 Streamlit harness（playwright-cli 用，不触 DB/生产文件）。

两种模式（st.query_params["mode"]）：

- 默认（矩阵交互）：直接构造含四态的假 payload（2 产品 × 8 行），详情 loader
  经 ``render_alert_matrix_detail(loaders=...)`` 注入假实现，避免触库；
  覆盖四态渲染、点击 🔴 懒加载详情、点击非 🔴 说明文案。
- ``?mode=cache``（缓存重建）：走真实的 ``_cached_alert_matrix_payload``
  （st.cache_data）+ 假 context 工厂，页面展示 ``generated_at`` 作为构建令牌，
  并提供「刷新缓存并重建矩阵」按钮 clear 缓存；E2E 据此验证
  "普通 rerun 命中缓存 / 刷新后矩阵重建"。
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

project_root = Path(__file__).resolve().parents[3]
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
    AlertMatrixContext,
)

st.set_page_config(page_title="alert-matrix-e2e", layout="wide")

PRODUCTS = ["M678", "Z571"]

ALERT_CELLS = [
    ("qtime_sheet_oos", "M678"),
    ("spc_sheet_oos", "M678"),
    ("spc_cpk_trend", "M678"),
    ("yield_trend_fluctuation", "M678"),
]
NO_DATA_CELL = ("ctq_sheet_oos", "M678")
ERROR_CELL = ("yield_lot_oos", "Z571")
ERROR_MESSAGE = "修饰工作簿读取失败，请确认文件可正常打开且未被锁定"
NO_DATA_MESSAGE = "修饰工作簿不存在"


def _cell(row_key: str, prod: str, state: str, message: str = "") -> dict[str, str]:
    return {"state": state, "detail_key": f"{row_key}|{prod}", "message": message}


def _build_fixture_payload() -> dict:
    cells: dict[tuple[str, str], dict[str, str]] = {}
    for row in MATRIX_ROWS:
        for prod in PRODUCTS:
            cells[(row.row_key, prod)] = _cell(row.row_key, prod, CELL_STATE_OK)
    for row_key, prod in ALERT_CELLS:
        cells[(row_key, prod)] = _cell(row_key, prod, CELL_STATE_ALERT)
    cells[NO_DATA_CELL] = _cell(*NO_DATA_CELL, CELL_STATE_NO_DATA, NO_DATA_MESSAGE)
    cells[ERROR_CELL] = _cell(*ERROR_CELL, CELL_STATE_ERROR, ERROR_MESSAGE)
    return {
        "products": PRODUCTS,
        "rows": [
            {
                "row_key": row.row_key,
                "display_name": row.display_name,
                "module_group": row.module_group,
                "time_scope": row.time_scope,
            }
            for row in MATRIX_ROWS
        ],
        "cells": cells,
        "signature": "fixture-e2e-sig",
        "generated_at": "2026-09-02T10:00:00",
        "reference_week": {
            "label": "2026-W35",
            "start": "2026-08-24",
            "end": "2026-08-31",
        },
    }


def _qtime_detail_loader(prod_code: str, reference_date: date) -> dict:
    """qtime 行假详情 loader：纯 DataFrame，渲染走真实 qtime 预警中心与图像管线。"""
    alerts = pd.DataFrame(
        [
            {
                "f_step": "15500",
                "t_step": "15600",
                "step_desc": "M3_DE->M3_STR",
                "timekey": "20260826020000",
                "lot_id": "L3MY67005AA",
                "q_spec": 1.0,
                "wait_time": 1.5,
                "over_hours": 0.5,
                "prodcode": prod_code,
            }
        ]
    )
    details = pd.DataFrame(
        [
            {
                "step_desc": "M3_DE->M3_STR",
                "lot_id": "L3MY67005AA",
                "q_spec": 1.0,
                "wait_time": 1.5,
                "timekey": "20260826020000",
                "prodcode": prod_code,
            }
        ]
    )
    return {
        "kind": "qtime",
        "alerts_df": alerts,
        "details_df": details,
        "total_lots": 1,
    }


if st.query_params.get("mode") == "cache":
    # 缓存重建模式：真实 st.cache_data 包装 + 假 context（全部单元格 no_data）
    from app.sections.inline_domain.monitor.alert_matrix_cache import (
        _cached_alert_matrix_payload,
    )

    def _context_factory() -> AlertMatrixContext:
        return AlertMatrixContext(
            reference_date=date(2026, 9, 2),
            inline_resource_dir=Path("output/tmp/alert-matrix-e2e-nonexistent"),
        )

    payload = _cached_alert_matrix_payload(
        tuple(PRODUCTS),
        "2026-08-31",
        "fixture-e2e-cache-sig",
        _context_factory,
    )
    st.caption(f"matrix-build-token: {payload['generated_at']}")
    render_alert_matrix_section(payload)

    def _clear_matrix_cache() -> None:
        _cached_alert_matrix_payload.clear()

    st.button(
        "刷新缓存并重建矩阵",
        key="matrix_fixture_refresh",
        on_click=_clear_matrix_cache,
    )
else:
    payload = _build_fixture_payload()
    render_alert_matrix_section(payload)
    render_alert_matrix_detail(
        payload,
        step_desc_map=None,
        loaders={"qtime_sheet_oos": _qtime_detail_loader},
    )
