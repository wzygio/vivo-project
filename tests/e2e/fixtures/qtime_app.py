"""Isolated Streamlit harness for Q-Time UI verification."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

project_root = Path(__file__).resolve().parents[3]
for import_path in (project_root, project_root / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from app.sections.qtime_domain.qtime_dashboard import render_qtime_dashboard
from src.qtime_domain.application.errors import QTimeDataAccessError


class FixtureQTimeService:
    def get_filter_options(self, shop: str) -> dict[str, tuple[str, ...]]:
        paths = {
            "ARRAY": ("M3_DE->M3_STR", "Shipping->Cutting"),
            "OLED": ("OLED_OUT->OLED_IN",),
            "TP": ("TP_OUT->TP_IN",),
        }
        return {
            "products": ("M626", "M678"),
            "step_descriptions": paths[shop],
        }

    def get_report(self, query) -> pd.DataFrame:
        products = query.products or ("M626",)
        if query.step_desc == "Shipping->Cutting":
            return pd.DataFrame()
        if products == ("M678",):
            raise QTimeDataAccessError(
                "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
            )
        lot_ids = [
            "L3MY67002AC",
            "L3MY67005AA",
            "L3MY6700CAA",
            "L3MY6700HAA",
            "L3MY68001AA",
            "L3MY68003AA",
            "L3MY68008AA",
            "L3MY6800GAA",
            "L3MY6800HAA",
            "L3MY6800IAA",
            "L3MY6800PAA",
            "L3MY6800QAA",
        ]
        wait_times = [0.41, 1.26, 1.18, 0.65, 0.51, 1.40, 1.14, 0.18, 0.09, 0.67, 1.21, 0.30]
        return pd.DataFrame(
            [
                {
                    "step_desc": query.step_desc,
                    "lot_id": lot_id,
                    "prod_qty": 1,
                    "sub_prod_type": "P",
                    "f_step": "15500",
                    "t_step": "15600",
                    "q_spec": 2.5,
                    "wait_time": wait_time,
                    "timekey": f"20260802{index:02d}0000",
                    "shop": query.shop,
                    "prodcode": products[0],
                }
                for index, (lot_id, wait_time) in enumerate(
                    zip(lot_ids, wait_times, strict=True),
                    start=1,
                )
            ]
        )


st.set_page_config(page_title="Q-Time E2E", layout="wide")
render_qtime_dashboard(FixtureQTimeService())
