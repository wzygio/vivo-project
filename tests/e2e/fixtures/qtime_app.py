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

from app.sections.indicator_domain.qtime.dashboard import render_qtime_dashboard
from src.indicator_domain.application.qtime.dtos import QTimeStepOption
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.application.qtime.service import QTimeMonitoringResult
from src.indicator_domain.core.qtime.alerts import build_qtime_alerts
from src.indicator_domain.core.qtime.decoration import apply_qtime_decoration

# fixture 假数据覆盖的产品集合；products=()（全产品）时返回全部
FIXTURE_PRODUCTS = ("M626", "M678")


class FixtureQTimeService:
    @property
    def decoration_path(self) -> Path:
        return Path("resources/indicator_domain/qtime/qtime_oos_decoration.xlsx")

    def get_filter_options(
        self,
        shop: str,
    ) -> dict[str, tuple[QTimeStepOption, ...]]:
        paths = {
            "ARRAY": (
                QTimeStepOption("M3_DE->M3_STR", "15500", "15600"),
                QTimeStepOption("PSI_ELA->PSI_PHT", "11300", "11400"),
                QTimeStepOption("Shipping->Cutting", "2X999", "31000"),
            ),
            "OLED": (QTimeStepOption("OLED_OUT->OLED_IN", "21100", "21200"),),
            "TP": (QTimeStepOption("TP_OUT->TP_IN", "31000", "31100"),),
        }
        return {
            "step_options": paths[shop],
        }

    def get_current_monitoring(
        self,
        *,
        shop: str,
        step_descriptions: tuple[str, ...],
        products: tuple[str, ...] = (),
        as_of=None,
    ) -> QTimeMonitoringResult:
        # 新管线恒以 products=()（全产品）调用；非空 products 按调用方过滤语义返回
        requested = tuple(products) or FIXTURE_PRODUCTS
        unknown = [product for product in requested if product not in FIXTURE_PRODUCTS]
        if unknown:
            raise AssertionError(f"fixture 不含这些产品线的假数据: {unknown}")
        if shop == "TP":
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
        raw_details = pd.DataFrame(
            [
                {
                    "step_desc": step_description,
                    "lot_id": lot_id,
                    "prod_qty": 1,
                    "sub_prod_type": "P",
                    "f_step": "15500",
                    "t_step": "15600",
                    "q_spec": 1.0,
                    "wait_time": wait_time,
                    "timekey": f"20260802{index:02d}0000",
                    "shop": shop,
                    "prodcode": prodcode,
                }
                for prodcode in requested
                for step_description in step_descriptions
                if step_description != "Shipping->Cutting"
                for index, (lot_id, wait_time) in enumerate(
                    zip(lot_ids, wait_times, strict=True), start=1
                )
            ]
        )
        decisions = pd.DataFrame(
            [
                {
                    "prodcode": prodcode,
                    "step_desc": step_descriptions[0],
                    "lot_id": "L3MY67005AA",
                    "timekey": "20260802020000",
                    "flag": False,
                }
                for prodcode in requested
            ]
        )
        decorated = apply_qtime_decoration(raw_details, decisions)
        return QTimeMonitoringResult(
            details=decorated.details,
            alerts=build_qtime_alerts(decorated.decoration),
            decoration=decorated.decoration,
            decisions=decisions,
            decoration_path=Path("resources/indicator_domain/qtime/qtime_oos_decoration.xlsx"),
        )

    def update_decisions(self, file_bytes: bytes):
        raise AssertionError("Fixture tests do not upload a decoration workbook")


st.set_page_config(page_title="Q-Time E2E", layout="wide")
render_qtime_dashboard(FixtureQTimeService())
