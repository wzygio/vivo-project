"""Isolated Streamlit harness for IJP overflow UI verification."""

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

from app.sections.qtime_domain.ijp_dashboard import render_ijp_dashboard
from src.qtime_domain.application.ijp.errors import IjpDataAccessError
from src.qtime_domain.core.ijp_overflow import (
    IJP_EQUIPMENTS,
    IJP_LINES,
    IJP_RS_CODES,
    PANEL_LOCATIONS,
)

_SAFE_ERROR = "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"


def _image(suffix: str, panel: str = "L3N464E03182CA") -> str:
    return (
        "C/VIEW/2W4A9/3CTV01/L3N4/64/E03/SOURCE/L3N464E03182.IMG/"
        f"{panel}{suffix}_2W400_68_PT_20260803_030008_FVG_C3DM1_RS.JPG"
    )


def _details() -> pd.DataFrame:
    rows = [
        ("2026-08-31 08:00:00", "M626", "L3N464E03182", "3CEE01-IK2-PR1", "C3DM1", "B0", 0.667),
        ("2026-08-31 08:05:00", "M626", "L3N464E03182", "3CEE01-IK2-PR1", "C3DM1", "B0", 0.667),
        ("2026-08-31 08:10:00", "M626", "L3N464E03182", "3CEE01-IK2-PR1", "C3RA1", "HT1", 0.333),
        ("2026-08-31 09:00:00", "M626", "L3N464E03183", "3CEE02-IK2-PR1", "C3DM2", "LT", 0.5),
        ("2026-08-31 09:30:00", "M626", "L3N464E03183", "3CEE02-IK2-PR1", "C3BH1", "HB2", 0.5),
        ("2026-09-01 06:00:00", "M678", "L3N464E03184", "3CEE04-IKT-PRT", "C3DM3", "T0", 1.0),
    ]
    records = []
    for print_time, productcode, glass_id, printer, rs_code, suffix, ratio in rows:
        panel = f"{glass_id}CA"
        image_name = _image(suffix, panel)
        records.append(
            {
                "print_time": print_time,
                "productcode": productcode,
                "glass_id": glass_id,
                "printer": printer,
                "panel_id": panel,
                "image_url": f"http://10.73.17.41/IMG_WEB/V3/{image_name}",
                "panel_location": _fixture_location(rs_code, suffix),
                "rs_code": rs_code,
                "code_ratio": ratio,
            }
        )
    return pd.DataFrame(records)


def _fixture_location(rs_code: str, suffix: str) -> str:
    mapping = {
        "B0": "BOTTOM",
        "HT1": "KONGTOP",
        "LT": "LEFTTOP",
        "HB2": "KONGBOTTOM",
        "T0": "TOP",
    }
    return mapping[suffix]


def _ratios() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": [
                "2026-08-30", "2026-08-30",
                "2026-08-31", "2026-08-31", "2026-08-31",
                "2026-09-01",
            ],
            "rs_code": ["C3DM1", "C3RA1", "C3DM1", "C3RA1", "C3BH1", "C3DM3"],
            "code_num": [2, 1, 2, 1, 1, 1],
            "ratio": [0.667, 0.333, 0.5, 0.25, 0.25, 1.0],
        }
    )


class FixtureIjpService:
    """正常/空/错误三分支：M678 触发错误，CODE=C3BH2 触发空结果。"""

    def get_filter_options(
        self,
        start_time,
        end_time,
        product_codes=(),
        picis=(),
    ) -> dict[str, tuple[str, ...]]:
        return {
            "product_codes": ("M626", "M678"),
            "product_names": ("PROD-B",) if product_codes == ("M678",) else ("PROD-A", "PROD-B"),
            "sub_prod_types": ("E", "P"),
            "picis": ("LOT1", "LOT2"),
            "cycles": ("CYC1", "CYC2"),
            "lines": IJP_LINES,
            "equipments": IJP_EQUIPMENTS,
            "codes": IJP_RS_CODES,
            "panel_locations": PANEL_LOCATIONS,
        }

    def get_daily_ratios(self, query) -> pd.DataFrame:
        self._guard(query)
        if query.codes == ("C3BH2",):
            return pd.DataFrame(columns=["day", "rs_code", "code_num", "ratio"])
        return _ratios()

    def get_details(self, query) -> pd.DataFrame:
        self._guard(query)
        if query.codes == ("C3BH2",):
            return pd.DataFrame()
        details = _details()
        if query.product_codes:
            details = details[details["productcode"].isin(query.product_codes)]
        if query.panel_locations:
            wanted = set(query.panel_locations)
            normalized = details["panel_location"].where(
                ~details["panel_location"].str.startswith("BOTTOM"), "BOTTOM"
            )
            details = details[normalized.isin(wanted)]
        return details.reset_index(drop=True)

    @staticmethod
    def _guard(query) -> None:
        if "M678" in query.product_codes:
            raise IjpDataAccessError(_SAFE_ERROR)


st.set_page_config(page_title="IJP溢流监控 E2E", layout="wide")
render_ijp_dashboard(FixtureIjpService())
