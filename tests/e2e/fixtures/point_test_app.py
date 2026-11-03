"""Actual page with explicit offline scenarios and an opt-in workbook scenario."""

from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import streamlit as st

from app.components import page_header
from app.sections.indicator_domain.point_test import dashboard
from app.utils.app_setup import AppSetup
from src.indicator_domain.application.point_test.service import PointTestReportService
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.shared_kernel.config import ConfigLoader
from tests.unit.indicator_domain.point_test.test_report import MemorySource, summary_rows


def forbid_database(*args, **kwargs):
    raise AssertionError("Point test must never connect to a database")


class FixtureSource(MemorySource):
    def read_sheets(self):
        if mode == "failure":
            raise RuntimeError("PRIVATE_POINT_TEST_FAILURE admin=true")
        if mode == "empty":
            return {"Z576-detail": pd.DataFrame(), "Z576-summary": pd.DataFrame()}
        source = dict(self.sheets)
        source["M626-detail"] = source["Z576-detail"].copy()
        source["M626-detail"].iloc[1, 3] = 7.5
        source["M626-summary"] = pd.DataFrame(summary_rows("M"))
        return source


AppSetup.initialize_app = staticmethod(lambda: None)
DatabaseManager.__init__ = forbid_database
ConfigLoader.get_enabled_products = classmethod(lambda cls: ["Z576", "M626"])
ConfigLoader.get_product_annotations = classmethod(lambda cls: {"M626": "测试注释"})
page_header.render_page_header = lambda **kwargs: st.title(kwargs["title"])
mode = st.selectbox("验证场景", ["normal", "empty", "failure", "live"], key="fixture_point_mode")
render_dashboard = dashboard.render_point_test_dashboard
dashboard.render_point_test_dashboard = lambda: render_dashboard(
    None if mode == "live" else PointTestReportService(FixtureSource()),
)
try:
    runpy.run_path(str(ROOT / "app/pages/密集点测试报表.py"), run_name="__main__")
finally:
    dashboard.render_point_test_dashboard = render_dashboard
