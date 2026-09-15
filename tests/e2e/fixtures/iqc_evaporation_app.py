"""Controlled browser scenarios using the real page; never a production route.

streamlit run tests/e2e/fixtures/iqc_evaporation_app.py --server.port=8518
"""

from pathlib import Path
import runpy
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from app.sections.iqc_domain import evaporation_dashboard
from src.iqc_domain.application.evaporation import EvaporationReportService, IqcSourceUnavailable
from tests.unit.test_iqc_evaporation import source_frame


class ScenarioSource:
    def read_report(self, start, end):
        if st.query_params.get("scenario") == "failure":
            raise IqcSourceUnavailable("private SQL connection details")
        frame = pd.concat([source_frame()] * 205, ignore_index=True)
        frame["iqc_result"] = ["OK"] * 200 + ["NG"] * 5
        frame["coa_result"] = ["NG"] * 200 + [None] * 5
        return frame


original_render = evaporation_dashboard.render_evaporation_dashboard
try:
    evaporation_dashboard.render_evaporation_dashboard = lambda: original_render(
        EvaporationReportService(ScenarioSource())
    )
    runpy.run_path(str(ROOT / "app/pages/IQC蒸镀材料报表.py"), run_name="__main__")
finally:
    evaporation_dashboard.render_evaporation_dashboard = original_render
