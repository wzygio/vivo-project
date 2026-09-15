"""Controlled browser scenarios using the real section and shared page header.

streamlit run tests/e2e/fixtures/iqc_evaporation_app.py --server.port=8518
"""

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from app.components.page_header import render_page_header
from app.sections.iqc_domain.eva_materials.evaporation_dashboard import render_evaporation_dashboard
from app.utils.app_setup import AppSetup
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService, IqcSourceUnavailable
from tests.unit.test_iqc_evaporation import source_frame


class ScenarioSource:
    def read_report(self, start, end):
        if st.query_params.get("scenario") == "failure":
            raise IqcSourceUnavailable("private SQL connection details")
        frame = pd.concat([source_frame()] * 205, ignore_index=True)
        frame['prod_code'] = 'M626'
        frame["iqc_result"] = ["OK"] * 200 + ["NG"] * 5
        frame["coa_result"] = ["NG"] * 200 + [None] * 5
        return pd.concat([
            frame, source_frame().assign(prod_code='M678', iqc_1=12.345),
            source_frame().assign(iqc_1=999.999),
        ], ignore_index=True)


st.set_page_config(page_title="IQC-蒸镀材料", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()
render_page_header(title="IQC-蒸镀材料", show_product_filter=False)
render_evaporation_dashboard(EvaporationReportService(ScenarioSource()))
