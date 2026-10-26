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
from src.iqc_domain.infrastructure.eva_materials.alert_workbook import EvaporationAlertWorkbook
from src.iqc_domain.infrastructure.eva_materials.product_scope import project_material_scope
from src.shared_kernel.config import ConfigLoader
from tests.unit.test_iqc_evaporation import source_frame


class ScenarioSource:
    def read_report(self, start, end, products):
        if st.query_params.get("scenario") == "failure":
            raise IqcSourceUnavailable("private SQL connection details")
        frame = pd.concat([source_frame()] * 205, ignore_index=True)
        frame.loc[140:199, 'mitemname'] = 'M626-MATERIAL'
        frame.loc[200:, 'mitemname'] = 'SHARED-MATERIAL'
        if st.query_params.get("scenario") == "empty":
            return frame.iloc[:0]
        membership = pd.DataFrame({
            'material_no': ['100024720', 'M626-MATERIAL', 'SHARED-MATERIAL', 'SHARED-MATERIAL'],
            'prod_code': ['M678', 'M626', 'M678', 'M626'],
            'batch_no': ['20260801', '20260802', '20260801', '20260802'],
        })
        return project_material_scope(frame.assign(prod_code='通用'), membership, products)


st.set_page_config(page_title="IQC-蒸镀材料", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()
render_page_header(title="IQC-蒸镀材料", show_product_filter=False)
render_evaporation_dashboard(EvaporationReportService(
    ScenarioSource(), enabled_products=ConfigLoader.get_enabled_products(),
), alert_store=EvaporationAlertWorkbook(ROOT / 'output/test-results/iqc_evaporation/alerts.xlsx'))
