import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
for path in (project_root, project_root / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import streamlit as st

from app.components.page_header import render_page_header
from app.sections.iqc_domain.demo_dashboard import render_demo_dashboard
from app.utils.app_setup import AppSetup

st.set_page_config(page_title="IQC-蒸镀材料", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()
render_page_header(title="IQC-蒸镀材料", show_product_filter=False)
render_demo_dashboard("inspection")
