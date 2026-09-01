import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent
project_root = None
for parent in [current_dir] + list(current_dir.parents):
    if (parent / "pyproject.toml").exists():
        project_root = parent
        break
if project_root:
    root_str = str(project_root)
    src_str = str(project_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

import streamlit as st

from app.components.page_header import render_page_header
from app.manager.session_manager import SessionManager
from app.sections.qtime_domain.ijp_dashboard import render_ijp_dashboard
from app.utils.app_setup import AppSetup
from src.qtime_domain.application.ijp.ijp_service import IjpReportService
from src.qtime_domain.composition import build_ijp_repository
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


st.set_page_config(
    page_title="IJP溢流监控报表",
    layout="wide",
    initial_sidebar_state="collapsed",
)
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
render_page_header(
    title="IJP溢流监控报表",
    config=active_config,
    cached_funcs=[],
)
repository = build_ijp_repository(DatabaseManager())
render_ijp_dashboard(IjpReportService(repository))
