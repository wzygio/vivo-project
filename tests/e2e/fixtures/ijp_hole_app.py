"""Run the actual page with a real hole service/SQL adapter and synthetic facts."""

import runpy
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))

import streamlit as st

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from src.indicator_domain import composition
from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
from src.indicator_domain.infrastructure.ijp_hole.repository import IjpHoleRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.infrastructure import db_handler
from tests.e2e.fixtures.ijp_hole_data import build_hole_engine


@st.cache_resource
def fixture_engine():
    return build_hole_engine()


class FaultRepository(IjpHoleRepository):
    def fetch_counts(self, query):
        if 'M678' in query.product_codes and len(query.product_codes) == 1:
            # Execute the adapter's real failure path.
            return IjpHoleRepository(SimpleNamespace(engine=None)).fetch_counts(query)
        return super().fetch_counts(query)


ConfigLoader.get_data_forward_policy = staticmethod(lambda: DataForwardPolicy(enabled=False))
service = IjpHoleReportService(FaultRepository(SimpleNamespace(engine=fixture_engine())),
                               ('M626', 'M678'), today_provider=lambda: date(2026, 9, 10))
composition.build_ijp_hole_service = lambda _db: service
db_handler.DatabaseManager = lambda: SimpleNamespace(engine=fixture_engine())
AppSetup.initialize_app = staticmethod(lambda: None)
SessionManager.get_active_config = staticmethod(lambda: SimpleNamespace())
page_header.render_page_header = lambda **kwargs: st.title(kwargs['title'])
# The existing border report has its own browser regression fixture.
from app.sections.indicator_domain.ijp import dashboard
composition.build_ijp_service = lambda _db: None
def render_border_stub(_service):
    from app.sections.indicator_domain.ijp.filters import render_ijp_filter_columns
    st.subheader('OLED IJP 溢流监控')
    render_ijp_filter_columns()


dashboard.render_ijp_dashboard = render_border_stub
runpy.run_path(str(ROOT / 'app/pages/IJP溢流监控报表.py'), run_name='__main__')
