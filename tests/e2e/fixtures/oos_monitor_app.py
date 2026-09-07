"""Controlled harness that executes the real automatic-warning page."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
for search_path in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_cache
from app.utils import app_setup, step_labels
from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.infrastructure.shared.oos_decision_repository import (
    OosDecisionWorkbookRepository,
)
from src.inline_domain.infrastructure.shared.oos_history_store import OosHistoryStore
import src.inline_domain.composition as composition
import src.shared_kernel.infrastructure.db_handler as db_handler

fixture_root = PROJECT_ROOT / "output" / "tmp" / "monitor-oos-production-e2e"
resources = fixture_root / "resources"
history_dir = fixture_root / "history"
resources.mkdir(parents=True, exist_ok=True)

facts = pd.DataFrame(
    [
        {
            "factory": "ARRAY",
            "prod_code": "M626",
            "step_id": "1100",
            "param_name": "PPA",
            "sheet_id": "S1",
            "sheet_start_time": pd.Timestamp("2026-09-05 08:00:00"),
            "sheet_max": 3.0,
            "sheet_min": 1.0,
            "sheet_mean": 2.0,
            "usl": 2.5,
            "lsl": 1.5,
            "oos_type": "USL",
        }
    ]
)
store = OosHistoryStore(history_dir)
store.update(
    "spc",
    "M626",
    facts,
    coverage_start=pd.Timestamp("2026-09-01"),
    coverage_end=pd.Timestamp("2026-09-08"),
)
pd.DataFrame(
    [{"prod_code": "M626", "step_id": "1100", "param_name": "PPA", "sheet_id": "S1", "flag": False}]
).to_excel(
    resources / "spc_sheet_oos_decoration.xlsx",
    sheet_name="M626__flags",
    index=False,
)
service = OosHistoryService(store, OosDecisionWorkbookRepository(resources))

# Replace external bootstrapping concerns only. The production page control
# flow and real history/application/dashboard path execute unchanged.
composition.build_oos_history_service = lambda resource_dir=None: service
app_setup.AppSetup.initialize_app = staticmethod(lambda: None)
page_header.render_page_header = lambda **kwargs: None
step_labels.get_cached_step_description_map = lambda _db: {"1100": "涂布"}
alert_matrix.render_alert_matrix_filter_bar = lambda *args, **kwargs: None
alert_matrix.render_alert_matrix_board = lambda *args, **kwargs: None
alert_matrix_cache.get_alert_matrix_cached_funcs = lambda: []
SessionManager.AVAILABLE_PRODUCTS = ["M626"]
SessionManager.get_active_config = staticmethod(lambda: {})
db_handler.DatabaseManager = lambda: object()

runpy.run_path(
    str(PROJECT_ROOT / "app" / "pages" / "自动预警看板.py"),
    run_name="__main__",
)
