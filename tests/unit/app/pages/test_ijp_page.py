from pathlib import Path
import runpy
from types import SimpleNamespace

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.sections.indicator_domain.ijp import dashboard as ijp_dashboard
from app.utils.app_setup import AppSetup
from src.indicator_domain import composition
from src.shared_kernel.infrastructure import db_handler


def test_ijp_page_is_a_thin_composition_layer(monkeypatch) -> None:
    events: list[object] = []
    database = object()
    service = object()
    active_config = SimpleNamespace()

    monkeypatch.setattr(
        ijp_dashboard.st,
        "set_page_config",
        lambda **kwargs: events.append(kwargs),
    )
    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: events.append("init")))
    monkeypatch.setattr(
        SessionManager,
        "get_active_config",
        staticmethod(lambda: events.append("config") or active_config),
    )
    monkeypatch.setattr(
        page_header,
        "render_page_header",
        lambda **kwargs: events.append(("header", kwargs)),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: database)
    monkeypatch.setattr(
        composition,
        "build_ijp_service",
        lambda received: events.append(("service", received)) or service,
    )
    monkeypatch.setattr(
        ijp_dashboard,
        "render_ijp_dashboard",
        lambda received: events.append(("dashboard", received)),
        raising=False,
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "IJP溢流监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert events == [
        {"page_title": "IJP溢流监控报表", "layout": "wide", "initial_sidebar_state": "collapsed"},
        "init",
        "config",
        (
            "header",
            {
                "title": "IJP溢流监控报表",
                "config": active_config,
                "cached_funcs": [],
                "show_product_filter": False,
            },
        ),
        ("service", database),
        ("dashboard", service),
    ]
