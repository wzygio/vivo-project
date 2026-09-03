from pathlib import Path
import runpy
from types import SimpleNamespace

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.sections.indicator_domain.qtime import dashboard as qtime_dashboard
from app.utils.app_setup import AppSetup
from src.indicator_domain import composition
from src.indicator_domain.application.qtime import cached_monitoring
from src.shared_kernel.infrastructure import db_handler


def test_qtime_page_is_a_thin_composition_layer(monkeypatch) -> None:
    events: list[object] = []
    database = object()
    service = object()
    active_config = SimpleNamespace(
        data_source=SimpleNamespace(product_code="M626"),
    )

    monkeypatch.setattr(
        qtime_dashboard.st,
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
        "build_qtime_service",
        lambda received: events.append(("service", received)) or service,
    )
    monkeypatch.setattr(
        qtime_dashboard,
        "render_qtime_dashboard",
        lambda received: events.append(("dashboard", received)),
        raising=False,
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "Q_Time监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert events == [
        {"page_title": "Q-Time监控报表", "layout": "wide", "initial_sidebar_state": "collapsed"},
        "init",
        "config",
        (
            "header",
            {
                "title": "Q-Time监控报表",
                "config": active_config,
                "cached_funcs": [cached_monitoring._cached_monitoring],
                "show_product_filter": False,
            },
        ),
        ("service", database),
        ("dashboard", service),
    ]


def test_portal_navigation_points_qtime_to_the_streamlit_page() -> None:
    config_path = Path(__file__).parents[4] / "app" / "static" / "config.js"
    config_text = config_path.read_text(encoding="utf-8")

    assert 'QTIME: "http://10.72.26.31:8503/Q_Time监控报表"' in config_text
    assert '{ name: "Q-time", url: LINKS.QTIME }' in config_text
    assert "{l:'', v:'Q-time', url: LINKS.QTIME }" in config_text
