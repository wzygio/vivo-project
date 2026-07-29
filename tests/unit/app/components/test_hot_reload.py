from pathlib import Path
from types import SimpleNamespace
import tomllib

import pytest

from app.components import page_header
from app.utils import reloader
from src.shared_kernel.config import ConfigLoader


PROJECT_ROOT = Path(__file__).resolve().parents[4]


def test_streamlit_run_on_save_is_enabled():
    config_path = PROJECT_ROOT / ".streamlit" / "config.toml"

    with config_path.open("rb") as config_file:
        config = tomllib.load(config_file)

    assert config["server"]["runOnSave"] is True


def test_render_page_header_checks_hot_reload_before_rendering(monkeypatch):
    class HotReloadChecked(Exception):
        pass

    class UnexpectedStreamlitAccess:
        def __getattr__(self, name):
            pytest.fail(f"Streamlit rendering started before hot-reload check: {name}")

    def stop_after_hot_reload_check():
        raise HotReloadChecked

    monkeypatch.setattr(page_header, "setup_hot_reload", stop_after_hot_reload_check)
    monkeypatch.setattr(page_header, "st", UnexpectedStreamlitAccess())

    with pytest.raises(HotReloadChecked):
        page_header.render_page_header()


def test_setup_hot_reload_unloads_modules_and_reruns_after_change(monkeypatch):
    session_state = {"last_code_revision": "old-revision"}
    events = []
    fake_streamlit = SimpleNamespace(
        session_state=session_state,
        rerun=lambda: events.append(
            ("rerun", session_state["last_code_revision"])
        ),
    )

    monkeypatch.setattr(page_header, "st", fake_streamlit)
    monkeypatch.setattr(
        ConfigLoader,
        "get_project_root",
        staticmethod(lambda: PROJECT_ROOT),
    )
    monkeypatch.setattr(
        reloader,
        "get_project_revision",
        lambda _project_root: "new-revision",
    )
    monkeypatch.setattr(
        reloader,
        "deep_reload_modules",
        lambda: events.append(("unload", None)),
    )

    changed = page_header.setup_hot_reload()

    assert changed is True
    assert events == [
        ("unload", None),
        ("rerun", "new-revision"),
    ]


def test_setup_hot_reload_only_records_the_initial_revision(monkeypatch):
    session_state = {}
    events = []
    fake_streamlit = SimpleNamespace(
        session_state=session_state,
        rerun=lambda: events.append("rerun"),
    )

    monkeypatch.setattr(page_header, "st", fake_streamlit)
    monkeypatch.setattr(
        ConfigLoader,
        "get_project_root",
        staticmethod(lambda: PROJECT_ROOT),
    )
    monkeypatch.setattr(
        reloader,
        "get_project_revision",
        lambda _project_root: "initial-revision",
    )
    monkeypatch.setattr(
        reloader,
        "deep_reload_modules",
        lambda: events.append("unload"),
    )

    changed = page_header.setup_hot_reload()

    assert changed is False
    assert session_state["last_code_revision"] == "initial-revision"
    assert events == []
