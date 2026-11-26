"""Global refresh overlap is validated and read from global.yaml on demand."""

import pytest

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.config_model import ApplicationConfig


@pytest.mark.parametrize("config", [{}, {"application": {}}])
def test_missing_refresh_settings_use_independent_defaults(monkeypatch, config):
    monkeypatch.setattr(ConfigLoader, "_load_yaml", lambda _path: config)
    assert ConfigLoader.get_incremental_refresh_days() == 7
    assert ConfigLoader.get_manual_refresh_days() == 14


def test_refresh_days_follow_global_yaml_changes(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    path = config_dir / "global.yaml"
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    for manual_days in (14, 21, 3):
        path.write_text(
            f"application:\n  incremental_refresh_days: 7\n  manual_refresh_days: {manual_days}\n",
            encoding="utf-8",
        )
        assert ConfigLoader.get_incremental_refresh_days() == 7
        assert ConfigLoader.get_manual_refresh_days() == manual_days


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "7", None, [], {}])
@pytest.mark.parametrize("key", ["incremental_refresh_days", "manual_refresh_days"])
def test_invalid_refresh_days_are_rejected(monkeypatch, value, key):
    monkeypatch.setattr(ConfigLoader, "_load_yaml", lambda _path: {
        "application": {key: value},
    })
    with pytest.raises(ValueError, match=f"application.{key}"):
        getattr(ConfigLoader, f"get_{key}")()
    with pytest.raises(ValueError, match=key):
        ApplicationConfig(cache_ttl_hours=12, **{key: value})


@pytest.mark.parametrize("application", [None, [], "invalid"])
def test_invalid_application_section_is_rejected(monkeypatch, application):
    monkeypatch.setattr(ConfigLoader, "_load_yaml", lambda _path: {"application": application})
    with pytest.raises(ValueError, match="application.*mapping"):
        ConfigLoader.get_incremental_refresh_days()
