from pathlib import Path
from datetime import date
import pytest

from src.shared_kernel.config import ConfigLoader


def test_spc_sheet_alert_exclusions_are_independent_of_decoration_exemptions(monkeypatch):
    config = {
        "spc": {"sheet_oos_alerts": {"excluded_param_name_contains": [" PPA ", "", None]}},
        "auto_decoration": {"exempt_param_name_contains": ["THK"]},
    }
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: config)
    assert ConfigLoader.get_spc_sheet_oos_alert_param_exclusions() == ["PPA"]
    assert ConfigLoader.get_auto_decoration_param_exemptions() == ["THK"]
    config["spc"] = {}
    assert ConfigLoader.get_spc_sheet_oos_alert_param_exclusions() == []


@pytest.mark.parametrize("value, expected", [(0, 0.0), (24, 24.0), (48, 48.0), (24.5, 24.5)])
def test_capability_alert_span_reads_domain_yaml(tmp_path, monkeypatch, value, expected):
    domain_dir = tmp_path / "config" / "domain"
    domain_dir.mkdir(parents=True)
    (domain_dir / "inline_domain.yaml").write_text(
        f"spc:\n  spc_cpk:\n    alert_min_span_hours: {value}\n", encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    assert ConfigLoader.get_spc_capability_alert_min_span_hours() == expected


def test_capability_alert_span_defaults_to_48_hours(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {})
    assert ConfigLoader.get_spc_capability_alert_min_span_hours() == 48


@pytest.mark.parametrize("value", [-1, True, None, "bad", float("nan"), float("inf")])
def test_invalid_capability_alert_span_is_rejected(monkeypatch, value):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "spc": {"spc_cpk": {"alert_min_span_hours": value}},
    })
    with pytest.raises(ValueError, match="alert_min_span_hours"):
        ConfigLoader.get_spc_capability_alert_min_span_hours()


def test_spc_point_policy_resolves_dates_and_ratio(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {"spc": {"point_decoration": {
        "enabled": True, "central_fraction": .5, "start_date": "2026-10-05", "end_date": "today",
    }}})
    assert ConfigLoader.get_spc_point_decoration_policy(today=date(2026, 10, 10)) == (.5, "2026-10-05", "2026-10-10")


@pytest.mark.parametrize("section", [{}, {"enabled": False}])
def test_missing_or_disabled_point_policy_keeps_legacy_mode(monkeypatch, section):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {"spc": {"point_decoration": section}})
    assert ConfigLoader.get_spc_point_decoration_policy() is None


@pytest.mark.parametrize("ratio", [0, -1, 1.1, float("nan"), True])
def test_invalid_point_fraction_is_rejected(monkeypatch, ratio):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {"spc": {"point_decoration": {
        "enabled": True, "central_fraction": ratio, "start_date": "2026-10-05",
    }}})
    with pytest.raises(ValueError, match="central_fraction"):
        ConfigLoader.get_spc_point_decoration_policy(today=date(2026, 10, 10))


def test_get_spc_line_chart_param_name_contains_normalizes_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config" / "domain"
    config_dir.mkdir(parents=True)
    (config_dir / "inline_domain.yaml").write_text(
        """
spc:
  chart:
    line_param_name_contains:
      - UNI
      - "  PROFILE  "
      - ""
      - null
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_line_chart_param_name_contains() == ["UNI", "PROFILE"]


def test_get_spc_capability_param_exemptions_normalizes_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config" / "domain"
    config_dir.mkdir(parents=True)
    (config_dir / "inline_domain.yaml").write_text(
        """
spc:
  spc_cpk:
    exempt_param_name_contains:
      - PPA
      - "  PROFILE  "
      - ""
      - null
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_capability_param_exemptions() == ["PPA", "PROFILE"]


def test_get_auto_decoration_param_exemptions_normalizes_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config" / "domain"
    config_dir.mkdir(parents=True)
    (config_dir / "inline_domain.yaml").write_text(
        """
auto_decoration:
  exempt_param_name_contains:
    - PPA
    - "  THK  "
    - ""
    - null
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_auto_decoration_param_exemptions() == ["PPA", "THK"]


def test_get_spc_period_box_source_reads_supported_value(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config" / "domain"
    config_dir.mkdir(parents=True)
    (config_dir / "inline_domain.yaml").write_text(
        """
spc:
  spc_cpk:
    period_box_source: sheet_mean
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_period_box_source() == "sheet_mean"


def test_get_spc_period_box_source_defaults_to_point_values_for_unknown_value(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config" / "domain"
    config_dir.mkdir(parents=True)
    (config_dir / "inline_domain.yaml").write_text(
        """
spc:
  spc_cpk:
    period_box_source: unknown
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_period_box_source() == "point_value"
