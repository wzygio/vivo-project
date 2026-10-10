from pathlib import Path
from datetime import date
import pytest

from src.shared_kernel.config import ConfigLoader


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
