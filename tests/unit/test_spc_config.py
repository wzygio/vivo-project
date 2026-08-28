from pathlib import Path

from src.shared_kernel.config import ConfigLoader


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
