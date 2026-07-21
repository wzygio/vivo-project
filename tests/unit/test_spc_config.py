from pathlib import Path

from src.shared_kernel.config import ConfigLoader


def test_get_spc_sheet_oos_clip_rules_reads_configured_rules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "spc_config.yaml").write_text(
        """
spc:
  sheet_oos_decoration:
    param_clip_rules:
      - param_name_contains: ppa
        lower_offset: -0.5
        upper_offset: 0.5
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_sheet_oos_clip_rules() == [
        {
            "param_name_contains": "ppa",
            "lower_offset": -0.5,
            "upper_offset": 0.5,
        }
    ]


def test_get_spc_period_box_source_reads_supported_value(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "spc_config.yaml").write_text(
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
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "spc_config.yaml").write_text(
        """
spc:
  spc_cpk:
    period_box_source: unknown
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_spc_period_box_source() == "point_value"
