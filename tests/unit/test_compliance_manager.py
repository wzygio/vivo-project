from pathlib import Path

import app.compliance.compliance_manager as compliance_manager


def _write_config(path: Path, rules: dict[str, bool], default: bool = False) -> None:
    lines = ["default: " + str(default).lower(), "rules:"]
    for key, value in rules.items():
        lines.append(f"  {key}: {str(value).lower()}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_get_compliance_config_matches_all_wildcard_segments(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.yaml"
    _write_config(config_path, {"ALL-Z571-ALL-M04": True})
    monkeypatch.setattr(compliance_manager, "CONFIG_PATH", config_path)

    assert compliance_manager.get_compliance_config("SPC", "Z571", "ARRAY", month=4) is True
    assert compliance_manager.get_compliance_config("CTQ", "Z571", "OLED", month=4) is True
    assert compliance_manager.get_compliance_config("AOI", "Z571", "TP", month=4) is True
    assert compliance_manager.get_compliance_config("SPC", "Z571", "ARRAY", month=5) is False
    assert compliance_manager.get_compliance_config("SPC", "M678", "ARRAY", month=4) is False


def test_get_compliance_config_prefers_more_specific_match_within_same_depth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.yaml"
    _write_config(
        config_path,
        {
            "ALL-Z571-ALL-M04": True,
            "SPC-Z571-ARRAY-M04": False,
        },
    )
    monkeypatch.setattr(compliance_manager, "CONFIG_PATH", config_path)

    assert compliance_manager.get_compliance_config("SPC", "Z571", "ARRAY", month=4) is False
    assert compliance_manager.get_compliance_config("SPC", "Z571", "OLED", month=4) is True
