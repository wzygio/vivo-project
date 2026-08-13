from pathlib import Path

import pandas as pd

import app.manager.compliance_manager as compliance_manager


def _write_config(path: Path, rows: list[dict[str, str]]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="规则配置")


def test_get_compliance_config_matches_four_dimensions_with_all(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_config(
        config_path,
        [
            {
                "厂别": "ALL",
                "产品型号": "Z571",
                "监控类型": "ALL",
                "月份": "M04",
            }
        ],
    )
    monkeypatch.setattr(compliance_manager, "CONFIG_PATH", config_path)

    assert compliance_manager.get_compliance_config("ARRAY", "Z571", "SPC", month=4) is True
    assert compliance_manager.get_compliance_config("OLED", "Z571", "CTQ", month="M04") is True
    assert compliance_manager.get_compliance_config("TP", "Z571", "AOI", month=5) is False
    assert compliance_manager.get_compliance_config("ARRAY", "M678", "SPC", month=4) is False


def test_get_compliance_config_treats_each_row_as_an_enabled_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_config(
        config_path,
        [
            {
                "厂别": "OLED",
                "产品型号": "ALL",
                "监控类型": "SPC",
                "月份": "ALL",
            },
            {
                "厂别": "ARRAY",
                "产品型号": "Z571",
                "监控类型": "CTQ",
                "月份": "M04",
            },
        ],
    )
    monkeypatch.setattr(compliance_manager, "CONFIG_PATH", config_path)

    assert compliance_manager.get_compliance_config("OLED", "M673", "SPC", month=12) is True
    assert compliance_manager.get_compliance_config("ARRAY", "Z571", "CTQ", month=4) is True
    assert compliance_manager.get_compliance_config("ARRAY", "Z571", "SPC", month=4) is False
