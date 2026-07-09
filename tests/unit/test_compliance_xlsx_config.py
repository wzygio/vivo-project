from pathlib import Path

import pandas as pd

import app.compliance.compliance_manager as compliance_manager
from src.shared_kernel.utils import excel_tools
from src.shared_kernel.compliance_config_excel import load_compliance_config_from_xlsx
from src.shared_kernel.config import ConfigLoader


def _write_compliance_config(path: Path) -> None:
    rules_df = pd.DataFrame(
        [
            {
                "规则键": "ALL-Z571-ALL-M04",
                "启用": True,
            },
            {
                "监控类型": "SPC",
                "产品型号": "Z571",
                "厂别": "ARRAY",
                "月份": "M04",
                "启用": False,
            },
        ]
    )
    default_df = pd.DataFrame([{"默认启用": False}])
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")
        default_df.to_excel(writer, index=False, sheet_name="默认配置")


def test_load_compliance_config_from_xlsx_builds_rules_dict(tmp_path: Path) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_compliance_config(config_path)

    config = load_compliance_config_from_xlsx(config_path)

    assert config == {
        "default": False,
        "rules": {
            "ALL-Z571-ALL-M04": True,
            "SPC-Z571-ARRAY-M04": False,
        },
    }


def test_get_compliance_config_uses_xlsx_rules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_compliance_config(config_path)
    monkeypatch.setattr(compliance_manager, "CONFIG_PATH", config_path)

    assert compliance_manager.get_compliance_config("SPC", "Z571", "ARRAY", month=4) is False
    assert compliance_manager.get_compliance_config("CTQ", "Z571", "OLED", month=4) is True
    assert compliance_manager.get_compliance_config("SPC", "Z571", "ARRAY", month=5) is False


def test_config_loader_reads_xlsx_before_legacy_yaml(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_compliance_config(config_dir / "compliance_config.xlsx")
    (config_dir / "compliance_config.yaml").write_text(
        "default: true\nrules:\n  SPC-Z571-ARRAY-M04: true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    config = ConfigLoader.get_compliance_config()

    assert config["default"] is False
    assert config["rules"]["SPC-Z571-ARRAY-M04"] is False


def test_load_compliance_config_from_encrypted_xlsx_uses_com_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    config_path.write_bytes(b"encrypted-placeholder")

    def fail_read_excel(*args, **kwargs):
        raise ValueError("File is not a zip file")

    def fake_read_via_com(excel_path: Path, sheet_name: str | None = None) -> pd.DataFrame:
        if sheet_name == "规则配置":
            return pd.DataFrame(
                [
                    {
                        "规则键": "ALL-Z571-ALL-M04",
                        "启用": True,
                    }
                ]
            )
        if sheet_name == "默认配置":
            return pd.DataFrame(
                [
                    {
                        "规则键": "SPC-M678-ARRAY-M07",
                        "启用": True,
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_excel", fail_read_excel)
    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", fake_read_via_com)

    config = load_compliance_config_from_xlsx(config_path)

    assert config == {
        "default": False,
        "rules": {
            "ALL-Z571-ALL-M04": True,
            "SPC-M678-ARRAY-M07": True,
        },
    }
