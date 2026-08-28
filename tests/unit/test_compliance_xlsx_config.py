from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.compliance_config_excel import (
    COMPLIANCE_EXCEL_COLUMNS,
    compliance_config_to_xlsx_bytes,
    load_compliance_config_from_xlsx,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.utils import excel_tools


def _write_compliance_config(path: Path) -> None:
    rules_df = pd.DataFrame(
        [
            {
                "厂别": "ALL",
                "产品型号": "Z571",
                "监控类型": "ALL",
                "月份": "M04",
                "周别": "W15",
            },
            {
                "厂别": "ARRAY",
                "产品型号": "M673",
                "监控类型": "SPC",
                "月份": 7,
                "备注": "obsolete",
            },
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")


def test_load_compliance_config_uses_only_four_dimensions(tmp_path: Path) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_compliance_config(config_path)

    config = load_compliance_config_from_xlsx(config_path)

    assert config == {
        "rules": [
            {
                "factory": "ALL",
                "prod_code": "Z571",
                "data_type": "ALL",
                "month": "M04",
            },
            {
                "factory": "ARRAY",
                "prod_code": "M673",
                "data_type": "SPC",
                "month": "M07",
            },
        ]
    }


def test_exported_workbook_contains_only_the_four_rule_columns(tmp_path: Path) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    _write_compliance_config(config_path)
    config = load_compliance_config_from_xlsx(config_path)

    xls = pd.read_excel(
        BytesIO(compliance_config_to_xlsx_bytes(config)),
        sheet_name=None,
        engine="openpyxl",
    )

    assert list(xls) == ["规则配置"]
    assert xls["规则配置"].columns.tolist() == list(COMPLIANCE_EXCEL_COLUMNS)


def test_config_loader_reads_the_shared_resources_workbook(
    tmp_path: Path,
    monkeypatch,
) -> None:
    resources_dir = tmp_path / "resources" / "inline_domain"
    resources_dir.mkdir(parents=True)
    _write_compliance_config(resources_dir / "compliance_config.xlsx")
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    config = ConfigLoader.get_compliance_config()

    assert config["rules"][1] == {
        "factory": "ARRAY",
        "prod_code": "M673",
        "data_type": "SPC",
        "month": "M07",
    }


def test_load_encrypted_workbook_uses_com_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "compliance_config.xlsx"
    config_path.write_bytes(b"encrypted-placeholder")

    def fail_read_excel(*args, **kwargs):
        raise ValueError("File is not a zip file")

    def fake_read_via_com(excel_path: Path, sheet_name: str | None = None) -> pd.DataFrame:
        assert sheet_name in {"规则配置", None}
        return pd.DataFrame(
            [
                {
                    "厂别": "OLED",
                    "产品型号": "Z571",
                    "监控类型": "CTQ",
                    "月份": "M04",
                    "周别": "W15",
                }
            ]
        )

    monkeypatch.setattr(pd, "read_excel", fail_read_excel)
    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", fake_read_via_com)

    config = load_compliance_config_from_xlsx(config_path)

    assert config == {
        "rules": [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "data_type": "CTQ",
                "month": "M04",
            }
        ]
    }


def test_unreadable_uploaded_workbook_does_not_silently_clear_rules(monkeypatch) -> None:
    def fail_read_excel(*args, **kwargs):
        raise ValueError("invalid workbook")

    monkeypatch.setattr(pd, "read_excel", fail_read_excel)

    with pytest.raises(ValueError, match="无法读取修饰配置工作簿"):
        load_compliance_config_from_xlsx(BytesIO(b"invalid"))
