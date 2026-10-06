from pathlib import Path
import json

import pytest
import yaml

from src.shared_kernel.config import ConfigLoader
from src.yield_domain.application.yield_service import YieldAnalysisService


def configure(monkeypatch, tmp_path: Path, resources: dict) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        yaml.safe_dump({"application": {"cache_ttl_hours": 12}, "resources": resources}),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))
    return config_dir


def test_global_registry_controls_directory_filename_and_absolute_paths(monkeypatch, tmp_path):
    external = tmp_path / "external" / "renamed.xlsx"
    config_dir = configure(monkeypatch, tmp_path, {
        "inline_domain": {"dir": "resources/custom", "files": {
            "relative": "resources/custom/spc/other.xlsx", "absolute": str(external),
        }, "directories": {"archive": "resources/custom/shared/archive"}},
    })
    (config_dir / "domain").mkdir()
    (config_dir / "domain" / "inline_domain.yaml").write_text(
        "resources:\n  dir: resources/obsolete\n  files:\n    relative: obsolete.xlsx\n",
        encoding="utf-8",
    )
    assert ConfigLoader.get_domain_resource_dir("inline_domain") == tmp_path / "resources/custom"
    assert ConfigLoader.get_domain_resource_path("inline_domain", "relative") == tmp_path / "resources/custom/spc/other.xlsx"
    assert ConfigLoader.get_domain_resource_path("inline_domain", "absolute") == external
    assert ConfigLoader.get_domain_resource_directory("inline_domain", "archive") == tmp_path / "resources/custom/shared/archive"


@pytest.mark.parametrize("entry", [None, "", 123, [], {}])
def test_invalid_registered_file_fails_instead_of_using_default(monkeypatch, tmp_path, entry):
    configure(monkeypatch, tmp_path, {"inline_domain": {
        "dir": "resources/inline_domain", "files": {"decision": entry},
    }})
    with pytest.raises(ValueError):
        ConfigLoader.get_domain_resource_path("inline_domain", "decision", "fallback.xlsx")


def test_unregistered_resource_fails_even_when_legacy_default_is_supplied(monkeypatch, tmp_path):
    configure(monkeypatch, tmp_path, {"inline_domain": {
        "dir": "resources/inline_domain", "files": {},
    }})
    with pytest.raises(KeyError):
        ConfigLoader.get_domain_resource_path("inline_domain", "missing", "fallback.xlsx")


@pytest.mark.parametrize("content", [None, "", "resources: [", "- not-a-mapping"])
def test_unreadable_global_config_cannot_select_a_legacy_resource(monkeypatch, tmp_path, content):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    if content is not None:
        (config_dir / "global.yaml").write_text(content, encoding="utf-8")
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))
    with pytest.raises((OSError, ValueError)):
        ConfigLoader.get_domain_resource_path("inline_domain", "decision", "fallback.xlsx")


def test_runtime_yield_paths_are_hydrated_without_changing_product_sheets(monkeypatch, tmp_path):
    config_dir = configure(monkeypatch, tmp_path, {"yield_domain": {
        "dir": "resources/yield_domain", "files": {
            "static_warning_lines": "resources/yield_domain/shared/limits-renamed.xlsx",
            "override_rates": "resources/yield_domain/sheet_lot/rates-renamed.xlsx",
            "yield_modifier_table": "resources/yield_domain/mwd_trend/targets-renamed.xlsx",
        },
    }})
    (config_dir / "products").mkdir()
    for product in ("P1", "P2"):
        (config_dir / "products" / f"{product}.yaml").write_text(
            yaml.safe_dump({"paths": {
                "static_warning_lines": {"sheet_name": product},
                "rate_override_config": {"sheet_name": product},
                "yield_modifier_config": {},
            }}), encoding="utf-8",
        )
        config = ConfigLoader.load_config(product)
        assert config.paths["static_warning_lines"].sheet_name == product
        assert config.paths["rate_override_config"].sheet_name == product
        assert Path(config.paths["static_warning_lines"].file_name) == tmp_path / "resources/yield_domain/shared/limits-renamed.xlsx"
        assert YieldAnalysisService.resolve_modifier_table_path(config, tmp_path / product) == tmp_path / "resources/yield_domain/mwd_trend/targets-renamed.xlsx"


def test_all_existing_resource_files_have_a_global_registry_entry():
    root = ConfigLoader.get_project_root()
    registry = yaml.safe_load((root / "config/global.yaml").read_text(encoding="utf-8"))["resources"]
    registered = {
        ConfigLoader.get_domain_resource_path(domain, key).resolve()
        for domain, values in registry.items() for key in values["files"]
    }
    existing = {path.resolve() for path in (root / "resources").rglob("*") if path.is_file()}
    assert existing <= registered


@pytest.mark.parametrize("report,date_column", [("inspection", "检验时间"), ("lifetime", "批次")])
def test_iqc_demo_reads_a_renamed_absolute_input(monkeypatch, tmp_path, report, date_column):
    from src.iqc_domain.infrastructure.demo_repository import read_demo_report

    source = tmp_path / "external" / "renamed-input.json"
    source.parent.mkdir()
    columns = ["报检日期", "检验时间"] if report == "inspection" else ["批次"]
    source.write_text(json.dumps({
        "columns": columns, "rows": [["2026-01-01"] * len(columns)],
    }), encoding="utf-8")
    configure(monkeypatch, tmp_path, {"iqc_domain": {
        "dir": "unused", "files": {f"demo_{report}": str(source)},
    }})
    frame = read_demo_report(report)
    assert len(frame) == 1
    assert str(frame[date_column].dtype) == "datetime64[ns]"
