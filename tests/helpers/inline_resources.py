"""Temporary resource registration for Inline producer/cache regression tests."""
from pathlib import Path

import yaml


def write_inline_resource_registry(project_root: Path) -> None:
    """Register the fixture workbooks without falling back to maintained files."""
    files = {
        f"{scope}_sheet_{event}_decoration": f"resources/inline_domain/{scope}_sheet_{event}_decoration.xlsx"
        for scope in ("spc", "ctq") for event in ("oos", "ooc")
    }
    files["spc_outlier_filters"] = "resources/inline_domain/spc_outlier_filters.xlsx"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "global.yaml").write_text(yaml.safe_dump({
        "application": {"cache_ttl_hours": 12},
        "resources": {"inline_domain": {"dir": "resources/inline_domain", "files": files}},
    }), encoding="utf-8")
