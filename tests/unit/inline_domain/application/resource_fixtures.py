"""Real global resource configuration for isolated Inline application tests."""

from pathlib import Path

import yaml


def write_inline_resource_config(project_root: Path) -> Path:
    """Register fixture workbooks without consulting maintained project resources."""
    resources = project_root / "resources" / "inline_domain"
    files = {
        f"{scope}_sheet_{alarm}_decoration": str(resources / f"{scope}_sheet_{alarm}_decoration.xlsx")
        for scope in ("spc", "ctq", "aoi_tt", "aoi_rs")
        for alarm in ("oos", "ooc")
    }
    files.update({
        "spc_cpk_cpm_decoration": str(resources / "spc_cpk_cpm_decoration.xlsx"),
        "spc_outlier_filters": str(resources / "spc_outlier_filters.csv"),
        "aoi_tt_particle_size_ratio_spec": str(resources / "aoi_tt_particle_size_ratio_spec.xlsx"),
        "compliance_config": str(resources / "compliance_config.xlsx"),
        "scrap_sheets": str(resources / "scrap_sheets.xlsx"),
        "monitor_summary_workbook": str(resources / "monitor-summary.xlsx"),
    })
    config_path = project_root / "config" / "global.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump({
        "resources": {"inline_domain": {"dir": str(resources), "files": files}},
    }, allow_unicode=True), encoding="utf-8")
    return resources
