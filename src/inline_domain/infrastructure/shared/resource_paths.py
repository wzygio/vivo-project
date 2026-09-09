"""Config-backed locations for Inline domain resource files."""

from __future__ import annotations

from pathlib import Path

from src.shared_kernel.config import ConfigLoader

SCOPE_RESOURCE_KEYS = {
    "spc": {"oos": "spc_sheet_oos_decoration", "ooc": "spc_sheet_ooc_decoration"},
    "ctq": {"oos": "ctq_sheet_oos_decoration", "ooc": "ctq_sheet_ooc_decoration"},
    "aoi_tt": {"oos": "aoi_tt_sheet_oos_decoration", "ooc": "aoi_tt_sheet_ooc_decoration"},
    "aoi_rs": {"oos": "aoi_rs_sheet_oos_decoration", "ooc": "aoi_rs_sheet_ooc_decoration"},
}
MONITOR_SUMMARY_FILE_NAME = "北极星报警率与CPK汇总.xlsx"


def scope_decoration_path(scope: str, alarm_type: str = "oos") -> Path:
    normalized_scope = str(scope).strip().lower()
    normalized_alarm = str(alarm_type).strip().lower()
    key = SCOPE_RESOURCE_KEYS[normalized_scope][normalized_alarm]
    default_name = f"{normalized_scope}_sheet_{normalized_alarm}_decoration.xlsx"
    return ConfigLoader.get_domain_resource_path("inline_domain", key, default_name)


def scope_resource_dir(scope: str, alarm_type: str = "oos") -> Path:
    return scope_decoration_path(scope, alarm_type).parent


def decision_workbook_paths(alarm_type: str = "oos") -> dict[str, Path]:
    return {scope: scope_decoration_path(scope, alarm_type) for scope in SCOPE_RESOURCE_KEYS}


def monitor_summary_workbook_path(resource_dir: Path | None = None) -> Path:
    """Resolve the shared warning read model from Inline resource config."""
    if resource_dir is not None:
        return Path(resource_dir) / "monitor" / MONITOR_SUMMARY_FILE_NAME
    return ConfigLoader.get_domain_resource_path(
        "inline_domain",
        "monitor_summary_workbook",
        f"monitor/{MONITOR_SUMMARY_FILE_NAME}",
    )
