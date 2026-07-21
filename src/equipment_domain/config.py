"""Typed runtime settings for the critical-parts domain."""

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from src.equipment_domain.core.parts_calculator import PartsAlertPolicy
from src.equipment_domain.infrastructure.fake_data import FabricationPolicy
from src.shared_kernel.config import ConfigLoader


@dataclass(frozen=True)
class EquipmentRuntimeConfig:
    """Validated, project-root-resolved settings consumed by equipment workflows."""

    source_excel_path: Path
    source_sheet_names: tuple[str, ...]
    csv_encoding: str
    snapshot_dir: Path
    snapshot_ttl_hours: int
    query_lookback_days: int
    query_source_table: str
    alert_policy: PartsAlertPolicy
    fabrication_policy: FabricationPolicy


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"equipment_config.yaml: '{name}' must be a mapping")
    return value


def get_equipment_runtime_config() -> EquipmentRuntimeConfig:
    """Load and validate the equipment-domain configuration on demand."""
    config = ConfigLoader.get_equipment_config()
    baseline = _mapping(config.get("baseline"), "baseline")
    snapshot = _mapping(config.get("snapshot"), "snapshot")
    query = _mapping(config.get("query"), "query")
    alert = _mapping(config.get("alert"), "alert")
    fabrication = _mapping(config.get("fabrication"), "fabrication")

    sheet_names = baseline.get("source_sheet_names")
    if not isinstance(sheet_names, list) or not sheet_names:
        raise ValueError(
            "equipment_config.yaml: 'baseline.source_sheet_names' must be a non-empty list"
        )
    normalized_sheet_names = tuple(str(name).strip() for name in sheet_names if str(name).strip())
    if not normalized_sheet_names:
        raise ValueError(
            "equipment_config.yaml: 'baseline.source_sheet_names' must contain a sheet name"
        )

    root = ConfigLoader.get_project_root()
    source_excel_path = Path(str(baseline["source_excel_path"]))
    snapshot_dir = Path(str(snapshot["directory"]))
    snapshot_ttl_hours = int(snapshot["ttl_hours"])
    query_lookback_days = int(query["lookback_days"])
    source_table = str(query["source_table"]).strip()
    if snapshot_ttl_hours <= 0:
        raise ValueError("equipment_config.yaml: 'snapshot.ttl_hours' must be positive")
    if query_lookback_days <= 0:
        raise ValueError("equipment_config.yaml: 'query.lookback_days' must be positive")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*)?", source_table):
        raise ValueError("equipment_config.yaml: 'query.source_table' is not a valid SQL identifier")

    return EquipmentRuntimeConfig(
        source_excel_path=(root / source_excel_path if not source_excel_path.is_absolute() else source_excel_path),
        source_sheet_names=normalized_sheet_names,
        csv_encoding=str(baseline.get("csv_encoding", "utf-8-sig")),
        snapshot_dir=(root / snapshot_dir if not snapshot_dir.is_absolute() else snapshot_dir),
        snapshot_ttl_hours=snapshot_ttl_hours,
        query_lookback_days=query_lookback_days,
        query_source_table=source_table,
        alert_policy=PartsAlertPolicy(
            warning_threshold=float(alert["warning_threshold"]),
            over_threshold=float(alert["over_threshold"]),
            decoration_growth_ratio=float(alert["decoration_growth_ratio"]),
            decoration_min_ratio=float(alert["decoration_min_ratio"]),
            decoration_max_ratio=float(alert["decoration_max_ratio"]),
            display_progress_max_ratio=float(alert["display_progress_max_ratio"]),
        ),
        fabrication_policy=FabricationPolicy(
            random_seed=int(fabrication["random_seed"]),
            initial_value_ratio_range=tuple(
                float(value) for value in fabrication["initial_value_ratio_range"]
            ),
            initial_lookback_days=int(fabrication["initial_lookback_days"]),
            update_increment_ratio=float(fabrication["update_increment_ratio"]),
            reset_ratio_range=tuple(
                float(value) for value in fabrication["reset_ratio_range"]
            ),
            snapshot_ttl_hours=int(fabrication["snapshot_ttl_hours"]),
        ),
    )
