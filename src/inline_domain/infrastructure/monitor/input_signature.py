"""Cheap dependency probes for configuration-driven Inline computations."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from pathlib import Path

from src.shared_kernel.config import ConfigLoader


def monitor_input_signature(
    project_root: Path,
    resource_root: Path,
    products: Iterable[str],
    *,
    excluded_paths: Iterable[Path] = (),
    include_configured_paths: bool = True,
) -> str:
    """Track source snapshots and editable inputs, excluding generated summaries."""
    excluded = {
        Path(path).resolve()
        for path in excluded_paths
    }
    paths: set[Path] = set()
    # Maintained files may be configured outside the domain resource directory.
    from src.inline_domain.infrastructure.shared.resource_paths import decision_workbook_paths

    if include_configured_paths:
        configured_paths = [
            *decision_workbook_paths("oos").values(),
            *decision_workbook_paths("ooc").values(),
            *(ConfigLoader.get_domain_resource_path("inline_domain", key, name)
              for key, name in (
                  ("spc_cpk_cpm_decoration", "spc_cpk_cpm_decoration.xlsx"),
                  ("spc_outlier_filters", "spc_outlier_filters.csv"),
                  ("aoi_tt_particle_size_ratio_spec", "aoi_tt_particle_size_ratio_spec.xlsx"),
                  ("compliance_config", "compliance_config.yaml"),
                  ("scrap_sheets", "scrap_sheets.xlsx"),
              )),
        ]
        paths.update(path for path in configured_paths if path.is_file() and path.resolve() not in excluded)
    for root in (resource_root, project_root / "config"):
        if root.exists():
            paths.update(
                path for path in root.rglob("*")
                if path.is_file()
                and path.suffix.lower() in {".xlsx", ".yaml", ".yml", ".csv", ".json"}
                and not path.name.startswith(("~$", "."))
                and path.resolve() not in excluded
            )
    for product in sorted(set(products)):
        root = project_root / "data" / product
        if root.exists():
            paths.update(root.glob("inline_measurements*"))
            paths.update(root.glob("aoi_rs_*"))
    parts = [ConfigLoader.get_report_cutoff_policy().signature]
    for path in sorted(paths):
        stat = path.stat()
        parts.append(f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
