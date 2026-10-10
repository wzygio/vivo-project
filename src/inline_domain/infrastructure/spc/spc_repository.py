"""Thin SPC projection over the shared measurement preparation port."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.inline_domain.application.ports.measurement_snapshot import (
    MeasurementPreparationPort,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.shared_kernel.snapshot_paths import inline_measurement_directory, snapshot_component


def spc_input_signature(project_root: Path, prod_code: str) -> str:
    """Detect updated product snapshots before cached Sheet/capability computations."""
    path = inline_measurement_directory(project_root / "data") / (
        f"inline_measurements_{snapshot_component(prod_code)}.parquet"
    )
    parts = []
    for source in [path, path.with_suffix(".policy")]:
        try:
            stat = source.stat()
            parts.append(f"{source.resolve()}:{stat.st_mtime_ns}:{stat.st_size}")
        except FileNotFoundError:
            parts.append(f"{source.resolve()}:missing")
    return "|".join(parts)


class SpcRepository:
    supports_shared_history_persistence = True

    """Expose the SPC data contract while delegating preparation to measurement."""

    def __init__(self, preparation: MeasurementPreparationPort) -> None:
        if preparation is None:
            raise ValueError("SPC repository requires a measurement preparation port")
        self._preparation = preparation

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        """Return the SPC projection prepared by the shared measurement pipeline."""
        return self._preparation.get_prepared_measurements(config, force_refresh)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """Return the product spec limits prepared by the shared measurement pipeline."""
        return self._preparation.get_spec_limits(prod_code)
