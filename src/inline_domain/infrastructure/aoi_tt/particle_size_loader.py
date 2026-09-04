"""Load ARRAY/TP defect counts grouped by Particle Size."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.infrastructure.shared.array_defect_data_loader import (
    ARRAY_PARTICLE_COUNT_COLUMNS,
    load_array_aoi_particle_size_counts,
)
from src.inline_domain.infrastructure.shared.tp_defect_data_loader import (
    load_tp_particle_size_counts,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


PARTICLE_SIZE_COUNT_COLUMNS = ARRAY_PARTICLE_COUNT_COLUMNS


def load_particle_size_counts(
    db_manager: "DatabaseManager",
    query: AoiTtQueryConfig,
    *,
    data_forward_policy: DataForwardPolicy | None = None,
    array_data_loader: Callable[..., pd.DataFrame] = load_array_aoi_particle_size_counts,
    tp_data_loader: Callable[..., pd.DataFrame] = load_tp_particle_size_counts,
) -> pd.DataFrame:
    """Return S/M/L/H defect counts for ARRAY/TP without SPC join multiplication."""
    requested_factory = query.factory.upper() if query.factory else None
    if requested_factory == "OLED":
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)
    if requested_factory and requested_factory not in {"ARRAY", "TP"}:
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)
    policy = data_forward_policy or ConfigLoader.get_data_forward_policy()
    display_start = pd.Timestamp(query.start_date)
    display_end = pd.Timestamp(query.end_date) + pd.Timedelta(days=1)
    source_start, source_end = policy.to_source_window(display_start, display_end)
    loaders = []
    if requested_factory in {None, "ARRAY"}:
        loaders.append(array_data_loader)
    if requested_factory in {None, "TP"}:
        loaders.append(tp_data_loader)
    frames = [
        loader(
            db_manager,
            prod_code=query.prod_code,
            start_time=source_start.to_pydatetime(),
            end_time=source_end.to_pydatetime(),
            step_id=query.step_id,
        )
        for loader in loaders
    ]
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if result.empty:
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)

    normalized = result.reindex(columns=PARTICLE_SIZE_COUNT_COLUMNS).copy()
    normalized["start_time"] = pd.to_datetime(normalized["start_time"], errors="coerce")
    normalized["particle_qty"] = pd.to_numeric(
        normalized["particle_qty"], errors="coerce"
    ).fillna(0)
    normalized = normalized.dropna(subset=["start_time"])
    return policy.shift_frame(normalized, ("start_time",)).reset_index(drop=True)
