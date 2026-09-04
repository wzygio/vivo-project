"""Load ARRAY AOI defect counts grouped by Particle Size."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.infrastructure.shared.array_defect_data_loader import (
    ARRAY_PARTICLE_COUNT_COLUMNS,
    load_array_aoi_particle_size_counts,
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
    data_loader: Callable[..., pd.DataFrame] = load_array_aoi_particle_size_counts,
) -> pd.DataFrame:
    """Return O/L AOI defect counts without multiplying rows through the SPC join."""
    if query.factory and query.factory.upper() != "ARRAY":
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)
    if query.tt_name and query.tt_name.upper() != "TDSUM":
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)
    policy = data_forward_policy or ConfigLoader.get_data_forward_policy()
    display_start = pd.Timestamp(query.start_date)
    display_end = pd.Timestamp(query.end_date) + pd.Timedelta(days=1)
    source_start, source_end = policy.to_source_window(display_start, display_end)
    result = data_loader(
        db_manager,
        prod_code=query.prod_code,
        start_time=source_start.to_pydatetime(),
        end_time=source_end.to_pydatetime(),
        step_id=query.step_id,
    )
    if result.empty:
        return pd.DataFrame(columns=PARTICLE_SIZE_COUNT_COLUMNS)

    normalized = result.reindex(columns=PARTICLE_SIZE_COUNT_COLUMNS).copy()
    normalized["start_time"] = pd.to_datetime(normalized["start_time"], errors="coerce")
    normalized["particle_qty"] = pd.to_numeric(
        normalized["particle_qty"], errors="coerce"
    ).fillna(0)
    normalized = normalized.dropna(subset=["start_time"])
    return policy.shift_frame(normalized, ("start_time",)).reset_index(drop=True)
