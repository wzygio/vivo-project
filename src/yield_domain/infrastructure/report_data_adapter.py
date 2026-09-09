"""Production adapter for Yield source facts and optional rate overrides."""
from pathlib import Path
from typing import Sequence

import pandas as pd

from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.infrastructure.repositories.yield_repository import PanelRepository, build_yield_snapshot_path
from src.yield_domain.infrastructure.rate_override_repository import load_rate_overrides


class YieldReportDataAdapter:
    def __init__(self, db_manager=None, data_dir: Path = Path("data")):
        self.db_manager = db_manager
        self.data_dir = data_dir

    def read_panel(self, query: YieldQueryConfig, policy: YieldDataPolicy, *, force_refresh: bool = False) -> pd.DataFrame:
        repo = PanelRepository(
            snapshot_path=build_yield_snapshot_path(self.data_dir, query.product_code, policy),
            data_policy=policy, use_snapshot=True, db_manager=self.db_manager,
        )
        return repo.get_panel_details(query, force_refresh=force_refresh)

    def read_array_times(self, lot_ids: Sequence[str], custom_times: dict[str, str]) -> pd.DataFrame:
        repo = PanelRepository(
            snapshot_path=self.data_dir / "unused-array-times.parquet",
            data_policy=YieldDataPolicy(), use_snapshot=False, db_manager=self.db_manager,
        )
        return repo.get_array_input_times(list(lot_ids), custom_times)

    def read_rate_overrides(self, path: Path | None, sheet_name: str) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        return load_rate_overrides(path, sheet_name)
