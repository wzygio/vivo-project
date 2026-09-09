"""Production adapter preserving existing equipment snapshot policies."""
import pandas as pd

from src.equipment_domain.infrastructure.data_loader import PartsRepository, load_spec_baseline, load_report_part_life_snapshots


class PartsReportDataAdapter:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def load_baseline(self, baseline_path: str) -> pd.DataFrame:
        return load_spec_baseline(baseline_path)

    def load_snapshots(self, spec_df: pd.DataFrame, *, as_of: pd.Timestamp, max_age_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
        return load_report_part_life_snapshots(self.db_manager, spec_df, as_of=as_of, max_age_days=max_age_days)

    def refresh(self, baseline_path: str) -> pd.DataFrame:
        return PartsRepository(self.db_manager, self.load_baseline(baseline_path)).get_snapshot(force_refresh=True)
