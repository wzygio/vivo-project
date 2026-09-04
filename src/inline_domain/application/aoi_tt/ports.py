"""Outbound data port consumed by the AOI TT report use case."""

from __future__ import annotations

from typing import Protocol

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig


class AoiTtDataPort(Protocol):
    def get_tt_details(self, query: AoiTtQueryConfig) -> pd.DataFrame: ...

    def get_particle_size_counts(self, query: AoiTtQueryConfig) -> pd.DataFrame: ...

    def get_particle_size_ratios(self) -> pd.DataFrame: ...

    def get_tt_spec_limits(self, prod_code: str) -> pd.DataFrame: ...
