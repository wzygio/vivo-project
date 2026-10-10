"""Outbound ports owned by the AOI_RS application."""

from __future__ import annotations

from typing import Protocol

import pandas as pd

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig


class AoiRsDataPort(Protocol):
    """Provide date-excluded display-time details/throughput and specifications."""

    def get_rs_details(self, query: AoiRsQueryConfig) -> pd.DataFrame: ...

    def get_pass_through(self, query: AoiRsQueryConfig) -> pd.DataFrame: ...

    def get_rs_spec_limits(self, prod_code: str) -> pd.DataFrame: ...

