from __future__ import annotations

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.monitor.monitor_repository import (
    InlineMonitorRepository,
)


class FakeSpcSource:
    def __init__(self) -> None:
        self.force_refresh = False

    def get_spc_measurements(
        self,
        config: SpcQueryConfig,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        self.force_refresh = force_refresh
        return pd.DataFrame({"prod_code": [config.prod_code]})

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame({"prod_code": [prod_code]})


class FakeScrapSource:
    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame({"prod_code": [prod_code], "data_type": ["报废"]})


def test_monitor_repository_exposes_monitor_data_capabilities() -> None:
    source = FakeSpcSource()
    repository = InlineMonitorRepository(source, FakeScrapSource())
    config = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-08-01",
        end_date="2026-08-13",
    )

    measurements = repository.get_spc_measurements(config, force_refresh=True)
    specs = repository.get_spc_spec_limits("M626")
    scrap = repository.get_scrap_data("M626")

    assert measurements["prod_code"].tolist() == ["M626"]
    assert specs["prod_code"].tolist() == ["M626"]
    assert scrap["data_type"].tolist() == ["报废"]
    assert source.force_refresh is True
