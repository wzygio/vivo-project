from __future__ import annotations

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.ctq.ctq_repository import CtqRepository


class FakeSpcDataPort:
    def __init__(self) -> None:
        self.received: SpcQueryConfig | None = None

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        assert force_refresh is True
        self.received = config
        return pd.DataFrame([{"param_name": "CTQ_PARAM", "data_type": "CTQ"}])

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"prod_code": prod_code}])


def test_ctq_repository_owns_ctq_projection_over_shared_measurements() -> None:
    source = FakeSpcDataPort()
    repository = CtqRepository(source)
    query = SpcQueryConfig(
        prod_code="M678",
        start_date="2026-08-01",
        end_date="2026-08-10",
        data_type_filter="SPC",
    )

    result = repository.get_spc_measurements(query, force_refresh=True)

    assert source.received is not None
    assert source.received.data_type_filter == "CTQ"
    assert query.data_type_filter == "SPC"
    assert result.to_dict("records") == [{"param_name": "CTQ_PARAM", "data_type": "CTQ"}]

