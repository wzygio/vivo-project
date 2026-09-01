from datetime import datetime

import pandas as pd

from src.qtime_domain.application.dtos import QTimeQuery
from src.qtime_domain.application.qtime_service import QTimeReportService


class FakeQTimeDataPort:
    def __init__(self) -> None:
        self.received_query: QTimeQuery | None = None

    def list_products(self) -> tuple[str, ...]:
        raise AssertionError("Page Header owns the Q-Time product selection")

    def list_step_descriptions(self, shop: str) -> tuple[str, ...]:
        assert shop == "ARRAY"
        return ("M3_DE->M3_STR",)

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame({"lot_id": ["L001"], "wait_time": [0.41]})


def test_service_exposes_filter_options_and_report_through_the_data_port() -> None:
    port = FakeQTimeDataPort()
    service = QTimeReportService(port)
    query = QTimeQuery(
        start_time=datetime(2026, 8, 2),
        end_time=datetime(2026, 9, 1),
        shop="ARRAY",
        step_descriptions=("M3_DE->M3_STR",),
    )

    options = service.get_filter_options("ARRAY")
    report = service.get_report(query)

    assert options == {"step_descriptions": ("M3_DE->M3_STR",)}
    assert report.to_dict("records") == [{"lot_id": "L001", "wait_time": 0.41}]
    assert port.received_query is query
