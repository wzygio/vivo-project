from datetime import datetime

import pandas as pd
import pytest

from src.qtime_domain.application.ijp.dtos import IjpQuery
from src.qtime_domain.application.ijp.errors import IjpDataAccessError
from src.qtime_domain.application.ijp.ijp_service import IjpReportService

START = datetime(2026, 8, 31, 7, 0)
END = datetime(2026, 9, 1, 7, 0)


class FakeIjpDataPort:
    def __init__(self) -> None:
        self.received_query: IjpQuery | None = None
        self.received_product_codes: tuple[str, ...] | None = None
        self.received_picis: tuple[str, ...] | None = None

    def list_product_codes(self) -> tuple[str, ...]:
        return ("M626", "M678")

    def list_product_names(self, product_codes: tuple[str, ...]) -> tuple[str, ...]:
        self.received_product_codes = product_codes
        return ("PROD-B",) if product_codes else ("PROD-A", "PROD-B")

    def list_sub_prod_types(self) -> tuple[str, ...]:
        return ("E", "P")

    def list_picis(self, start_time, end_time, product_codes) -> tuple[str, ...]:
        assert (start_time, end_time) == (START, END)
        return ("LOT1",)

    def list_cycles(self, start_time, end_time, product_codes, picis) -> tuple[str, ...]:
        self.received_picis = picis
        return ("CYC1",)

    def fetch_daily_ratios(self, query: IjpQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame(
            {"day": ["2026-08-31"], "rs_code": ["C3DM1"], "code_num": [3], "ratio": [1.0]}
        )

    def fetch_details(self, query: IjpQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame({"glass_id": ["G1"], "code_ratio": [0.667]})


class FailingIjpDataPort(FakeIjpDataPort):
    def fetch_details(self, query: IjpQuery) -> pd.DataFrame:
        raise IjpDataAccessError("IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。")


def _query() -> IjpQuery:
    return IjpQuery(start_time=START, end_time=END)


def test_service_exposes_filter_options_with_cascading_inputs() -> None:
    port = FakeIjpDataPort()
    service = IjpReportService(port)

    options = service.get_filter_options(START, END, ("M678",), ("LOT1",))

    assert options["product_codes"] == ("M626", "M678")
    assert options["product_names"] == ("PROD-B",)
    assert options["sub_prod_types"] == ("E", "P")
    assert options["picis"] == ("LOT1",)
    assert options["cycles"] == ("CYC1",)
    assert options["lines"] == ("3CEE01", "3CEE02", "3CEE04")
    assert len(options["equipments"]) == 5
    assert len(options["codes"]) == 12
    assert "TOP" in options["panel_locations"]
    assert port.received_product_codes == ("M678",)
    assert port.received_picis == ("LOT1",)


def test_service_delegates_report_reads_to_the_port() -> None:
    port = FakeIjpDataPort()
    service = IjpReportService(port)
    query = _query()

    ratios = service.get_daily_ratios(query)
    details = service.get_details(query)

    assert ratios.to_dict("records") == [
        {"day": "2026-08-31", "rs_code": "C3DM1", "code_num": 3, "ratio": 1.0}
    ]
    assert details.to_dict("records") == [{"glass_id": "G1", "code_ratio": 0.667}]
    assert port.received_query is query


def test_service_propagates_the_stable_data_access_error() -> None:
    service = IjpReportService(FailingIjpDataPort())

    with pytest.raises(IjpDataAccessError) as caught:
        service.get_details(_query())

    assert str(caught.value) == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
