from datetime import date, datetime, time

import pandas as pd
import pytest

from src.indicator_domain import composition
from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.application.ijp.service import (
    IjpReportService,
    build_reporting_window,
)
from src.shared_kernel.config import ConfigLoader
from src.indicator_domain.application.ijp.settings import IjpSettings

START = datetime(2026, 8, 31, 7, 0)
END = datetime(2026, 9, 1, 7, 0)
TODAY = date(2026, 9, 7)
REPORT_START = datetime(2026, 8, 1)
REPORT_END = datetime.combine(TODAY, time.max)


class FakeIjpDataPort:
    def __init__(self) -> None:
        self.received_query: IjpQuery | None = None
        self.received_product_codes: tuple[str, ...] | None = None

    def list_product_codes(self) -> tuple[str, ...]:
        return ("M626", "M678")

    def list_picis(self, start_time, end_time, product_codes) -> tuple[str, ...]:
        assert (start_time, end_time) == (REPORT_START, REPORT_END)
        self.received_product_codes = product_codes
        return ("LOT1",)

    def fetch_glass_ratios(self, query: IjpQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame(
            {
                "productcode": ["M626"],
                "line": ["3CEE01"],
                "printer": ["3CEE01-IK2-PR1"],
                "glass_id": ["2026-08-31"],
                "rs_code": ["C3DM1"],
                "code_num": [3],
                "ratio": [1.0],
            }
        )

    def fetch_details(self, query: IjpQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame({"glass_id": ["G1"], "code_ratio": [0.667]})


class FailingIjpDataPort(FakeIjpDataPort):
    def fetch_details(self, query: IjpQuery) -> pd.DataFrame:
        raise IjpDataAccessError("IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。")


def _query(**overrides: object) -> IjpQuery:
    return IjpQuery(start_time=START, end_time=END, **overrides)


def _service(
    port: FakeIjpDataPort,
    *,
    enabled_product_codes: tuple[str, ...] = (),
    work_order_types: tuple[str, ...] = (),
) -> IjpReportService:
    return IjpReportService(
        port,
        enabled_product_codes=enabled_product_codes,
        work_order_types=work_order_types,
        today_provider=lambda: TODAY,
    )


def test_reporting_window_runs_from_previous_month_start_through_today() -> None:
    start_time, end_time = build_reporting_window(date(2026, 1, 7))

    assert start_time == datetime(2025, 12, 1)
    assert end_time == datetime.combine(date(2026, 1, 7), time.max)


def test_service_exposes_filter_options_with_cascading_inputs() -> None:
    port = FakeIjpDataPort()
    service = _service(port)

    options = service.get_filter_options(("M678",))

    assert options["product_codes"] == ("M626", "M678")
    assert options["picis"] == ("LOT1",)
    assert options["lines"] == ("3CEE01", "3CEE02", "3CEE04")
    assert options["codes"] == tuple(f"C3DM{i}" for i in range(6))
    assert "equipments" not in options
    assert "panel_locations" not in options
    assert port.received_product_codes == ("M678",)


def test_service_scopes_options_and_queries_to_enabled_products() -> None:
    port = FakeIjpDataPort()
    service = _service(
        port,
        enabled_product_codes=("M678", "Z571"),
        work_order_types=("P", "LCFG"),
    )

    options = service.get_filter_options()
    service.get_details(_query(work_order_types=("UNCONTROLLED",)))

    assert options["product_codes"] == ("M678",)
    assert port.received_product_codes == ("M678", "Z571")
    assert port.received_query is not None
    assert port.received_query.product_codes == ("M678", "Z571")
    assert port.received_query.work_order_types == ("P", "LCFG")
    assert port.received_query.start_time == START
    assert port.received_query.end_time == END


def test_composition_builds_ijp_service_with_global_enabled_products(
    monkeypatch,
) -> None:
    port = FakeIjpDataPort()
    database = object()
    monkeypatch.setattr(
        composition,
        "build_ijp_repository",
        lambda received: port if received is database else None,
    )
    monkeypatch.setattr(
        ConfigLoader,
        "get_enabled_products",
        classmethod(lambda cls: ["M678"]),
    )
    monkeypatch.setattr(
        ConfigLoader,
        "get_work_order_types",
        classmethod(lambda cls: ["P", "LCFG"]),
    )

    service = composition.build_ijp_service(database)
    service._today_provider = lambda: TODAY
    options = service.get_filter_options()
    service.get_details(_query())

    assert options["product_codes"] == ("M678",)
    assert port.received_query is not None
    assert port.received_query.work_order_types == ("P", "LCFG")


def test_global_work_order_types_are_loaded_as_a_nonempty_allowlist(
    tmp_path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        "data_source:\n  work_order_types: [P, LCFG, P]\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)

    assert ConfigLoader.get_work_order_types() == ["P", "LCFG"]


def test_service_delegates_report_reads_to_the_port() -> None:
    port = FakeIjpDataPort()
    service = _service(port)
    query = _query()

    ratios = service.get_glass_ratios(query)
    details = service.get_details(query)

    assert ratios.to_dict("records") == [
        {
            "productcode": "M626",
            "line": "3CEE01",
            "printer": "3CEE01-IK2-PR1",
            "glass_id": "2026-08-31",
            "rs_code": "C3DM1",
            "code_num": 3,
            "ratio": 1.0,
        }
    ]
    assert details.to_dict("records") == [{"glass_id": "G1", "code_ratio": 0.667}]
    assert port.received_query is not query
    assert port.received_query.start_time == START
    assert port.received_query.end_time == END


def test_service_propagates_the_stable_data_access_error() -> None:
    service = _service(FailingIjpDataPort())

    with pytest.raises(IjpDataAccessError) as caught:
        service.get_details(_query())

    assert str(caught.value) == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"


def test_config_allowlist_applies_to_empty_selection_and_rejects_bypass():
    port = FakeIjpDataPort()
    service = IjpReportService(port, settings=IjpSettings(codes=("C3DM1", "C3DM2")))
    service.get_printer_ratios(_query())
    assert port.received_query.codes == ("C3DM1", "C3DM2")
    assert port.received_query.start_time == START
    assert port.received_query.end_time == END
    with pytest.raises(ValueError, match="配置允许范围"):
        service.get_details(_query(codes=("C3RA1",)))


def test_selected_dates_are_used_for_batch_options():
    class WindowPort(FakeIjpDataPort):
        def list_picis(self, start_time, end_time, product_codes):
            assert (start_time, end_time) == (START, END)
            return ("CUSTOM",)
    assert _service(WindowPort()).get_filter_options(
        start_time=START, end_time=END,
    )["picis"] == ("CUSTOM",)
