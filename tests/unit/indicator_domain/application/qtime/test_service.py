from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import pandas as pd

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.service import QTimeReportService


class FakeQTimeDataPort:
    def __init__(self) -> None:
        self.received_query: QTimeQuery | None = None

    def list_products(self) -> tuple[str, ...]:
        raise AssertionError("Page Header owns the Q-Time product selection")

    def list_step_options(self, shop: str) -> tuple[QTimeStepOption, ...]:
        assert shop == "ARRAY"
        return (
            QTimeStepOption(
                step_desc="M3_DE->M3_STR",
                f_step="15500",
                t_step="15600",
            ),
        )

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        self.received_query = query
        return pd.DataFrame({"lot_id": ["L001"], "wait_time": [0.41]})


class FakeQTimeDecorationPort:
    decoration_path = Path("resources/qtime_domain/qtime_oos_decoration.xlsx")

    def load_decisions(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prodcode": "M626",
                    "step_desc": "Half Cutting->EVA&TFE",
                    "lot_id": "L001",
                    "timekey": "20260902010000",
                    "flag": False,
                }
            ]
        )

    def save_decisions(self, decisions: pd.DataFrame) -> None:
        raise AssertionError("This test only reads decoration decisions")


class RecordingQTimeDecorationPort(FakeQTimeDecorationPort):
    def __init__(self) -> None:
        self.saved_decisions: pd.DataFrame | None = None

    def save_decisions(self, decisions: pd.DataFrame) -> None:
        self.saved_decisions = decisions.copy()


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

    assert options == {
        "step_options": (
            QTimeStepOption(
                step_desc="M3_DE->M3_STR",
                f_step="15500",
                t_step="15600",
            ),
        )
    }
    assert report.to_dict("records") == [{"lot_id": "L001", "wait_time": 0.41}]
    assert port.received_query is query


def test_service_queries_from_previous_month_start_through_the_current_day() -> None:
    port = FakeQTimeDataPort()
    service = QTimeReportService(port)

    service.get_current_report(
        shop="OLED",
        step_descriptions=("Half Cutting->EVA&TFE",),
        products=("M626",),
        as_of=date(2026, 9, 2),
    )

    assert port.received_query == QTimeQuery(
        start_time=datetime(2026, 8, 1),
        end_time=datetime(2026, 9, 3),
        shop="OLED",
        step_descriptions=("Half Cutting->EVA&TFE",),
        products=("M626",),
    )


def test_service_returns_decorated_details_and_confirmed_qtime_alerts() -> None:
    port = FakeQTimeDataPort()
    port.fetch_details = lambda query: pd.DataFrame(
        [
            {
                "shop": "OLED",
                "prodcode": "M626",
                "f_step": "21100",
                "t_step": "21200",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "q_spec": 24.0,
                "wait_time": 25.0,
            }
        ]
    )
    service = QTimeReportService(port, FakeQTimeDecorationPort())

    result = service.get_current_monitoring(
        shop="OLED",
        step_descriptions=("Half Cutting->EVA&TFE",),
        products=("M626",),
        as_of=date(2026, 9, 2),
    )

    assert result.details.loc[0, "wait_time"] == 25.0
    assert result.alerts["lot_id"].tolist() == ["L001"]
    assert result.decoration.loc[0, "flag"] == False  # noqa: E712


def test_service_persists_a_valid_uploaded_qtime_decision_ledger() -> None:
    decoration_port = RecordingQTimeDecorationPort()
    service = QTimeReportService(FakeQTimeDataPort(), decoration_port)
    ledger = pd.DataFrame(
        [
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "flag": False,
            }
        ]
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        ledger.to_excel(writer, index=False, sheet_name="决策台账")

    outcome = service.update_decisions(output.getvalue())

    assert outcome.status == "success"
    assert decoration_port.saved_decisions is not None
    assert decoration_port.saved_decisions.to_dict("records") == ledger.to_dict("records")
