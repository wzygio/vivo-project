from datetime import datetime

import pytest
from pydantic import ValidationError

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption


def test_step_option_labels_the_original_station_codes_with_the_description() -> None:
    option = QTimeStepOption(
        step_desc="Half Cutting->EVA&TFE",
        f_step="21100",
        t_step="21200",
    )

    assert option.label == "21100 → 21200｜Half Cutting->EVA&TFE"
from src.indicator_domain.core.qtime.shop import classify_shop


def test_qtime_query_requires_a_positive_half_open_time_window() -> None:
    with pytest.raises(ValidationError, match="结束时间必须晚于开始时间"):
        QTimeQuery(
            start_time=datetime(2026, 9, 1, 8, 0),
            end_time=datetime(2026, 9, 1, 8, 0),
            shop="ARRAY",
            step_descriptions=("M3_DE->M3_STR",),
        )


@pytest.mark.parametrize(
    ("f_step", "expected"),
    [("15500", "ARRAY"), ("25500", "OLED"), ("35500", "TP"), ("", "TP")],
)
def test_shop_classification_matches_the_finereport_contract(
    f_step: str,
    expected: str,
) -> None:
    assert classify_shop(f_step) == expected


def test_qtime_query_normalizes_path_and_product_filters() -> None:
    query = QTimeQuery(
        start_time=datetime(2026, 8, 1),
        end_time=datetime(2026, 9, 1),
        shop="ARRAY",
        step_descriptions=("  A->B  ", "", "A->B", " B->C "),
        products=(" M626 ", "", "M626", " M678"),
    )

    assert query.step_descriptions == ("A->B", "B->C")
    assert query.products == ("M626", "M678")


def test_qtime_query_requires_at_least_one_path() -> None:
    with pytest.raises(ValidationError):
        QTimeQuery(
            start_time=datetime(2026, 8, 1),
            end_time=datetime(2026, 9, 1),
            shop="ARRAY",
            step_descriptions=(),
        )
