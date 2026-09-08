from datetime import datetime

import pytest
from pydantic import ValidationError

from src.indicator_domain.application.ijp.dtos import IjpQuery


def test_ijp_query_accepts_a_closed_time_window_with_optional_filters() -> None:
    query = IjpQuery(
        start_time=datetime(2026, 8, 31, 7, 0),
        end_time=datetime(2026, 9, 1, 7, 0),
        product_codes=[" M626 ", "M626", "", None, "M678"],
        work_order_types=[" P ", "P", "ESLC"],
    )

    assert query.product_codes == ("M626", "M678")
    assert query.work_order_types == ("P", "ESLC")
    assert query.detail_limit == 5000


@pytest.mark.parametrize(
    "removed_filter",
    ["glass_ids", "equipments", "panel_locations", "cycles"],
)
def test_ijp_query_rejects_removed_filters(removed_filter: str) -> None:
    with pytest.raises(ValidationError):
        IjpQuery(
            start_time=datetime(2026, 8, 31, 7, 0),
            end_time=datetime(2026, 9, 1, 7, 0),
            **{removed_filter: ("legacy-value",)},
        )


def test_ijp_query_rejects_an_inverted_time_window() -> None:
    with pytest.raises(ValidationError) as caught:
        IjpQuery(
            start_time=datetime(2026, 9, 1, 7, 0),
            end_time=datetime(2026, 8, 31, 7, 0),
        )

    assert "结束时间不能早于开始时间" in str(caught.value)


def test_ijp_query_is_frozen() -> None:
    query = IjpQuery(
        start_time=datetime(2026, 8, 31, 7, 0),
        end_time=datetime(2026, 9, 1, 7, 0),
    )

    with pytest.raises(ValidationError):
        query.start_time = datetime(2026, 8, 30, 7, 0)
