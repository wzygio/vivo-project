import pandas as pd

from yield_domain.core.sheet_lot.sheet_lot_processor import (
    _filter_lots_by_warehousing_window,
)


def test_filters_lots_before_first_day_of_month_two_months_ago():
    lot_base = pd.DataFrame(
        {
            "lot_id": ["APRIL", "MAY_FIRST", "JULY", "INVALID"],
            "warehousing_time": ["20260430", "20260501", "20260714", "not-a-date"],
        }
    )

    result = _filter_lots_by_warehousing_window(
        lot_base, as_of=pd.Timestamp("2026-07-14")
    )

    assert result["lot_id"].tolist() == ["MAY_FIRST", "JULY"]


def test_warehousing_window_handles_year_boundary():
    lot_base = pd.DataFrame(
        {
            "lot_id": ["OCTOBER", "NOVEMBER_FIRST"],
            "warehousing_time": ["20251031", "20251101"],
        }
    )

    result = _filter_lots_by_warehousing_window(
        lot_base, as_of=pd.Timestamp("2026-01-14")
    )

    assert result["lot_id"].tolist() == ["NOVEMBER_FIRST"]
