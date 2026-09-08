import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.snapshot_window import inline_snapshot_window_start


@pytest.mark.parametrize("end,start", [
    ("2026-09-07", "2026-06-01"),
    ("2026-01-01", "2025-10-01"),
    ("2026-03-31", "2025-12-01"),
    ("2026-12-31", "2026-09-01"),
])
def test_source_window_keeps_three_preceding_calendar_months(end, start):
    assert inline_snapshot_window_start(end) == pd.Timestamp(start)
