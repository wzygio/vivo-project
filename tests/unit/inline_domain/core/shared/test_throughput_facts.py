from __future__ import annotations

import pandas as pd

from src.inline_domain.core.shared.throughput_facts import build_daily_throughput_facts


def test_daily_throughput_keeps_each_physical_sheet_once() -> None:
    source = pd.DataFrame(
        [
            {"factory": "ARRAY", "sheet_id": "S1", "step_id": "10", "param_name": "P1", "sheet_start_time": "2026-08-10 08:00"},
            {"factory": "ARRAY", "sheet_id": "S1", "step_id": "10", "param_name": "P1", "sheet_start_time": "2026-08-10 09:00"},
            {"factory": "ARRAY", "sheet_id": "S1", "step_id": "10", "param_name": "P2", "sheet_start_time": "2026-08-10 09:00"},
        ]
    )

    result = build_daily_throughput_facts(
        source,
        scope="SPC",
        prod_code="M626",
        event_time_column="sheet_start_time",
        item_id_column="sheet_id",
    )

    assert result.to_dict("records") == [
        {
            "scope": "spc",
            "factory": "ARRAY",
            "prod_code": "M626",
            "event_date": pd.Timestamp("2026-08-10"),
            "item_id": "S1",
        }
    ]
