import pandas as pd

from src.indicator_domain.core.qtime.alerts import build_qtime_alerts


def test_qtime_alerts_include_only_confirmed_real_over_spec_records() -> None:
    decoration = pd.DataFrame(
        [
            {"lot_id": "L001", "timekey": "20260901010000", "flag": True},
            {"lot_id": "L002", "timekey": "20260902010000", "flag": False},
            {"lot_id": "L003", "timekey": "20260903010000", "flag": "Delete"},
            {"lot_id": "L004", "timekey": "20260901020000", "flag": "不修饰"},
        ]
    )

    result = build_qtime_alerts(decoration)

    assert result["lot_id"].tolist() == ["L002", "L004"]
