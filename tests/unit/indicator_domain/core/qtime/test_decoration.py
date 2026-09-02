import pandas as pd

from src.indicator_domain.core.qtime.decoration import (
    apply_qtime_decoration,
    build_qtime_oos_detail,
)


def test_qtime_oos_detail_contains_only_wait_times_above_the_specification() -> None:
    details = pd.DataFrame(
        [
            _row(lot_id="L001", wait_time=25.1, q_spec=24.0),
            _row(lot_id="L002", wait_time=24.0, q_spec=24.0),
            _row(lot_id="L003", wait_time=10.0, q_spec=24.0),
        ]
    )

    result = build_qtime_oos_detail(details)

    assert result["lot_id"].tolist() == ["L001"]
    assert result.loc[0, "over_hours"] == 1.1


def test_qtime_decoration_applies_true_false_and_delete_actions() -> None:
    details = pd.DataFrame(
        [
            _row(lot_id="L001", wait_time=25.1, q_spec=24.0),
            _row(lot_id="L002", wait_time=26.0, q_spec=24.0),
            _row(lot_id="L003", wait_time=27.0, q_spec=24.0),
        ]
    )
    decisions = pd.DataFrame(
        [
            _decision("L001", True),
            _decision("L002", False),
            _decision("L003", "Delete"),
        ]
    )

    result = apply_qtime_decoration(details, decisions)

    assert result.details["lot_id"].tolist() == ["L001", "L002"]
    assert result.details.loc[result.details["lot_id"] == "L001", "wait_time"].item() < 24.0
    assert result.details.loc[result.details["lot_id"] == "L002", "wait_time"].item() == 26.0
    assert result.decoration["flag"].tolist() == [True, False, "Delete"]


def _row(*, lot_id: str, wait_time: float, q_spec: float) -> dict[str, object]:
    return {
        "shop": "OLED",
        "prodcode": "M626",
        "f_step": "21100",
        "t_step": "21200",
        "step_desc": "Half Cutting->EVA&TFE",
        "lot_id": lot_id,
        "timekey": f"20260901{lot_id[-1]}0000000000",
        "q_spec": q_spec,
        "wait_time": wait_time,
    }


def _decision(lot_id: str, flag: object) -> dict[str, object]:
    row = _row(lot_id=lot_id, wait_time=0.0, q_spec=0.0)
    return {
        "prodcode": row["prodcode"],
        "step_desc": row["step_desc"],
        "lot_id": row["lot_id"],
        "timekey": row["timekey"],
        "flag": flag,
    }
