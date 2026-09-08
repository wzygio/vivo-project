import pandas as pd

from src.indicator_domain.core.qtime.decoration import (
    apply_qtime_decoration,
    build_qtime_oos_detail,
    apply_qtime_spec_overrides,
    constrain_qtime_display,
)


def test_spec_override_is_scoped_to_route_and_preserves_source():
    source = pd.DataFrame([_row(lot_id="L1", wait_time=400, q_spec=200)])
    source["f_step"] = "21200"
    source["t_step"] = "21300"
    other = pd.DataFrame([_row(lot_id="L2", wait_time=20, q_spec=24)])
    result = apply_qtime_spec_overrides(
        pd.concat([source, other], ignore_index=True), {"OLED/21200/21300": 370}
    )
    assert result.q_spec.tolist() == [370, 24]
    assert result.q_spec_raw.tolist() == [200, 24]
    assert source.q_spec.tolist() == [200]


def test_display_is_strictly_below_spec_even_for_false_and_equal_values():
    source = pd.DataFrame([
        _row(lot_id="L001", wait_time=400, q_spec=370),
        _row(lot_id="L002", wait_time=370, q_spec=370),
        _row(lot_id="L003", wait_time=100, q_spec=370),
    ])
    prepared = apply_qtime_spec_overrides(source, {})
    decorated = apply_qtime_decoration(prepared, pd.DataFrame([_decision("L001", False)]))
    result = constrain_qtime_display(decorated.details)
    assert result.wait_time.lt(result.q_spec).all()
    assert result.wait_time_raw.tolist() == [400, 370, 100]
    assert result.wait_time.iloc[2] == 100
    assert decorated.decoration.wait_time.tolist() == [400]
    pd.testing.assert_frame_equal(result, constrain_qtime_display(decorated.details))


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
