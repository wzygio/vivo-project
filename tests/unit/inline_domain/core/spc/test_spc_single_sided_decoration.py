import hashlib

import pandas as pd
import pytest

from src.inline_domain.core.shared.sheet_oos_decoration import OOS_KEY_COLUMNS, apply_sheet_oos_decoration
from src.inline_domain.core.spc.spc_point_decoration import apply_spc_point_decoration
from src.inline_domain.application.shared.decorated_data import prepare_decorated_data


def _points():
    return pd.DataFrame([
        dict(prod_code="Z553", factory="OLED", step_id="2120A", param_name="SE_EV2_T1_UNI",
             sheet_id="S1", site_name=f"P{i}", unit_id="EQ1", param_value=value,
             data_type="SPC", sheet_start_time=pd.Timestamp("2026-11-02"))
        for i, value in enumerate([1., 12.])
    ])


def _specs():
    return pd.DataFrame([
        dict(prod_code="Z553", step_id="2120A", param_name="SE_EV2_T1_UNI", usl=10., lsl=None)
    ])


def test_shared_sheet_decoration_clips_2120a_with_only_usl():
    source = _points()
    features = source.iloc[[0]].drop(columns=["site_name", "unit_id", "param_value"]).assign(
        sheet_max=12., sheet_min=1., sheet_mean=6.5, usl=10., lsl=None,
    )

    result = apply_sheet_oos_decoration(source, features)

    assert result.param_value.iloc[0] == 1.
    assert 8.5 <= result.param_value.iloc[1] <= 9.5


def test_spc_2120a_executes_traditional_then_special_with_only_usl():
    source = _points()
    traditional = apply_spc_point_decoration(source, _specs(), pd.DataFrame())
    special = apply_spc_point_decoration(
        source, _specs(), pd.DataFrame(), point_policy=(.5, "2026-10-05", "2026-11-03"),
    )

    assert 8.5 <= traditional.param_value.iloc[1] <= 9.5
    assert special.param_value.iloc[1] < traditional.param_value.iloc[1]
    assert special.usl.eq(10.).all()
    assert special.lsl.isna().all()


@pytest.mark.parametrize("lower,upper,value,traditional_range,special_range", [
    (None, 10., 12., (8.5, 9.5), (6.75, 7.25)),
    (10., None, 8., (10.5, 11.5), (12.75, 13.25)),
    (None, -10., -8., (-11.5, -10.5), (-13.25, -12.75)),
    (-10., None, -12., (-9.5, -8.5), (-7.25, -6.75)),
])
def test_single_sided_direction_and_special_margin_hash(lower, upper, value, traditional_range, special_range):
    source = _points().iloc[[0]].assign(param_value=value)
    limits = _specs().assign(lsl=lower, usl=upper)
    before = source.copy(deep=True)
    traditional = apply_spc_point_decoration(source, limits, pd.DataFrame())
    special = apply_spc_point_decoration(
        source, limits, pd.DataFrame(), point_policy=(.5, "2026-10-05", "2026-11-03"),
    )
    assert traditional_range[0] <= traditional.param_value.iloc[0] <= traditional_range[1]
    assert special_range[0] <= special.param_value.iloc[0] <= special_range[1]
    row = traditional.iloc[0]
    side = "upper" if upper is not None else "lower"
    seed = "|".join(str(row[k]) for k in [
        "prod_code", "step_id", "param_name", "sheet_id", "site_name", "unit_id", "param_value",
    ]) + "|" + side
    h = int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16) / 0xFFFFFFFFFFFF
    span = abs(upper if upper is not None else lower)
    margin = (.05 + h * .1) * span * .5
    bound = (upper - span * .25) if upper is not None else (lower + span * .25)
    assert special.param_value.iloc[0] == pytest.approx(bound - margin if side == "upper" else bound + margin)
    pd.testing.assert_frame_equal(source, before)
    assert not any(c.startswith("_oos_") or c == "_decorate" for c in special.columns)


@pytest.mark.parametrize("flag", [False, "Delete"])
def test_spc_single_sided_manual_preservation_and_date_window(flag):
    source = _points().assign(param_value=12.)
    decisions = source[OOS_KEY_COLUMNS].drop_duplicates().assign(flag=flag)
    result = apply_spc_point_decoration(
        source, _specs(), decisions, point_policy=(.5, "2026-10-05", "2026-11-03"),
    )
    assert result.param_value.eq(12.).all()
    outside = apply_spc_point_decoration(
        source, _specs(), pd.DataFrame(), point_policy=(.5, "2026-10-05", "2026-10-31"),
    )
    assert outside.param_value.between(8.5, 9.5).all()


def test_single_sided_zero_span_and_unmatched_spec_preserve_values():
    source = _points()
    for limits in [_specs().assign(usl=0.), _specs().assign(usl=float("inf")), _specs().assign(prod_code="OTHER")]:
        result = apply_spc_point_decoration(
            source, limits, pd.DataFrame(), point_policy=(.5, "2026-10-05", "2026-11-03"),
        )
        assert result.param_value.tolist() == source.param_value.tolist()


@pytest.mark.parametrize("flag", [True, False, "Delete"])
def test_shared_lsl_only_decoration_respects_three_state_decisions(flag):
    source = _points()
    features = source.iloc[[0]].drop(columns=["site_name", "unit_id", "param_value"]).assign(
        sheet_max=12., sheet_min=1., sheet_mean=6.5, usl=None, lsl=10.,
    )
    decisions = features.assign(flag=flag)
    result = apply_sheet_oos_decoration(source, features, decisions)
    if flag == "Delete":
        assert result.empty
    elif flag is False:
        assert result.param_value.tolist() == source.param_value.tolist()
    else:
        assert 10.5 <= result.param_value.iloc[0] <= 11.5
        assert result.param_value.iloc[1] == 12.


def test_single_sided_special_does_not_create_an_opposite_boundary():
    upper_only = apply_spc_point_decoration(
        _points().assign(param_value=-100.), _specs(), pd.DataFrame(),
        point_policy=(.5, "2026-10-05", "2026-11-03"),
    )
    lower_only = apply_spc_point_decoration(
        _points().assign(param_value=100.), _specs().assign(usl=None, lsl=10.), pd.DataFrame(),
        point_policy=(.5, "2026-10-05", "2026-11-03"),
    )
    assert upper_only.param_value.eq(-100.).all()
    assert lower_only.param_value.eq(100.).all()


def test_2120a_preparation_recomputes_features_from_single_sided_decorated_points(tmp_path):
    class Decisions:
        def load_sheet_oos_decisions(self, *_args):
            return pd.DataFrame()

    result = prepare_decorated_data(
        _points(), _specs(), "Z553", "spc", product_dir=tmp_path, persist=False,
        decoration_port=Decisions(), spc_point_policy=(.5, "2026-10-05", "2026-11-03"),
    )
    assert result.raw_measurements_df.param_value.iloc[0] == 1.
    assert result.raw_measurements_df.param_value.iloc[1] < 7.5
    assert result.original_sheet_features_df.sheet_max.iloc[0] == 12.
    assert result.sheet_features_df.sheet_max.iloc[0] < 7.5
    assert result.sheet_features_df.sheet_mean.iloc[0] == pytest.approx(result.raw_measurements_df.param_value.mean())
