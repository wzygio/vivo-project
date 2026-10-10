from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.shared.sheet_oos_decoration import apply_spc_point_decoration, OOS_KEY_COLUMNS
from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report


POLICY = (.5, "2026-10-05", "2026-10-07")


def points(values, times=None):
    return pd.DataFrame([
        dict(prod_code="P", factory="ARRAY", step_id="1", param_name="CD", sheet_id=f"S{i // 2}",
             site_name=f"P{i}", unit_id="EQ", param_value=value, data_type="SPC",
             sheet_start_time=pd.Timestamp(day))
        for i, (value, day) in enumerate(zip(values, times or ["2026-10-05"] * len(values)))
    ])


def specs(lsl=2., usl=10.):
    return pd.DataFrame([dict(prod_code="P", step_id="1", param_name="CD", lsl=lsl, usl=usl)])


def test_band_changes_all_outside_points_and_keeps_inside_points_and_source():
    source = points([1., 3., 4., 5., 8., 9., 11.])
    before = source.copy(deep=True)
    result = apply_spc_point_decoration(source, specs(), pd.DataFrame(), point_policy=POLICY)
    assert result.param_value.between(4., 8.).all()
    assert result.param_value.iloc[2:5].tolist() == [4., 5., 8.]
    assert result.usl.eq(10.).all() and result.lsl.eq(2.).all()
    assert len(result) == len(source)
    pd.testing.assert_frame_equal(source, before)
    shuffled = apply_spc_point_decoration(source.sample(frac=1, random_state=1), specs(), pd.DataFrame(), point_policy=POLICY)
    pd.testing.assert_series_equal(result.set_index("site_name").param_value.sort_index(), shuffled.set_index("site_name").param_value.sort_index())


def test_window_includes_end_day_and_outside_uses_legacy_clipping():
    source = points([11.] * 4, ["2026-10-04 23:59:59", "2026-10-05", "2026-10-07 23:59:59", "2026-10-08"])
    values = apply_spc_point_decoration(source, specs(), pd.DataFrame(), point_policy=POLICY).param_value
    assert values.iloc[[1, 2]].between(4., 8.).all()
    assert values.iloc[[0, 3]].between(8.8, 9.6).all()


@pytest.mark.parametrize("flag", [False, "Delete"])
def test_manual_preservation_has_priority_over_band(flag):
    source = points([3., 11.])
    decisions = source[OOS_KEY_COLUMNS].drop_duplicates().assign(flag=flag)
    result = apply_spc_point_decoration(source, specs(), decisions, point_policy=POLICY)
    assert result.param_value.tolist() == [3., 11.]


@pytest.mark.parametrize("lower,upper", [(None, 10.), (2., None), (10., 2.), (2., float("inf"))])
def test_invalid_double_sided_spec_does_not_change_values(lower, upper):
    source = points([3., 11.])
    result = apply_spc_point_decoration(source, specs(lower, upper), pd.DataFrame(), point_policy=POLICY)
    assert result.param_value.tolist() == [3., 11.]


def test_features_and_capability_use_the_same_decorated_points(tmp_path):
    class Decisions:
        def load_sheet_oos_decisions(self, *_a):
            return pd.DataFrame()

    result = prepare_decorated_data(
        points([3., 9., 5., 11.]), specs(), "P", "spc", product_dir=tmp_path,
        persist=False, decoration_port=Decisions(), spc_point_policy=POLICY,
    )
    data = result.raw_measurements_df
    expected_means = data.groupby("sheet_id").param_value.mean()
    pd.testing.assert_series_equal(result.sheet_features_df.set_index("sheet_id").sheet_mean.sort_index(), expected_means.sort_index(), check_names=False)
    capability = build_period_capability_report(
        result.sheet_features_df, date(2026, 10, 12), data, "point_value", previous_week_only=True,
    ).iloc[0]
    mean, sigma = expected_means.mean(), data.param_value.std(ddof=1)
    assert capability["point_count"] == 4
    assert capability["cpk"] == pytest.approx(min(10. - mean, mean - 2.) / (3. * sigma))
    assert capability["cpm"] == pytest.approx((8. / (6. * sigma)) / (1. + abs(2. * (mean - 6.) / 8.)))
