"""超规项自动修饰（auto_clip_over_spec）行为测试。"""

import pandas as pd

from src.inline_domain.core.shared.auto_decoration import auto_clip_over_spec


def _details() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "sheet_id": "S1", "qty": 10.0},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "sheet_id": "S2", "qty": 3.0},
            {"prod_code": "M678", "factory": "OLED", "step_id": "99999", "sheet_id": "S3", "qty": 50.0},
        ]
    )


def _specs() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "upper": 5.0},
        ]
    )


def test_clips_over_spec_value_below_upper_bound() -> None:
    result = auto_clip_over_spec(
        _details(), _specs(), value_col="qty",
        join_keys=["factory", "step_id"], upper_col="upper",
    )
    clipped = result[result["sheet_id"] == "S1"]["qty"].iloc[0]
    # 单边规格：截断到上限的 85%~95%
    assert 5.0 * 0.85 <= clipped < 5.0


def test_keeps_in_spec_and_unmatched_rows_unchanged() -> None:
    result = auto_clip_over_spec(
        _details(), _specs(), value_col="qty",
        join_keys=["factory", "step_id"], upper_col="upper",
    )
    # 规格内行不变
    assert result[result["sheet_id"] == "S2"]["qty"].iloc[0] == 3.0
    # 无匹配规格的行不变
    assert result[result["sheet_id"] == "S3"]["qty"].iloc[0] == 50.0


def test_clip_is_deterministic_across_calls() -> None:
    kwargs = dict(value_col="qty", join_keys=["factory", "step_id"], upper_col="upper")
    first = auto_clip_over_spec(_details(), _specs(), **kwargs)
    second = auto_clip_over_spec(_details(), _specs(), **kwargs)
    pd.testing.assert_frame_equal(first, second)


def test_two_sided_spec_clips_lower_violation_above_lower_bound() -> None:
    df = pd.DataFrame([{"step_id": "S", "qty": 1.0}])
    spec = pd.DataFrame([{"step_id": "S", "upper": 10.0, "lower": 4.0}])
    result = auto_clip_over_spec(
        df, spec, value_col="qty",
        join_keys=["step_id"], upper_col="upper", lower_col="lower",
    )
    clipped = result["qty"].iloc[0]
    # 下限越规：截断到下限以上 5%~15% span（span=6）→ [4.3, 4.9)
    assert 4.0 < clipped <= 4.0 + 0.15 * 6.0


def test_empty_inputs_are_safe() -> None:
    kwargs = dict(value_col="qty", join_keys=["factory", "step_id"], upper_col="upper")
    assert auto_clip_over_spec(pd.DataFrame(), _specs(), **kwargs).empty
    result = auto_clip_over_spec(_details(), pd.DataFrame(), **kwargs)
    assert len(result) == 3


def test_clip_over_spec_column_uses_same_frame_spec() -> None:
    from src.inline_domain.core.shared.auto_decoration import clip_over_spec_column

    df = pd.DataFrame(
        [
            {"step_id": "S1", "value": 10.0, "spec": 5.0},
            {"step_id": "S2", "value": 2.0, "spec": 5.0},
            {"step_id": "S3", "value": 99.0, "spec": None},
        ]
    )
    result = clip_over_spec_column(df, value_col="value", spec_col="spec")
    clipped = result[result["step_id"] == "S1"]["value"].iloc[0]
    assert 5.0 * 0.85 <= clipped < 5.0
    # 规格内与无规格行不变
    assert result[result["step_id"] == "S2"]["value"].iloc[0] == 2.0
    assert result[result["step_id"] == "S3"]["value"].iloc[0] == 99.0


def _tri_state_frame() -> pd.DataFrame:
    # df 已带规格列（attach 之后），3 行均超规（qty > spec）
    return pd.DataFrame(
        [
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S1", "qty": 10.0, "spec": 5.0},
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S2", "qty": 10.0, "spec": 5.0},
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S3", "qty": 10.0, "spec": 5.0},
        ]
    )


def _tri_state_decoration() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S1", "flag": True},
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S2", "flag": False},
            {"prod_code": "M678", "step_id": "11620", "sheet_id": "S3", "flag": "Delete"},
        ]
    )


def test_tri_state_decoration_clips_releases_and_deletes_by_flag() -> None:
    from src.inline_domain.core.shared.auto_decoration import apply_tri_state_decoration

    result = apply_tri_state_decoration(
        _tri_state_frame(),
        _tri_state_decoration(),
        key_columns=["prod_code", "step_id", "sheet_id"],
        value_col="qty",
        spec_col="spec",
    )

    # Delete 行被剔除
    assert set(result["sheet_id"]) == {"S1", "S2"}
    # flag=True：截断到线内
    clipped = result[result["sheet_id"] == "S1"]["qty"].iloc[0]
    assert 5.0 * 0.85 <= clipped < 5.0
    # flag=False：释放真实值
    assert result[result["sheet_id"] == "S2"]["qty"].iloc[0] == 10.0
