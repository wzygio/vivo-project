import pandas as pd

from app.charts.yield_domain.mwd_chart import prepare_union_data_for_filter
from app.components.code_selector import (
    build_batch_code_options_by_group,
    create_group_batch_selection_ui,
    _build_code_options_by_group,
    _calculate_eligible_series,
    _get_default_group,
    _prepare_processed_dataframe,
)


class _Context:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _BatchSelectorStreamlit:
    def __init__(
        self,
        *,
        selected_groups: list[str],
        selected_codes: list[str],
        query_clicked: bool,
    ) -> None:
        self.session_state: dict[str, object] = {}
        self.selected_groups = selected_groups
        self.selected_codes = selected_codes
        self.query_clicked = query_clicked
        self.button_kwargs: dict[str, object] = {}
        self.multiselect_calls: list[dict[str, object]] = []

    def container(self):
        return _Context()

    def columns(self, spec, **_kwargs):
        return [_Context() for _ in spec]

    def multiselect(self, label, *, options, key, **_kwargs):
        selected = self.selected_groups if label == "不良 Group" else self.selected_codes
        self.session_state[key] = selected
        self.multiselect_calls.append(
            {"label": label, "options": options, "key": key, **_kwargs}
        )
        return selected

    def metric(self, *_args, **_kwargs):
        return None

    def button(self, _label, **kwargs):
        self.button_kwargs = kwargs
        return self.query_clicked

    def info(self, *_args, **_kwargs):
        return None


def test_rate_filter_prefers_monthly_grain_from_mwd_dict() -> None:
    source_data = {
        "monthly": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.00005,
                },
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "高发Code",
                    "defect_rate": 0.0002,
                },
            ]
        ),
        "daily": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.01,
                },
            ]
        ),
    }

    processed_df = _prepare_processed_dataframe(source_data)

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert ("Array_Mura", "RGB黑斑") not in eligible.index
    assert eligible.loc[("Array_Mura", "高发Code")] == 0.0002


def test_rate_filter_uses_monthly_average_metric_when_present() -> None:
    processed_df = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "RGB黑斑",
                "defect_rate": 0.02,
                "monthly_avg_rate": 0.00005,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发Code",
                "defect_rate": 0.002,
                "monthly_avg_rate": 0.0002,
            },
        ]
    )

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )
    options = _build_code_options_by_group(["Array_Mura"], eligible)

    assert options["Array_Mura"] == ["---请选择---", "高发Code"]


def test_rate_filter_falls_back_to_defect_rate_without_monthly_context() -> None:
    processed_df = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "Lot高发Code",
                "defect_rate": 0.0002,
            },
        ]
    )

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert eligible.loc[("Array_Mura", "Lot高发Code")] == 0.0002


def test_union_filter_payload_preserves_monthly_average_for_threshold() -> None:
    mwd_data = {
        "monthly": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.00005,
                }
            ]
        ),
        "daily": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.01,
                }
            ]
        ),
    }
    lot_data = {
        "code_level_details": {
            "Array_Mura": pd.DataFrame(
                [
                    {
                        "defect_group": "Array_Mura",
                        "defect_desc": "RGB黑斑",
                        "defect_rate": 0.02,
                    }
                ]
            )
        }
    }

    payload = prepare_union_data_for_filter(mwd_data, lot_data, pd.DataFrame())
    eligible = _calculate_eligible_series(
        payload,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert payload.loc[0, "defect_rate"] == 0.02
    assert payload.loc[0, "monthly_avg_rate"] == 0.00005
    assert ("Array_Mura", "RGB黑斑") not in eligible.index


def test_default_group_prefers_group_with_selectable_codes() -> None:
    options = {
        "Array_Line": ["---请选择---"],
        "Array_Mura": ["---请选择---", "高发Code"],
    }

    assert _get_default_group(["Array_Line", "Array_Mura"], options) == "Array_Mura"


def test_batch_code_options_return_all_eligible_codes_without_placeholder() -> None:
    source_data = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发CodeA",
                "monthly_avg_rate": 0.0003,
                "defect_rate": 0.003,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发CodeB",
                "monthly_avg_rate": 0.0002,
                "defect_rate": 0.002,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "低发Code",
                "monthly_avg_rate": 0.00001,
                "defect_rate": 0.01,
            },
        ]
    )

    options = build_batch_code_options_by_group(
        source_data,
        rate_threshold=0.0001,
    )

    assert options == {"Array_Mura": ["高发CodeA", "高发CodeB"]}


def test_batch_code_options_keep_eligible_codes_across_groups() -> None:
    source_data = pd.DataFrame(
        [
            {
                "defect_group": "Array_Line",
                "defect_desc": "线亮点",
                "monthly_avg_rate": 0.0002,
                "defect_rate": 0.0002,
            },
            {
                "defect_group": "Array_Pixel",
                "defect_desc": "暗点",
                "monthly_avg_rate": 0.0003,
                "defect_rate": 0.0003,
            },
            {
                "defect_group": "Array_Pixel",
                "defect_desc": "低发暗点",
                "monthly_avg_rate": 0.00001,
                "defect_rate": 0.005,
            },
        ]
    )

    options = build_batch_code_options_by_group(
        source_data,
        rate_threshold=0.0001,
    )

    assert options == {
        "Array_Line": ["线亮点"],
        "Array_Pixel": ["暗点"],
    }


def test_batch_selector_requires_query_before_rendering(monkeypatch) -> None:
    fake_st = _BatchSelectorStreamlit(
        selected_groups=["Array_Mura"],
        selected_codes=["高发Code"],
        query_clicked=False,
    )
    monkeypatch.setattr("app.components.code_selector.st", fake_st)

    selection = create_group_batch_selection_ui(
        source_data=pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "高发Code",
                    "monthly_avg_rate": 0.0002,
                }
            ]
        ),
        key_prefix="unified_focus",
    )

    assert selection["should_render"] is False
    assert fake_st.button_kwargs == {
        "type": "primary",
        "width": "stretch",
        "disabled": False,
    }


def test_batch_selector_starts_empty_and_filters_codes_by_selected_groups(monkeypatch) -> None:
    fake_st = _BatchSelectorStreamlit(
        selected_groups=["Array_Mura"],
        selected_codes=["高发Code"],
        query_clicked=False,
    )
    monkeypatch.setattr("app.components.code_selector.st", fake_st)
    source_data = pd.DataFrame(
        [
            {
                "defect_group": "Array_Line",
                "defect_desc": "线亮点",
                "monthly_avg_rate": 0.0002,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发Code",
                "monthly_avg_rate": 0.0003,
            },
        ]
    )

    selection = create_group_batch_selection_ui(source_data, "unified_focus")

    assert fake_st.multiselect_calls == [
        {
            "label": "不良 Group",
            "options": ["Array_Line", "Array_Mura"],
            "key": "unified_focus_groups",
            "help": "可多选；下拉列表支持 Select all。",
            "placeholder": "请选择 Defect Group",
        },
        {
            "label": "不良 Code",
            "options": ["高发Code"],
            "key": "unified_focus_codes",
            "help": "可多选；下拉列表支持 Select all。",
            "placeholder": "请选择 Defect Code",
        },
    ]
    assert selection["codes_by_group"] == {"Array_Mura": ["高发Code"]}


def test_batch_selector_does_not_default_group_to_all(monkeypatch) -> None:
    fake_st = _BatchSelectorStreamlit(
        selected_groups=[],
        selected_codes=[],
        query_clicked=False,
    )
    monkeypatch.setattr("app.components.code_selector.st", fake_st)
    source_data = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发Code",
                "monthly_avg_rate": 0.0003,
            }
        ]
    )

    selection = create_group_batch_selection_ui(source_data, "unified_focus")

    assert fake_st.session_state["unified_focus_groups"] == []
    assert selection["groups"] == []
    assert selection["codes_by_group"] == {}
    assert selection["should_render"] is False


def test_batch_selector_only_renders_the_queried_filter_signature(monkeypatch) -> None:
    fake_st = _BatchSelectorStreamlit(
        selected_groups=["Array_Line", "Array_Mura"],
        selected_codes=["线亮点", "高发Code"],
        query_clicked=True,
    )
    monkeypatch.setattr("app.components.code_selector.st", fake_st)
    source_data = pd.DataFrame(
        [
            {
                "defect_group": "Array_Line",
                "defect_desc": "线亮点",
                "monthly_avg_rate": 0.0002,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发Code",
                "monthly_avg_rate": 0.0003,
            },
        ]
    )

    queried = create_group_batch_selection_ui(source_data, "unified_focus")
    fake_st.query_clicked = False
    unchanged = create_group_batch_selection_ui(source_data, "unified_focus")
    fake_st.selected_groups = ["Array_Mura"]
    fake_st.selected_codes = ["高发Code"]
    changed = create_group_batch_selection_ui(source_data, "unified_focus")

    assert queried["should_render"] is True
    assert unchanged["should_render"] is True
    assert changed["should_render"] is False
