from types import SimpleNamespace

import pandas as pd
import plotly.graph_objects as go

from app.sections import yield_dashboard


class _Context:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlit:
    def __init__(self):
        self.plotly_keys = []
        self.session_state = {}
        self.tab_defaults = []

    def markdown(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def caption(self, *args, **kwargs):
        return None

    def dataframe(self, *args, **kwargs):
        return None

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [_Context() for _ in range(count)]

    def tabs(self, labels, *, default=None, **kwargs):
        self.tab_defaults.append(default)
        return [_Context() for _ in labels]

    def expander(self, *args, **kwargs):
        return _Context()

    def plotly_chart(self, *args, **kwargs):
        self.plotly_keys.append(kwargs.get("key"))
        return SimpleNamespace(selection={"points": [{"x": "LOT-1"}]})


class _SelectionStreamlit(_FakeStreamlit):
    def __init__(self, points):
        super().__init__()
        self.points = points

    def plotly_chart(self, *args, **kwargs):
        self.plotly_keys.append(kwargs.get("key"))
        return SimpleNamespace(selection={"points": self.points})


class _LegacyTabsStreamlit(_FakeStreamlit):
    """模拟 Streamlit 1.49：st.tabs 不接受 default 参数。"""

    def __init__(self):
        super().__init__()
        self.tab_labels = []

    def tabs(self, labels):
        self.tab_labels = list(labels)
        return [_Context() for _ in labels]


def _trend_frame(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "defect_desc": [code],
            "defect_rate": [0.0002],
            "time_period": ["2026-06"],
        }
    )


def _lot_frame(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "defect_desc": [code],
            "defect_rate": [0.0002],
            "lot_id": ["LOT-1"],
            "warehousing_time": ["20260601"],
            "array_input_time": ["2026-06-01 08:00:00"],
            "defect_panel_count": [1],
        }
    )


def test_compact_expander_assigns_unique_plotly_keys(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(yield_dashboard, "st", fake_st)
    monkeypatch.setattr(yield_dashboard, "create_code_trend_chart", lambda *args, **kwargs: go.Figure())
    monkeypatch.setattr(yield_dashboard, "create_mapping_heatmap", lambda *args, **kwargs: go.Figure())
    monkeypatch.setattr(yield_dashboard, "create_lot_defect_chart", lambda *args, **kwargs: go.Figure())
    monkeypatch.setattr(yield_dashboard, "create_sheet_defect_chart", lambda *args, **kwargs: go.Figure())

    mwd_code_data = {
        "monthly": pd.concat([_trend_frame("CODE-A"), _trend_frame("CODE-B")], ignore_index=True),
        "weekly": pd.concat([_trend_frame("CODE-A"), _trend_frame("CODE-B")], ignore_index=True),
        "daily": pd.concat([_trend_frame("CODE-A"), _trend_frame("CODE-B")], ignore_index=True),
    }
    lot_data = {"code_level_details": {"GROUP-A": pd.concat([_lot_frame("CODE-A"), _lot_frame("CODE-B")])}}
    sheet_data = {
        "group_level_summary_for_table": pd.DataFrame(
            {
                "sheet_id": ["SHEET-1"],
                "lot_id": ["LOT-1"],
                "warehousing_time": ["20260601"],
                "array_input_time": ["2026-06-01 08:00:00"],
            }
        ),
        "code_level_details": {
            "GROUP-A": pd.DataFrame(
                {
                    "sheet_id": ["SHEET-1", "SHEET-1"],
                    "lot_id": ["LOT-1", "LOT-1"],
                    "defect_desc": ["CODE-A", "CODE-B"],
                    "defect_rate": [0.0002, 0.0003],
                    "defect_panel_count": [1, 1],
                }
            )
        },
    }
    mapping_data = pd.DataFrame(
        {
            "defect_group": ["GROUP-A", "GROUP-A"],
            "defect_desc": ["CODE-A", "CODE-B"],
            "batch_no": ["BATCH-1", "BATCH-1"],
            "batch_total_input": [100, 100],
            "panel_id": ["1-1", "1-1"],
        }
    )

    for code in ["CODE-A", "CODE-B"]:
        yield_dashboard.render_code_compact_expander(
            mwd_code_data=mwd_code_data,
            lot_data=lot_data,
            sheet_data=sheet_data,
            mapping_data=mapping_data,
            curr_group="GROUP-A",
            curr_code=code,
            curr_warning=0.001,
            hotspot_scripts=[],
            product_code="TEST",
            expanded=True,
        )

    assert all(fake_st.plotly_keys)
    assert len(fake_st.plotly_keys) == len(set(fake_st.plotly_keys))


def test_compact_mapping_defaults_to_penultimate_batch(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(yield_dashboard, "st", fake_st)
    monkeypatch.setattr(yield_dashboard, "create_mapping_heatmap", lambda *args, **kwargs: go.Figure())

    mapping_data = pd.DataFrame(
        {
            "defect_group": ["GROUP-A"] * 3,
            "defect_desc": ["CODE-A"] * 3,
            "batch_no": ["BATCH-1", "BATCH-2", "BATCH-3"],
            "batch_total_input": [100, 100, 100],
            "panel_id": ["1-1", "1-1", "1-1"],
        }
    )

    yield_dashboard._render_compact_mapping_section(
        mapping_data=mapping_data,
        curr_group="GROUP-A",
        curr_code="CODE-A",
        hotspot_scripts=[],
        product_code="TEST",
    )

    assert fake_st.tab_defaults == ["BATCH-2 (100)"]


def test_compact_mapping_supports_legacy_streamlit_tabs(monkeypatch):
    fake_st = _LegacyTabsStreamlit()
    monkeypatch.setattr(yield_dashboard, "st", fake_st)
    monkeypatch.setattr(yield_dashboard, "create_mapping_heatmap", lambda *args, **kwargs: go.Figure())

    mapping_data = pd.DataFrame(
        {
            "defect_group": ["GROUP-A"] * 2,
            "defect_desc": ["CODE-A"] * 2,
            "batch_no": ["BATCH-1", "BATCH-2"],
            "batch_total_input": [100, 100],
            "panel_id": ["1-1", "1-1"],
        }
    )

    yield_dashboard._render_compact_mapping_section(
        mapping_data=mapping_data,
        curr_group="GROUP-A",
        curr_code="CODE-A",
        hotspot_scripts=[],
        product_code="TEST",
    )

    assert fake_st.tab_labels == ["BATCH-1 (100)", "BATCH-2 (100)"]
    assert len(fake_st.plotly_keys) == 2


def test_batch_render_collects_every_payload_before_rendering(monkeypatch):
    events = []

    class _Gate:
        def __init__(self):
            self.jobs = []

        def stage(self, job):
            self.jobs.append(job)

        def collect(self):
            payloads = [job() for job in self.jobs]
            events.append(("collected", len(payloads)))
            return payloads

    monkeypatch.setattr(yield_dashboard, "RenderGate", _Gate)
    monkeypatch.setattr(
        yield_dashboard,
        "_build_compact_render_payload",
        lambda **kwargs: {"curr_code": kwargs["curr_code"]},
    )
    monkeypatch.setattr(
        yield_dashboard,
        "_render_compact_payload",
        lambda payload: events.append(("rendered", payload["curr_code"])),
    )
    monkeypatch.setattr(yield_dashboard.st, "markdown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(yield_dashboard.st, "divider", lambda *_args, **_kwargs: None)

    yield_dashboard.render_code_compact_expanders(
        selected_groups=["G1", "G2"],
        codes_by_group={"G1": ["A", "B"], "G2": ["C"]},
        warning_lines=None,
        mwd_code_data={},
        lot_data={},
        sheet_data={},
        mapping_data=None,
        hotspot_scripts=[],
    )

    assert events == [
        ("collected", 3),
        ("rendered", "A"),
        ("rendered", "B"),
        ("rendered", "C"),
    ]


def test_compact_lot_blank_click_clears_sheet_selection(monkeypatch):
    fake_st = _SelectionStreamlit(points=[])
    selection_key = "compact_sheet_lot_" + yield_dashboard._state_key_fragment(
        "GROUP-A", "CODE-A"
    )
    fake_st.session_state[selection_key] = "LOT-1"
    monkeypatch.setattr(yield_dashboard, "st", fake_st)
    monkeypatch.setattr(
        yield_dashboard,
        "create_lot_defect_chart",
        lambda *args, **kwargs: go.Figure(),
    )

    result = yield_dashboard._render_compact_lot_chart(
        lot_data={"code_level_details": {"GROUP-A": _lot_frame("CODE-A")}},
        curr_group="GROUP-A",
        curr_code="CODE-A",
        curr_warning=0.001,
    )

    assert result == ""
    assert fake_st.session_state[selection_key] == ""
