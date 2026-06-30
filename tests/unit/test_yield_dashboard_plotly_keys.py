from types import SimpleNamespace
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
for path in [PROJECT_ROOT, SRC_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

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

    def tabs(self, labels):
        return [_Context() for _ in labels]

    def expander(self, *args, **kwargs):
        return _Context()

    def plotly_chart(self, *args, **kwargs):
        self.plotly_keys.append(kwargs.get("key"))
        return SimpleNamespace(selection={"points": [{"x": "LOT-1"}]})


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
