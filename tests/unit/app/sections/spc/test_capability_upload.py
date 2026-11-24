from contextlib import nullcontext
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.sections.inline_domain.spc import spc_dashboard


def _setup_upload(monkeypatch, tmp_path, metric: str):
    uploaded = pd.DataFrame([{
        "prod_code": "M626", "factory": "ARRAY", "step_id": "1L650",
        "param_name": "CD1", "period_type": "week", "period_label": "2026-W38",
        f"{metric.lower()}_corrected": 1.2, "flag": True,
    }])
    buffer = BytesIO()
    uploaded.to_excel(buffer, index=False)
    ui = MagicMock()
    ui.columns.return_value = [nullcontext(), nullcontext()]
    ui.file_uploader.return_value = buffer
    ui.button.return_value = True
    monkeypatch.setattr(spc_dashboard, "st", ui)
    result = SimpleNamespace(
        decoration_df=uploaded,
        decoration_path=tmp_path / "decoration.xlsx",
        decoration_sheet="M626" if metric == "CPK" else "M626_cpm",
    )
    return ui, result, uploaded


@pytest.mark.parametrize("metric", ["CPK", "CPM"])
@pytest.mark.parametrize("written", [False, True])
def test_capability_upload_refreshes_only_after_successful_write(monkeypatch, tmp_path, metric, written):
    ui, result, uploaded = _setup_upload(monkeypatch, tmp_path, metric)
    writer = MagicMock(side_effect=None if written else OSError("file in use"))
    monkeypatch.setattr(spc_dashboard, "update_capability_decisions", writer)

    spc_dashboard.render_capability_decoration_admin(result, metric_label=metric, show_expander=False)

    writer.assert_called_once()
    path, sheet, indicator, edits = writer.call_args.args
    assert (path, sheet, indicator) == (result.decoration_path, result.decoration_sheet, metric.lower())
    pd.testing.assert_frame_equal(edits, uploaded, check_dtype=False)
    assert ui.success.called is written
    ui.cache_data.clear.assert_not_called()
    assert ui.rerun.called is written
    assert ui.error.called is not written


@pytest.mark.parametrize("failure", ["read", "missing_columns"])
def test_invalid_upload_never_writes_or_refreshes(monkeypatch, tmp_path, failure):
    ui, result, uploaded = _setup_upload(monkeypatch, tmp_path, "CPK")
    reader = MagicMock(
        side_effect=ValueError("Unreadable upload") if failure == "read" else None,
        return_value=uploaded.drop(columns="flag"),
    )
    writer = MagicMock()
    monkeypatch.setattr(spc_dashboard, "read_uploaded_excel_sheet", reader)
    monkeypatch.setattr(spc_dashboard, "update_capability_decisions", writer)

    spc_dashboard.render_capability_decoration_admin(result, show_expander=False)

    writer.assert_not_called()
    ui.error.assert_called_once()
    ui.success.assert_not_called()
    ui.cache_data.clear.assert_not_called()
    ui.rerun.assert_not_called()
