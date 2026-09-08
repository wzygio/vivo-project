from __future__ import annotations

from io import BytesIO

import pandas as pd

from app.sections.inline_domain.monitor.ooc_decision_admin import _normalize_upload


def _workbook(frame: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        frame.to_excel(writer, sheet_name="决策台账", index=False)
    return output.getvalue()


def test_normalize_upload_supports_scope_specific_keys() -> None:
    source = pd.DataFrame(
        [
            {
                "prod_code": "M626",
                "step_id": "100",
                "tt_name": "TT",
                "sheet_id": "S1",
                "flag": "False",
            }
        ]
    )

    result, error = _normalize_upload(
        _workbook(source), ("prod_code", "step_id", "tt_name", "sheet_id")
    )

    assert error is None
    assert result is not None
    assert result["flag"].tolist() == [False]


def test_normalize_upload_rejects_duplicate_keys() -> None:
    source = pd.DataFrame(
        [
            {"prod_code": "M626", "step_id": "100", "tt_name": "TT", "sheet_id": "S1", "flag": True},
            {"prod_code": "M626", "step_id": "100", "tt_name": "TT", "sheet_id": "S1", "flag": False},
        ]
    )

    result, error = _normalize_upload(
        _workbook(source), ("prod_code", "step_id", "tt_name", "sheet_id")
    )

    assert result is None
    assert error is not None and "重复" in error
