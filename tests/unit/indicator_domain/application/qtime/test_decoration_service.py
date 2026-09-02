from io import BytesIO

import pandas as pd

from src.indicator_domain.application.qtime.decoration_service import (
    build_qtime_decoration_workbook,
    parse_qtime_decision_upload,
)


def test_qtime_decision_upload_accepts_a_valid_three_state_ledger() -> None:
    ledger = pd.DataFrame(
        [
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "flag": "不修饰",
            },
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L002",
                "timekey": "20260902020000",
                "flag": "Delete",
            },
        ]
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        ledger.to_excel(writer, index=False, sheet_name="决策台账")

    result = parse_qtime_decision_upload(output.getvalue())

    assert result.status == "success"
    assert result.decisions is not None
    assert result.decisions["flag"].tolist() == [False, "Delete"]


def test_download_workbook_prefills_current_rows_and_preserves_stored_decisions() -> None:
    decoration = pd.DataFrame(
        [
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "flag": True,
            }
        ]
    )
    decisions = pd.DataFrame(
        [
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "flag": False,
            },
            {
                "prodcode": "M626",
                "step_desc": "Shipping->Cutting",
                "lot_id": "L999",
                "timekey": "20260801010000",
                "flag": "Delete",
            },
        ]
    )

    workbook = build_qtime_decoration_workbook(decoration, decisions)
    ledger = pd.read_excel(BytesIO(workbook), sheet_name="决策台账")

    assert ledger["lot_id"].tolist() == ["L001", "L999"]
    assert ledger["flag"].tolist() == [False, "Delete"]
