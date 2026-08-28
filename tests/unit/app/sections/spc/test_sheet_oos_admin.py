"""Sheet OOS 修饰管理区下载/上传纯逻辑与薄壳 UI 的测试（PRD §5.9）。"""
from __future__ import annotations

from contextlib import nullcontext
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from app.sections.spc import sheet_oos_admin, spc_dashboard
from app.sections.spc.sheet_oos_admin import (
    DECISION_DOWNLOAD_COLUMNS,
    apply_decision_upload,
    build_decision_download_sheets,
    handle_decision_upload,
    parse_decision_upload,
    validate_decision_upload,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_COLUMNS,
    OOS_KEY_COLUMNS,
    SheetOosDecorationResult,
    get_decision_sheet_name,
)
from src.inline_domain.application.shared.decorated_features import (
    InMemoryFeaturesSource,
    fetch_decorated_features,
)
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.utils.excel_tools import (
    WorkbookWriteResult,
    replace_workbook_sheets,
)


def _decision_row(flag=True, sheet_id: str = "S1") -> dict:
    return {
        "prod_code": "P1",
        "step_id": "15260",
        "param_name": "4PP_Rs",
        "sheet_id": sheet_id,
        "flag": flag,
    }


def _decision_df(*flags) -> pd.DataFrame:
    rows = [_decision_row(flag, sheet_id=f"S{index}") for index, flag in enumerate(flags, start=1)]
    return pd.DataFrame(rows, columns=DECISION_DOWNLOAD_COLUMNS)


def _decoration_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "P1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-24",
                "sheet_max": 13.0,
                "sheet_min": 9.0,
                "sheet_mean": 11.0,
                "usl": 12.0,
                "lsl": 8.0,
                "oos_type": "USL",
                "flag": True,
            }
        ],
        columns=OOS_DECORATION_COLUMNS,
    )


def _make_result(
    tmp_path: Path,
    decision_df: pd.DataFrame | None = None,
    decoration_df: pd.DataFrame | None = None,
) -> SheetOosDecorationResult:
    return SheetOosDecorationResult(
        raw_measurements_df=pd.DataFrame(),
        decoration_df=decoration_df if decoration_df is not None else _decoration_df(),
        decoration_path=tmp_path / "spc_sheet_oos_decoration.xlsx",
        decoration_sheet="M678",
        decision_sheet=get_decision_sheet_name("M678"),
        decision_df=decision_df,
        refresh_reason="",
    )


def _xlsx_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# ---------------------------------------------------------------------------
# 下载
# ---------------------------------------------------------------------------


def test_download_sheets_contain_detail_and_decision_ledger(tmp_path: Path) -> None:
    result = _make_result(tmp_path, decision_df=_decision_df(True, "Delete"))

    sheets = build_decision_download_sheets(result)

    assert list(sheets) == ["当前明细", "决策台账"]
    assert list(sheets["当前明细"].columns) == OOS_DECORATION_COLUMNS
    assert len(sheets["当前明细"]) == 1
    assert list(sheets["决策台账"].columns) == DECISION_DOWNLOAD_COLUMNS
    assert sheets["决策台账"]["flag"].tolist() == [True, "Delete"]


def test_download_sheets_empty_frames_keep_headers(tmp_path: Path) -> None:
    result = _make_result(tmp_path, decision_df=None, decoration_df=pd.DataFrame())

    sheets = build_decision_download_sheets(result)

    assert sheets["当前明细"].empty
    assert list(sheets["当前明细"].columns) == OOS_DECORATION_COLUMNS
    assert sheets["决策台账"].empty
    assert list(sheets["决策台账"].columns) == DECISION_DOWNLOAD_COLUMNS


def test_download_bytes_do_not_contain_refresh_meta(tmp_path: Path) -> None:
    result = _make_result(tmp_path, decision_df=_decision_df(True))

    data = spc_dashboard._excel_bytes(build_decision_download_sheets(result))
    parsed = pd.read_excel(BytesIO(data), sheet_name=None)

    assert set(parsed) == {"当前明细", "决策台账"}
    assert "__refresh_meta__" not in parsed


# ---------------------------------------------------------------------------
# 上传校验
# ---------------------------------------------------------------------------


def test_validate_rejects_missing_key_columns() -> None:
    df = _decision_df(True).drop(columns=["sheet_id"])

    ok, error, normalized = validate_decision_upload(df)

    assert not ok
    assert "sheet_id" in error
    assert normalized is None


def test_validate_rejects_invalid_flag() -> None:
    df = _decision_df("maybe")

    ok, error, normalized = validate_decision_upload(df)

    assert not ok
    assert "flag" in error
    assert normalized is None


def test_validate_rejects_duplicate_keys() -> None:
    df = pd.DataFrame([_decision_row(True), _decision_row(False)])

    ok, error, normalized = validate_decision_upload(df)

    assert not ok
    assert "重复" in error
    assert normalized is None


def test_validate_accepts_and_normalizes_supported_flags() -> None:
    df = _decision_df(True, "false", 0, "否", "Delete", "TRUE")

    ok, error, normalized = validate_decision_upload(df)

    assert ok, error
    assert normalized["flag"].tolist() == [True, False, False, False, "Delete", True]
    assert list(normalized.columns) == DECISION_DOWNLOAD_COLUMNS


def test_validate_accepts_empty_frame_with_headers() -> None:
    df = pd.DataFrame(columns=DECISION_DOWNLOAD_COLUMNS)

    ok, error, normalized = validate_decision_upload(df)

    assert ok, error
    assert normalized.empty
    assert list(normalized.columns) == DECISION_DOWNLOAD_COLUMNS


# ---------------------------------------------------------------------------
# 上传解析（sheet 选择）
# ---------------------------------------------------------------------------


def test_parse_prefers_decision_ledger_sheet() -> None:
    legacy = _decision_df(False).assign(extra="noise")
    ledger = _decision_df("Delete")
    payload = _xlsx_bytes({"当前明细": legacy, "决策台账": ledger})

    ok, error, normalized = parse_decision_upload(payload)

    assert ok, error
    assert normalized["flag"].tolist() == ["Delete"]


def test_parse_legacy_single_sheet_extracts_keys_and_flag() -> None:
    legacy = _decision_df(False).assign(sheet_mean=10.0)
    payload = _xlsx_bytes({"修饰表": legacy})

    ok, error, normalized = parse_decision_upload(payload)

    assert ok, error
    assert normalized["flag"].tolist() == [False]
    assert list(normalized.columns) == DECISION_DOWNLOAD_COLUMNS


def test_parse_unreadable_bytes_returns_error() -> None:
    ok, error, normalized = parse_decision_upload(b"not-an-xlsx")

    assert not ok
    assert error
    assert normalized is None


# ---------------------------------------------------------------------------
# 上传写入（tmp_path 真实工作簿）
# ---------------------------------------------------------------------------


def test_apply_writes_flags_sheet_and_keeps_product_sheet(tmp_path: Path) -> None:
    sentinel = pd.DataFrame({"prod_code": ["ORIGINAL"], "flag": [True]})
    workbook = tmp_path / "spc_sheet_oos_decoration.xlsx"
    write = replace_workbook_sheets(
        workbook, {"M678": sentinel, "M678__flags": _decision_df(False)}
    )
    assert write.written
    result = _make_result(tmp_path, decision_df=_decision_df(False))

    outcome = apply_decision_upload(result, _decision_df("Delete"))

    assert outcome.status == "success"
    sheets = pd.read_excel(workbook, sheet_name=None)
    # 产品明细 sheet 不得被覆盖
    assert sheets["M678"]["prod_code"].tolist() == ["ORIGINAL"]
    flags = sheets["M678__flags"]
    assert flags["flag"].tolist() == ["Delete"]
    assert set(flags.columns) == set(DECISION_DOWNLOAD_COLUMNS)


def test_apply_identical_content_skips_rewrite(tmp_path: Path, monkeypatch) -> None:
    workbook = tmp_path / "spc_sheet_oos_decoration.xlsx"
    write = replace_workbook_sheets(workbook, {"M678__flags": _decision_df(True)})
    assert write.written
    # 上传内容（旧式 "true" 文本）与现有决策规范化后完全一致
    result = _make_result(tmp_path, decision_df=_decision_df(True))
    calls: list = []
    monkeypatch.setattr(
        sheet_oos_admin,
        "replace_workbook_sheets",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    mtime_before = workbook.stat().st_mtime_ns

    outcome = apply_decision_upload(result, _decision_df("true"))

    assert outcome.status == "unchanged"
    assert "无需更新" in outcome.message
    assert calls == []
    assert workbook.stat().st_mtime_ns == mtime_before


def test_apply_empty_upload_clears_flags(tmp_path: Path) -> None:
    workbook = tmp_path / "spc_sheet_oos_decoration.xlsx"
    write = replace_workbook_sheets(workbook, {"M678__flags": _decision_df(True, "Delete")})
    assert write.written
    result = _make_result(tmp_path, decision_df=_decision_df(True, "Delete"))

    outcome = apply_decision_upload(result, pd.DataFrame(columns=DECISION_DOWNLOAD_COLUMNS))

    assert outcome.status == "success"
    assert "清空" in outcome.message
    flags = pd.read_excel(workbook, sheet_name="M678__flags")
    assert flags.empty
    assert list(flags.columns) == DECISION_DOWNLOAD_COLUMNS


def test_apply_write_failure_returns_error(tmp_path: Path, monkeypatch) -> None:
    result = _make_result(tmp_path, decision_df=_decision_df(False))
    monkeypatch.setattr(
        sheet_oos_admin,
        "replace_workbook_sheets",
        lambda path, sheets: WorkbookWriteResult(
            written=False,
            path=path,
            updated_sheets=(),
            error="文件被占用，请关闭 Excel 后重试",
        ),
    )

    outcome = apply_decision_upload(result, _decision_df("Delete"))

    assert outcome.status == "error"
    assert "请关闭 Excel 后重试" in outcome.message


def test_handle_decision_upload_rejects_invalid_bytes(tmp_path: Path) -> None:
    result = _make_result(tmp_path)

    outcome = handle_decision_upload(result, b"not-an-xlsx")

    assert outcome.status == "error"


# ---------------------------------------------------------------------------
# UI 薄壳
# ---------------------------------------------------------------------------


class _FakeUpload:
    def __init__(self, payload: bytes):
        self._payload = payload

    def getbuffer(self) -> bytes:
        return self._payload


def _patch_admin_st(monkeypatch, upload: _FakeUpload | None, messages: dict) -> None:
    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_a, **_k: None)
    monkeypatch.setattr(spc_dashboard.st, "markdown", lambda *_a, **_k: None)
    monkeypatch.setattr(spc_dashboard.st, "columns", lambda *_a, **_k: [FakeColumn(), FakeColumn()])
    monkeypatch.setattr(spc_dashboard.st, "download_button", lambda *_a, **_k: None)
    monkeypatch.setattr(spc_dashboard.st, "file_uploader", lambda *_a, **_k: upload)
    monkeypatch.setattr(spc_dashboard.st, "button", lambda *_a, **_k: True)
    monkeypatch.setattr(spc_dashboard.st, "success", messages["success"].append)
    monkeypatch.setattr(spc_dashboard.st, "error", messages["error"].append)
    monkeypatch.setattr(spc_dashboard.st, "info", messages["info"].append)
    monkeypatch.setattr(
        spc_dashboard.st, "rerun", lambda: messages["rerun"].append(True)
    )


def test_admin_upload_success_shows_success_and_reruns(monkeypatch, tmp_path: Path) -> None:
    workbook = tmp_path / "spc_sheet_oos_decoration.xlsx"
    write = replace_workbook_sheets(workbook, {"M678__flags": _decision_df(False)})
    assert write.written
    result = _make_result(tmp_path, decision_df=_decision_df(False))
    messages: dict[str, list] = {"success": [], "error": [], "info": [], "rerun": []}
    upload = _FakeUpload(_xlsx_bytes({"决策台账": _decision_df("Delete")}))
    _patch_admin_st(monkeypatch, upload, messages)

    spc_dashboard.render_sheet_oos_decoration_admin(result)

    assert messages["success"]
    assert messages["rerun"] == [True]
    assert messages["error"] == []
    flags = pd.read_excel(workbook, sheet_name="M678__flags")
    assert flags["flag"].tolist() == ["Delete"]


def test_admin_upload_failure_shows_error_without_rerun(monkeypatch, tmp_path: Path) -> None:
    result = _make_result(tmp_path, decision_df=_decision_df(False))
    messages: dict[str, list] = {"success": [], "error": [], "info": [], "rerun": []}
    upload = _FakeUpload(_xlsx_bytes({"决策台账": _decision_df("Delete")}))
    _patch_admin_st(monkeypatch, upload, messages)
    monkeypatch.setattr(
        sheet_oos_admin,
        "replace_workbook_sheets",
        lambda path, sheets: WorkbookWriteResult(
            written=False,
            path=path,
            updated_sheets=(),
            error="文件被占用，请关闭 Excel 后重试",
        ),
    )

    spc_dashboard.render_sheet_oos_decoration_admin(result)

    assert messages["error"]
    assert "请关闭 Excel 后重试" in messages["error"][0]
    assert messages["success"] == []
    assert messages["rerun"] == []


# ---------------------------------------------------------------------------
# 缓存 payload 链路回归（缺陷：decision_df 在 payload 边界丢失）
# ---------------------------------------------------------------------------


def test_cached_payload_decisions_flow_to_download_and_unchanged_upload(
    monkeypatch, tmp_path: Path
) -> None:
    """fetch_decorated_features → SPC view model 重建后：
    下载产出包含真实决策台账行；规范化一致的上传判定为 unchanged。"""
    fetch_decorated_features.clear()
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    resources = tmp_path / "resources"
    resources.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(
        resources / "spc_sheet_oos_decoration.xlsx", engine="openpyxl"
    ) as writer:
        _decision_df(False).to_excel(writer, sheet_name="M678__flags", index=False)

    measurements_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "sheet_start_time": "2026-08-05 09:00:00",
                "sheet_id": "S1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "site_name": "P1",
                "param_value": 10.0,
                "data_type": "SPC",
            }
        ]
    )
    spec_df = pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "usl": 12.0,
                "lsl": 8.0,
                "ucl": 11.5,
                "lcl": 8.5,
                "target": 10.0,
            }
        ]
    )
    payload = fetch_decorated_features(
        InMemoryFeaturesSource(measurements_df, spec_df),
        "M678",
        "spc",
        "2026-08-01",
        "2026-08-10",
        "admin-chain-payload",
    )
    view_model = SpcReportService._view_model_from_payload(
        {"sheet_oos_decoration": payload["sheet_oos_decoration"]}
    )
    result = view_model.sheet_oos_decoration_result

    assert result is not None
    assert result.decision_df is not None and not result.decision_df.empty
    assert result.decision_sheet == "M678__flags"
    assert result.refresh_reason

    sheets = build_decision_download_sheets(result)
    assert sheets["决策台账"]["flag"].tolist() == [False]

    outcome = apply_decision_upload(result, _decision_df(False))
    assert outcome.status == "unchanged"
