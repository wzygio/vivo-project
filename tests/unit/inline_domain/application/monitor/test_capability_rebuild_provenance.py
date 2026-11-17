import hashlib
import json
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from src.inline_domain.application.monitor.capability_rebuild_provenance import (
    RebuildProvenanceError,
    validate_rebuild_manifest,
    write_rebuild_manifest,
    workbook_signature,
)


@pytest.fixture
def inputs(tmp_path):
    for metric in ("cpk", "cpm"):
        pd.DataFrame({"prod_code": ["M626"], "period_label": ["2026-W46"],
                      "total_projects": [10]}).to_parquet(tmp_path / f"{metric}-weekly.parquet")
    return tmp_path, {"as_of": "2026-11-17", "products": ["M626", "M678"],
                      "weeks_by_metric": {"cpk": ["2026-W46"], "cpm": ["2026-W45", "2026-W46"]},
                      "source_signature": "source-v1", "ledger_signature": "ledger-v1"}


def test_round_trip_binds_scope_and_native_file_bytes(inputs):
    directory, kwargs = inputs
    written = write_rebuild_manifest(directory, **kwargs)
    validated = validate_rebuild_manifest(directory, **kwargs)
    assert validated == written
    assert validated["scope"]["as_of"] == "2026-11-17"
    assert validated["files"]["cpk"]["sha256"] == hashlib.sha256(
        (directory / "cpk-weekly.parquet").read_bytes()).hexdigest()
    assert "source-v1" not in (directory / "rebuild-inputs.json").read_text()
    assert validate_rebuild_manifest(directory, **{**kwargs, "products": ["M678", "M626"]}) == written


@pytest.mark.parametrize("field,value", [
    ("source_signature", "source-v2"), ("ledger_signature", "ledger-v2"),
    ("as_of", "2026-11-18"), ("products", ["M626"]),
    ("weeks_by_metric", {"cpk": ["2026-W45"], "cpm": ["2026-W45", "2026-W46"]}),
])
def test_rejects_stale_inputs_or_wrong_scope(inputs, field, value):
    directory, kwargs = inputs
    write_rebuild_manifest(directory, **kwargs)
    with pytest.raises(RebuildProvenanceError):
        validate_rebuild_manifest(directory, **{**kwargs, field: value})


@pytest.mark.parametrize("damage", ["modified", "missing", "empty"])
def test_rejects_changed_or_missing_weekly_file(inputs, damage):
    directory, kwargs = inputs
    write_rebuild_manifest(directory, **kwargs)
    target = directory / "cpk-weekly.parquet"
    if damage == "missing":
        target.unlink()
    elif damage == "empty":
        target.write_bytes(b"")
    else:
        pd.DataFrame({"different": [1]}).to_parquet(target)
    with pytest.raises(RebuildProvenanceError):
        validate_rebuild_manifest(directory, **kwargs)


@pytest.mark.parametrize("damage", ["missing", "invalid_json", "wrong_version", "wrong_file_path"])
def test_rejects_missing_or_damaged_manifest(inputs, damage):
    directory, kwargs = inputs
    if damage != "missing":
        write_rebuild_manifest(directory, **kwargs)
        path = directory / "rebuild-inputs.json"
        if damage == "invalid_json":
            path.write_text("{broken", encoding="utf-8")
        else:
            contents = json.loads(path.read_text(encoding="utf-8"))
            if damage == "wrong_version":
                contents["version"] = 99
            else:
                contents["files"]["cpk"]["filename"] = "../different.parquet"
            path.write_text(json.dumps(contents), encoding="utf-8")
    with pytest.raises(RebuildProvenanceError):
        validate_rebuild_manifest(directory, **kwargs)


@pytest.mark.parametrize("change", [
    {"source_signature": ""}, {"ledger_signature": ""}, {"products": ["M626", "M626"]},
    {"weeks_by_metric": {"cpk": ["2025-W53"]}}, {"weeks_by_metric": {"invalid": ["2026-W46"]}},
])
def test_rejects_invalid_manifest_inputs_before_publishing(inputs, change):
    directory, kwargs = inputs
    with pytest.raises(RebuildProvenanceError):
        write_rebuild_manifest(directory, **{**kwargs, **change})
    assert not (directory / "rebuild-inputs.json").exists()


def test_atomic_publish_failure_keeps_previous_manifest_and_removes_temp(inputs, monkeypatch):
    from src.inline_domain.application.monitor import capability_rebuild_provenance as module

    directory, kwargs = inputs
    write_rebuild_manifest(directory, **kwargs)
    before = (directory / "rebuild-inputs.json").read_bytes()

    def fail_replace(*args):
        raise OSError("fixture publish failure")

    monkeypatch.setattr(module.os, "replace", fail_replace)
    with pytest.raises(RebuildProvenanceError):
        write_rebuild_manifest(directory, **{**kwargs, "source_signature": "source-v2"})
    assert (directory / "rebuild-inputs.json").read_bytes() == before
    assert not list(directory.glob("*.tmp"))


def test_workbook_signature_is_stable_across_numeric_blanks_and_trailing_ranges():
    ordinary = {"M626 CPK": pd.DataFrame([[1, 1.25, None], [2, 0, ""]],
                                        columns=["产品", "数量", "备注"])}
    protected = {"M626 CPK": pd.DataFrame([
        [1.0, Decimal("1.2500"), float("nan"), None],
        [Decimal("2.00"), -0.0, pd.NA, ""],
        [None, " ", float("nan"), None],
    ], columns=["产品", "数量", "备注", None], index=[5, 6, 7])}
    original = protected["M626 CPK"].copy(deep=True)
    assert workbook_signature(ordinary) == workbook_signature(protected)
    pd.testing.assert_frame_equal(protected["M626 CPK"], original)


@pytest.mark.parametrize("change", ["value", "header", "sheet", "middle_blank", "text_whitespace"])
def test_workbook_signature_detects_logical_edits(change):
    frame = pd.DataFrame({"产品": ["M626", "M678"], "总数": [10, 20]})
    original = workbook_signature({"CPK": frame})
    edited = frame.copy()
    name = "CPK"
    if change == "value":
        edited.loc[0, "总数"] = 11
    elif change == "header":
        edited.rename(columns={"总数": "新总数"}, inplace=True)
    elif change == "sheet":
        name = "CPM"
    elif change == "middle_blank":
        edited = pd.concat([frame.iloc[:1], pd.DataFrame([[None, None]], columns=frame.columns), frame.iloc[1:]], ignore_index=True)
    else:
        edited.loc[0, "产品"] = " M626"
    assert workbook_signature({name: edited}) != original


def test_workbook_signature_retains_nonblank_headers_and_sheet_identity():
    first = {"B": pd.DataFrame({"项目": [1]}), "A": pd.DataFrame({"项目": [2]})}
    assert workbook_signature(first) == workbook_signature(dict(reversed(list(first.items()))))
    assert workbook_signature({"A": pd.DataFrame(columns=["header"])}) != workbook_signature({"A": pd.DataFrame()})


def test_workbook_signature_preserves_large_integers_dates_and_boolean_types():
    number = 1234567890123456789012345678901234567890
    assert workbook_signature({"A": pd.DataFrame({"number": [number]})}) == workbook_signature(
        {"A": pd.DataFrame({"number": [Decimal(str(number) + ".000")]})})
    assert workbook_signature({"A": pd.DataFrame({"day": [date(2026, 11, 17)]})}) == workbook_signature(
        {"A": pd.DataFrame({"day": [datetime(2026, 11, 17)]})})
    assert workbook_signature({"A": pd.DataFrame({"flag": [False]})}) != workbook_signature(
        {"A": pd.DataFrame({"flag": [0]})})
