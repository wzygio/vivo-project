from copy import deepcopy

import pandas as pd
import pytest

from app.manager import compliance_manager as manager


def payload():
    return {"products": ["M626", "M678"],
            "rows": [{"row_key": "spc", "display_name": "SPC 单片异常"}],
            "cells": {("spc", "M626"): {"state": "alert", "message": "原始预警", "detail_key": "spc|M626"},
                      ("spc", "M678"): {"state": "error", "message": "读取失败"}},
            "signature": "unchanged"}


def test_template_and_live_display_override_preserve_cached_payload(tmp_path):
    path = tmp_path / "config.xlsx"
    raw = payload()
    original = deepcopy(raw)
    manager.write_matrix_template(raw["rows"], raw["products"], path)
    frame = pd.read_excel(path, sheet_name=manager.SHEET_NAME)
    assert frame.columns.tolist() == ["监控参数", "M626", "M678"]
    assert frame.iloc[0].tolist() == ["SPC 单片异常", False, False]
    assert manager.apply_matrix_compliance(raw, manager.load_matrix_config(path)) == raw
    frame.loc[0, "M626"] = True
    frame.to_excel(path, sheet_name=manager.SHEET_NAME, index=False)
    view = manager.apply_matrix_compliance(raw, manager.load_matrix_config(path))
    assert view["cells"][("spc", "M626")]["state"] == "ok"
    assert view["cells"][("spc", "M626")]["message"] == ""
    assert view["cells"][("spc", "M678")]["state"] == "error"
    assert raw == original
    assert view["signature"] == raw["signature"]
    frame.loc[0, "M626"] = False
    frame.to_excel(path, sheet_name=manager.SHEET_NAME, index=False)
    assert manager.apply_matrix_compliance(raw, manager.load_matrix_config(path)) == raw


@pytest.mark.parametrize("value", ["yes", "1", "foo", None])
def test_invalid_switch_rejected(value):
    with pytest.raises(ValueError):
        manager.parse_matrix_config(pd.DataFrame({"监控参数": ["SPC"], "M626": [value]}))


def test_text_switches_and_duplicate_row_validation():
    frame = pd.DataFrame({"监控参数": ["SPC"], "M626": [" true "], "M678": ["False"]})
    assert manager.parse_matrix_config(frame) == {("SPC", "M626"): True, ("SPC", "M678"): False}
    with pytest.raises(ValueError):
        manager.parse_matrix_config(pd.concat([frame, frame]))


def test_legacy_backend_rules_disabled_and_cache_signature_constant(tmp_path, monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    monkeypatch.setattr(manager, "CONFIG_PATH", tmp_path / "config.xlsx")
    before = manager.get_compliance_file_signature()
    manager.write_matrix_template(payload()["rows"], payload()["products"])
    assert manager.get_compliance_file_signature() == before
    assert ConfigLoader.get_compliance_config() == {"rules": []}


def test_missing_or_wrong_sheet_does_not_silently_enable_compliance(tmp_path):
    with pytest.raises(FileNotFoundError):
        manager.load_matrix_config(tmp_path / "missing.xlsx")
    path = tmp_path / "wrong.xlsx"
    pd.DataFrame({"厂别": ["ALL"]}).to_excel(path, sheet_name="规则配置", index=False)
    with pytest.raises(ValueError):
        manager.load_matrix_config(path)
