import json

import pytest


def test_help_does_not_resolve_configuration_or_create_a_store(monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from tools.initialize_yield_alert_controls import main

    monkeypatch.setattr(ConfigLoader, "get_enabled_products",
        lambda **_: pytest.fail("help read configuration"))
    with pytest.raises(SystemExit) as stopped:
        main(["--help"])
    assert stopped.value.code == 0


def test_invalid_product_is_rejected_before_composition(monkeypatch, capsys):
    from src.yield_domain import composition
    from tools.initialize_yield_alert_controls import main

    monkeypatch.setattr(composition, "build_yield_alert_results",
        lambda: pytest.fail("invalid product created a store"))
    assert main(["--product", "DISABLED"], enabled_products=["P1"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "unavailable"


def test_operator_initialization_is_idempotent_and_preserves_history(tmp_path, capsys):
    import pandas as pd

    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets
    from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
    from tools.initialize_yield_alert_controls import main

    path = tmp_path / "lot.xlsx"
    rows = pd.DataFrame([{"超规 Lot ID": "L1", "异常 Code": "D1", "入库时间": "2026/10/15"}])
    meta = pd.DataFrame([{"generated_at": "2026-10-20T10:00:00+08:00"}])
    note = pd.DataFrame([{"note": "retained"}])
    assert replace_workbook_sheets(path, {"P1": rows, "P1__meta": meta,
        "manual": note}).written
    store = YieldAlertWorkbookStore({"lot": path, "trend": tmp_path / "trend.xlsx"})
    assert main(["--product", "P1"], store=store, enabled_products=["P1"]) == 0
    assert json.loads(capsys.readouterr().out)["created"] is True
    flags = pd.read_excel(path, sheet_name="P1__flags").assign(flag=False)
    assert replace_workbook_sheets(path, {"P1__flags": flags}).written
    before = path.read_bytes()
    assert main(["--product", "P1"], store=store, enabled_products=["P1"]) == 0
    assert json.loads(capsys.readouterr().out)["created"] is False
    assert path.read_bytes() == before
    assert pd.read_excel(path, sheet_name="P1__flags")["flag"].tolist() == [False]
    assert pd.read_excel(path, sheet_name="P1__meta")["generated_at"].tolist() == meta["generated_at"].tolist()
    assert pd.read_excel(path, sheet_name="manual").to_dict("records") == [{"note": "retained"}]


def test_missing_publication_and_errors_do_not_disclose_internal_details(capsys):
    from tools.initialize_yield_alert_controls import main

    class BrokenStore:
        def initialize_lot_controls(self, product):
            raise OSError("private-path secret-driver-text")

    assert main(["--product", "P1"], store=BrokenStore(), enabled_products=["P1"]) == 2
    output = capsys.readouterr().out
    assert json.loads(output)["error"] == "result_unavailable"
    assert "private-path" not in output and "secret-driver-text" not in output


def test_default_store_is_resolved_only_after_strict_enabled_scope(monkeypatch, capsys):
    from types import SimpleNamespace

    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain import composition
    from tools.initialize_yield_alert_controls import main

    calls = []
    monkeypatch.setattr(ConfigLoader, "get_enabled_products",
        lambda **kwargs: calls.append(kwargs) or ["P1"])
    store = SimpleNamespace(initialize_lot_controls=lambda product:
        calls.append(product) or True)
    monkeypatch.setattr(composition, "build_yield_alert_results",
        lambda: SimpleNamespace(store=store))
    assert main(["--product", "P1"]) == 0
    assert calls == [{"strict": True}, "P1"]
    assert json.loads(capsys.readouterr().out)["initialized_products"] == 1


def test_missing_actual_publication_returns_two_without_creating_workbook(tmp_path, capsys):
    from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
    from tools.initialize_yield_alert_controls import main

    path = tmp_path / "missing.xlsx"
    store = YieldAlertWorkbookStore({"lot": path, "trend": tmp_path / "trend.xlsx"})
    assert main(["--product", "P1"], store=store, enabled_products=["P1"]) == 2
    assert not path.exists()
    assert json.loads(capsys.readouterr().out)["error"] == "result_unavailable"


def test_missing_product_exits_two_before_any_configuration(monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from tools.initialize_yield_alert_controls import main

    monkeypatch.setattr(ConfigLoader, "get_enabled_products",
        lambda **_: pytest.fail("invalid invocation read configuration"))
    with pytest.raises(SystemExit) as stopped:
        main([])
    assert stopped.value.code == 2
