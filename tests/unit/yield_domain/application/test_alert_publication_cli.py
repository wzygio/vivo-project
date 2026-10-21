import pytest

from src.shared_kernel.config import ConfigLoader
from tools.publish_yield_alert_results import main


def test_cli_covers_enabled_products_and_isolates_failure(monkeypatch, capsys):
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **kwargs: ["P1", "P2"])
    calls = []
    def publish(product):
        calls.append(product)
        if product == "P1":
            raise RuntimeError("secret-driver-text")
        return {"product": product, "status": "success"}
    assert main([], producer=publish) == 1
    assert calls == ["P1", "P2"]
    assert "secret-driver-text" not in capsys.readouterr().out


def test_cli_selected_scope_and_check_do_not_run_other_products(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **kwargs: ["P1", "P2"])
    calls = []
    assert main(["--products", "P2"], producer=lambda product:
                calls.append(product) or {"status": "success"}) == 0
    assert calls == ["P2"]
    assert main(["--check"], producer=lambda product: pytest.fail("check ran calculation")) == 0


def test_cli_passes_explicit_window_and_rejects_invalid_window_before_calculation(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **kwargs: ["P1"])
    calls = []
    def producer(product, **kwargs):
        calls.append((product, kwargs))
        return {"status": "success"}
    assert main(["--start-date", "2026-08-01", "--end-date", "2026-10-21"], producer=producer) == 0
    assert calls == [("P1", {"analysis_start_date": "2026-08-01", "analysis_end_date": "2026-10-21"})]
    for args in (["--start-date", "2026-08-01"],
                 ["--start-date", "2026-10-21", "--end-date", "2026-08-01"]):
        with pytest.raises(SystemExit):
            main(args, producer=lambda *a, **k: pytest.fail("invalid window ran calculation"))
