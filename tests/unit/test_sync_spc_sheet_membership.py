"""The maintenance entry validates scope before constructing any producer."""
from datetime import date
import json

import pytest

from src.shared_kernel.config import ConfigLoader
from tools.sync_spc_sheet_membership import main


@pytest.fixture(autouse=True)
def products(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda: ["M626", "M678"])


def test_check_does_not_start_production(capsys):
    def forbidden(*args):
        pytest.fail("check started production")
    assert main(["--check", "--product", "M626"], producer=forbidden, today=date(2026, 11, 4)) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "validated", "products": ["M626"], "start_date": "2026-10-01", "reference_date": "2026-11-04"}


@pytest.mark.parametrize("args", [
    ["--product", "UNKNOWN"], ["--reference-date", "2026-11-05"],
    ["--start-date", "2026-11-05"], ["--start-date", "2026-01-01"],
    ["--start-date", "2026-10-27"],
])
def test_invalid_selection_precedes_producer(args):
    def forbidden(*values):
        pytest.fail("invalid request started production")
    with pytest.raises(SystemExit) as exc:
        main(args, producer=forbidden, today=date(2026, 11, 4))
    assert exc.value.code == 2


def test_failures_are_explicit_without_exception_or_source_secrets(capsys):
    calls = []
    def producer(product, start, reference):
        calls.append((product, start, reference))
        if product == "M678":
            raise OSError("secret connection details")
        return {"product": product, "status": "success", "prepared_points": 500, "prepared_sheets": 7}
    assert main([], producer=producer, today=date(2026, 11, 4)) == 2
    payload = json.loads(capsys.readouterr().out)
    assert len(calls) == 2
    assert payload["results"][1] == {"product": "M678", "status": "error", "code": "SPC_MEMBERSHIP_SYNC_FAILED"}
    assert "secret" not in json.dumps(payload)
