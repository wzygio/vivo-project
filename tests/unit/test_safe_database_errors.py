"""Database failure logs retain diagnostic IDs without driver payloads."""

import logging
from types import SimpleNamespace
from uuid import UUID

import pytest

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs import data_loader
from src.shared_kernel.infrastructure import db_handler


FAKE_DRIVER_MESSAGE = (
    "password=fictional-secret; SQL=SELECT confidential_column; "
    "parameters={'token': 'fictional-token'}"
)


def _fail(*_args, **_kwargs):
    raise RuntimeError(FAKE_DRIVER_MESSAGE)


def _assert_safe_error(caplog, code, exception_type="RuntimeError"):
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(errors) == 1
    record = errors[0]
    message = record.getMessage()
    assert f"code={code}" in message
    assert f"exception_type={exception_type}" in message
    correlation_id = message.split("correlation_id=", 1)[1].split()[0]
    assert UUID(correlation_id).version == 4
    assert record.exc_info is None
    assert record.stack_info is None
    for value in ("fictional-secret", "confidential_column", "fictional-token"):
        assert value not in caplog.text
    return correlation_id


@pytest.mark.parametrize("failure_stage", ["create", "connect", "configuration"])
def test_engine_failure_logs_safe_diagnostics_and_keeps_none_result(
    monkeypatch, caplog, failure_stage,
):
    # Never load .env or consult the machine's actual DB configuration.
    monkeypatch.setattr(db_handler, "load_dotenv", lambda **_kwargs: None)
    settings = {
        "DB_PASSWORD": "fictional-secret",
        "DB_USER": "fixture_user",
        "DB_HOST": "fixture.invalid",
        "DB_PORT": "5432",
        "DB_DATABASE": "fixture_db",
    }
    if failure_stage == "configuration":
        settings.clear()
    monkeypatch.setattr(db_handler.os, "getenv", lambda key, default=None: settings.get(key, default))
    factory = _fail if failure_stage == "create" else lambda *_a, **_kw: SimpleNamespace(connect=_fail)
    monkeypatch.setattr(db_handler, "create_engine", factory)
    manager = object.__new__(db_handler.DatabaseManager)

    with caplog.at_level(logging.ERROR):
        result = manager._create_engine()

    assert result is None
    _assert_safe_error(
        caplog, "DATABASE_ENGINE_UNAVAILABLE",
        "ValueError" if failure_stage == "configuration" else "RuntimeError",
    )


@pytest.mark.parametrize("operation", ["details", "pass_through", "spec_limits"])
@pytest.mark.parametrize("engine_available", [True, False])
def test_aoi_rs_failure_preserves_empty_contract_without_driver_details(
    monkeypatch, caplog, operation, engine_available,
):
    monkeypatch.setattr(data_loader.pd, "read_sql", _fail)
    manager = SimpleNamespace(engine=object() if engine_available else None)
    query = AoiRsQueryConfig(
        start_date="2026-08-01", end_date="2026-08-31", prod_code="M678",
    )
    operations = {
        "details": (lambda: data_loader.load_rs_details(manager, query), data_loader.RS_DETAIL_COLUMNS),
        "pass_through": (lambda: data_loader.load_pass_through(manager, query), data_loader.PASS_THROUGH_COLUMNS),
        "spec_limits": (
            lambda: data_loader.load_rs_spec_limits(manager, "M678"),
            ["prod_code", "factory", "type_flag", "step_id", "rs_code", "code_desc", "spec"],
        ),
    }
    action, columns = operations[operation]

    with caplog.at_level(logging.ERROR):
        result = action()

    assert result.empty
    assert list(result.columns) == columns
    _assert_safe_error(
        caplog, "AOI_RS_QUERY_FAILED",
        "RuntimeError" if engine_available else "ValueError",
    )


def test_separate_failures_have_distinct_correlation_ids(monkeypatch, caplog):
    monkeypatch.setattr(data_loader.pd, "read_sql", _fail)
    manager = SimpleNamespace(engine=object())
    ids = []
    for _ in range(2):
        caplog.clear()
        with caplog.at_level(logging.ERROR):
            data_loader.load_rs_spec_limits(manager, "M678")
        ids.append(_assert_safe_error(caplog, "AOI_RS_QUERY_FAILED"))
    assert ids[0] != ids[1]
