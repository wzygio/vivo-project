from __future__ import annotations

import ast
from pathlib import Path

import pytest

from inline_domain.infrastructure.spc.spc_repository import SpcRepository
from src.inline_domain.infrastructure.measurement.measurement_preparation import (
    InlineMeasurementPreparationRepository,
)


PROJECT_ROOT = Path(__file__).parents[3]
INLINE_ROOT = PROJECT_ROOT / "src" / "inline_domain"


def test_spc_and_aoi_tt_adapters_do_not_own_database_queries() -> None:
    violations: list[str] = []
    for relative_root in (
        Path("infrastructure/spc"),
        Path("infrastructure/aoi_tt"),
    ):
        for path in (INLINE_ROOT / relative_root).rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            imports = {
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, (ast.Import, ast.ImportFrom))
                for alias in node.names
            }
            if any(name == "sqlalchemy" or name.startswith("sqlalchemy.") for name in imports):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} imports sqlalchemy")
            if "read_sql" in source:
                violations.append(f"{path.relative_to(PROJECT_ROOT)} executes SQL")

    assert violations == []


def test_preparation_repository_rejects_missing_monitor_ports() -> None:
    with pytest.raises(ValueError, match="requires raw measurement"):
        InlineMeasurementPreparationRepository(None, None, None)


def test_spc_repository_rejects_missing_preparation_port() -> None:
    with pytest.raises(ValueError, match="requires a measurement preparation port"):
        SpcRepository(None)


def test_monitor_application_service_does_not_import_infrastructure() -> None:
    path = INLINE_ROOT / "application" / "monitor" / "monitor_service.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    infrastructure_imports = [
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module
        and node.module.startswith("src.inline_domain.infrastructure")
    ]

    assert infrastructure_imports == []


def test_monitor_infrastructure_contains_no_reusable_measurement_dao() -> None:
    monitor_root = INLINE_ROOT / "infrastructure" / "monitor"
    forbidden_modules = {
        "measurement_data_loader.py",
        "measurement_metadata_loader.py",
        "measurement_snapshot_repository.py",
        "main_process_history_repository.py",
    }

    assert forbidden_modules.isdisjoint(path.name for path in monitor_root.glob("*.py"))


def test_shared_measurement_dao_lives_in_measurement_infrastructure() -> None:
    measurement_root = INLINE_ROOT / "infrastructure" / "measurement"
    expected_modules = {
        "measurement_data_loader.py",
        "measurement_metadata_loader.py",
        "measurement_snapshot_repository.py",
        "main_process_history_repository.py",
    }

    assert expected_modules <= {path.name for path in measurement_root.glob("*.py")}
