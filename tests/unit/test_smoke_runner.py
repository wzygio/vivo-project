from pathlib import Path
import sys

import pytest

from tools.smoke import build_pytest_args, configure_import_paths, resolve_smoke_targets


def test_smoke_defaults_to_complete_unit_suite() -> None:
    assert resolve_smoke_targets("all", Path.cwd()) == (Path("tests/unit"),)


def test_spc_smoke_resolves_only_existing_spc_and_cpm_tests() -> None:
    targets = resolve_smoke_targets("spc", Path.cwd())
    target_names = {target.name for target in targets}

    assert "test_spc_cpm_calculator.py" in target_names
    assert "test_cpm_page_alerts.py" in target_names
    assert all("yield" not in name for name in target_names)
    assert all((Path.cwd() / target).is_file() for target in targets)


def test_equipment_smoke_resolves_core_and_service_contracts() -> None:
    targets = resolve_smoke_targets("equipment", Path.cwd())

    assert Path("tests/unit/test_equipment_parts.py") in targets
    assert Path("tests/unit/test_equipment_data_fabricator.py") in targets
    assert Path("tests/unit/test_parts_service_cache.py") in targets


def test_yield_smoke_resolves_yield_contracts_without_spc_tests() -> None:
    targets = resolve_smoke_targets("yield", Path.cwd())
    target_names = {target.name for target in targets}

    assert "test_yield_global_data_policy.py" in target_names
    assert "test_mapping_random_modification.py" in target_names
    assert all(not name.startswith("test_spc_") for name in target_names)


def test_invalid_smoke_area_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported smoke area"):
        resolve_smoke_targets("unknown", Path.cwd())


def test_empty_domain_target_cannot_pass_silently(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="resolved no test files"):
        resolve_smoke_targets("spc", tmp_path)


def test_pytest_args_keep_native_exit_semantics() -> None:
    args = build_pytest_args((Path("tests/unit/test_spc_cpm_calculator.py"),))

    assert args[:2] == ("-q", "--tb=short")
    assert "tests/unit/test_spc_cpm_calculator.py" in args


def test_smoke_configures_repo_and_src_import_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sys, "path", [])

    configure_import_paths(tmp_path)

    assert sys.path[:2] == [str(tmp_path / "src"), str(tmp_path)]
