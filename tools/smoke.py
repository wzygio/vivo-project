"""Run a conservative, explicitly scoped unit-test smoke check."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import os
from pathlib import Path
import sys


SMOKE_PATTERNS: dict[str, tuple[str, ...]] = {
    "spc": (
        "tests/unit/app/pages/test_spc_*.py",
        "tests/unit/app/pages/test_ctq_*.py",
        "tests/unit/app/sections/ctq/test_ctq_*.py",
        "tests/unit/app/sections/monitor/test_monitor_*.py",
        "tests/unit/app/sections/spc/test_spc_*.py",
        "tests/unit/inline_domain/**/*.py",
        "tests/unit/test_spc_*.py",
    ),
    "equipment": (
        "tests/unit/test_equipment_*.py",
        "tests/unit/test_parts_service_cache.py",
    ),
    "yield": (
        "tests/unit/test_abnormal_detector.py",
        "tests/unit/test_capping_mechanism.py",
        "tests/unit/test_code_*.py",
        "tests/unit/test_compliance_*.py",
        "tests/unit/test_defect_panel_count_alignment.py",
        "tests/unit/test_mapping_*.py",
        "tests/unit/test_override_logic.py",
        "tests/unit/test_shadow_ema.py",
        "tests/unit/test_sheet_lot_*.py",
        "tests/unit/test_yield_*.py",
    ),
}


def resolve_smoke_targets(area: str, repo_root: Path) -> tuple[Path, ...]:
    """Resolve an explicit area to existing repository-relative pytest targets."""
    normalized_area = area.strip().lower()
    if normalized_area == "all":
        unit_dir = repo_root / "tests" / "unit"
        if not unit_dir.is_dir():
            raise RuntimeError("Smoke area 'all' resolved no test directory")
        return (Path("tests/unit"),)

    patterns = SMOKE_PATTERNS.get(normalized_area)
    if patterns is None:
        supported = ", ".join((*SMOKE_PATTERNS, "all"))
        raise ValueError(f"Unsupported smoke area '{area}'. Choose one of: {supported}")

    relative_targets = {
        path.relative_to(repo_root)
        for pattern in patterns
        for path in repo_root.glob(pattern)
        if path.is_file()
    }
    if not relative_targets:
        raise RuntimeError(f"Smoke area '{normalized_area}' resolved no test files")
    return tuple(sorted(relative_targets, key=lambda path: path.as_posix()))


def build_pytest_args(targets: Sequence[Path]) -> tuple[str, ...]:
    """Build pytest arguments without changing pytest's native exit semantics."""
    if not targets:
        raise ValueError("At least one pytest target is required")
    return (
        "-q",
        "--tb=short",
        *(target.as_posix() for target in targets),
    )


def configure_import_paths(repo_root: Path) -> None:
    """Mirror the Harness PYTHONPATH contract inside the smoke process."""
    for path in (repo_root, repo_root / "src"):
        path_text = str(path)
        if path_text in sys.path:
            sys.path.remove(path_text)
        sys.path.insert(0, path_text)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run an explicit domain smoke check; defaults to the complete unit suite.",
    )
    parser.add_argument(
        "area",
        nargs="?",
        default="all",
        choices=(*SMOKE_PATTERNS, "all"),
        help="Test scope. Fast domains are opt-in; default: all.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    targets = resolve_smoke_targets(args.area, repo_root)
    print(f"[smoke] area={args.area}", flush=True)
    for target in targets:
        print(f"[smoke] target={target.as_posix()}", flush=True)

    configure_import_paths(repo_root)
    import pytest

    os.chdir(repo_root)
    return int(pytest.main(list(build_pytest_args(targets))))


if __name__ == "__main__":
    raise SystemExit(main())
