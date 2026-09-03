"""Architecture guardrails for data-decoration code.

Business rules belong to ``core``.  Core code must not know how Excel files,
sidecar files, or other persistence mechanisms are implemented.
"""

from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
FORBIDDEN_CORE_IMPORTS = {
    "openpyxl",
    "src.shared_kernel.utils.excel_tools",
}
FORBIDDEN_CORE_CALLS = {
    "read_excel",
    "ExcelWriter",
    "to_excel",
    "read_workbook_sheet",
    "replace_workbook_sheet",
    "replace_workbook_sheets",
    "read_text",
    "write_text",
}


def _core_python_files() -> list[Path]:
    return sorted(path for path in SRC_ROOT.glob("**/core/**/*.py") if path.is_file())


def _qualified_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _qualified_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def test_core_does_not_depend_on_excel_persistence() -> None:
    violations: list[str] = []
    for path in _core_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        relative = path.relative_to(PROJECT_ROOT)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if any(
                        alias.name == name or alias.name.startswith(f"{name}.")
                        for name in FORBIDDEN_CORE_IMPORTS
                    ):
                        violations.append(f"{relative}:{node.lineno}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if ".infrastructure" in module:
                    violations.append(f"{relative}:{node.lineno}: from {module}")
                if any(
                    module == name or module.startswith(f"{name}.")
                    for name in FORBIDDEN_CORE_IMPORTS
                ):
                    violations.append(f"{relative}:{node.lineno}: from {module}")
            elif isinstance(node, ast.Call):
                call_name = _qualified_name(node.func).split(".")[-1]
                if call_name in FORBIDDEN_CORE_CALLS:
                    violations.append(f"{relative}:{node.lineno}: call {call_name}")

    assert violations == [], "Core contains persistence concerns:\n" + "\n".join(violations)


def test_spc_measurement_correction_rule_is_owned_by_core() -> None:
    core_rule = SRC_ROOT / "inline_domain/core/shared/measurement_correction.py"
    infrastructure_rule = (
        SRC_ROOT / "inline_domain/infrastructure/spc/spc_data_correction.py"
    )

    assert core_rule.is_file()
    assert not infrastructure_rule.exists()
