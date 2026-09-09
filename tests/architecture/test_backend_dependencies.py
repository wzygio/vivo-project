"""Enforce the ratchet towards consumer-owned outbound ports.

Exceptions are exact source/target pairs, not whole-domain exemptions. Removing
one raises a stale-entry failure so this remains a reviewed migration register.
Cross-domain collaboration uses explicitly listed public contract modules.
"""

from __future__ import annotations

import ast
from pathlib import Path, PurePosixPath

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Each value explains why the edge is deliberately retained in H4.
LEGACY_EDGES = {
    ("equipment_domain/application/parts_service.py", "equipment_domain.composition"):
        "One compatibility default resolver for static callers; injected data ports bypass it.",
    ("equipment_domain/application/parts_service.py", "shared_kernel.infrastructure.db_handler"):
        "TYPE_CHECKING-only compatibility annotation for the existing DatabaseManager argument.",
    ("yield_domain/application/alert_service.py", "yield_domain.infrastructure.data_loader"):
        "Legacy Yield alert configuration loader; migrate with alert assembly.",
    ("yield_domain/application/modifier_table_service.py", "yield_domain.infrastructure.modifier_table_repository"):
        "Yield's separate user-maintained modifier workbook is outside Inline migration.",
    ("yield_domain/application/yield_service.py", "yield_domain.composition"):
        "One compatibility default resolver; repository, signatures and overrides live behind a port.",
    ("yield_domain/application/yield_service.py", "shared_kernel.infrastructure.db_handler"):
        "TYPE_CHECKING-only compatibility annotation for the existing DatabaseManager argument.",
    ("iqc_domain/application/demo_reports.py", "iqc_domain.infrastructure.demo_repository"):
        "Static IQC demo only; no live business data use case yet.",
    ("inline_domain/application/shared/decoration_defaults.py", "inline_domain.composition"):
        "Single compatibility default resolver for existing free-function callers; injected ports bypass it.",
    ("inline_domain/application/shared/throughput_persistence.py", "inline_domain.composition"):
        "Existing throughput producer compatibility entry; service itself now consumes a Protocol.",
}

# No cross-domain imports currently needed. Add an exact public module here
# only with a documented contract; private symbols are forbidden even then.
PUBLIC_CROSS_DOMAIN_MODULES: set[str] = set()
PERSISTENCE_PACKAGES = {"sqlalchemy", "psycopg2", "openpyxl", "pyarrow"}


def imported_targets(source: str, relative_path: str):
    """Resolve plain/src-prefixed and relative imports without importing code."""
    path = PurePosixPath(relative_path)
    package = list(path.parent.parts)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.removeprefix("src."), None
        elif isinstance(node, ast.ImportFrom):
            base = (node.module or "").split(".") if node.module else []
            if node.level:
                base = package[:len(package) - node.level + 1] + base
            module = ".".join(base).removeprefix("src.")
            for alias in node.names:
                # Including the imported name catches `from x import
                # infrastructure` and `from .. import composition` too.
                yield module, alias.name


def forbidden_edges(source: str, relative_path: str) -> set[tuple[str, str]]:
    parts = PurePosixPath(relative_path).parts
    layer = "core" if "core" in parts else "application" if "application" in parts else None
    if layer is None:
        return set()
    source_domain = parts[0]
    result = set()
    for module, symbol in imported_targets(source, relative_path):
        target = module.split(".")
        extended = target + ([symbol] if symbol else [])
        forbidden = "infrastructure" in extended or "composition" in extended
        if layer == "core":
            forbidden |= "application" in extended
            forbidden |= target[0] in PERSISTENCE_PACKAGES
            forbidden |= module == "shared_kernel.utils.excel_tools" or ".".join(extended) == "shared_kernel.utils.excel_tools"
        target_domain = target[0]
        if target_domain.endswith("_domain") and target_domain != source_domain:
            forbidden |= module not in PUBLIC_CROSS_DOMAIN_MODULES
            forbidden |= bool(symbol and symbol.startswith("_"))
        if forbidden:
            # Preserve module-only edges for exact migration entries, but a
            # from-package import of a forbidden module is its own edge.
            edge_target = f"{module}.{symbol}" if symbol in {"infrastructure", "composition", "application"} else module
            result.add((relative_path, edge_target))
    return result


@pytest.mark.parametrize("source", [
    "import src.inline_domain.infrastructure.shared.decoration_store",
    "from inline_domain.infrastructure.shared import decoration_store",
    "from ...infrastructure.shared import decoration_store",
    "from ... import infrastructure",
    "from ... import composition",
])
def test_guard_rejects_application_persistence_and_composition_imports(source):
    assert forbidden_edges(source, "inline_domain/application/shared/example.py")


@pytest.mark.parametrize("source", [
    "from ..application import service",
    "import sqlalchemy",
    "from shared_kernel.utils import excel_tools",
    "from src.yield_domain.core.calculator import _private_rule",
])
def test_guard_rejects_core_outward_or_cross_domain_imports(source):
    assert forbidden_edges(source, "inline_domain/core/example.py")


def test_consumer_port_import_is_allowed():
    assert not forbidden_edges(
        "from .decoration_ports import SheetDecorationPort",
        "inline_domain/application/shared/example.py",
    )


def test_backend_dependencies_only_have_reviewed_legacy_edges():
    actual = set()
    for path in (ROOT / "src").rglob("*.py"):
        actual |= forbidden_edges(path.read_text(encoding="utf-8-sig"), path.relative_to(ROOT / "src").as_posix())
    unexpected = actual - LEGACY_EDGES.keys()
    stale = LEGACY_EDGES.keys() - actual
    assert not unexpected, "New architecture violations:\n" + "\n".join(map(str, sorted(unexpected)))
    assert not stale, "Remove migrated legacy exceptions:\n" + "\n".join(map(str, sorted(stale)))
