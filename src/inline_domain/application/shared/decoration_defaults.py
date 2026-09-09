"""Compatibility wiring for existing free-function callers.

This is the one explicit application -> composition exception for decoration.
New callers may pass consumer-owned ports; resolving defaults is delayed until
an actual I/O operation so injected use cases need no infrastructure bootstrap.
"""

from pathlib import Path

from src.inline_domain.application.shared.decoration_ports import DecorationPort


def default_decoration_port() -> DecorationPort:
    from src.inline_domain.composition import build_decoration_port

    return build_decoration_port()


def load_sheet_oos_decisions(*args, **kwargs):
    return default_decoration_port().load_sheet_oos_decisions(*args, **kwargs)


def persist_sheet_oos_decoration_outcome(*args, **kwargs):
    return default_decoration_port().persist_sheet_oos_decoration_outcome(*args, **kwargs)


def load_capability_decoration(*args, **kwargs):
    return default_decoration_port().load_capability_decoration(*args, **kwargs)


def persist_capability_decoration(*args, **kwargs):
    return default_decoration_port().persist_capability_decoration(*args, **kwargs)


def get_capability_decoration_signature(product_dir: Path) -> tuple[int, int]:
    return default_decoration_port().get_capability_decoration_signature(product_dir)


def get_decision_file_stat(workbook_path: Path) -> tuple[int, int] | None:
    return default_decoration_port().get_decision_file_stat(workbook_path)


def scope_resource_dir(scope: str, alarm_type: str = "oos") -> Path:
    return default_decoration_port().scope_resource_dir(scope, alarm_type)
