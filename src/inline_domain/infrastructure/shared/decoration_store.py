"""Workbook adapter implementing the Inline consumer-owned decoration ports."""

from pathlib import Path

from src.inline_domain.infrastructure.shared import sheet_oos_decoration_repository as sheet
from src.inline_domain.infrastructure.shared.resource_paths import scope_resource_dir
from src.inline_domain.infrastructure.spc import capability_decoration_repository as capability


class ExcelDecorationStore:
    load_sheet_oos_decisions = staticmethod(sheet.load_sheet_oos_decisions)
    persist_sheet_oos_decoration_outcome = staticmethod(sheet.persist_sheet_oos_decoration_outcome)
    load_capability_decoration = staticmethod(capability.load_capability_decoration)
    persist_capability_decoration = staticmethod(capability.persist_capability_decoration)
    get_capability_decoration_signature = staticmethod(capability.get_capability_decoration_signature)
    scope_resource_dir = staticmethod(scope_resource_dir)

    @staticmethod
    def get_decision_file_stat(workbook_path: Path) -> tuple[int, int] | None:
        try:
            stat = Path(workbook_path).stat()
        except OSError:
            return None
        return int(stat.st_mtime_ns), int(stat.st_size)
