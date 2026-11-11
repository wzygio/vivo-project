"""Project-local SPC point export, one workbook per product."""


class ExportError(RuntimeError):
    """An actionable export failure safe to display without database details."""
