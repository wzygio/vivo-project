"""Stable failures at the generated-workbook input boundary."""


class ExcelAlarmReadError(ValueError):
    """A present generated workbook cannot be read or violates its contract."""
