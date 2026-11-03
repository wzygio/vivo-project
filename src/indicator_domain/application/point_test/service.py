"""Load and validate a coherent point-test report without database access."""

import pandas as pd

from src.indicator_domain.application.point_test.ports import PointTestSource
from src.indicator_domain.core.point_test.report import parse_report


class PointTestReportService:
    def __init__(self, source: PointTestSource) -> None:
        self._source = source

    def read_report(self) -> dict[str, dict[str, pd.DataFrame]]:
        return parse_report(self._source.read_sheets())
