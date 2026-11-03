"""Public service behavior using a known workbook-shaped source."""

import pandas as pd
import pytest

from src.indicator_domain.application.point_test.service import PointTestReportService


def summary_rows(sheet_prefix="S", *, blocks=2):
    rows = []
    for stage in ("调整前", "调整后", "修正后"):
        for block in range(blocks):
            rows.append([stage, "膜位", "MAX", "MIN", "AVE", "STD", "Δ/（MAX-MIN)"]
                        + [None, "ID", "膜位", "MAX", "MIN", "AVE", "STD", "Δ/（MAX-MIN)"]
                        + [None, "ID", "膜位", "MAX", "MIN", "AVE", "STD", "Δ/（MAX-MIN)"])
            for position in ("1AA0", "2EU0"):
                row = []
                for group in range(3):
                    if group:
                        row.append(None)
                    row.extend([f"{sheet_prefix}{block}{group}" if position == "1AA0" else None,
                                position, 13.023, 12.507, 12.717272727272729,
                                0.1642194208423217, 0.516])
                rows.append(row)
            rows.append([None] * 23)
    return rows


class MemorySource:
    def __init__(self, sheets=None):
        self.sheets = sheets if sheets is not None else {
            "Z576-detail": pd.DataFrame([
                ["SHEET_ID", "SITE_NAME", "PARAM_NAME", "PARAM_VALUE", "SHEET_END_TIME", "LATEST_FLAG"],
                ["S00", 1, "CD1", 12.543, 46316.57506944444, "T"],
            ]),
            "Z576-summary": pd.DataFrame(summary_rows()),
            "Sheet3": pd.DataFrame([["ignored"]]),
        }

    def read_sheets(self):
        return self.sheets


def test_report_keeps_stages_blocks_sheet_ids_and_values_separate():
    report = PointTestReportService(MemorySource()).read_report()

    assert list(report) == ["Z576"]
    detail = report["Z576"]["detail"]
    assert detail.loc[0, "PARAM_VALUE"] == 12.543
    assert detail.loc[0, "LATEST_FLAG"] == "T"
    assert str(detail.loc[0, "SHEET_END_TIME"]) == "2026-10-21 13:48:06"
    summary = report["Z576"]["summary"]
    assert summary.groupby("阶段", sort=False).size().to_dict() == {
        "调整前": 12, "调整后": 12, "修正后": 12,
    }
    row = summary[(summary["阶段"] == "调整前") & (summary["SHEET_ID"] == "S00")]
    assert row["膜位"].tolist() == ["1AA0", "2EU0"]
    assert row.iloc[0]["AVE"] == pytest.approx(12.717272727272729)
    assert set(summary["SHEET_ID"]) == {"S00", "S01", "S02", "S10", "S11", "S12"}


def test_product_pairs_are_discovered_without_mixing_products():
    source = MemorySource()
    source.sheets.update({
        "M626-detail": source.sheets["Z576-detail"].copy(),
        "M626-summary": pd.DataFrame(summary_rows("OTHER")),
    })
    report = PointTestReportService(source).read_report()
    assert list(report) == ["Z576", "M626"]
    assert set(report["M626"]["summary"]["SHEET_ID"]) == {
        "OTHER00", "OTHER01", "OTHER02", "OTHER10", "OTHER11", "OTHER12",
    }
    assert set(report["M626"]["summary"]["产品"]) == {"M626"}
    assert set(report["Z576"]["summary"]["产品"]) == {"Z576"}


@pytest.mark.parametrize("problem", ["orphan", "no_pairs", "detail_header", "summary_header",
                                      "missing_stage", "missing_id", "missing_metric", "duplicate"])
def test_invalid_workbooks_fail_instead_of_showing_partial_results(problem):
    source = MemorySource()
    summary = source.sheets["Z576-summary"]
    if problem == "orphan":
        del source.sheets["Z576-summary"]
    elif problem == "no_pairs":
        source.sheets = {"Sheet3": pd.DataFrame()}
    elif problem == "detail_header":
        source.sheets["Z576-detail"].iloc[0, 0] = "WRONG"
    elif problem == "summary_header":
        summary.iloc[0, 2] = "AVERAGE"
    elif problem == "missing_stage":
        source.sheets["Z576-summary"] = summary.iloc[:8]
    elif problem == "missing_id":
        # At the next block boundary, an earlier sheet's ID must not leak.
        summary.iloc[5, 0] = None
    elif problem == "missing_metric":
        summary.iloc[1, 2] = None
    else:
        source.sheets["Z576-summary"] = pd.concat([summary, summary.iloc[:3]], ignore_index=True)
    with pytest.raises(ValueError):
        PointTestReportService(source).read_report()


def test_empty_pairs_are_distinct_from_invalid_sources():
    report = PointTestReportService(MemorySource({
        "Z576-detail": pd.DataFrame(), "Z576-summary": pd.DataFrame(),
    })).read_report()
    assert report["Z576"]["detail"].empty
    assert report["Z576"]["summary"].empty
