from datetime import date

import pandas as pd

from app.sections.iqc_domain.report_table import build_report_table
from src.iqc_domain.application.demo_reports import filter_demo_report, load_demo_report


def test_workbook_values_and_blanks_are_preserved():
    inspection = load_demo_report("inspection")
    lifetime = load_demo_report("lifetime")
    assert inspection.shape == (128, 25)
    assert inspection.iloc[0]["IQC-1"] == 63.081
    assert inspection["IQC-2"].isna().all()
    assert lifetime.shape == (12, 20)
    assert lifetime.iloc[0]["AVG"] == 0.9752
    assert lifetime.loc[lifetime["类别"] != "衰减比例", "AVG"].isna().all()


def test_filters_combine_and_include_end_date_without_mutating_source():
    source = load_demo_report("inspection")
    original = source.copy(deep=True)
    result = filter_demo_report(
        source, {"特性项目": ["HPLC-A"]}, "报检日期",
        (date(2026, 8, 20), date(2026, 8, 20)), "GP-RH1080",
    )
    assert len(result) == 16
    assert result["特性项目"].eq("HPLC-A").all()
    pd.testing.assert_frame_equal(source, original)
    assert filter_demo_report(
        source, {}, "报检日期", (date(2026, 8, 1), date(2026, 9, 30)), "[not a regex]",
    ).empty


def test_html_preserves_percentages_blanks_and_escapes_values():
    lifetime = load_demo_report("lifetime")
    html = build_report_table(lifetime, "lifetime")
    assert html.count("97.52%") == 44
    assert "<td>nan</td>" not in html
    assert "<td></td>" in html
    unsafe = pd.DataFrame({"<script>": ["<img src=x onerror=alert(1)>"]})
    escaped = build_report_table(unsafe, "inspection")
    assert "<script>" not in escaped
    assert "<img" not in escaped
    assert "&lt;img" in escaped
