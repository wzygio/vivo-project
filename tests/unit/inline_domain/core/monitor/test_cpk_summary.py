import pandas as pd
import pytest

from src.inline_domain.core.monitor.cpk_summary import (
    build_current_cpk_detail, build_current_cpk_records,
    build_cpk_summary_from_records, normalize_cpk_records,
)
from src.inline_domain.core.spc.spc_calculator import calculate_cpk


def test_year_cpk_uses_all_sheet_means_not_monthly_cpk_average():
    features = pd.DataFrame([
        {"prod_code": "M626", "factory": "ARRAY", "step_id": "1",
         "param_name": "CD", "sheet_id": str(index), "sheet_start_time": time,
         "sheet_mean": mean, "usl": 20, "lsl": 1}
        for index, (time, mean) in enumerate([
            ("2026-08-01", 2), ("2026-08-02", 3),
            ("2026-09-07", 15), ("2026-09-07", 16),
        ])
    ])
    detail = build_current_cpk_detail(features, end_date=pd.Timestamp("2026-09-07"))
    year = detail[detail.period_type.eq("year")].iloc[0]
    expected = calculate_cpk(9, pd.Series([2, 3, 15, 16]).std(ddof=1), 20, 1)
    assert year.cpk == pytest.approx(expected)
    assert year.sample_count == 4
    assert set(detail.period_label) == {"2026", "2026-Q3", "2026-09", "2026-W37"}


def test_legacy_counts_unknown_and_multi_product_rate_weighted():
    rows = normalize_cpk_records(pd.DataFrame([
        {"产品": product, "周期类型": "月度", "时间标签": "2026-08",
         "显示标签": "M8", "CPK总项目数": count, "Cpk≥1.33达标率": rate}
        for product, count, rate in [("M626", 10, .5), ("M673", 30, .9)]
    ]))
    assert rows["达标项目数"].isna().all()
    result = build_cpk_summary_from_records(
        rows, end_date=pd.Timestamp("2026-09-07"), expected_products=["M626", "M673"],
    )
    assert result.M8.tolist() == [40, "—", "—", "80.00%"]
    assert result.M9.tolist() == ["—"] * 4


def test_cpk_threshold_and_invalid_inputs_have_distinct_counts():
    detail = pd.DataFrame({
        "prod_code": ["M626"] * 4, "period_label": ["2026"] * 4,
        "cpk": [1.33, 1.32, float("inf"), float("nan")],
    })
    rows = build_current_cpk_records(
        detail, products=["M626"], factory_key="ALL", end_date=pd.Timestamp("2026-09-07"),
    )
    year = rows.iloc[0]
    assert year["CPK总项目数"] == 3
    assert year["达标项目数"] == 2
    assert year["预警项目数"] == 1
    assert year["Cpk≥1.33达标率"] == pytest.approx(2 / 3)


def test_year_boundary_includes_iso_week_from_previous_year():
    frame = pd.DataFrame([
        {"prod_code": "M626", "factory": "ARRAY", "step_id": "1", "param_name": "CD",
         "sheet_id": str(i), "sheet_start_time": time, "sheet_mean": i + 3, "usl": 10, "lsl": 1}
        for i, time in enumerate(["2025-12-31", "2026-01-01"])
    ])
    result = build_current_cpk_detail(frame, end_date=pd.Timestamp("2026-01-01"))
    assert result[result.period_type.eq("week")].iloc[0].sample_count == 2
    assert result[result.period_type.eq("year")].iloc[0].sample_count == 1
