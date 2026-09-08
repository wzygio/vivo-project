import pandas as pd
import pytest

from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk, plan_latest_cpk
from src.inline_domain.core.monitor.cpk_summary import normalize_cpk_records


def detail(**changes):
    row = dict(prod_code="M626", factory="ARRAY", step_id="100", param_name="CD",
               period_type="week", period_label="2026-W36", cpk_corrected=1.2, flag=False)
    return {**row, **changes}


def baseline(label="2026-W36", kind="周度", total=10):
    return {"产品": "M626", "周期类型": kind, "时间标签": label, "显示标签": label,
            "监控类型": "SPC", "厂别": "ARRAY", "CPK总项目数": total, "Cpk≥1.33达标率": .9}


def plan(rows, details):
    return plan_latest_cpk(normalize_cpk_records(pd.DataFrame(rows)),
                           normalize_latest_cpk(pd.DataFrame(details), "M626"),
                           products=["M626"], factories=["ARRAY"], factory_key="ARRAY",
                           as_of=pd.Timestamp("2026-09-08"))


def test_deduplicates_projects_excludes_decorated_and_preserves_other_periods():
    updates, status = plan(
        [baseline(), baseline("2026-09", "月度"), baseline("2026", "年度")],
        [detail(), detail(), detail(param_name="B", flag=True),
         detail(param_name="C", flag="Delete"), detail(param_name="D", cpk_corrected=1.33)],
    )
    assert updates["时间标签"].tolist() == ["2026-W36"]
    assert updates["预警项目数"].tolist() == [1]
    assert updates["达标项目数"].tolist() == [9]
    assert updates["Cpk≥1.33达标率"].tolist() == [.9]
    assert status["source"].tolist() == ["excel", "preserved"]


@pytest.mark.parametrize("total", [0, -1, None])
def test_invalid_denominator_is_not_overwritten(total):
    updates, _ = plan([baseline(total=total)], [detail()])
    assert updates.empty


def test_alert_count_over_denominator_is_rejected():
    with pytest.raises(ValueError, match="超过"):
        plan([baseline(total=1)], [detail(), detail(param_name="B")])


def test_latest_week_rolls_across_iso_year():
    records = normalize_cpk_records(pd.DataFrame([baseline("2025-W52")]))
    source = normalize_latest_cpk(pd.DataFrame([detail(period_label="2025-W52")]), "M626")
    updates, _ = plan_latest_cpk(records, source, products=["M626"], factories=["ARRAY"],
                                  factory_key="ARRAY", as_of=pd.Timestamp("2026-01-01"))
    assert updates["时间标签"].tolist() == ["2025-W52"]


@pytest.mark.parametrize("change", [{"flag": "bad"},
                                    {"param_name": None}, {"prod_code": "M678"}])
def test_rejects_invalid_source(change):
    with pytest.raises(ValueError):
        normalize_latest_cpk(pd.DataFrame([detail(**change)]), "M626")


def test_conflicting_duplicates_rejected_and_blank_rows_ignored():
    assert len(normalize_latest_cpk(pd.DataFrame([detail(), {}]), "M626")) == 1
    with pytest.raises(ValueError, match="冲突"):
        normalize_latest_cpk(pd.DataFrame([detail(), detail(flag=True)]), "M626")


def test_placeholder_rows_and_decorated_nan_do_not_block_valid_update():
    updates, _ = plan([baseline()], [detail(flag=True, cpk_corrected=None),
                                    {"cpk_replacement": 1.38}])
    assert updates["预警项目数"].tolist() == [0]


def test_unclassifiable_latest_period_preserved_but_other_period_can_update():
    updates, status = plan([baseline(), baseline("2026-09", "月度")],
                          [detail(cpk_corrected=None),
                           detail(period_type="month", period_label="2026-09")])
    assert updates["时间标签"].tolist() == ["2026-09"]
    assert status["source"].tolist() == ["preserved", "excel"]


def test_unrelated_invalid_value_does_not_block_latest_period():
    updates, _ = plan([baseline()], [detail(), detail(period_label="2026-W35", cpk_corrected=None)])
    assert updates["预警项目数"].tolist() == [1]
