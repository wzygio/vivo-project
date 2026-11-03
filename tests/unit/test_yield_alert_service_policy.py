from src.yield_domain.application.alert_service import AlertService


def test_alert_service_consumes_pre_filtered_data_without_app_config(tmp_path) -> None:
    alerts = AlertService.get_dashboard_alerts(
        mwd_group_data={},
        mwd_code_data={},
        product_dir=tmp_path,
    )

    assert alerts == []


def test_alert_service_disables_monthly_but_keeps_iso_week_comparisons(tmp_path) -> None:
    import pandas as pd

    group_monthly = pd.DataFrame(
        {
            "defect_group": ["MonthlyGroup", "MonthlyGroup"],
            "time_period": ["2026-08", "2026-09"],
            "defect_rate": [0.001, 0.004],
        }
    )
    group_weekly = pd.DataFrame(
        {
            "defect_group": ["WeeklyGroup", "WeeklyGroup"],
            "time_period": ["2026-W36", "2026-W37"],
            "defect_rate": [0.010, 0.013],
        }
    )

    alerts = AlertService.get_dashboard_alerts(
        mwd_group_data={"monthly": group_monthly, "weekly": group_weekly},
        mwd_code_data={},
        product_dir=tmp_path,
    )

    assert len(alerts) == 1
    weekly_alert = alerts[0]
    assert "2026-W37" in weekly_alert
    assert "上周 1.00%" in weekly_alert
    assert "上月" not in weekly_alert


def test_alert_service_returns_structured_records_with_period_scope(tmp_path) -> None:
    import pandas as pd

    code_monthly = pd.DataFrame(
        {
            "defect_desc": ["Particle"] * 3,
            "defect_group": ["DEFECT"] * 3,
            "time_period": ["2026-06", "2026-07", "2026-08"],
            "defect_rate": [0.001, 0.001, 0.005],
        }
    )
    code_weekly = pd.DataFrame(
        {
            "defect_desc": ["Scratch"] * 3,
            "defect_group": ["DEFECT"] * 3,
            "time_period": ["2026-W32", "2026-W33", "2026-W34"],
            "defect_rate": [0.010, 0.010, 0.015],
        }
    )

    records = AlertService.get_dashboard_alert_records(
        mwd_group_data={},
        mwd_code_data={"monthly": code_monthly, "weekly": code_weekly},
    )

    assert len(records) == 1
    by_scope = {rec["period_scope"]: rec for rec in records}
    assert "monthly" not in by_scope
    assert by_scope["weekly"]["defect_desc"] == "Scratch"
    assert by_scope["weekly"]["time_period"] == "2026-W34"


def test_alert_service_records_empty_when_no_abnormality() -> None:
    assert AlertService.get_dashboard_alert_records(mwd_group_data={}, mwd_code_data={}) == []


def test_weekly_trend_keeps_iso_year_boundary() -> None:
    import pandas as pd

    weekly = pd.DataFrame({'defect_group': ['G', 'G'],
                           'time_period': ['2026-W53', '2027-W01'],
                           'defect_rate': [.001, .004]})
    records = AlertService.get_dashboard_alert_records({'weekly': weekly}, {})
    assert len(records) == 1
    assert records[0]['time_period'] == '2027-W01'
    assert records[0]['period_scope'] == 'weekly'
