from src.yield_domain.application.alert_service import AlertService


def test_alert_service_consumes_pre_filtered_data_without_app_config(tmp_path) -> None:
    alerts = AlertService.get_dashboard_alerts(
        mwd_group_data={},
        mwd_code_data={},
        product_dir=tmp_path,
    )

    assert alerts == []
