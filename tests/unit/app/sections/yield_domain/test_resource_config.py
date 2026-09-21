from types import SimpleNamespace
import pytest

from app.sections.yield_domain import resource_config


@pytest.mark.parametrize("settings,expected", [({}, 0.0001), ({"dashboard": {"code_monthly_rate_threshold": 0.0002}}, 0.0002)])
def test_threshold_uses_domain_configuration(monkeypatch, settings, expected):
    monkeypatch.setattr(resource_config.ConfigLoader, "load_domain_config", lambda domain: settings)
    assert resource_config.get_code_monthly_rate_threshold() == expected


@pytest.mark.parametrize("value", [-0.1, 1.1, "invalid", None, True, float("nan"), float("inf")])
def test_invalid_threshold_fails_explicitly(monkeypatch, value):
    monkeypatch.setattr(resource_config.ConfigLoader, "load_domain_config", lambda domain: {
        "dashboard": {"code_monthly_rate_threshold": value},
    })
    with pytest.raises(ValueError):
        resource_config.get_code_monthly_rate_threshold()


def test_mapping_configuration_waits_for_manual_refresh(monkeypatch, mock_config):
    monkeypatch.setattr(resource_config, "st", SimpleNamespace(session_state={}))
    current_scripts = [{"target_code": "old"}]
    reads = []

    def inject(config):
        reads.append(config.data_source.product_code)
        config.processing["mapping_hotspot_script"] = current_scripts.copy()

    monkeypatch.setattr(resource_config.ExcelService, "inject_mapping_config_to_config", inject)
    original = mock_config.model_dump()
    first = resource_config.get_dashboard_resource_config(mock_config, "revision-1")
    current_scripts[:] = [{"target_code": "new"}]
    second = resource_config.get_dashboard_resource_config(mock_config, "revision-1")
    assert second.processing["mapping_hotspot_script"] == [{"target_code": "old"}]
    first.processing["mapping_hotspot_script"][0]["target_code"] = "mutated"
    third = resource_config.get_dashboard_resource_config(mock_config, "revision-1")
    assert third.processing["mapping_hotspot_script"] == [{"target_code": "old"}]
    refreshed = resource_config.get_dashboard_resource_config(mock_config, "revision-2")
    assert refreshed.processing["mapping_hotspot_script"] == [{"target_code": "new"}]
    assert len(reads) == 2
    assert mock_config.model_dump() == original


def test_mapping_configuration_is_isolated_by_product(monkeypatch, mock_config):
    monkeypatch.setattr(resource_config, "st", SimpleNamespace(session_state={}))
    reads = []

    def inject(config):
        product = config.data_source.product_code
        reads.append(product)
        config.processing["mapping_hotspot_script"] = [{"target_product": product}]

    monkeypatch.setattr(resource_config.ExcelService, "inject_mapping_config_to_config", inject)
    resource_config.get_dashboard_resource_config(mock_config, "revision-1")
    other = mock_config.model_copy(deep=True)
    other.data_source.product_code = "OTHER"
    resource_config.get_dashboard_resource_config(other, "revision-1")
    restored = resource_config.get_dashboard_resource_config(mock_config, "revision-1")
    assert restored.processing["mapping_hotspot_script"] == [
        {"target_product": mock_config.data_source.product_code}
    ]
    assert reads == [mock_config.data_source.product_code, "OTHER"]
