"""Strict scope reads never turn empty or invalid configuration into M678."""
import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize('registry,expected', [
    ({'enabled_products': []}, []),
    ({'enabled_products': ['M678','M626','M678']}, ['M678','M626']),
])
def test_strict_scope_preserves_empty_and_order(monkeypatch, registry, expected):
    monkeypatch.setattr(ConfigLoader, '_load_yaml', staticmethod(lambda path: {'product_registry': registry}))
    assert ConfigLoader.get_enabled_products(strict=True) == expected


@pytest.mark.parametrize('config', [
    {}, {'product_registry': None}, {'product_registry': {}},
    {'product_registry': {'enabled_products': 'M678'}},
    {'product_registry': {'enabled_products': [None]}},
    {'product_registry': {'enabled_products': ['']}},
])
def test_strict_scope_rejects_missing_or_malformed_allowlist(monkeypatch, config):
    monkeypatch.setattr(ConfigLoader, '_load_yaml', staticmethod(lambda path: config))
    with pytest.raises(ValueError, match='^ENABLED_PRODUCT_SCOPE_INVALID$'):
        ConfigLoader.get_enabled_products(strict=True)


def test_existing_non_strict_callers_keep_their_compatibility_default(monkeypatch):
    monkeypatch.setattr(ConfigLoader, '_load_yaml', staticmethod(lambda path: {}))
    assert ConfigLoader.get_enabled_products() == ['M678']
