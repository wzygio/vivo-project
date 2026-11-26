import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize("section, expected", [
    ({}, (0.5, 0.25)),
    ({"sheet_threshold_ratio": 0.8}, (0.8, 0.25)),
    ({"sheet_replacement_ratio": 0}, (0.5, 0.0)),
    ({"sheet_threshold_ratio": 1, "sheet_replacement_ratio": 0.4}, (1.0, 0.4)),
])
def test_special_sheet_ratios(monkeypatch, section, expected):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": section},
    })
    assert ConfigLoader.get_aoi_rs_special_decoration_sheet_ratios() == expected


@pytest.mark.parametrize("key", ["sheet_threshold_ratio", "sheet_replacement_ratio"])
@pytest.mark.parametrize("value", [None, True, False, "0.5", -0.1, float("nan"), float("inf"), [], {}])
def test_invalid_special_sheet_ratios_are_rejected(monkeypatch, key, value):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": {key: value}},
    })
    with pytest.raises(ValueError, match=key):
        ConfigLoader.get_aoi_rs_special_decoration_sheet_ratios()


@pytest.mark.parametrize("section, expected", [
    ({}, []),
    ({"special_decoration": {}}, []),
    ({"special_decoration": {"factories": []}}, []),
    ({"special_decoration": {"factories": ["OLED"]}}, []),
    ({"special_decoration": {"enabled": False, "factories": ["OLED"]}}, []),
    ({"special_decoration": {"enabled": True}}, ["OLED"]),
    ({"special_decoration": {"enabled": True, "factories": []}}, []),
    ({"special_decoration": {"enabled": True, "factories": [" OLED ", "array", "OLED"]}}, ["OLED", "ARRAY"]),
])
def test_special_decoration_factories(monkeypatch, section, expected):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {"aoi_rs": section})
    assert ConfigLoader.get_aoi_rs_special_decoration_factories() == expected


@pytest.mark.parametrize("product,expected", [
    ("Z622", ["OLED", "ARRAY"]),
    (" z622 ", ["OLED", "ARRAY"]),
    ("M626", ["OLED"]),
    (None, ["OLED"]),
])
def test_special_factories_can_limit_products(monkeypatch, product, expected):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": {
            "enabled": True, "factories": ["OLED", "ARRAY", "TP"],
            "products_by_factory": {" array ": [" z622 "], "TP": []},
        }},
    })
    assert ConfigLoader.get_aoi_rs_special_decoration_factories(product) == expected


def test_configured_special_decoration_covers_only_z622_in_array():
    assert ConfigLoader.get_aoi_rs_special_decoration_factories("Z622") == ["OLED", "ARRAY"]
    assert ConfigLoader.get_aoi_rs_special_decoration_factories("M626") == ["OLED"]


@pytest.mark.parametrize("products_by_factory", [
    None, [],
    {"ARRAY": "Z622"}, {"ARRAY": None}, {"ARRAY": [False]}, {"ARRAY": ["*"]},
    {"ARRAY": [""]}, {"ARRAY": ["Z622"], " array ": "*"}, {1: "*"},
])
def test_invalid_product_allowlist_is_rejected(monkeypatch, products_by_factory):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": {
            "enabled": True, "products_by_factory": products_by_factory,
        }},
    })
    with pytest.raises(ValueError, match="products_by_factory"):
        ConfigLoader.get_aoi_rs_special_decoration_factories("Z622")


@pytest.mark.parametrize("factories", ["OLED", None, [None], [1], [""]])
def test_invalid_factory_configuration_is_rejected(monkeypatch, factories):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": {"enabled": True, "factories": factories}},
    })
    with pytest.raises(ValueError, match="factories"):
        ConfigLoader.get_aoi_rs_special_decoration_factories()


@pytest.mark.parametrize("enabled", ["false", "true", None, 0, 1])
def test_special_decoration_enabled_requires_boolean(monkeypatch, enabled):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": {"enabled": enabled}},
    })
    with pytest.raises(ValueError, match="enabled"):
        ConfigLoader.get_aoi_rs_special_decoration_factories()


def test_special_window_defaults_to_september_21_through_today(monkeypatch):
    from datetime import date
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {"aoi_rs": {}})
    assert ConfigLoader.get_aoi_rs_special_decoration_window(today=date(2026, 10, 10)) == (
        "2026-09-21", "2026-10-10",
    )


@pytest.mark.parametrize("section", [
    {"start_date": "2026-09-50"},
    {"start_date": "2026-09-23", "end_date": "2026-09-21"},
    {"end_date": None},
])
def test_invalid_special_window_is_rejected(monkeypatch, section):
    from datetime import date
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "aoi_rs": {"special_decoration": section},
    })
    with pytest.raises(ValueError, match="date"):
        ConfigLoader.get_aoi_rs_special_decoration_window(today=date(2026, 10, 10))
