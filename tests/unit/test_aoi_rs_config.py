import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize("section, expected", [
    ({}, ["OLED"]),
    ({"special_decoration": {}}, ["OLED"]),
    ({"special_decoration": {"factories": []}}, []),
    ({"special_decoration": {"factories": [" OLED ", "array", "OLED"]}}, ["OLED", "ARRAY"]),
])
def test_special_decoration_factories(monkeypatch, section, expected):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {"aoi_rs": section})
    assert ConfigLoader.get_aoi_rs_special_decoration_factories() == expected


@pytest.mark.parametrize("factories", ["OLED", None, [None], [1], [""]])
def test_invalid_factory_configuration_is_rejected(monkeypatch, factories):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "aoi_rs": {"special_decoration": {"factories": factories}},
    })
    with pytest.raises(ValueError, match="factories"):
        ConfigLoader.get_aoi_rs_special_decoration_factories()
