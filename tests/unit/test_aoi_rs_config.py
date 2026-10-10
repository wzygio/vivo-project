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


def test_special_window_defaults_to_september_21_through_today(monkeypatch):
    from datetime import date
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {"aoi_rs": {}})
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
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "aoi_rs": {"special_decoration": section},
    })
    with pytest.raises(ValueError, match="date"):
        ConfigLoader.get_aoi_rs_special_decoration_window(today=date(2026, 10, 10))
