from datetime import date

import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize("section, expected", [
    ({}, []), ({"enabled": False}, []),
    ({"enabled": True}, ["OLED"]),
    ({"enabled": True, "factories": []}, []),
    ({"enabled": True, "factories": [" oled ", "TP", "OLED"]}, ["OLED", "TP"]),
])
def test_tt_special_factory_scope(monkeypatch, section, expected):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "aoi_tt": {"special_decoration": section},
        "aoi_rs": {"special_decoration": {"sheet_threshold_ratio": 0.9}},
    })
    assert ConfigLoader.get_aoi_tt_special_decoration_factories() == expected
    assert ConfigLoader.get_aoi_tt_special_decoration_sheet_ratios() == (0.5, 0.25)
    assert ConfigLoader.get_aoi_tt_special_decoration_window(today=date(2026, 10, 21)) == (
        "2026-09-21", "2026-10-21",
    )


def test_tt_special_custom_ratios_and_dates(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "aoi_tt": {"special_decoration": {
            "sheet_threshold_ratio": 0.8, "sheet_replacement_ratio": 0.1,
            "start_date": "2026-10-01", "end_date": "2026-10-10",
        }},
    })
    assert ConfigLoader.get_aoi_tt_special_decoration_sheet_ratios() == (0.8, 0.1)
    assert ConfigLoader.get_aoi_tt_special_decoration_window() == ("2026-10-01", "2026-10-10")


@pytest.mark.parametrize("key", ["sheet_threshold_ratio", "sheet_replacement_ratio"])
@pytest.mark.parametrize("value", [True, None, "0.5", -1, float("inf"), float("nan")])
def test_invalid_tt_ratios_fail_explicitly(monkeypatch, key, value):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _: {
        "aoi_tt": {"special_decoration": {key: value}},
    })
    with pytest.raises(ValueError, match=f"aoi_tt.special_decoration.{key}"):
        ConfigLoader.get_aoi_tt_special_decoration_sheet_ratios()
