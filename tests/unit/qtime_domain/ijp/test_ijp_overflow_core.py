from src.qtime_domain.core.ijp_overflow import (
    IMAGE_URL_PREFIX,
    IJP_EQUIPMENTS,
    IJP_LINES,
    IJP_RS_CODES,
    PANEL_LOCATIONS,
    build_image_url,
    extract_panel_id,
    map_bottom_breakout,
    map_panel_location,
)

SAMPLE_IMAGE = (
    "C/VIEW/2W4A9/3CTV01/L3N4/64/E03/SOURCE/L3N464E03182.IMG/"
    "L3N464E03182CARB0_2W400_68_-372708_-411245_PT_20260803_030008_FVG_C3DM1_RS.JPG"
)


def image_with_suffix(suffix: str) -> str:
    return (
        "C/VIEW/2W4A9/3CTV01/L3N4/64/E03/SOURCE/L3N464E03182.IMG/"
        f"L3N464E03182CA{suffix}_2W400_68_PT_20260803_030008_FVG_C3DM1_RS.JPG"
    )


def test_whitelists_match_the_finereport_contract() -> None:
    assert IJP_EQUIPMENTS == (
        "3CEE01-IK2-PR1",
        "3CEE01-IK2-PR2",
        "3CEE02-IK2-PR1",
        "3CEE02-IK2-PR2",
        "3CEE04-IKT-PRT",
    )
    assert IJP_RS_CODES == (
        "C3DM0", "C3DM1", "C3DM2", "C3DM3", "C3DM4", "C3DM5",
        "C3RA1", "C3RA2", "C3RA3", "C3ZC1", "C3BH1", "C3BH2",
    )
    assert IJP_LINES == ("3CEE01", "3CEE02", "3CEE04")
    assert PANEL_LOCATIONS == (
        "TOP", "BOTTOM", "LEFT", "RIGHT",
        "LEFTTOP", "RIGHTTOP", "LEFTBOTTOM", "RIGHTBOTTOM",
    )


def test_extract_panel_id_uses_the_one_based_substring_57_14_contract() -> None:
    assert extract_panel_id(SAMPLE_IMAGE) == SAMPLE_IMAGE[56:70]
    assert len(extract_panel_id(SAMPLE_IMAGE)) == 14
    assert extract_panel_id("") is None
    assert extract_panel_id(None) is None
    assert extract_panel_id("SHORT") is None


def test_build_image_url_prefixes_the_image_name() -> None:
    assert build_image_url(SAMPLE_IMAGE) == IMAGE_URL_PREFIX + SAMPLE_IMAGE
    assert build_image_url("") is None


def test_panel_location_maps_the_c3dm_suffix_contract() -> None:
    cases = {
        "LT": "LEFTTOP",
        "L0": "LEFT",
        "L9": "LEFT",
        "T0": "TOP",
        "T7": "TOP",
        "RT": "RIGHTTOP",
        "R0": "RIGHT",
        "R9": "RIGHT",
        "RB": "RIGHTBOTTOM",
        "LB": "LEFTBOTTOM",
        "B0": "BOTTOM",
        "B9": "BOTTOM",
    }
    for suffix, expected in cases.items():
        assert map_panel_location("C3DM1", image_with_suffix(suffix)) == expected, suffix


def test_panel_location_maps_non_c3dm_codes_to_the_kong_series() -> None:
    cases = {
        "HL0": "KONGLEFT",
        "HL9": "KONGLEFT",
        "HT0": "KONGTOP",
        "HT5": "KONGTOP",
        "HR1": "KONGRIGHT",
        "HB8": "KONGBOTTOM",
    }
    for suffix, expected in cases.items():
        assert map_panel_location("C3RA1", image_with_suffix(suffix)) == expected, suffix


def test_panel_location_returns_none_for_unmappable_names() -> None:
    assert map_panel_location("C3DM1", image_with_suffix("XX")) is None
    assert map_panel_location("C3RA1", image_with_suffix("B0")) is None
    assert map_panel_location("C3DM1", "") is None
    assert map_panel_location("C3DM1", None) is None
    assert map_panel_location("C3DM1", "no/slashes") is None


def test_bottom_breakout_expands_b0_to_b9_into_bottom0_to_bottom9() -> None:
    assert map_bottom_breakout(image_with_suffix("B0")) == "BOTTOM0"
    assert map_bottom_breakout(image_with_suffix("B7")) == "BOTTOM7"
    assert map_bottom_breakout(image_with_suffix("T0")) is None
    assert map_bottom_breakout(image_with_suffix("LT")) is None
    assert map_bottom_breakout("") is None
