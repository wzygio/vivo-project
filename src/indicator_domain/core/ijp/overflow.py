"""IJP overflow whitelist constants and pure panel mapping rules.

PANEL_LOCATION/PANEL_ID 规则复刻 FineReport `SERACH1` 数据集语义：
`split_part` 是 PG 专属函数，因此位置映射在此以纯 Python 实现，
仓储层只查询原始列（RS_CODE、RS_DEFECT_IMAGE_NAME）。
"""

from __future__ import annotations

IJP_EQUIPMENTS: tuple[str, ...] = (
    "3CEE01-IK2-PR1",
    "3CEE01-IK2-PR2",
    "3CEE02-IK2-PR1",
    "3CEE02-IK2-PR2",
    "3CEE04-IKT-PRT",
)
IJP_RS_CODES: tuple[str, ...] = (
    "C3DM0",
    "C3DM1",
    "C3DM2",
    "C3DM3",
    "C3DM4",
    "C3DM5",
    "C3RA1",
    "C3RA2",
    "C3RA3",
    "C3ZC1",
    "C3BH1",
    "C3BH2",
)
IJP_LINES: tuple[str, ...] = tuple(dict.fromkeys(equip[:6] for equip in IJP_EQUIPMENTS))
PANEL_LOCATIONS: tuple[str, ...] = (
    "TOP",
    "BOTTOM",
    "LEFT",
    "RIGHT",
    "LEFTTOP",
    "RIGHTTOP",
    "LEFTBOTTOM",
    "RIGHTBOTTOM",
)
IMAGE_URL_PREFIX = "http://10.73.17.41/IMG_WEB/V3/"

# SUBSTRING(RS_DEFECT_IMAGE_NAME, 57, 14) 的 0-based 等价切片。
_PANEL_ID_OFFSET = 56
_PANEL_ID_LENGTH = 14


def extract_panel_id(image_name: str | None) -> str | None:
    """Extract PANEL_ID as SUBSTRING(image_name, 57, 14); None when unavailable."""
    if not image_name:
        return None
    panel_id = image_name[_PANEL_ID_OFFSET : _PANEL_ID_OFFSET + _PANEL_ID_LENGTH]
    return panel_id or None


def build_image_url(image_name: str | None) -> str | None:
    """Prefix the defect image name with the IMG_WEB base URL."""
    if not image_name:
        return None
    return f"{IMAGE_URL_PREFIX}{image_name}"


def map_panel_location(rs_code: str | None, image_name: str | None) -> str | None:
    """Map the image-name suffix to a panel location (SERACH1 first branch).

    C3DM% codes use the last 2 characters of the 10th '/'-segment's first
    '_'-token; other whitelist codes use the last 3 characters (KONG series).
    """
    token = _location_token(image_name)
    suffix2 = token[-2:]
    suffix3 = token[-3:]
    if rs_code and rs_code.startswith("C3DM"):
        if suffix2 == "LT":
            return "LEFTTOP"
        if suffix2 == "RT":
            return "RIGHTTOP"
        if suffix2 == "RB":
            return "RIGHTBOTTOM"
        if suffix2 == "LB":
            return "LEFTBOTTOM"
        if len(suffix2) == 2 and suffix2[1].isdigit():
            head = suffix2[0]
            if head == "L":
                return "LEFT"
            if head == "T":
                return "TOP"
            if head == "R":
                return "RIGHT"
            if head == "B":
                return "BOTTOM"
        return None
    if len(suffix3) == 3 and suffix3[2].isdigit():
        head2 = suffix3[:2]
        if head2 == "HL":
            return "KONGLEFT"
        if head2 == "HT":
            return "KONGTOP"
        if head2 == "HR":
            return "KONGRIGHT"
        if head2 == "HB":
            return "KONGBOTTOM"
    return None


def map_bottom_breakout(image_name: str | None) -> str | None:
    """Return the BOTTOM0~BOTTOM9 breakout label (SERACH1 UNION ALL second branch)."""
    token = _location_token(image_name)
    suffix2 = token[-2:]
    if len(suffix2) == 2 and suffix2[0] == "B" and suffix2[1].isdigit():
        return f"BOTTOM{suffix2[1]}"
    return None


def _location_token(image_name: str | None) -> str:
    """Return split_part(split_part(image_name, '/', 10), '_', 1) equivalent."""
    if not image_name:
        return ""
    parts = image_name.split("/")
    if len(parts) < 10:
        return ""
    return parts[9].split("_", 1)[0]
