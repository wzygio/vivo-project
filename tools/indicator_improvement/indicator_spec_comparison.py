# -*- coding: utf-8 -*-
"""Specification comparison rules for the offline indicator-improvement tool."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Iterable, Optional


_NUMBER_PATTERN = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
_UNIT_PATTERN = r"(?:[%％]|μm|um|UM|Å|Ω|ohm|H|h|V|v)?"
_EMPTY_VALUES = {"", "/", "\\", "nan", "none", "null", "-"}


@dataclass(frozen=True)
class SpecConstraint:
    metric: str
    relation: str
    value: float
    unit: str


@dataclass(frozen=True)
class ParsedSpec:
    raw_text: str
    normalized_text: str
    constraints: tuple[SpecConstraint, ...]
    comparable: bool
    reason: str


@dataclass(frozen=True)
class SpecComparison:
    old_spec: ParsedSpec
    new_spec: ParsedSpec
    is_tightened: bool
    reason: str


def normalize_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in _EMPTY_VALUES:
        return ""
    return text


def make_spec_identity(
    factory: str,
    department: str,
    process_layer: str,
    station: str,
    monitor_factor: str,
    rs_code: str,
    description: str,
    occurrence: int,
) -> str:
    parts = [
        _identity_part(factory),
        _identity_part(department),
        _identity_part(process_layer),
        _identity_part(station),
        _identity_part(monitor_factor),
        _identity_part(rs_code),
        _identity_part(description),
    ]
    return "|".join(parts) + f"#{occurrence}"


def make_display_name(
    factory: str,
    department: str,
    monitor_factor: str,
    description: str,
    process_layer: str,
    station: str,
    rs_code: str,
    occurrence: int,
) -> str:
    fallback_parts = [
        _identity_part(factory),
        _identity_part(department),
        _identity_part(process_layer),
        _identity_part(station),
        _identity_part(monitor_factor),
        _identity_part(rs_code),
        _identity_part(description),
    ]
    return " / ".join(part for part in fallback_parts if part != "NA") + f" #{occurrence}"


def parse_monitor_spec(raw_spec: object) -> ParsedSpec:
    raw_text = normalize_cell(raw_spec)
    if raw_text == "":
        return ParsedSpec("", "", tuple(), False, "empty spec")

    normalized = _normalize_spec_text(raw_text)
    segments = _split_spec_segments(normalized)
    constraints: list[SpecConstraint] = []
    skipped_segments: list[str] = []

    for segment in segments:
        parsed = _parse_segment(segment)
        if parsed:
            constraints.extend(parsed)
        else:
            skipped_segments.append(segment)

    if not constraints:
        return ParsedSpec(raw_text, normalized, tuple(), False, "no comparable numeric constraint")

    if skipped_segments and _contains_number(" ".join(skipped_segments)):
        return ParsedSpec(raw_text, normalized, tuple(constraints), False, "contains ambiguous numeric segment")

    return ParsedSpec(raw_text, normalized, tuple(constraints), True, "parsed")


def compare_specs(old_raw: object, new_raw: object) -> SpecComparison:
    old_spec = parse_monitor_spec(old_raw)
    new_spec = parse_monitor_spec(new_raw)
    if not old_spec.comparable or not new_spec.comparable:
        return SpecComparison(old_spec, new_spec, False, "uncomparable spec")

    old_map = _constraint_map(old_spec.constraints)
    new_map = _constraint_map(new_spec.constraints)
    old_keys = set(old_map)
    new_keys = set(new_map)
    common_keys = old_keys & new_keys
    if not common_keys:
        return SpecComparison(old_spec, new_spec, False, "no common comparable constraint")

    if old_keys - new_keys:
        return SpecComparison(old_spec, new_spec, False, "new spec misses old comparable constraint")

    tightened_reasons: list[str] = []
    for key in sorted(common_keys):
        old_constraint = old_map[key]
        new_constraint = new_map[key]
        relation = old_constraint.relation
        if relation in {"upper", "tolerance"}:
            if _less_than(new_constraint.value, old_constraint.value):
                tightened_reasons.append(f"{relation}:{old_constraint.value:g}->{new_constraint.value:g}")
            elif _greater_than(new_constraint.value, old_constraint.value):
                return SpecComparison(old_spec, new_spec, False, f"{relation} loosened")
        elif relation == "lower":
            if _greater_than(new_constraint.value, old_constraint.value):
                tightened_reasons.append(f"{relation}:{old_constraint.value:g}->{new_constraint.value:g}")
            elif _less_than(new_constraint.value, old_constraint.value):
                return SpecComparison(old_spec, new_spec, False, f"{relation} loosened")

    extra_new_constraints = new_keys - old_keys
    if extra_new_constraints:
        tightened_reasons.extend(f"added:{key[0]}:{key[1]}" for key in sorted(extra_new_constraints))

    if tightened_reasons:
        return SpecComparison(old_spec, new_spec, True, "; ".join(tightened_reasons))
    return SpecComparison(old_spec, new_spec, False, "no stricter numeric constraint")


def constraints_to_text(constraints: Iterable[SpecConstraint]) -> str:
    return "; ".join(
        f"{item.metric}:{item.relation}:{item.value:g}{item.unit}"
        for item in constraints
    )


def _identity_part(value: object) -> str:
    text = normalize_cell(value)
    return text if text else "NA"


def _normalize_spec_text(text: str) -> str:
    replacements = {
        "＜": "<",
        "＞": ">",
        "≤": "<=",
        "≥": ">=",
        "±": "±",
        "－": "-",
        "—": "-",
        "～": "~",
        "：": ":",
        "，": ",",
        "％": "%",
        "μ": "μ",
        "μm": "μm",
        "um": "μm",
        "UM": "μm",
    }
    normalized = text
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\s+", " ", normalized.replace("\n", ";"))
    return normalized.strip()


def _split_spec_segments(text: str) -> list[str]:
    raw_segments = re.split(r"[;,；]", text)
    segments: list[str] = []
    for raw_segment in raw_segments:
        segment = raw_segment.strip()
        if not segment:
            continue
        if "→" in segment:
            segment = segment.split("→")[-1].strip()
        if "->" in segment:
            segment = segment.split("->")[-1].strip()
        if segment:
            segments.append(segment)
    return segments


def _parse_segment(segment: str) -> list[SpecConstraint]:
    tolerance = _parse_tolerance(segment)
    if tolerance is not None:
        return [tolerance]

    relation = _parse_relation(segment)
    if relation is not None:
        return [relation]

    range_constraints = _parse_range(segment)
    if range_constraints:
        return range_constraints

    labeled_upper = _parse_labeled_upper(segment)
    if labeled_upper is not None:
        return [labeled_upper]

    plain_upper = _parse_plain_single_number(segment)
    if plain_upper is not None:
        return [plain_upper]

    return []


def _parse_tolerance(segment: str) -> Optional[SpecConstraint]:
    match = re.search(rf"±\s*({_NUMBER_PATTERN})\s*({_UNIT_PATTERN})", segment, flags=re.IGNORECASE)
    if not match:
        return None
    return SpecConstraint("target", "tolerance", float(match.group(1)), _normalize_unit(match.group(2)))


def _parse_relation(segment: str) -> Optional[SpecConstraint]:
    match = re.search(rf"(.{{0,20}}?)(<=|>=|<|>)\s*({_NUMBER_PATTERN})\s*({_UNIT_PATTERN})", segment, flags=re.IGNORECASE)
    if not match:
        return None
    label = _metric_label(match.group(1))
    relation_token = match.group(2)
    relation = "upper" if relation_token in {"<=", "<"} else "lower"
    return SpecConstraint(label, relation, float(match.group(3)), _normalize_unit(match.group(4)))


def _parse_range(segment: str) -> list[SpecConstraint]:
    match = re.search(
        rf"({_NUMBER_PATTERN})\s*~\s*({_NUMBER_PATTERN})\s*({_UNIT_PATTERN})",
        segment,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    metric = _metric_label(segment[: match.start()])
    unit = _normalize_unit(match.group(3))
    return [
        SpecConstraint(metric, "lower", float(match.group(1)), unit),
        SpecConstraint(metric, "upper", float(match.group(2)), unit),
    ]


def _parse_labeled_upper(segment: str) -> Optional[SpecConstraint]:
    match = re.search(rf"([A-Za-z%％.]+)\s*:?\s*({_NUMBER_PATTERN})\s*({_UNIT_PATTERN})$", segment)
    if not match:
        return None
    label = _metric_label(match.group(1))
    if label == "value":
        return None
    return SpecConstraint(label, "upper", float(match.group(2)), _normalize_unit(match.group(3)))


def _parse_plain_single_number(segment: str) -> Optional[SpecConstraint]:
    stripped = segment.strip()
    match = re.fullmatch(rf"({_NUMBER_PATTERN})\s*({_UNIT_PATTERN})", stripped, flags=re.IGNORECASE)
    if not match:
        return None
    return SpecConstraint("value", "upper", float(match.group(1)), _normalize_unit(match.group(2)))


def _metric_label(prefix: str) -> str:
    label = re.sub(r"[^A-Za-z0-9_%]+", "", prefix).strip("_%")
    if not label:
        return "value"
    if label.lower() in {"target", "tar"}:
        return "target"
    if label.upper().replace(".", "") == "U":
        return "U%"
    return label


def _normalize_unit(unit: str) -> str:
    if unit in {"％"}:
        return "%"
    if unit.lower() == "um":
        return "μm"
    if unit.lower() == "h":
        return "H"
    if unit.lower() == "v":
        return "V"
    return unit


def _contains_number(text: str) -> bool:
    return re.search(_NUMBER_PATTERN, text) is not None


def _constraint_map(constraints: tuple[SpecConstraint, ...]) -> dict[tuple[str, str, str], SpecConstraint]:
    result: dict[tuple[str, str, str], SpecConstraint] = {}
    for constraint in constraints:
        key = (constraint.metric, constraint.relation, constraint.unit)
        if key in result:
            existing = result[key]
            if constraint.relation in {"upper", "tolerance"}:
                value = min(existing.value, constraint.value)
            else:
                value = max(existing.value, constraint.value)
            result[key] = SpecConstraint(constraint.metric, constraint.relation, value, constraint.unit)
        else:
            result[key] = constraint
    return result


def _less_than(left: float, right: float) -> bool:
    return left < right and not math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)


def _greater_than(left: float, right: float) -> bool:
    return left > right and not math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
