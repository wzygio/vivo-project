from __future__ import annotations

from app.utils.step_labels import format_step_label


def test_format_step_label_appends_description() -> None:
    assert format_step_label("11620", {"11620": "贴膜"}) == "11620 贴膜"


def test_format_step_label_returns_bare_step_id_when_missing() -> None:
    assert format_step_label("11620", {"11630": "切割"}) == "11620"
    assert format_step_label("11620", {}) == "11620"


def test_format_step_label_returns_bare_step_id_when_map_is_none() -> None:
    assert format_step_label("11620", None) == "11620"
