from dataclasses import dataclass
from typing import Any, Mapping


DEFAULT_MAPPING_ROW_LABELS = (
    "1A",
    "1B",
    "1C",
    "1D",
    "1E",
    "2A",
    "2B",
    "2C",
    "2D",
    "2E",
)
DEFAULT_MAPPING_COLUMN_LABELS = tuple(
    f"{chr(ord('A') + index)}0"
    for index in range(19)
)


@dataclass(frozen=True)
class MappingLayout:
    row_labels: tuple[str, ...]
    column_labels: tuple[str, ...]

    @property
    def shape(self) -> tuple[int, int]:
        return len(self.row_labels), len(self.column_labels)


DEFAULT_MAPPING_LAYOUT = MappingLayout(
    row_labels=DEFAULT_MAPPING_ROW_LABELS,
    column_labels=DEFAULT_MAPPING_COLUMN_LABELS,
)


def resolve_mapping_layout(
    config: Mapping[str, Any] | MappingLayout | None,
) -> MappingLayout:
    """Return a validated product Mapping layout or the standard 10×19 layout."""
    if isinstance(config, MappingLayout):
        return config
    if not isinstance(config, Mapping):
        return DEFAULT_MAPPING_LAYOUT

    row_labels = _normalize_labels(config.get("row_labels"))
    column_labels = _normalize_labels(config.get("column_labels"))
    if row_labels is None or column_labels is None:
        return DEFAULT_MAPPING_LAYOUT

    return MappingLayout(
        row_labels=row_labels,
        column_labels=column_labels,
    )


def _normalize_labels(values: Any) -> tuple[str, ...] | None:
    if not isinstance(values, (list, tuple)) or not values:
        return None

    labels = tuple(str(value).strip().upper() for value in values)
    if any(not label for label in labels) or len(set(labels)) != len(labels):
        return None
    return labels
