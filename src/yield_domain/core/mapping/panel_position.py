import hashlib

import numpy as np

from yield_domain.core.mapping.layout import MappingLayout, resolve_mapping_layout


def _stable_panel_position_seed(panel_id: str, batch_no: str) -> int:
    seed_text = f"{panel_id}-{batch_no}".encode("utf-8")
    digest = hashlib.sha256(seed_text).digest()
    return int.from_bytes(digest[:8], byteorder="big") % (2**32 - 1)


def get_deterministically_modified_panel_id(
    panel_id: str,
    batch_no: str,
    mapping_layout: MappingLayout | dict | None = None,
) -> str:
    """Apply a reproducible small position offset to a panel id."""
    layout = resolve_mapping_layout(mapping_layout)
    coords = parse_panel_id_to_coords(panel_id, layout)
    if coords is None:
        return panel_id

    original_row, original_col = coords
    rng = np.random.default_rng(_stable_panel_position_seed(panel_id, batch_no))
    row_offset = rng.integers(-2, 3)
    col_offset = rng.integers(-2, 3)

    new_row = max(0, min(len(layout.row_labels) - 1, original_row + row_offset))
    new_col = max(
        0,
        min(len(layout.column_labels) - 1, original_col + col_offset),
    )
    if new_row == original_row and new_col == original_col:
        return panel_id

    return reconstruct_panel_id(
        panel_id,
        new_row,
        new_col,
        layout,
    )


def parse_panel_id_to_coords(
    panel_id: str,
    mapping_layout: MappingLayout | dict | None = None,
) -> tuple[int, int] | None:
    """Parse a panel id into numeric sheet row and column coordinates."""
    if not isinstance(panel_id, str) or len(panel_id) < 15:
        return None
    row_code, col_code = panel_id[11:13], panel_id[13:15]
    layout = resolve_mapping_layout(mapping_layout)
    row_map = {label: index for index, label in enumerate(layout.row_labels)}
    column_map = {
        label: index
        for index, label in enumerate(layout.column_labels)
    }
    row_index = row_map.get(row_code)
    col_map_index = column_map.get(col_code)
    if row_index is not None and col_map_index is not None:
        return row_index, col_map_index
    return None


def reconstruct_panel_id(
    original_panel_id: str,
    new_row: int,
    new_col: int,
    mapping_layout: MappingLayout | dict | None = None,
) -> str:
    """Rebuild a panel id from numeric sheet row and column coordinates."""
    layout = resolve_mapping_layout(mapping_layout)
    sheet_id = original_panel_id[:11]
    return (
        f"{sheet_id}"
        f"{layout.row_labels[new_row]}"
        f"{layout.column_labels[new_col]}"
    )
