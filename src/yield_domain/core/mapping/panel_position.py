import numpy as np


def get_deterministically_modified_panel_id(panel_id: str, batch_no: str) -> str:
    """Apply a reproducible small position offset to a panel id."""
    coords = parse_panel_id_to_coords(panel_id)
    if coords is None:
        return panel_id

    original_row, original_col = coords
    seed = hash(f"{panel_id}-{batch_no}")
    np.random.seed(seed % (2**32 - 1))
    row_offset = np.random.randint(-2, 3)
    col_offset = np.random.randint(-2, 3)

    new_row = max(0, min(9, original_row + row_offset))
    new_col = max(0, min(18, original_col + col_offset))
    if new_row == original_row and new_col == original_col:
        return panel_id

    return reconstruct_panel_id(panel_id, new_row, new_col)


def parse_panel_id_to_coords(panel_id: str) -> tuple[int, int] | None:
    """Parse a panel id into numeric sheet row and column coordinates."""
    if not isinstance(panel_id, str) or len(panel_id) < 15:
        return None
    row_code, col_code = panel_id[11:13], panel_id[13:15]
    row_map = {
        '1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4,
        '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9,
    }
    row_index = row_map.get(row_code)
    col_map_index = ord(col_code[0]) - ord('A')
    if row_index is not None and 0 <= col_map_index < 19:
        return row_index, col_map_index
    return None


def reconstruct_panel_id(original_panel_id: str, new_row: int, new_col: int) -> str:
    """Rebuild a panel id from numeric sheet row and column coordinates."""
    sheet_id = original_panel_id[:11]
    row_rev_map = {
        0: '1A', 1: '1B', 2: '1C', 3: '1D', 4: '1E',
        5: '2A', 6: '2B', 7: '2C', 8: '2D', 9: '2E',
    }
    col_char = chr(ord('A') + new_col)
    return f"{sheet_id}{row_rev_map[new_row]}{col_char}0"

