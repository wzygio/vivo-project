import pandas as pd

from app.charts.sheet_lot_chart import create_mapping_heatmap
from app.sections.yield_dashboard import _prepare_mapping_matrices
from src.shared_kernel.config import ConfigLoader
from yield_domain.core.mapping.panel_position import parse_panel_id_to_coords


Z517_MAPPING_LAYOUT = {
    "row_labels": ["1A", "1B", "1C", "1D", "2A", "2B", "2C", "2D"],
    "column_labels": [
        "A0",
        "B0",
        "C0",
        "D0",
        "E0",
        "F0",
        "G0",
        "H0",
        "J0",
        "K0",
        "L0",
    ],
}


def _mapping_data(panel_id: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "defect_group": ["Array_Line"],
            "defect_desc": ["CodeA"],
            "batch_no": ["2026/07/01"],
            "batch_total_input": [100],
            "panel_id": [panel_id],
        }
    )


def test_default_mapping_layout_remains_ten_by_nineteen() -> None:
    _, matrices, _, _ = _prepare_mapping_matrices(
        mapping_data=_mapping_data("SHEET0000012ES0"),
        curr_group="Array_Line",
        curr_code="CodeA",
        hotspot_scripts=[],
        product_code="M673",
        mapping_layout=None,
    )

    matrix = matrices["2026/07/01"]
    assert matrix.shape == (10, 19)
    assert matrix.iloc[9, 18] == 1


def test_z517_mapping_layout_keeps_2d_l0_at_last_cell() -> None:
    _, matrices, _, _ = _prepare_mapping_matrices(
        mapping_data=_mapping_data("SHEET0000012DL0"),
        curr_group="Array_Line",
        curr_code="CodeA",
        hotspot_scripts=[],
        product_code="Z517",
        mapping_layout=Z517_MAPPING_LAYOUT,
    )

    matrix = matrices["2026/07/01"]
    assert matrix.shape == (8, 11)
    assert matrix.iloc[7, 10] == 1


def test_z517_heatmap_uses_configured_axis_labels() -> None:
    figure = create_mapping_heatmap(
        pd.DataFrame(0, index=range(8), columns=range(11)),
        "Z517 Mapping",
        1,
        mapping_layout=Z517_MAPPING_LAYOUT,
    )

    assert list(figure.layout.yaxis.ticktext) == Z517_MAPPING_LAYOUT["row_labels"]
    assert list(figure.layout.xaxis.ticktext) == Z517_MAPPING_LAYOUT["column_labels"]


def test_z517_panel_position_parser_skips_i_column() -> None:
    assert parse_panel_id_to_coords(
        "SHEET0000012DL0",
        mapping_layout=Z517_MAPPING_LAYOUT,
    ) == (7, 10)


def test_z517_product_config_declares_its_mapping_layout() -> None:
    config = ConfigLoader.load_config("Z517")

    assert config.processing["mapping_layout"] == Z517_MAPPING_LAYOUT
