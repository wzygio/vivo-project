import pandas as pd

import app.sections.yield_dashboard as yield_dashboard
from app.charts.sheet_lot_chart import create_mapping_heatmap
from app.sections.yield_dashboard import _prepare_mapping_matrices
from src.shared_kernel.config import ConfigLoader
from yield_domain.core.mapping.layout import (
    MappingLayout,
    resolve_mapping_layout,
)
from yield_domain.core.mapping.panel_position import (
    _stable_panel_position_seed,
    parse_panel_id_to_coords,
)


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


def test_resolving_an_existing_mapping_layout_is_idempotent() -> None:
    layout = resolve_mapping_layout(Z517_MAPPING_LAYOUT)

    assert isinstance(layout, MappingLayout)
    assert resolve_mapping_layout(layout) is layout


def test_panel_position_seed_does_not_depend_on_python_hash_randomization() -> None:
    first_seed = _stable_panel_position_seed("SHEET0000012DL0", "2026/07/01")
    second_seed = _stable_panel_position_seed("SHEET0000012DL0", "2026/07/01")

    assert first_seed == second_seed


def test_compact_mapping_section_passes_resolved_layout_to_heatmap(monkeypatch) -> None:
    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    captured: dict[str, object] = {}
    monkeypatch.setattr(yield_dashboard.st, "markdown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(yield_dashboard.st, "warning", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        yield_dashboard.st,
        "tabs",
        lambda labels, **_kwargs: [_Tab() for _ in labels],
    )
    monkeypatch.setattr(yield_dashboard.st, "plotly_chart", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        yield_dashboard,
        "create_mapping_heatmap",
        lambda *_args, mapping_layout=None, **_kwargs: captured.setdefault(
            "mapping_layout", mapping_layout
        ),
    )
    monkeypatch.setattr(
        yield_dashboard,
        "_apply_compact_chart_layout",
        lambda figure, _height: figure,
    )

    yield_dashboard._render_compact_mapping_section(
        mapping_data=_mapping_data("SHEET0000012DL0"),
        curr_group="Array_Line",
        curr_code="CodeA",
        hotspot_scripts=[],
        product_code="Z517",
        mapping_layout=Z517_MAPPING_LAYOUT,
    )

    assert isinstance(captured["mapping_layout"], MappingLayout)
