from contextlib import nullcontext

import pandas as pd

from app.sections.equipment_domain import parts_filters
from app.sections.equipment_domain import parts_dashboard


def test_measurement_time_is_displayed_as_date_without_mutating_source(monkeypatch):
    frames = []
    monkeypatch.setattr(parts_dashboard.st, "dataframe", lambda df, **kwargs: frames.append(df))
    source = pd.DataFrame({"测量时间": ["2026-09-10 14:37:07", None]})
    original = source.copy()
    parts_dashboard.render_parts_table(source)
    assert frames[0]["测量时间"].tolist() == ["2026-09-10", ""]
    pd.testing.assert_frame_equal(source, original)


def _report_df() -> pd.DataFrame:
    return pd.DataFrame({
        "厂别": ["Array", "Array", "Array", "TP"],
        "设备类型": ["PVD", "PVD", "ETCH", "CVD"],
        "备件类型": ["Target", "Mask", "Shield", "Target"],
        "膜层": ["MO", "TI", "ITO", "ITO"],
    })


def test_parts_filters_are_multi_select_and_empty_by_default(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    selections = {
        "厂别": ["Array"],
        "设备类型": ["PVD"],
        "备件类型": ["Target"],
    }

    def fake_multiselect(label, options, **kwargs):
        calls.append({"label": label, "options": options, **kwargs})
        return selections[label]

    monkeypatch.setattr(parts_filters.st, "columns", lambda *_args, **_kwargs: [
        nullcontext(),
        nullcontext(),
        nullcontext(),
    ])
    monkeypatch.setattr(parts_filters.st, "multiselect", fake_multiselect)

    selected = parts_filters.render_parts_filters(_report_df())

    assert selected == (["Array"], ["PVD"], ["Target"])
    assert [call["label"] for call in calls] == ["厂别", "设备类型", "备件类型"]
    assert all(call["default"] == [] for call in calls)
    assert calls[0]["options"] == ["Array", "TP"]
    assert calls[1]["options"] == ["ETCH", "PVD"]
    assert calls[2]["options"] == ["Mask", "Target"]


def test_cascading_filter_options_follow_factory_then_equipment() -> None:
    factories, equipment_types, part_types = (
        parts_filters.build_cascading_filter_options(
            _report_df(),
            selected_factories=["Array"],
            selected_equipment_types=["PVD"],
        )
    )

    assert factories == ["Array", "TP"]
    assert equipment_types == ["ETCH", "PVD"]
    assert part_types == ["Mask", "Target"]


def test_parts_filters_apply_all_selected_dimensions() -> None:
    filtered = parts_filters.apply_parts_filters(
        _report_df(),
        selected_factories=["Array"],
        selected_equipment_types=["PVD"],
        selected_part_types=["Target"],
    )

    assert filtered.index.tolist() == [0]


def test_empty_parts_filter_selection_keeps_all_rows() -> None:
    report_df = _report_df()

    filtered = parts_filters.apply_parts_filters(
        report_df,
        selected_factories=[],
        selected_equipment_types=[],
        selected_part_types=[],
    )

    pd.testing.assert_frame_equal(filtered, report_df)
    assert filtered is not report_df


def test_trend_row_requires_an_explicit_table_selection() -> None:
    report_df = _report_df()

    assert parts_filters.get_selected_parts_row(
        report_df,
        {"selection": {"rows": []}},
    ) is None

    selected = parts_filters.get_selected_parts_row(
        report_df,
        {"selection": {"rows": [1]}},
    )
    assert selected is not None
    assert selected["备件类型"] == "Mask"


def test_parts_tables_never_expose_parameter_columns(monkeypatch) -> None:
    captured_column_orders: list[list[str]] = []
    captured_frames: list[pd.DataFrame] = []

    def fake_dataframe(_df, **kwargs):
        captured_frames.append(_df.copy())
        captured_column_orders.append(list(kwargs["column_order"]))
        return {"selection": {"rows": []}}

    monkeypatch.setattr(parts_dashboard.st, "dataframe", fake_dataframe)
    report_df = pd.DataFrame({
        "厂别": ["Array"],
        "备件类型": ["Target"],
        "设备类型": ["PVD"],
        "膜层": ["MO"],
        "制程": ["DEPO"],
        "寿命规格": [41000.0],
        "站点": ["1K200"],
        "机台号-腔室": ["3AFS01-SPU-PM5"],
        "参数名称": ["%TRGTLIFE%_G_MAX"],
        "匹配参数名": ["P5_TRGTLIFE_G_MAX"],
        "测量值": [30000.0],
        "原始测量值": [45000.0],
        "数据修饰": ["超规修饰"],
        "使用进度": [73.17],
        "预警状态": ["正常"],
        "测量时间": ["2026-07-15 08:30:00"],
    })

    parts_dashboard.render_parts_table(report_df)
    parts_dashboard.render_parts_table_selectable(report_df)

    assert "参数名称" not in parts_dashboard.PARTS_TABLE_COLUMN_ORDER
    assert "匹配参数名" not in parts_dashboard.PARTS_TABLE_COLUMN_ORDER
    assert len(captured_column_orders) == 2
    for column_order in captured_column_orders:
        assert "参数名称" not in column_order
        assert "匹配参数名" not in column_order
        assert all(not column.startswith("__FABRICATED_PART__") for column in column_order)
    for frame in captured_frames:
        assert set(frame.columns) == set(parts_dashboard.PARTS_TABLE_COLUMN_ORDER)
        assert frame["测量值"].tolist() == [30000.0]
    assert report_df["原始测量值"].tolist() == [45000.0]
    assert report_df["数据修饰"].tolist() == ["超规修饰"]


def test_station_display_preserves_codes_and_removes_excel_decimal_suffix(monkeypatch) -> None:
    captured = []
    monkeypatch.setattr(
        parts_dashboard.st, "dataframe", lambda frame, **kwargs: captured.append(frame)
    )
    source = pd.DataFrame({"站点": [12200.0, "18200.0", "00120.00", "1K500", "13500.0 43500.0", None]})
    original = source.copy(deep=True)

    parts_dashboard.render_parts_table(source)

    assert captured[0]["站点"].iloc[:5].tolist() == [
        "12200", "18200", "00120", "1K500", "13500 43500",
    ]
    assert pd.isna(captured[0]["站点"].iloc[5])
    pd.testing.assert_frame_equal(source, original)
