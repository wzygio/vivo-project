import pandas as pd

from app.sections import parts_dashboard


def test_parts_tables_never_expose_parameter_columns(monkeypatch) -> None:
    captured_column_orders: list[list[str]] = []

    def fake_dataframe(_df, **kwargs):
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
