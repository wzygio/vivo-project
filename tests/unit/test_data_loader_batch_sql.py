import pandas as pd

from src.yield_domain.infrastructure import data_loader


class DummyDbManager:
    def __init__(self) -> None:
        self.engine = object()


def test_load_panel_details_uses_productrequest_description_for_batch_no(monkeypatch) -> None:
    captured_sql = {}

    def fake_read_sql(sql_query, engine) -> pd.DataFrame:
        captured_sql["text"] = str(sql_query)
        return pd.DataFrame(
            [
                {
                    "batch_no": "2025/11/14",
                    "lot_id": "L3MR5A0B0",
                    "sheet_id": "L3MR5A0B001",
                    "panel_id": "L3MR5A0B00101",
                    "warehousing_time": "20251114",
                    "prod_code": "M660",
                    "defect_code": None,
                    "defect_desc": None,
                    "defect_group": None,
                }
            ]
        )

    monkeypatch.setattr(data_loader.pd, "read_sql", fake_read_sql)

    result = data_loader.load_panel_details(
        db_manager=DummyDbManager(),
        start_date="2025-11-14",
        end_date="2025-11-14",
        prod_code="M660",
        work_order_types=["ESLC", "P"],
    )

    sql_text = captured_sql["text"]
    normalized_sql = " ".join(sql_text.lower().split())

    assert result.loc[0, "batch_no"] == "2025/11/14"
    assert "spot_glass_batch_info" not in normalized_sql
    assert "dwt_yield_result_pnl" in normalized_sql
    assert "dwr_mes_productrequest" in normalized_sql
    assert "to_char(to_date(substring(r.description from '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}'), 'yyyy/mm/dd'), 'yyyy/mm/dd') as batch_no" in normalized_sql
    assert "substr(r.description, 1, 10) as batch_no" not in normalized_sql
    assert "d.sub_prod_id = r.productrequestname" in normalized_sql
    assert "d.oper_group = 'ct'" in normalized_sql
