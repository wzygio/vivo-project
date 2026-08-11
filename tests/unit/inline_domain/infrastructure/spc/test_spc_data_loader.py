import pandas as pd

from src.inline_domain.infrastructure.spc import data_loader


class DummyDbManager:
    def __init__(self) -> None:
        self.engine = object()


def test_load_spc_measurements_keeps_mt_ch_spc_parameters(monkeypatch) -> None:
    captured_sql: dict[str, str] = {}

    def fake_read_sql(sql_query, engine, **_kwargs) -> pd.DataFrame:
        captured_sql["text"] = str(sql_query)
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": "M626",
                    "sheet_start_time": "2026-06-02 09:00:00",
                    "sheet_id": "S1",
                    "step_id": "10140",
                    "param_name": "PPA_B_X",
                    "site_name": "P1",
                    "unit_id": "3CEE02-PPA",
                    "param_value": "1.23",
                }
                ,
                {
                    "factory": "ARRAY",
                    "prod_code": "M626",
                    "sheet_start_time": "2026-06-02 09:00:00",
                    "sheet_id": "S2",
                    "step_id": "10140",
                    "param_name": "AVG_LOSS_RATE",
                    "site_name": "P1",
                    "unit_id": "3CEE02-PPA",
                    "param_value": "2.34",
                },
                {
                    "factory": "ARRAY",
                    "prod_code": "M626",
                    "sheet_start_time": "2026-06-02 09:00:00",
                    "sheet_id": "S3",
                    "step_id": "10140",
                    "param_name": "MT_CH_PRESS_A",
                    "site_name": "P1",
                    "unit_id": "3CEE02-PPA",
                    "param_value": "3.45",
                },
            ]
        )

    monkeypatch.setattr(data_loader.pd, "read_sql", fake_read_sql)

    result = data_loader.load_spc_measurements(
        db_manager=DummyDbManager(),
        start_str="2026-06-01",
        end_str="2026-06-30",
        prod_code="M626",
    )

    normalized_sql = " ".join(captured_sql["text"].lower().split())
    assert "t.unit_id" in normalized_sql
    assert "upper(t.param_name) not like '%loss%'" in normalized_sql
    assert "upper(t.param_name) not like '%mt_ch" not in normalized_sql
    assert "unit_id" in result.columns
    assert result.loc[0, "unit_id"] == "3CEE02-PPA"
    assert result["param_name"].tolist() == ["PPA_B_X", "MT_CH_PRESS_A"]


def test_load_spc_spec_limits_returns_normalized_main_process_route(monkeypatch) -> None:
    captured_sql: dict[str, str] = {}

    def fake_read_sql(sql_query, engine, **_kwargs) -> pd.DataFrame:
        captured_sql["text"] = str(sql_query)
        return pd.DataFrame(
            [
                {
                    "prod_code": "M626",
                    "step_id": "15260",
                    "param_name": "4PP_Rs",
                    "usl": "1.5",
                    "lsl": "0.5",
                    "ucl": "1.3",
                    "lcl": "0.7",
                    "main_step_id": "15100",
                    "main_eqp_type": "CHAMBER",
                },
                {
                    "prod_code": "M626",
                    "step_id": "15270",
                    "param_name": "CD",
                    "usl": "2.0",
                    "lsl": None,
                    "ucl": None,
                    "lcl": None,
                    "main_step_id": None,
                    "main_eqp_type": " ",
                },
            ]
        )

    monkeypatch.setattr(data_loader.pd, "read_sql", fake_read_sql)

    result = data_loader.load_spc_spec_limits(DummyDbManager(), "M626")

    normalized_sql = " ".join(captured_sql["text"].lower().split())
    assert "main_step_id" in normalized_sql
    assert "main_eqp_type" in normalized_sql
    assert result[["main_step_id", "main_eqp_type"]].to_dict("records") == [
        {"main_step_id": "15100", "main_eqp_type": "CHAMBER"},
        {"main_step_id": "15270", "main_eqp_type": "EQP"},
    ]
