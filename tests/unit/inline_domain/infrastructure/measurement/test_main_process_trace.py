import pandas as pd

from src.inline_domain.infrastructure.shared.main_process_trace import (
    apply_main_process_history,
    attach_main_process_spec,
)
from src.inline_domain.infrastructure.shared.main_process_history_repository import (
    InlineMainProcessHistoryRepository,
)


class DummyDbManager:
    def __init__(self) -> None:
        self.engine = object()


def test_apply_main_process_history_selects_nearest_prior_out_without_row_expansion() -> None:
    measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "sheet_id": "S1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "site_name": "P1",
                "sheet_start_time": "2026-08-10 10:00:00",
                "unit_id": "MEASURE-EQP",
                "main_step_id": "15100",
                "main_eqp_type": "CHAMBER",
                "param_value": 1.0,
            },
            {
                "factory": "ARRAY",
                "sheet_id": "S1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "site_name": "P2",
                "sheet_start_time": "2026-08-10 10:00:00",
                "unit_id": "MEASURE-EQP",
                "main_step_id": "15100",
                "main_eqp_type": "CHAMBER",
                "param_value": 1.1,
            },
        ]
    )
    history = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "main_eqp_type": "CHAMBER",
                "sheet_id": "S1",
                "main_step_id": "15100",
                "main_process_unit_id": "OLD-CHAMBER",
                "main_process_event_time": "2026-08-09 08:00:00",
                "main_process_trace_source": "array_sub_unit_sht",
                "source_rank": 1,
            },
            {
                "factory": "ARRAY",
                "main_eqp_type": "CHAMBER",
                "sheet_id": "S1",
                "main_step_id": "15100",
                "main_process_unit_id": "NEAREST-CHAMBER",
                "main_process_event_time": "2026-08-10 09:30:00",
                "main_process_trace_source": "array_sub_unit_sht",
                "source_rank": 1,
            },
            {
                "factory": "ARRAY",
                "main_eqp_type": "CHAMBER",
                "sheet_id": "S1",
                "main_step_id": "15100",
                "main_process_unit_id": "FUTURE-CHAMBER",
                "main_process_event_time": "2026-08-10 10:30:00",
                "main_process_trace_source": "array_sub_unit_sht",
                "source_rank": 1,
            },
        ]
    )

    result = apply_main_process_history(measurements, history)

    assert len(result) == len(measurements)
    assert result["site_name"].tolist() == ["P1", "P2"]
    assert result["main_process_unit_id"].tolist() == [
        "NEAREST-CHAMBER",
        "NEAREST-CHAMBER",
    ]
    assert result["main_process_event_time"].tolist() == [
        pd.Timestamp("2026-08-10 09:30:00"),
        pd.Timestamp("2026-08-10 09:30:00"),
    ]


def test_apply_main_process_history_uses_route_specific_missing_history_fallbacks() -> None:
    measurements = pd.DataFrame(
        [
            {
                "factory": "OLED",
                "sheet_id": "G1",
                "sheet_start_time": "2026-08-10 10:00:00",
                "unit_id": "MEASURE-EQP",
                "main_step_id": "21200",
                "main_eqp_type": "EQP",
            },
            {
                "factory": "OLED",
                "sheet_id": "G2",
                "sheet_start_time": "2026-08-10 10:00:00",
                "unit_id": "MEASURE-CHAMBER-LIKE-VALUE",
                "main_step_id": "21200-CVD",
                "main_eqp_type": "CHAMBER",
            },
        ]
    )

    result = apply_main_process_history(measurements, pd.DataFrame())

    assert result["main_process_unit_id"].tolist() == ["MEASURE-EQP", "UNKNOWN"]
    assert result["main_process_trace_source"].tolist() == [
        "measurement_unit_fallback",
        "unmatched_chamber",
    ]
    assert result["main_process_event_time"].isna().all()


def test_attach_main_process_spec_uses_three_key_route_and_defaults_missing_spec() -> None:
    measurements = pd.DataFrame(
        [
            {
                "prod_code": "M626",
                "factory": "ARRAY",
                "sheet_id": "S1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
            },
            {
                "prod_code": "M626",
                "factory": "ARRAY",
                "sheet_id": "S2",
                "step_id": "15270",
                "param_name": "CD",
            },
        ]
    )
    specifications = pd.DataFrame(
        [
            {
                "prod_code": "M626",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "main_step_id": "15100",
                "main_eqp_type": "CHAMBER",
            }
        ]
    )

    result = attach_main_process_spec(measurements, specifications)

    assert len(result) == len(measurements)
    assert result[["main_step_id", "main_eqp_type"]].to_dict("records") == [
        {"main_step_id": "15100", "main_eqp_type": "CHAMBER"},
        {"main_step_id": "15270", "main_eqp_type": "EQP"},
    ]


def test_load_main_process_history_routes_array_eqp_to_sheet_out_history(monkeypatch) -> None:
    captured: dict[str, object] = {}
    routed_measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "sheet_id": "S1",
                "main_step_id": "15100",
                "main_eqp_type": "EQP",
            }
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        captured["sql"] = " ".join(str(sql_query).lower().split())
        captured["params"] = params
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "main_eqp_type": "EQP",
                    "sheet_id": "S1",
                    "main_step_id": "15100",
                    "main_process_unit_id": "ARRAY-EQP-01",
                    "event_timekey": "20260806093000123456",
                    "main_process_trace_source": "array_sht",
                    "source_rank": 1,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = InlineMainProcessHistoryRepository(DummyDbManager()).get_main_process_history(
        routed_measurements,
        history_start="2026-07-01",
        history_end="2026-08-10",
    )

    assert "mdw.dwt_inout_sht" in captured["sql"]
    assert "inout_type = 'out'" in captured["sql"]
    assert captured["params"] == {
        "history_start": "20260627",
        "history_end": "20260806",
        "material_ids": ["S1"],
        "main_step_ids": ["15100"],
    }
    assert result.loc[0, "main_process_unit_id"] == "ARRAY-EQP-01"
    assert result.loc[0, "main_process_event_time"] == pd.Timestamp("2026-08-10 09:30:00")


def test_load_main_process_history_routes_array_chamber_with_sub_unit_priority(monkeypatch) -> None:
    captured_sql: list[str] = []
    routed_measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "sheet_id": "S1",
                "main_step_id": "15100",
                "main_eqp_type": "CHAMBER",
            }
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        captured_sql.append(" ".join(str(sql_query).lower().split()))
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "main_eqp_type": "CHAMBER",
                    "sheet_id": "S1",
                    "main_step_id": "15100",
                    "main_process_unit_id": "ARRAY-CVD-CH01",
                    "event_timekey": "20260806093000123456",
                    "main_process_trace_source": "array_sub_unit_sht",
                    "source_rank": 1,
                },
                {
                    "factory": "ARRAY",
                    "main_eqp_type": "CHAMBER",
                    "sheet_id": "S1",
                    "main_step_id": "15100",
                    "main_process_unit_id": "ARRAY-CH02",
                    "event_timekey": "20260810094000123456",
                    "main_process_trace_source": "array_unit_sht",
                    "source_rank": 2,
                },
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = InlineMainProcessHistoryRepository(DummyDbManager()).get_main_process_history(
        routed_measurements, "2026-07-01", "2026-08-10"
    )

    assert len(captured_sql) == 1
    assert "mdw.dwt_inout_sub_unit_sht" in captured_sql[0]
    assert "mdw.dwt_inout_unit_sht" in captured_sql[0]
    assert "split_part(sub_unit_id, '-', 2)" in captured_sql[0]
    assert result[["main_process_trace_source", "source_rank"]].to_dict("records") == [
        {"main_process_trace_source": "array_sub_unit_sht", "source_rank": 1},
        {"main_process_trace_source": "array_unit_sht", "source_rank": 2},
    ]


def test_load_main_process_history_routes_oled_and_tp_eqp_to_glass_history(monkeypatch) -> None:
    captured_sql: list[str] = []
    routed_measurements = pd.DataFrame(
        [
            {
                "factory": "OLED",
                "sheet_id": "G1",
                "main_step_id": "21200",
                "main_eqp_type": "EQP",
            },
            {
                "factory": "TP",
                "sheet_id": "G2",
                "main_step_id": "41100",
                "main_eqp_type": "EQP",
            },
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        normalized_sql = " ".join(str(sql_query).lower().split())
        captured_sql.append(normalized_sql)
        factory = "OLED" if "'oled' as factory" in normalized_sql else "TP"
        material_id = params["material_ids"][0]
        main_step_id = params["main_step_ids"][0]
        return pd.DataFrame(
            [
                {
                    "factory": factory,
                    "main_eqp_type": "EQP",
                    "sheet_id": material_id,
                    "main_step_id": main_step_id,
                    "main_process_unit_id": f"{factory}-EQP-01",
                    "event_timekey": "20260810093000123456",
                    "main_process_trace_source": f"{factory.lower()}_gls",
                    "source_rank": 1,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = InlineMainProcessHistoryRepository(DummyDbManager()).get_main_process_history(
        routed_measurements, "2026-07-01", "2026-08-10"
    )

    assert len(captured_sql) == 2
    assert all("mdw.dwt_inout_gls" in sql for sql in captured_sql)
    assert any("factory like 'oled%'" in sql for sql in captured_sql)
    assert any("factory = 'tp'" in sql for sql in captured_sql)
    assert set(result["factory"]) == {"OLED", "TP"}
    assert set(result["main_process_unit_id"]) == {"OLED-EQP-01", "TP-EQP-01"}


def test_load_main_process_history_routes_tp_chamber_to_glass_sub_unit(monkeypatch) -> None:
    captured_sql: list[str] = []
    routed_measurements = pd.DataFrame(
        [
            {
                "factory": "TP",
                "sheet_id": "G2",
                "main_step_id": "41100",
                "main_eqp_type": "CHAMBER",
            }
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        captured_sql.append(" ".join(str(sql_query).lower().split()))
        return pd.DataFrame(
            [
                {
                    "factory": "TP",
                    "main_eqp_type": "CHAMBER",
                    "sheet_id": "G2",
                    "main_step_id": "41100",
                    "main_process_unit_id": "TP-CVD-CH01",
                    "event_timekey": "20260810093000123456",
                    "main_process_trace_source": "tp_sub_unit_gls",
                    "source_rank": 1,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = InlineMainProcessHistoryRepository(DummyDbManager()).get_main_process_history(
        routed_measurements, "2026-07-01", "2026-08-10"
    )

    assert len(captured_sql) == 1
    assert "mdw.dwt_inout_sub_unit_gls" in captured_sql[0]
    assert "factory = 'tp'" in captured_sql[0]
    assert "split_part(sub_unit_id, '-', 2)" in captured_sql[0]
    assert result.loc[0, "main_process_unit_id"] == "TP-CVD-CH01"


def test_load_main_process_history_normalizes_oled_cvd_chamber_route(monkeypatch) -> None:
    captured_sql: list[str] = []
    routed_measurements = pd.DataFrame(
        [
            {
                "factory": "OLED",
                "sheet_id": "G1",
                "main_step_id": "21200-CVD",
                "main_eqp_type": "CHAMBER",
            }
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        captured_sql.append(" ".join(str(sql_query).lower().split()))
        return pd.DataFrame(
            [
                {
                    "factory": "OLED",
                    "main_eqp_type": "CHAMBER",
                    "sheet_id": "G1",
                    "main_step_id": "21200-CVD",
                    "main_process_unit_id": "OLED-CVD-CH01",
                    "event_timekey": "20260810093000123456",
                    "main_process_trace_source": "oled_sub_unit_gls",
                    "source_rank": 1,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = InlineMainProcessHistoryRepository(DummyDbManager()).get_main_process_history(
        routed_measurements, "2026-07-01", "2026-08-10"
    )

    assert len(captured_sql) == 1
    assert "mdw.dwt_inout_sub_unit_gls" in captured_sql[0]
    assert "mdw.dwd_mes_oled_oper_layer_v" in captured_sql[0]
    assert "21200-cvd1" in captured_sql[0]
    assert "21200-cvd" in captured_sql[0]
    assert result.loc[0, "main_step_id"] == "21200-CVD"


def test_enrich_measurements_with_main_process_trace_returns_routed_point_payload(monkeypatch) -> None:
    measurements = pd.DataFrame(
        [
            {
                "prod_code": "M626",
                "factory": "ARRAY",
                "sheet_id": "S1",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "site_name": "P1",
                "sheet_start_time": "2026-08-10 10:00:00",
                "unit_id": "MEASURE-EQP",
                "param_value": 1.0,
            }
        ]
    )
    specifications = pd.DataFrame(
        [
            {
                "prod_code": "M626",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "main_step_id": "15100",
                "main_eqp_type": "EQP",
            }
        ]
    )

    def fake_read_sql(sql_query, engine, params) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "main_eqp_type": "EQP",
                    "sheet_id": "S1",
                    "main_step_id": "15100",
                    "main_process_unit_id": "MAIN-EQP-01",
                    "event_timekey": "20260806093000123456",
                    "main_process_trace_source": "array_sht",
                    "source_rank": 1,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    routed = attach_main_process_spec(measurements, specifications)
    history = InlineMainProcessHistoryRepository(
        DummyDbManager()
    ).get_main_process_history(
        routed,
        history_start="2026-07-01",
        history_end="2026-08-10",
    )
    result = apply_main_process_history(routed, history)

    assert len(result) == 1
    assert result.loc[0, "main_step_id"] == "15100"
    assert result.loc[0, "main_eqp_type"] == "EQP"
    assert result.loc[0, "main_process_unit_id"] == "MAIN-EQP-01"
    assert result.loc[0, "main_process_trace_source"] == "array_sht"
