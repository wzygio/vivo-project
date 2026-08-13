"""AOI_TT 应用服务测试：payload 组装、指标元数据、空数据降级。"""

import pandas as pd

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService


def _details_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-07-15 08:00:00"),
                "sheet_id": "SHT-A01",
                "lot_id": "LOT-A1",
                "step_id": "11620",
                "tt_name": "TDSUM",
                "tt_qty": 3,
            },
            {
                "factory": "TP",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-02 11:00:00"),
                "sheet_id": "GLS-T01",
                "lot_id": "LOT-T1",
                "step_id": "43620",
                "tt_name": "TOTAL_O_L",
                "tt_qty": 5,
            },
        ]
    )


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "step_id": "11620",
                "tt_name": "TDSUM",
                "usl": 5.0,
                "ucl": 3.0,
            }
        ]
    )


class FakeAoiTtPort:
    def __init__(self, details: pd.DataFrame, spec: pd.DataFrame) -> None:
        self.details = details
        self.spec = spec

    def get_tt_details(self, _query) -> pd.DataFrame:
        return self.details

    def get_tt_spec_limits(self, _prod_code: str) -> pd.DataFrame:
        return self.spec


def _config_json() -> str:
    return AoiTtQueryConfig(
        prod_code="M678", start_date="2026-07-01", end_date="2026-08-10"
    ).model_dump_json()


def test_service_builds_view_model_with_indicators(monkeypatch) -> None:
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=FakeAoiTtPort(_details_df(), _spec_df()),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert len(view_model.tt_details_df) == 2
    assert len(view_model.spec_df) == 1
    # 指标粒度 = 厂别 + 站点 + TT 参数名
    indicators = view_model.indicators_df
    assert set(indicators.columns) >= {"prod_code", "factory", "step_id", "tt_name"}
    assert set(indicators["tt_name"]) == {"TDSUM", "TOTAL_O_L"}
    row = indicators[indicators["tt_name"] == "TDSUM"].iloc[0]
    assert row["factory"] == "ARRAY"
    assert row["step_id"] == "11620"


def test_service_returns_empty_view_model_when_no_details(monkeypatch) -> None:
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=FakeAoiTtPort(pd.DataFrame(), pd.DataFrame()),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.tt_details_df.empty
    assert view_model.indicators_df.empty


def test_service_tolerates_loader_exception(monkeypatch) -> None:
    class FailingAoiTtPort(FakeAoiTtPort):
        def get_tt_details(self, _query) -> pd.DataFrame:
            raise RuntimeError("db down")

    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=FailingAoiTtPort(pd.DataFrame(), pd.DataFrame()),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.tt_details_df.empty
    assert view_model.indicators_df.empty


def test_service_reads_through_application_data_port() -> None:
    class FakeAoiTtPort:
        def get_tt_details(self, _query) -> pd.DataFrame:
            return _details_df()

        def get_tt_spec_limits(self, _prod_code: str) -> pd.DataFrame:
            return _spec_df()

    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=FakeAoiTtPort(),
        query_config_json=_config_json(),
        snapshot_signature="port-test",
    )

    assert len(view_model.tt_details_df) == 2
    assert len(view_model.spec_df) == 1
