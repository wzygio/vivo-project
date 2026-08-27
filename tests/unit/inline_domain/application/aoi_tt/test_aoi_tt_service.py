"""AOI_TT 应用服务测试：payload 组装、指标元数据、空数据降级。"""

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
from src.shared_kernel.config import ConfigLoader


@pytest.fixture(autouse=True)
def _tmp_project_root(monkeypatch, tmp_path: Path) -> Path:
    """修饰工作簿重定向到 tmp_path，避免测试写入仓库 resources/。"""
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))
    (tmp_path / "resources" / "inline_domain").mkdir(parents=True, exist_ok=True)
    return tmp_path


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


def test_service_auto_clips_over_spec_tt_qty() -> None:
    """超规 tt_qty 被截断到 USL 以下（确定性伪随机），规格内行不变。"""

    class OverSpecPort:
        def get_tt_details(self, _query) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "factory": "ARRAY", "prod_code": "M678",
                        "start_time": pd.Timestamp("2026-07-15 08:00:00"),
                        "sheet_id": "SHT-A01", "lot_id": "LOT-A1",
                        "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 99.0,
                    }
                ]
            )

        def get_tt_spec_limits(self, _prod_code: str) -> pd.DataFrame:
            return _spec_df()  # TDSUM usl=5.0

    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=OverSpecPort(),
        query_config_json=_config_json(),
        snapshot_signature="clip-test",
    )

    clipped = view_model.tt_details_df["tt_qty"].iloc[0]
    assert 5.0 * 0.85 <= clipped < 5.0


def test_service_releases_configured_exempt_parameter(monkeypatch) -> None:
    details = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-07-15 08:00:00"),
                "sheet_id": "SHT-PPA",
                "lot_id": "LOT-PPA",
                "step_id": "11620",
                "tt_name": "PPA_B_X",
                "tt_qty": 99.0,
            }
        ]
    )
    specs = pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "step_id": "11620",
                "tt_name": "PPA_B_X",
                "usl": 5.0,
            }
        ]
    )
    monkeypatch.setattr(
        ConfigLoader,
        "get_auto_decoration_param_exemptions",
        staticmethod(lambda: ["PPA"]),
    )
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=FakeAoiTtPort(details, specs),
        query_config_json=_config_json(),
        snapshot_signature="exempt-ppa",
    )

    assert view_model.tt_details_df["tt_qty"].tolist() == [99.0]


def _over_spec_port() -> type:
    class OverSpecPort:
        def get_tt_details(self, _query) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "factory": "ARRAY", "prod_code": "M678",
                        "start_time": pd.Timestamp("2026-07-15 08:00:00"),
                        "sheet_id": "SHT-A01", "lot_id": "LOT-A1",
                        "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 99.0,
                    },
                    {
                        "factory": "ARRAY", "prod_code": "M678",
                        "start_time": pd.Timestamp("2026-07-16 08:00:00"),
                        "sheet_id": "SHT-A02", "lot_id": "LOT-A2",
                        "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 88.0,
                    },
                ]
            )

        def get_tt_spec_limits(self, _prod_code: str) -> pd.DataFrame:
            return _spec_df()  # TDSUM usl=5.0

    return OverSpecPort


def _write_aoi_tt_workbook(resources: Path, rows: list[dict]) -> Path:
    path = resources / "aoi_tt_sheet_oos_decoration.xlsx"
    pd.DataFrame(rows).to_excel(path, sheet_name="M678", index=False, engine="openpyxl")
    return path


def test_service_persists_aoi_tt_decoration_workbook(_tmp_project_root: Path) -> None:
    """无工作簿时按默认自动截断，并生成用户可编辑的修饰工作簿。"""
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=_over_spec_port()(),
        query_config_json=_config_json(),
        snapshot_signature="wb-default",
    )

    assert (view_model.tt_details_df["tt_qty"] < 5.0).all()
    workbook = _tmp_project_root / "resources" / "inline_domain" / "aoi_tt_sheet_oos_decoration.xlsx"
    assert workbook.exists()
    persisted = pd.read_excel(workbook, sheet_name="M678")
    assert set(persisted["sheet_id"]) == {"SHT-A01", "SHT-A02"}
    assert persisted["flag"].tolist() == [True, True]


def test_service_respects_flag_false_and_delete(_tmp_project_root: Path) -> None:
    resources = _tmp_project_root / "resources" / "inline_domain"
    _write_aoi_tt_workbook(
        resources,
        [
            {"factory": "ARRAY", "prod_code": "M678", "step_id": "11620",
             "tt_name": "TDSUM", "sheet_id": "SHT-A01", "flag": False},
            {"factory": "ARRAY", "prod_code": "M678", "step_id": "11620",
             "tt_name": "TDSUM", "sheet_id": "SHT-A02", "flag": "Delete"},
        ],
    )
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()

    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _data_port=_over_spec_port()(),
        query_config_json=_config_json(),
        snapshot_signature="wb-flags",
    )

    # Delete 行被剔除；flag=False 行释放真实值 99.0
    assert view_model.tt_details_df["sheet_id"].tolist() == ["SHT-A01"]
    assert view_model.tt_details_df["tt_qty"].iloc[0] == 99.0
