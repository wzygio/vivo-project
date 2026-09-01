"""AOI_RS 应用服务测试：payload 组装、指标元数据、空数据降级。"""

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs import aoi_rs_service
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
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
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_qty": 3,
            },
            {
                "factory": "TP",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-02 11:00:00"),
                "sheet_id": "GLS-T01",
                "lot_id": "LOT-T1",
                "step_id": "43629",
                "rs_code": "T3DMR",
                "code_qty": 5,
            },
        ]
    )


def _pass_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-07-15 07:00:00"),
                "sheet_id": "SHT-A01",
                "lot_id": "LOT-A1",
                "step_id": "11629",
            }
        ]
    )


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "type_flag": "MWD_RATIO",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_desc": "PHT责M1残留",
                "spec": 0.5,
            }
        ]
    )


def _data_port(details, pass_through, spec) -> SimpleNamespace:
    return SimpleNamespace(
        get_rs_details=lambda _query: details,
        get_pass_through=lambda _query: pass_through,
        get_rs_spec_limits=lambda _prod_code: spec,
    )


def _config_json() -> str:
    return AoiRsQueryConfig(
        prod_code="M678", start_date="2026-07-01", end_date="2026-08-10"
    ).model_dump_json()


def test_service_reads_report_inputs_through_aoi_rs_data_port() -> None:
    data_port = SimpleNamespace(
        get_rs_details=lambda _query: _details_df(),
        get_pass_through=lambda _query: _pass_df(),
        get_rs_spec_limits=lambda _prod_code: _spec_df(),
    )
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=data_port,
        query_config_json=_config_json(),
        snapshot_signature="port-tracer",
    )

    assert len(view_model.rs_details_df) == 2
    assert len(view_model.pass_through_df) == 1
    assert view_model.indicators_df.loc[0, "code_desc"] == "PHT责M1残留"


def test_service_builds_view_model_with_indicators_and_code_desc(monkeypatch) -> None:
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), _spec_df()),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert len(view_model.rs_details_df) == 2
    assert len(view_model.pass_through_df) == 1
    assert len(view_model.spec_df) == 1
    # 指标粒度 = 厂别 + 站点 + RS Code，并带出规格表的中文名称
    indicators = view_model.indicators_df
    assert set(indicators.columns) >= {"prod_code", "factory", "step_id", "rs_code", "code_desc"}
    row = indicators[indicators["rs_code"] == "A1PPS"].iloc[0]
    assert row["code_desc"] == "PHT责M1残留"
    # 无规格的 Code 也要保留在指标中（code_desc 允许为空）
    assert "T3DMR" in set(indicators["rs_code"])


def test_service_returns_empty_view_model_when_no_details(monkeypatch) -> None:
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(pd.DataFrame(), pd.DataFrame(), pd.DataFrame()),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.rs_details_df.empty
    assert view_model.indicators_df.empty


def test_service_tolerates_loader_exception(monkeypatch) -> None:
    def _boom(*_args, **_kw):
        raise RuntimeError("db down")

    AoiRsReportService.fetch_aoi_rs_report_payload.clear()
    data_port = _data_port(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    data_port.get_rs_details = _boom

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=data_port,
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.rs_details_df.empty
    assert view_model.indicators_df.empty


def test_service_returns_decorated_lot_and_sheet_points(monkeypatch) -> None:
    """修饰在 service 层完成（D4）：payload 直接给出图表就绪的修饰后点帧。"""
    spec_df = pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "type_flag": "LOT_RATIO",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_desc": "PHT责M1残留",
                "spec": 2.0,
            },
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "type_flag": "SHEET_ID",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_desc": "PHT责M1残留",
                "spec": 2.0,
            },
        ]
    )
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), spec_df),
        query_config_json=_config_json(),
        snapshot_signature="clip-test",
    )

    # 原始明细保持原值（源数据不修饰）
    details = view_model.rs_details_df
    assert details[details["rs_code"] == "A1PPS"]["code_qty"].iloc[0] == 3

    # By Sheet：rs_qty=3 超过 spec=2，被截断到线内；spec 列不外泄
    sheet_points = view_model.sheet_points_df
    a1_sheet = sheet_points[sheet_points["rs_code"] == "A1PPS"]
    assert not a1_sheet.empty
    assert a1_sheet["rs_qty"].iloc[0] < 2.0
    assert "spec" not in sheet_points.columns

    # By Lot：value = 3/1 = 3 超过 spec=2，被截断到线内
    lot_points = view_model.lot_points_df
    a1_lot = lot_points[lot_points["rs_code"] == "A1PPS"]
    assert not a1_lot.empty
    assert a1_lot["value"].iloc[0] < 2.0
    assert "spec" not in lot_points.columns

    # 无规格的 Code 保持真实值
    t3_sheet = sheet_points[sheet_points["rs_code"] == "T3DMR"]
    assert t3_sheet["rs_qty"].iloc[0] == 5


def test_service_passes_scope_revision_and_decision_signature_to_prepare(monkeypatch) -> None:
    """product_revision/decision_signature 进入缓存 key 并透传到 core 门控（scope='aoi_rs'）。"""
    captured: dict[str, object] = {}

    def fake_prepare(_lot_points_df, _sheet_points_df, _spec_df, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(lot_points_df=pd.DataFrame(), sheet_points_df=pd.DataFrame())

    monkeypatch.setattr(aoi_rs_service, "prepare_aoi_rs_decoration", fake_prepare)
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), _spec_df()),
        query_config_json=_config_json(),
        snapshot_signature="gate-pass-through",
        product_revision="R9",
        decision_signature="sig-x",
    )

    assert captured["scope"] == "aoi_rs"
    assert captured["prod_code"] == "M678"
    assert captured["product_revision"] == "R9"
    assert captured["decision_signature"] == "sig-x"


def _chart_spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "type_flag": "LOT_RATIO",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_desc": "PHT责M1残留",
                "spec": 2.0,
            },
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "type_flag": "SHEET_ID",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_desc": "PHT责M1残留",
                "spec": 2.0,
            },
        ]
    )


def test_service_persists_aoi_rs_decoration_workbook(
    monkeypatch, _tmp_project_root: Path
) -> None:
    """默认全部截断，并生成带 chart_kind 维度的用户修饰工作簿。"""
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), _chart_spec_df()),
        query_config_json=_config_json(),
        snapshot_signature="wb-default",
    )

    a1_sheet = view_model.sheet_points_df[view_model.sheet_points_df["rs_code"] == "A1PPS"]
    assert a1_sheet["rs_qty"].iloc[0] < 2.0
    a1_lot = view_model.lot_points_df[view_model.lot_points_df["rs_code"] == "A1PPS"]
    assert a1_lot["value"].iloc[0] < 2.0

    workbook = _tmp_project_root / "resources" / "inline_domain" / "aoi_rs_sheet_oos_decoration.xlsx"
    assert workbook.exists()
    persisted = pd.read_excel(workbook, sheet_name="M678")
    assert set(persisted["chart_kind"]) == {"lot", "sheet"}
    assert persisted["flag"].tolist() == [True, True]


def test_service_respects_aoi_rs_flag_false_and_delete(
    monkeypatch, _tmp_project_root: Path
) -> None:
    # sheet 图 SHT-A01 释放真实值；lot 图 LOT-A1 整行删除
    # （AOI 的决策唯一来源是 <产品>__flags，写入产品 sheet 的 flag 不生效）
    pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11629",
             "rs_code": "A1PPS", "chart_kind": "sheet", "point_id": "SHT-A01", "flag": False},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11629",
             "rs_code": "A1PPS", "chart_kind": "lot", "point_id": "LOT-A1", "flag": "Delete"},
        ]
    ).to_excel(
        _tmp_project_root / "resources" / "inline_domain" / "aoi_rs_sheet_oos_decoration.xlsx",
        sheet_name="M678__flags",
        index=False,
        engine="openpyxl",
    )
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), _chart_spec_df()),
        query_config_json=_config_json(),
        snapshot_signature="wb-flags",
    )

    a1_sheet = view_model.sheet_points_df[view_model.sheet_points_df["rs_code"] == "A1PPS"]
    assert a1_sheet["rs_qty"].iloc[0] == 3  # 释放真实值
    a1_lot = view_model.lot_points_df[view_model.lot_points_df["rs_code"] == "A1PPS"]
    assert a1_lot.empty  # Delete 行被剔除
    # 无规格的 T3DMR 不受影响
    t3_sheet = view_model.sheet_points_df[view_model.sheet_points_df["rs_code"] == "T3DMR"]
    assert t3_sheet["rs_qty"].iloc[0] == 5
