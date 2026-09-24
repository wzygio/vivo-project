"""AOI_RS 应用服务测试：payload 组装、指标元数据、空数据降级。"""

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs import aoi_rs_service
from src.inline_domain.application.aoi_rs.aoi_rs_service import (
    AoiRsReportBuildError,
    AoiRsReportService,
)
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


def test_service_surfaces_loader_exception_without_caching(monkeypatch) -> None:
    def _boom(*_args, **_kw):
        raise RuntimeError("db down")

    AoiRsReportService.fetch_aoi_rs_report_payload.clear()
    data_port = _data_port(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    data_port.get_rs_details = _boom

    for _ in range(2):
        with pytest.raises(AoiRsReportBuildError, match="AOI_RS report generation failed"):
            AoiRsReportService.get_aoi_rs_report_data(
                _data_port=data_port,
                query_config_json=_config_json(),
                snapshot_signature="test",
            )


def test_service_returns_decorated_lot_and_sheet_points(monkeypatch) -> None:
    """修饰在 service 层完成（D4）：payload 直接给出图表就绪的修饰后点帧。"""
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: ["ARRAY"])
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

    source = _details_df()
    original = source.copy(deep=True)
    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(source, _pass_df(), spec_df),
        query_config_json=_config_json(),
        snapshot_signature="clip-test",
    )

    # 源事实不变，前端明细使用投影，保证月周天汇总接续 Sheet/Lot 修饰。
    pd.testing.assert_frame_equal(source, original)
    details = view_model.rs_details_df

    # By Sheet：rs_qty=3 超过 spec=2，被截断到线内；spec 列不外泄
    sheet_points = view_model.sheet_points_df
    a1_sheet = sheet_points[sheet_points["rs_code"] == "A1PPS"]
    assert not a1_sheet.empty
    assert 0 <= a1_sheet["rs_qty"].iloc[0] <= 1.0
    assert details[details["rs_code"] == "A1PPS"]["code_qty"].iloc[0] == a1_sheet["rs_qty"].iloc[0]
    assert "spec" not in sheet_points.columns

    # By Lot：Sheet 截断后已低于 spec=2，保留重算结果。
    lot_points = view_model.lot_points_df
    a1_lot = lot_points[lot_points["rs_code"] == "A1PPS"]
    assert not a1_lot.empty
    assert a1_lot["value"].iloc[0] == a1_sheet["rs_qty"].iloc[0]
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


def test_filtered_query_does_not_write_derived_parquet(monkeypatch) -> None:
    updates: list[object] = []
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, *args, **kwargs: updates.append((args, kwargs)),
    )
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    config = json.loads(_config_json())
    config["step_id"] = "11629"
    AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(_details_df(), _pass_df(), _chart_spec_df()),
        query_config_json=json.dumps(config),
        snapshot_signature="filtered-history-gate",
    )

    assert updates == []


def test_report_and_shared_charts_use_sequential_projection(monkeypatch) -> None:
    from datetime import date
    from app.charts.inline_domain import aoi_rs_charts

    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: ["ARRAY"])

    source = _details_df().iloc[[0]].copy()
    source["code_qty"] = 8
    source["start_time"] = pd.Timestamp("2026-08-09")
    companion = source.assign(sheet_id="SHT-A02", lot_id="LOT-A2", code_qty=1)
    source = pd.concat([source, companion], ignore_index=True)
    throughput = source.drop(columns=["rs_code", "code_qty"])
    spec = _chart_spec_df()
    spec.loc[spec.type_flag.eq("SHEET_ID"), "spec"] = 10
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()
    report = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=_data_port(source, throughput, spec),
        query_config_json=_config_json(), snapshot_signature="sequential-render",
    )
    assert report.rs_details_df.code_qty.tolist() == [0, 1]
    assert report.sheet_points_df.rs_qty.tolist() == [0, 1]
    assert report.lot_points_df.value.tolist() == [0, 1]
    assert source.code_qty.tolist() == [8, 1]
    trends = []
    original = aoi_rs_charts.create_aoi_rs_trend_chart

    def capture(**kwargs):
        trends.append(kwargs["trend_df"])
        return original(**kwargs)

    monkeypatch.setattr(aoi_rs_charts, "create_aoi_rs_trend_chart", capture)
    groups = list(aoi_rs_charts.iter_aoi_rs_chart_groups(
        **vars(report), end_date=date(2026, 8, 10),
    ))
    assert len(groups) == 1 and len(groups[0].figures) == 3
    assert trends[0].value.eq(.5).all()


@pytest.mark.parametrize("sheet_limit", [2., 10.])
def test_default_oled_only_special_rules_and_factory_config_invalidates_cache(
    _tmp_project_root, sheet_limit,
) -> None:
    import yaml

    source = pd.concat([
        _details_df().iloc[[0]].assign(factory=factory, code_qty=8)
        for factory in ["ARRAY", "OLED", "TP"]
    ], ignore_index=True)
    original = source.copy(deep=True)
    spec = pd.concat([
        _chart_spec_df().assign(factory=factory)
        for factory in ["ARRAY", "OLED", "TP"]
    ], ignore_index=True)
    spec.loc[spec.type_flag.eq("SHEET_ID"), "spec"] = sheet_limit
    calls = []
    port = _data_port(source, source, spec)

    def load(_query):
        calls.append(1)
        return source

    port.get_rs_details = load
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()
    config_path = _tmp_project_root / "config/domain/inline_domain.yaml"
    config_path.parent.mkdir(parents=True)

    for factories in [["OLED"], ["ARRAY", "TP"], []]:
        config_path.write_text(yaml.safe_dump({
            "aoi_rs": {"special_decoration": {"factories": factories}},
        }), encoding="utf-8")
        report = AoiRsReportService.get_aoi_rs_report_data(
            _data_port=port, query_config_json=_config_json(), snapshot_signature="factory-toggle",
        )
        for factory in ["ARRAY", "OLED", "TP"]:
            lot = report.lot_points_df.set_index("factory").loc[factory, "value"]
            sheet = report.sheet_points_df.set_index("factory").loc[factory, "rs_qty"]
            detail = report.rs_details_df.set_index("factory").loc[factory, "code_qty"]
            if factory in factories:
                assert 0 <= sheet <= 1
                assert lot == sheet == detail
                if sheet_limit == 10:
                    assert lot == 0
            else:
                assert 1.7 <= lot <= 1.9
                assert (1.7 <= sheet <= 1.9) if sheet_limit == 2 else sheet == 8
                assert detail == 8
    assert len(calls) == 3  # no explicit cache clear between configuration changes
    pd.testing.assert_frame_equal(source, original)


def test_period_cap_reads_same_factory_configuration_for_page_and_pdf(monkeypatch) -> None:
    from datetime import date
    from src.inline_domain.application.aoi_rs.decoration_service import build_aoi_rs_period_trend

    frames = []
    for factory in ["ARRAY", "OLED"]:
        frames.extend([
            _details_df().iloc[[0]].assign(factory=factory, code_qty=10,
                                          start_time=pd.Timestamp("2026-08-01")),
            _details_df().iloc[[0]].assign(factory=factory, code_qty=0, sheet_id="OTHER",
                                          start_time=pd.Timestamp("2026-08-09")),
        ])
    source = pd.concat(frames, ignore_index=True)
    for factories in [["OLED"], ["ARRAY"], []]:
        monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: factories)
        trend = build_aoi_rs_period_trend(source, source, date(2026, 8, 10))
        for factory in ["ARRAY", "OLED"]:
            rows = trend[trend.factory.eq(factory)]
            assert rows[rows.period_type.eq("month")].value.iloc[0] == 5
            for period_type in ["week", "day"]:
                maximum = rows[rows.period_type.eq(period_type)].value.max()
                assert maximum == (6.5 if factory in factories else 10)
