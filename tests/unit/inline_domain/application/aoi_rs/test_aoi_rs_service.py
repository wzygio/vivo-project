"""AOI_RS 应用服务测试：payload 组装、指标元数据、空数据降级。"""

from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.aoi_rs import aoi_rs_service
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
from src.inline_domain.infrastructure.aoi_rs.data_loader import AoiRsQueryConfig


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


def _patch_loaders(monkeypatch, details, pass_through, spec) -> None:
    monkeypatch.setattr(aoi_rs_service, "load_rs_details", lambda *_args, **_kw: details)
    monkeypatch.setattr(aoi_rs_service, "load_pass_through", lambda *_args, **_kw: pass_through)
    monkeypatch.setattr(aoi_rs_service, "load_rs_spec_limits", lambda *_args, **_kw: spec)


def _config_json() -> str:
    return AoiRsQueryConfig(
        prod_code="M678", start_date="2026-07-01", end_date="2026-08-10"
    ).model_dump_json()


def test_service_builds_view_model_with_indicators_and_code_desc(monkeypatch) -> None:
    _patch_loaders(monkeypatch, _details_df(), _pass_df(), _spec_df())
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _db_manager=SimpleNamespace(engine=None),
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
    _patch_loaders(monkeypatch, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _db_manager=SimpleNamespace(engine=None),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.rs_details_df.empty
    assert view_model.indicators_df.empty


def test_service_tolerates_loader_exception(monkeypatch) -> None:
    def _boom(*_args, **_kw):
        raise RuntimeError("db down")

    monkeypatch.setattr(aoi_rs_service, "load_rs_details", _boom)
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _db_manager=SimpleNamespace(engine=None),
        query_config_json=_config_json(),
        snapshot_signature="test",
    )

    assert view_model.rs_details_df.empty
    assert view_model.indicators_df.empty


def test_service_auto_clips_over_spec_code_qty(monkeypatch) -> None:
    """超规 code_qty 被截断到 spec 以下；无规格的 Code 保持原值。"""
    _patch_loaders(monkeypatch, _details_df(), _pass_df(), _spec_df())
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()

    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _db_manager=SimpleNamespace(engine=None),
        query_config_json=_config_json(),
        snapshot_signature="clip-test",
    )

    details = view_model.rs_details_df
    clipped = details[details["rs_code"] == "A1PPS"]["code_qty"].iloc[0]
    assert 0.5 * 0.85 <= clipped < 0.5
    # T3DMR 无规格，保持原值
    untouched = details[details["rs_code"] == "T3DMR"]["code_qty"].iloc[0]
    assert untouched == 5
