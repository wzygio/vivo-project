"""Product warning decisions using temporary workbooks and real report controls."""
from datetime import date, datetime

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.shared import alert_decision_admin
from src.inline_domain.application.monitor.trend_alert_results import TrendAlertResults
from src.inline_domain.core.shared.density_trend_alerts import EVALUATION_VALUE_COLUMNS
from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
from src.inline_domain.infrastructure.monitor.trend_alert_workbook import TrendAlertWorkbook, trend_decision_keys
from src.inline_domain.infrastructure.shared.alert_decisions import AlertDecisionWorkbook
from src.inline_domain.infrastructure.shared.oos_history_store import OosHistoryStore
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import persist_sheet_oos_decoration_outcome
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, decision_revision, upload_revision
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
@pytest.mark.parametrize("kind", ["oos", "ooc"])
def test_sheet_decisions_survive_reinspection_and_manual_edits(tmp_path, scope, kind):
    keys = list(OosHistoryStore.contract_for(scope).key_columns)
    path = tmp_path / f"{scope}_{kind}.xlsx"
    row = dict(prod_code="M", factory="ARRAY", step_id="0100", param_name="THK", sheet_id="0001",
               rs_code="RS", tt_name="TDSUM", point_id="0001", chart_kind="sheet")
    source = pd.DataFrame([{**{key: row[key] for key in keys}, "sheet_start_time": "2026-10-26",
                            "start_time": "2026-10-26", "factory": "ARRAY"}])
    def refresh(facts):
        return persist_sheet_oos_decoration_outcome(tmp_path, facts, path.name, "M", keys,
            scope=scope, alarm_type=kind.upper(), force=True)
    refresh(source)
    reader = ExcelAlarmStore({scope: path})
    admin = AlertDecisionWorkbook(path, keys, default=True)
    exported = admin.download("M")
    assert exported["决策台账"].step_id.tolist() == ["0100"]
    original = upload_revision(exported[VERSION_SHEET])
    changes = exported["决策台账"].assign(flag=False)
    admin.update("M", changes, revision=original)
    assert reader.read(scope, "M")["frame"].flag.tolist() == [False]
    with pytest.raises(ValueError, match="重新下载"):
        admin.update("M", changes.assign(flag="Delete"), revision=original)
    # An edit made directly in Excel takes effect before the next report refresh.
    assert replace_workbook_sheets(path, {"M__flags": changes.assign(flag="Delete")}).written
    assert reader.read(scope, "M")["frame"].flag.tolist() == ["Delete"]
    refresh(source.iloc[:0])
    assert reader.read(scope, "M")["frame"].empty
    assert admin.download("M")["决策台账"].flag.tolist() == ["Delete"]
    refresh(source)
    assert reader.read(scope, "M")["frame"].flag.tolist() == ["Delete"]


@pytest.mark.parametrize("scope", ["aoi_rs", "aoi_tt"])
def test_trend_decision_survives_empty_result_and_excel_dates(tmp_path, scope):
    day = date(2026, 10, 27)
    path = tmp_path / "trend.xlsx"
    trend = TrendAlertResults(TrendAlertWorkbook(path), scope=scope, ttl_seconds=3600,
                               policy="fixture", clock=lambda: datetime(2026, 10, 27, 8))
    parameter = "rs_code" if scope == "aoi_rs" else "tt_name"
    row = dict.fromkeys(EVALUATION_VALUE_COLUMNS, 1)
    row.update(prod_code="M", factory="ARRAY", step_id="0100", target_date=pd.Timestamp("2026-10-26"),
               daily_evaluable=True, is_alert=True, daily_alert=True)
    row[parameter] = "RS" if scope == "aoi_rs" else "TDSUM"
    facts = pd.DataFrame([row])
    trend.publish("M", day, facts)
    keys = trend_decision_keys(facts)
    admin = AlertDecisionWorkbook(path, keys, default=False)
    assert trend.read("M", day)["state"] == "alert"
    def decide(flag):
        download = admin.download("M")
        admin.update("M", download["决策台账"].assign(flag=flag), revision=upload_revision(download[VERSION_SHEET]))
    decide(True)
    assert trend.read("M", day)["state"] == "ok"
    decide("Delete")
    assert pd.read_excel(path, sheet_name="M")["flag"].tolist() == ["Delete"]
    assert trend.read("M", day)["evaluation"].empty
    trend.publish("M", day, pd.DataFrame())
    assert admin.download("M")["决策台账"].flag.tolist() == ["Delete"]
    trend.publish("M", day, facts.assign(target_date="2026-10-26"))
    assert pd.read_excel(path, sheet_name="M")["flag"].tolist() == ["Delete"]
    assert trend.read("M", day)["evaluation"].empty
    decide(False)
    assert trend.read("M", day)["state"] == "alert"


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
def test_report_warning_center_recognizes_admin_ture(tmp_path, monkeypatch, scope):
    path = tmp_path / "fixture.xlsx"
    keys = list(OosHistoryStore.contract_for(scope).key_columns)
    store = AlertDecisionWorkbook(path, keys, default=True)
    monkeypatch.setattr(alert_decision_admin, "build_alert_decision_workbook", lambda *_: store)
    app = AppTest.from_string(
        "from app.sections.inline_domain.shared.alert_decision_admin import render_inline_alert_decisions\n"
        f"render_inline_alert_decisions({scope!r}, 'M')"
    )
    app.run()
    assert not app.exception and not app.get("file_uploader")
    app.query_params["admin"] = "ture"
    app.run()
    assert not app.exception and len(app.get("file_uploader")) == 1
    assert len(app.get("download_button")) == 1


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_capability_admin_version_covers_saved_replacement_and_delete(tmp_path, metric):
    from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration
    from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS
    from src.inline_domain.infrastructure.spc.capability_decoration_repository import update_capability_decisions
    from tests.unit.inline_domain.core.spc.test_capability_ledger_passing_rows import capability

    day = date(2026, 11, 17)
    sheet = "M678" if metric == "cpk" else "M678_cpm"
    replacement = f"{metric}_replacement"
    other = "cpm" if metric == "cpk" else "cpk"
    rows = capability(metric, .8).assign(step_id="0100", **{other: .75})
    def publish(frame):
        return prepare_capability_decoration(frame, tmp_path, metric=metric, sheet_name=sheet, reference_date=day)
    initial = publish(rows)
    ledger, path = initial.decoration_df, initial.decoration_path
    version = decision_revision(sheet, ledger, CPK_KEY_COLUMNS, controls=["flag", replacement])
    manual = ledger.assign(**{replacement: 1.38 if ledger[replacement].iloc[0] != 1.38 else 1.37})
    update_capability_decisions(path, sheet, metric, manual, revision=version)
    with pytest.raises(ValueError, match="重新下载"):
        update_capability_decisions(path, sheet, metric, ledger.assign(flag="Delete"), revision=version)
    current = pd.read_excel(path, sheet_name=sheet, dtype=object)
    version = decision_revision(sheet, current, CPK_KEY_COLUMNS, controls=["flag", replacement])
    update_capability_decisions(path, sheet, metric, current.assign(flag="Delete"), revision=version)
    publish(rows.iloc[:0])
    assert pd.isna(pd.read_excel(path, sheet_name=sheet)[f"{metric}_corrected"].iloc[0])
    displayed = publish(rows)
    assert pd.isna(displayed.period_capability_df[metric].iloc[0])
    assert displayed.period_capability_df[other].tolist() == [.75]
    assert displayed.decoration_df.flag.tolist() == ["Delete"]
    assert displayed.decoration_df.step_id.tolist() == ["0100"]
    update_capability_decisions(path, sheet, metric, manual.assign(flag=False))
    assert publish(rows).period_capability_df[metric].tolist() == [.8]
