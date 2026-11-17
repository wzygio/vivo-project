"""All thirteen matrix indicators treat absent result files/tables as passing."""
from datetime import date

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import alert_matrix_cache as cache
from app.sections.inline_domain.monitor import alert_matrix_service as matrix
from src.shared_kernel.config import ConfigLoader


@pytest.fixture
def isolated_resources(tmp_path, monkeypatch):
    def resource(domain, key, default=None):
        return tmp_path / f"{domain}_{key}.xlsx"

    def forbidden(*args, **kwargs):
        pytest.fail("Missing result input triggered database access or report production")

    from src.yield_domain.application.yield_service import YieldAnalysisService
    from src.indicator_domain.application.ijp.service import IjpReportService
    from src.equipment_domain.application.parts_service import PartsReportService
    from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService

    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", resource)
    monkeypatch.setattr(pd, "read_sql", forbidden)
    monkeypatch.setattr(cache, "DatabaseManager", forbidden)
    for owner, name in (
        (YieldAnalysisService, "get_lot_defect_rates"),
        (YieldAnalysisService, "get_mwd_trend_data"),
        (IjpReportService, "get_alert_report"),
        (PartsReportService, "get_report_data"),
        (EvaporationReportService, "get_report"),
    ):
        monkeypatch.setattr(owner, name, forbidden)
    cache._cached_alert_matrix_cell.clear()
    yield resource
    cache._cached_alert_matrix_cell.clear()


@pytest.mark.parametrize("condition", ["missing_file", "missing_sheet"])
def test_all_matrix_indicators_with_absent_inputs_render_green(isolated_resources, condition):
    resource = isolated_resources
    if condition == "missing_sheet":
        for domain, key in (
            ("inline_domain", "spc_sheet_oos_decoration"),
            ("inline_domain", "ctq_sheet_oos_decoration"),
            ("inline_domain", "aoi_tt_sheet_oos_decoration"),
            ("inline_domain", "aoi_rs_sheet_oos_decoration"),
            ("inline_domain", "spc_cpk_cpm_decoration"),
            ("yield_domain", "lot_oos_results"),
            ("yield_domain", "trend_alert_results"),
            ("indicator_domain", "qtime_alerts"),
            ("indicator_domain", "ijp_alerts"),
            ("equipment_domain", "parts_alerts"),
            ("iqc_domain", "evaporation_alerts"),
        ):
            pd.DataFrame().to_excel(resource(domain, key), sheet_name="other", index=False)
    paths_before = {path: path.read_bytes() for path in resource("unused", "unused").parent.glob("*.xlsx")}
    payload = cache.get_cached_alert_matrix(reference_date=date.today(), products=["Z622"],
        _revision_provider=lambda *args: "isolated-missing")
    assert len(payload["cells"]) == 13
    assert all(cell["state"] in {"ok", "no_data"} for cell in payload["cells"].values()), payload["cells"]

    app = AppTest.from_string('''
import streamlit as st
from app.sections.inline_domain.monitor.alert_matrix import render_alert_matrix_section
from app.sections.inline_domain.monitor.alert_matrix_detail import render_alert_matrix_detail
payload = st.session_state["payload"]
render_alert_matrix_section(payload, filter_selection=("ALL", ["Z622"], ["ARRAY", "OLED", "TP"]))
render_alert_matrix_detail(payload)
''')
    app.session_state["payload"] = payload
    app.run()
    assert not app.exception
    cells = [button for button in app.button if str(button.key).startswith("matrix_cell_")]
    assert len(cells) == 13 and all(button.label == "🟢" for button in cells)
    for row in matrix.MATRIX_ROWS:
        app.button(key=f"matrix_cell_{row.row_key}_Z622").click().run()
        assert not app.exception and not app.error and not app.dataframe
    # Preserve existing inputs. Q-Time retains its previously authorized header initialization.
    qtime_path = resource("indicator_domain", "qtime_alerts")
    assert all(path == qtime_path or path.read_bytes() == before for path, before in paths_before.items())
    assert {path for path in qtime_path.parent.glob("*.xlsx")} <= {*paths_before, qtime_path}
