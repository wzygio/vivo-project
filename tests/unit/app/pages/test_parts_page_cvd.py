from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.components import page_header
from app.manager.session_manager import SessionManager
from src.equipment_domain.application.parts_service import PartsReportService, PartsReportViewModel
from src.shared_kernel.infrastructure import db_handler


PAGE = Path(__file__).resolve().parents[4] / "app" / "pages" / "关键备件报表.py"


@pytest.mark.parametrize("mode", ["normal", "empty", "failure"])
def test_parts_page_cvd_public_states(monkeypatch, mode):
    frame = pd.DataFrame({
        "厂别": ["OLED", "Array"], "设备类型": ["CVD", "PVD"],
        "备件类型": ["Diffuser", "Target"],
        "测量值": ["2026-01-31", 30000.0], "寿命规格": ["2年", 41000.0],
        "使用进度": [32.05, 73.17], "预警状态": ["正常", "正常"],
        "测量时间": pd.to_datetime(["2026-09-22", "2026-09-22"]),
        "进度": [None, 352], "原始测量值": [46053.5, 30000],
    })
    if mode == "empty":
        frame = frame.iloc[:0]

    def get_report(**kwargs):
        assert kwargs["cvd_path"].endswith("0921-CVD.xlsx")
        assert kwargs["cvd_signature"]
        if mode == "failure":
            raise ValueError("internal admin=true configuration path")
        return PartsReportViewModel(frame, len(frame), 0, 0, len(frame), "2026-09-22")

    monkeypatch.setattr(PartsReportService, "get_report_data", staticmethod(get_report))
    monkeypatch.setattr(SessionManager, "get_active_config", staticmethod(lambda: {}))
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(page_header, "render_page_header", lambda *args, **kwargs: None)

    app = AppTest.from_file(str(PAGE)).run()
    assert not app.exception
    if mode == "failure":
        assert app.error[0].value == "备件寿命数据暂时无法加载，请稍后重试。"
        assert not app.dataframe
    elif mode == "empty":
        assert app.info[0].value == "当前筛选条件下没有数据。"
        assert not app.dataframe
    else:
        assert "进度" not in app.dataframe[0].value
        assert "原始测量值" not in app.dataframe[0].value
        assert app.dataframe[0].value.iloc[0]["测量值"] == "2026-01-31"
        assert "CVD" in app.multiselect[1].options
        app.multiselect[1].select("CVD").run()
        assert not app.exception
        assert len(app.dataframe[0].value) == 1
        assert app.metric[0].value == "1"
