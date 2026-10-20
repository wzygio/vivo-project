"""合并资料页使用隔离文件验证，不访问数据库或维护中的资料。"""

import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import fitz
import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from src.shared_kernel.config import ConfigLoader


PAGE = Path(__file__).parents[4] / "app/pages/专项资料-台账周报.py"


@pytest.fixture
def document_dirs(tmp_path, monkeypatch):
    project = tmp_path / "project"
    analysis = tmp_path / "analysis"
    project.mkdir()
    analysis.mkdir()
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        ConfigLoader, "get_domain_resource_directory",
        lambda domain, key: {("shared_kernel", "project_files"): project,
                             ("yield_domain", "analysis_files"): analysis}[(domain, key)],
    )
    header = ModuleType("app.components.page_header")
    header.perform_hard_reset = lambda: None
    monkeypatch.setitem(sys.modules, header.__name__, header)
    excel = ModuleType("yield_domain.application.excel_service")
    excel.ExcelService = SimpleNamespace(
        load_and_clean_data=lambda *args, **kwargs: pd.DataFrame(
            {"No.": [1], "Issue描述": ["测试台账"], "状态": ["Open"]}
        ),
        get_file_timestamp=lambda path: Path(path).stat().st_mtime,
    )
    monkeypatch.setitem(sys.modules, excel.__name__, excel)
    ppt = ModuleType("yield_domain.application.ppt_service")
    ppt.PPTService = object
    monkeypatch.setitem(sys.modules, ppt.__name__, ppt)
    return project, analysis


def write_pdf(path: Path, pages: int) -> None:
    with fitz.open() as document:
        for _ in range(pages):
            document.new_page()
        document.save(str(path))


def test_same_named_reports_keep_independent_downloads_and_previews(document_dirs):
    project, analysis = document_dirs
    write_pdf(project / "同名报告.pdf", 1)
    write_pdf(analysis / "同名报告.pdf", 2)
    app = AppTest.from_file(str(PAGE)).run()

    assert not app.exception
    assert len(app.get("download_button")) == 2
    app.button(key="view_analysis_同名报告.pdf").click().run()
    assert not app.exception
    assert app.session_state.viewing_source == "analysis"
    assert any("共加载 2 页内容" in item.value for item in app.info)

    app.button(key="view_project_同名报告.pdf").click().run()
    assert not app.exception
    assert app.session_state.viewing_source == "project"
    assert any("共加载 1 页内容" in item.value for item in app.info)


def test_empty_analysis_group_and_excel_editing_still_render(document_dirs):
    project, _ = document_dirs
    (project / "测试台账.xlsx").write_bytes(b"isolated workbook fixture")
    app = AppTest.from_file(str(PAGE)).run()

    assert not app.exception
    assert any("暂无解析报告" in item.value for item in app.info)
    assert not any("刷新缓存" in item.label for item in app.button)
    app.button(key="view_project_测试台账.xlsx").click().run()
    assert not app.exception
    assert app.session_state.viewing_type == "EXCEL"
    assert len(app.dataframe) == 1
    assert any("正在编辑" in item.value for item in app.subheader)


def test_invalid_pdf_shows_safe_error_and_closes_preview(document_dirs):
    _, analysis = document_dirs
    (analysis / "损坏.pdf").write_bytes(b"invalid pdf")
    app = AppTest.from_file(str(PAGE)).run()
    app.button(key="view_analysis_损坏.pdf").click().run()

    assert not app.exception
    assert app.error
    assert app.session_state.viewing_file is None
    assert all(str(analysis) not in item.value for item in app.error)
    assert not app.subheader


def test_analysis_upload_replaces_only_selected_source(document_dirs, monkeypatch):
    project, analysis = document_dirs
    (project / "同名报告.pdf").write_bytes(b"project original")
    (analysis / "同名报告.pdf").write_bytes(b"analysis original")
    uploaded = SimpleNamespace(name="同名报告.pdf", getbuffer=lambda: b"analysis replacement")
    monkeypatch.setattr(
        st, "file_uploader",
        lambda *args, **kwargs: [uploaded] if kwargs["key"].endswith("analysis") else [],
    )
    app = AppTest.from_file(str(PAGE)).run()
    app.selectbox(key="document_upload_source").select("analysis").run()
    next(button for button in app.button if "确认上传" in button.label).click().run()

    assert not app.exception
    assert (project / "同名报告.pdf").read_bytes() == b"project original"
    assert (analysis / "同名报告.pdf").read_bytes() == b"analysis replacement"


def test_legacy_analysis_link_redirects_to_merged_page(monkeypatch):
    destinations = []
    monkeypatch.setattr(st, "switch_page", destinations.append)

    runpy.run_path(str(PAGE.with_name("专项资料-解析报告.py")))

    assert destinations == ["pages/专项资料-台账周报.py"]
