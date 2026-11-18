"""合并资料页使用隔离文件验证，不访问数据库或维护中的资料。"""

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import fitz
import pandas as pd
import pytest
import streamlit as st
import yaml
from streamlit.testing.v1 import AppTest

from src.shared_kernel.config import ConfigLoader


PAGE = Path(__file__).parents[4] / "app/pages/专项资料-台账周报.py"


@pytest.fixture
def document_dirs(tmp_path, monkeypatch):
    collection_keys = {
        "ledger": "project_ledgers",
        "weekly": "project_weekly_reports",
        "analysis": "project_analysis_reports",
        "archive": "project_archives",
    }
    directories = {source: tmp_path / f"custom-{source}" for source in collection_keys}
    for directory in directories.values():
        directory.mkdir()
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        yaml.safe_dump({
            "resources": {"shared_kernel": {"directories": {
                key: str(directories[source]) for source, key in collection_keys.items()
            }}},
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
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
    return directories


def write_pdf(path: Path, pages: int) -> None:
    with fitz.open() as document:
        for _ in range(pages):
            document.new_page()
        document.save(str(path))


def test_same_named_reports_keep_independent_downloads_and_previews(document_dirs):
    write_pdf(document_dirs["archive"] / "同名报告.pdf", 1)
    write_pdf(document_dirs["analysis"] / "同名报告.pdf", 2)
    app = AppTest.from_file(str(PAGE)).run()

    assert not app.exception
    assert len(app.get("download_button")) == 2
    metadata = app.session_state._state._new_widget_state.widget_metadata
    assert metadata[app.selectbox(key="document_upload_source").id].fragment_id
    app.button(key="view_analysis_同名报告.pdf").click().run()
    assert not app.exception
    assert app.session_state.viewing_source == "analysis"
    assert any("共加载 2 页内容" in item.value for item in app.info)

    app.button(key="view_archive_同名报告.pdf").click().run()
    assert not app.exception
    assert app.session_state.viewing_source == "archive"
    assert any("共加载 1 页内容" in item.value for item in app.info)


def test_empty_analysis_group_and_excel_editing_still_render(document_dirs):
    (document_dirs["ledger"] / "测试记录.xlsx").write_bytes(b"isolated workbook fixture")
    (document_dirs["weekly"] / "测试记录.md").write_text("# 测试", encoding="utf-8")
    app = AppTest.from_file(str(PAGE)).run()

    assert not app.exception
    assert any("暂无解析报告" in item.value for item in app.info)
    assert app.button(key="view_weekly_测试记录.md")
    assert not any("刷新缓存" in item.label for item in app.button)
    app.button(key="view_ledger_测试记录.xlsx").click().run()
    assert not app.exception
    assert app.session_state.viewing_type == "EXCEL"
    assert len(app.dataframe) == 1
    assert any("正在编辑" in item.value for item in app.subheader)


def test_invalid_pdf_shows_safe_error_and_closes_preview(document_dirs):
    analysis = document_dirs["analysis"]
    (analysis / "损坏.pdf").write_bytes(b"invalid pdf")
    app = AppTest.from_file(str(PAGE)).run()
    app.button(key="view_analysis_损坏.pdf").click().run()

    assert not app.exception
    assert app.error
    assert app.session_state.viewing_file is None
    assert all(str(analysis) not in item.value for item in app.error)
    assert not app.subheader


@pytest.mark.parametrize("source", ["ledger", "weekly", "analysis", "archive"])
def test_upload_replaces_only_selected_source(document_dirs, monkeypatch, source):
    filename = "同名报告.pdf"
    for directory in document_dirs.values():
        (directory / filename).write_bytes(b"original")
    uploaded = SimpleNamespace(name=filename, getbuffer=lambda: b"replacement")
    monkeypatch.setattr(
        st, "file_uploader",
        lambda *args, **kwargs: [uploaded] if kwargs["key"] == f"document_uploader_{source}" else [],
    )
    app = AppTest.from_file(str(PAGE)).run()
    app.selectbox(key="document_upload_source").select(source).run()
    next(button for button in app.button if "确认上传" in button.label).click().run()

    assert not app.exception
    for category, directory in document_dirs.items():
        assert (directory / filename).read_bytes() == (
            b"replacement" if category == source else b"original"
        )


def test_legacy_analysis_page_is_removed():
    assert not PAGE.with_name("专项资料-解析报告.py").exists()
