"""RS PDF exports embed complete main report images and preserve selection scope."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
from threading import Event

import pandas as pd
import plotly.graph_objects as go
import pymupdf
import pytest

from app.sections.inline_domain.aoi_rs import aoi_rs_export
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportViewModel


START = date(2026, 8, 1)
END = date(2026, 9, 22)
FACTORIES = ("ARRAY", "OLED", "TP")
FRAME_NAMES = (
    "rs_details_df", "pass_through_df", "spec_df", "indicators_df",
    "lot_points_df", "sheet_points_df",
)
RENDER_EXPORT_STATUS = aoi_rs_export._render_export_status.__wrapped__


def _report(product: str, *, empty: bool = False) -> AoiRsReportViewModel:
    indicators = pd.DataFrame() if empty else pd.DataFrame(
        [{"prod_code": product, "factory": "ARRAY", "step_id": "11629", "rs_code": "RS1"}]
    )
    return AoiRsReportViewModel(
        rs_details_df=indicators.copy(),
        pass_through_df=pd.DataFrame(),
        spec_df=pd.DataFrame(),
        indicators_df=indicators,
        lot_points_df=pd.DataFrame(),
        sheet_points_df=pd.DataFrame(),
    )


def _group(*, code: str = "RS1", factory: str = "ARRAY") -> SimpleNamespace:
    return SimpleNamespace(
        factory=factory,
        step_id="11629",
        code=code,
        code_name=f"{code}（测试）",
        figures=tuple(
            (slot, go.Figure(go.Scatter(x=[1, 2], y=[1, 2])))
            for slot in ("trend", "lot", "sheet")
        ),
    )


@pytest.fixture
def export_root(monkeypatch, tmp_path):
    monkeypatch.setattr(aoi_rs_export.ConfigLoader, "get_project_root", lambda: tmp_path)
    return tmp_path


@pytest.fixture
def rendered_batches(monkeypatch, export_root):
    batches = []

    def write_images(fig, file, **kwargs):
        assert kwargs == {"format": "png", "width": 600, "height": 460, "scale": 2}
        assert len(fig) == len(file)
        batches.append(list(fig))
        for index, filename in enumerate(file):
            pixmap = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 60, 46), False)
            pixmap.clear_with(245 - index)
            pixmap.save(str(filename))

    def groups(**kwargs):
        assert kwargs["end_date"] == END
        if kwargs["indicators_df"].empty:
            return iter(())
        return iter([_group()])

    monkeypatch.setattr(aoi_rs_export.pio, "write_images", write_images)
    monkeypatch.setattr(aoi_rs_export, "iter_aoi_rs_chart_groups", groups)
    return batches


def _build(products, load_report, *, factories=FACTORIES):
    return aoi_rs_export.build_aoi_rs_pdf(
        products,
        factories=factories,
        load_report=load_report,
        start_date=START,
        end_date=END,
        step_desc_map={"11629": "测试站点"},
    )


def _assert_three_column_row(images):
    assert len(images) == 3
    boxes = [image["bbox"] for image in images]
    assert all(box[1] == pytest.approx(boxes[0][1]) for box in boxes)
    assert boxes[0][2] <= boxes[1][0] < boxes[1][2] <= boxes[2][0]


def test_exports_self_contained_pdf_with_one_three_image_row_per_product(rendered_batches, export_root):
    queried = []

    def load(product):
        queried.append(product)
        return _report(product)

    payload = _build(("M678", "M999", "M678"), load)

    assert payload.startswith(b"%PDF-")
    assert queried == ["M678", "M999"]
    assert [len(batch) for batch in rendered_batches] == [6]
    assert not list(export_root.rglob("*.png"))
    assert not list((export_root / "output" / "tmp").glob("aoi_rs_export_*"))
    with pymupdf.open(stream=payload, filetype="pdf") as report:
        assert len(report) == 2
        for page, product in zip(report, queried):
            assert page.rect.width > page.rect.height
            assert page.rect.width == pytest.approx(1191, abs=1)
            assert product in page.get_text()
            assert "11629" in page.get_text()
            assert "RS1" in page.get_text()
            _assert_three_column_row(page.get_image_info())
            # Every image survives after the source files have been cleaned up.
            assert all(report.extract_image(image[0])["image"] for image in page.get_images())
            assert page.get_pixmap().width > 0


def test_pdf_paginates_after_two_indicator_rows_and_preserves_all_renderer_batches(monkeypatch, rendered_batches):
    monkeypatch.setattr(
        aoi_rs_export, "iter_aoi_rs_chart_groups",
        lambda **kwargs: iter(_group(code=f"RS{index}") for index in range(9)),
    )

    payload = _build(("M678",), _report)

    assert [len(batch) for batch in rendered_batches] == [24, 3]
    with pymupdf.open(stream=payload, filetype="pdf") as report:
        assert len(report) == 5
        assert [len(page.get_image_info()) for page in report] == [6, 6, 6, 6, 3]
        for page in report:
            images = page.get_image_info()
            _assert_three_column_row(images[:3])
            if len(images) == 6:
                _assert_three_column_row(images[3:])
                assert images[0]["bbox"][3] < images[3]["bbox"][1]


def test_factory_filter_applies_to_all_frames_without_mutating_loaded_report(monkeypatch, rendered_batches):
    source = pd.DataFrame([
        {"prod_code": "M678", "factory": factory, "step_id": "11629", "rs_code": "RS1"}
        for factory in FACTORIES
    ])
    report = AoiRsReportViewModel(**{name: source.copy() for name in FRAME_NAMES})
    calls = []

    def groups(**kwargs):
        for name in FRAME_NAMES:
            assert kwargs[name]["factory"].tolist() == ["OLED"]
        return iter([_group(factory="OLED")])

    def load(product):
        calls.append(product)
        return report

    monkeypatch.setattr(aoi_rs_export, "iter_aoi_rs_chart_groups", groups)
    payload = _build(("M678",), load, factories=("OLED",))

    assert calls == ["M678"]
    for name in FRAME_NAMES:
        pd.testing.assert_frame_equal(getattr(report, name), source)
    with pymupdf.open(stream=payload, filetype="pdf") as pdf:
        assert "OLED" in pdf[0].get_text()
        assert "ARRAY" not in pdf[0].get_text()
        assert "TP" not in pdf[0].get_text()


def test_factory_change_starts_a_separate_page(monkeypatch, rendered_batches):
    monkeypatch.setattr(
        aoi_rs_export, "iter_aoi_rs_chart_groups",
        lambda **kwargs: iter([_group(factory="ARRAY"), _group(factory="OLED")]),
    )
    payload = _build(("M678",), _report)

    with pymupdf.open(stream=payload, filetype="pdf") as pdf:
        assert len(pdf) == 2
        assert "ARRAY" in pdf[0].get_text()
        assert "OLED" in pdf[1].get_text()
        assert all(len(page.get_image_info()) == 3 for page in pdf)


def test_unknown_factory_is_rejected_before_loading_reports(rendered_batches):
    def forbidden(_product):
        pytest.fail("Invalid factory must be rejected before application calls")

    with pytest.raises(aoi_rs_export.AoiRsExportError):
        _build(("M678",), forbidden, factories=("UNKNOWN",))
    assert not rendered_batches


def test_empty_products_are_documented_after_report_pages(rendered_batches):
    payload = _build(("EMPTY", "M678"), lambda product: _report(product, empty=product == "EMPTY"))

    with pymupdf.open(stream=payload, filetype="pdf") as pdf:
        assert len(pdf) == 2
        assert "M678" in pdf[0].get_text()
        assert len(pdf[0].get_image_info()) == 3
        assert "EMPTY" in pdf[-1].get_text()
        assert not pdf[-1].get_image_info()


@pytest.mark.parametrize("products", [(), ("EMPTY",)])
def test_all_empty_export_raises_instead_of_returning_an_empty_report(products, rendered_batches, export_root):
    with pytest.raises(aoi_rs_export.AoiRsExportError):
        _build(products, lambda product: _report(product, empty=True))
    assert not rendered_batches
    assert not list(export_root.rglob("aoi_rs_export_*"))


def test_product_load_failure_aborts_whole_export_and_cleans_resources(rendered_batches, export_root):
    def load(product):
        if product == "BROKEN":
            raise RuntimeError("query failed")
        return _report(product)

    with pytest.raises(RuntimeError, match="query failed"):
        _build(("M678", "BROKEN"), load)
    assert not list(export_root.rglob("aoi_rs_export_*"))


def test_renderer_failure_does_not_leave_temporary_images_or_partial_report(monkeypatch, rendered_batches, export_root):
    def fail(**kwargs):
        Path(kwargs["file"][0]).write_bytes(b"partial image")
        raise RuntimeError("renderer failed")

    monkeypatch.setattr(aoi_rs_export.pio, "write_images", fail)
    with pytest.raises(RuntimeError, match="renderer failed"):
        _build(("M678",), _report)
    assert not list(export_root.rglob("aoi_rs_export_*"))
    assert not list(export_root.rglob("*.png"))
    assert not list(export_root.rglob("*.pdf"))


def test_report_size_limit_raises_and_cleans_resources(monkeypatch, rendered_batches, export_root):
    monkeypatch.setattr(aoi_rs_export, "MAX_REPORT_BYTES", 1)
    with pytest.raises(aoi_rs_export.AoiRsExportError):
        _build(("M678",), _report)
    assert [len(batch) for batch in rendered_batches] == [3]
    assert not list(export_root.rglob("aoi_rs_export_*"))


@pytest.mark.parametrize("query_params", [{}, {"admin": "false"}, {"admin": "True"}])
def test_non_admin_export_returns_before_reading_configuration(monkeypatch, query_params):
    class PublicStreamlit:
        def __init__(self):
            self.query_params = query_params

        def __getattr__(self, name):
            pytest.fail(f"Non-admin export must not access Streamlit {name}")

    def forbidden():
        pytest.fail("Non-admin export must not read product configuration")

    monkeypatch.setattr(aoi_rs_export, "st", PublicStreamlit())
    monkeypatch.setattr(aoi_rs_export.ConfigLoader, "get_enabled_products", forbidden)

    aoi_rs_export.render_aoi_rs_export(
        current_product="M678",
        start_date=START,
        end_date=END,
        db_manager=object(),
        step_desc_map={},
    )


@pytest.mark.parametrize(
    ("previous", "expected"),
    [(None, ("M999", "M678")), ([], ("M999", "M678")), (["DISABLED", "M678"], ("M678",))],
)
def test_admin_selection_is_enabled_scoped_and_export_is_deferred(monkeypatch, previous, expected, export_statuses):
    downloads = []
    calls = []
    key = aoi_rs_export.PRODUCT_SELECTION_KEY
    state = {} if previous is None else {key: previous}

    def select(_label, *, options, key, **kwargs):
        if key == aoi_rs_export.PRODUCT_SELECTION_KEY:
            assert options == ["M999", "M678"]
            assert "DISABLED" not in state[key]
        else:
            assert key == aoi_rs_export.FACTORY_SELECTION_KEY
            assert tuple(options) == ("ARRAY", "OLED", "TP")
        return state[key]

    ui = SimpleNamespace(
        query_params={"admin": "true"},
        session_state=state,
        expander=lambda *args, **kwargs: nullcontext(),
        multiselect=select,
        caption=lambda *args: None,
        download_button=lambda *args, **kwargs: downloads.append(kwargs),
    )

    def build(products, **kwargs):
        assert kwargs["factories"] == FACTORIES
        calls.append(products)
        return b"complete pdf"

    monkeypatch.setattr(aoi_rs_export, "st", ui)
    monkeypatch.setattr(aoi_rs_export.ConfigLoader, "get_enabled_products", lambda: ["M999", "M678"])
    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", build)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    assert calls == []
    if previous is None:
        assert state[key] == ["M999", "M678"]
    assert state[aoi_rs_export.FACTORY_SELECTION_KEY] == list(FACTORIES)
    assert len(downloads) == 1
    assert downloads[0]["mime"] == "application/pdf"
    assert downloads[0]["file_name"].endswith(".pdf")
    assert downloads[0]["on_click"] == "ignore"
    assert callable(downloads[0]["data"])
    assert downloads[0]["data"]() == b"complete pdf"
    assert calls == [expected]


@pytest.fixture
def export_statuses(monkeypatch):
    statuses = []
    monkeypatch.setattr(aoi_rs_export, "_render_export_status", statuses.append)
    # Callback tests have no live Streamlit fragment to register the media reference.
    monkeypatch.setattr(aoi_rs_export._ExportStatus, "wait_download_ready", lambda self: True)
    return statuses


@pytest.fixture
def admin_downloads(monkeypatch, export_statuses):
    downloads = []
    key = aoi_rs_export.PRODUCT_SELECTION_KEY
    state = {key: []}
    ui = SimpleNamespace(
        query_params={"admin": "true"},
        session_state=state,
        expander=lambda *args, **kwargs: nullcontext(),
        multiselect=lambda _label, *, key, **kwargs: state[key],
        caption=lambda *args: None,
        download_button=lambda *args, **kwargs: downloads.append(kwargs),
    )
    monkeypatch.setattr(aoi_rs_export, "st", ui)
    monkeypatch.setattr(aoi_rs_export.ConfigLoader, "get_enabled_products", lambda: ["M999", "M678"])
    return downloads


def test_download_loads_each_product_with_its_own_query_and_cache_signatures(monkeypatch, admin_downloads):
    manager = object()
    ports = {product: object() for product in ("M999", "M678")}
    reports = {product: _report(product) for product in ports}
    repository_calls = []
    service_calls = []
    signature_calls = []
    returned_reports = []
    labels = {"11629": "测试站点"}

    def repository(db_manager, product):
        repository_calls.append((db_manager, product))
        return ports[product]

    def snapshot(namespace, product, scopes):
        signature_calls.append((namespace, product, scopes))
        return f"snapshot:{product}"

    def revision(scope, product):
        assert scope == "aoi_rs_sheet_oos"
        return f"revision:{product}"

    def decision(scope, product):
        assert scope == "aoi_rs"
        return f"decision:{product}"

    def report_service(**kwargs):
        service_calls.append(kwargs)
        query = json.loads(kwargs["query_config_json"])
        return reports[query["prod_code"]]

    def build(products, *, factories, load_report, start_date, end_date, step_desc_map):
        assert factories == ("ARRAY", "OLED", "TP")
        assert products == ("M999", "M678")
        assert (start_date, end_date) == (START, END)
        assert step_desc_map == labels
        returned_reports.extend(load_report(product) for product in products)
        return b"complete pdf"

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_repository", repository)
    monkeypatch.setattr(aoi_rs_export, "build_indicator_product_cache_signature", snapshot)
    monkeypatch.setattr(aoi_rs_export, "get_indicator_product_revision", revision)
    monkeypatch.setattr(aoi_rs_export, "get_scope_decision_signature", decision)
    monkeypatch.setattr(aoi_rs_export.AoiRsReportService, "get_aoi_rs_report_data", report_service)
    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", build)

    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=manager, step_desc_map=labels,
    )

    assert repository_calls == service_calls == signature_calls == []
    assert admin_downloads[0]["data"]() == b"complete pdf"
    assert repository_calls == [(manager, "M999"), (manager, "M678")]
    assert signature_calls == [
        ("aoi_rs_report_v1", product, ("aoi_rs_sheet_oos",)) for product in ports
    ]
    assert all(actual is reports[product] for actual, product in zip(returned_reports, ports))
    assert len(service_calls) == 2
    for call, product in zip(service_calls, ports):
        assert call["_data_port"] is ports[product]
        assert json.loads(call["query_config_json"]) == {
            "prod_code": product, "start_date": "2026-08-01", "end_date": "2026-09-22",
            "factory": None, "step_id": None, "rs_code": None,
        }
        assert call["snapshot_signature"] == f"snapshot:{product}"
        assert call["product_revision"] == f"revision:{product}"
        assert call["decision_signature"] == f"decision:{product}"


def test_download_failure_hides_raw_exception_details(monkeypatch, admin_downloads, export_statuses):
    logged = []
    synthetic_detail = r"SYNTHETIC_SECRET in C:\private\synthetic-config.yaml"

    def fail(*args, **kwargs):
        assert export_statuses[0].get()[0] == "busy"
        raise OSError(synthetic_detail)

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", fail)
    monkeypatch.setattr(
        aoi_rs_export, "logger",
        SimpleNamespace(exception=lambda *args: logged.append(args)),
    )
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    with pytest.raises(RuntimeError) as raised:
        admin_downloads[0]["data"]()

    assert "RS 报告导出失败" in str(raised.value)
    assert "SYNTHETIC_SECRET" not in str(raised.value)
    assert "synthetic-config.yaml" not in str(raised.value)
    assert raised.value.__suppress_context__ is True
    assert raised.value.__cause__ is None
    assert len(logged) == 1
    kind, message = export_statuses[0].get()
    assert kind == "error"
    assert message == str(raised.value)
    assert "SYNTHETIC_SECRET" not in message
    assert "synthetic-config.yaml" not in message


def test_download_status_transitions_from_idle_through_busy_to_success(monkeypatch, admin_downloads, export_statuses):
    def build(*args, **kwargs):
        kind, message = export_statuses[0].get()
        assert kind == "busy"
        assert "正在生成" in message
        return b"complete pdf"

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", build)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    assert len(export_statuses) == 1
    assert export_statuses[0].get() == ("idle", "")
    assert admin_downloads[0]["data"]() == b"complete pdf"
    kind, message = export_statuses[0].get()
    assert kind == "success"
    assert "已生成" in message


def test_download_empty_data_shows_specific_safe_error(monkeypatch, admin_downloads, export_statuses):
    message = "所选产品暂无可导出的 RS 图像。"

    def empty(*args, **kwargs):
        assert export_statuses[0].get()[0] == "busy"
        raise aoi_rs_export.AoiRsExportError(message)

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", empty)
    monkeypatch.setattr(aoi_rs_export, "logger", SimpleNamespace(exception=lambda *args: None))
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    with pytest.raises(RuntimeError, match=message) as raised:
        admin_downloads[0]["data"]()

    assert export_statuses[0].get() == ("error", message)
    assert raised.value.__suppress_context__ is True


def test_download_failure_after_rerun_updates_the_same_visible_status(monkeypatch, admin_downloads, export_statuses):
    entered = Event()
    release = Event()
    message = "所选产品暂无可导出的 RS 图像。"

    def blocked_build(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=5)
        raise aoi_rs_export.AoiRsExportError(message)

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", blocked_build)
    monkeypatch.setattr(aoi_rs_export, "logger", SimpleNamespace(exception=lambda *args: None))
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        old_download = executor.submit(admin_downloads[0]["data"])
        try:
            assert entered.wait(timeout=5)
            assert export_statuses[0].get()[0] == "busy"
            aoi_rs_export.st.session_state[aoi_rs_export.PRODUCT_SELECTION_KEY] = ["M999"]
            aoi_rs_export.render_aoi_rs_export(
                current_product="M999", start_date=START, end_date=END,
                db_manager=object(), step_desc_map={},
            )
            assert len(export_statuses) == 2
            assert export_statuses[1] is export_statuses[0]
            assert export_statuses[1].get()[0] == "busy"
        finally:
            release.set()
        with pytest.raises(RuntimeError, match=message):
            old_download.result(timeout=5)

    assert export_statuses[1].get() == ("error", message)


def test_second_download_during_active_export_does_not_start_another_renderer(monkeypatch, admin_downloads, export_statuses):
    entered = Event()
    release = Event()
    builds = []

    def blocked_build(products, **kwargs):
        builds.append(products)
        entered.set()
        assert release.wait(timeout=5)
        return b"complete pdf"

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", blocked_build)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        first_download = executor.submit(admin_downloads[0]["data"])
        try:
            assert entered.wait(timeout=5)
            aoi_rs_export.render_aoi_rs_export(
                current_product="M678", start_date=START, end_date=END,
                db_manager=object(), step_desc_map={},
            )
            with pytest.raises(RuntimeError, match="正在生成"):
                admin_downloads[1]["data"]()
            assert builds == [("M999", "M678")]
            assert export_statuses[1].get()[0] == "busy"
        finally:
            release.set()
        assert first_download.result(timeout=5) == b"complete pdf"

    assert export_statuses[1].get()[0] == "success"


@pytest.mark.parametrize("selection", [["OLED"], []])
def test_download_captures_factory_selection_before_background_execution(monkeypatch, admin_downloads, selection):
    factories_key = aoi_rs_export.FACTORY_SELECTION_KEY
    aoi_rs_export.st.session_state[factories_key] = selection
    captured = []

    def build(products, *, factories, **kwargs):
        captured.append(factories)
        return b"complete pdf"

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", build)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )
    aoi_rs_export.st.session_state[factories_key] = ["TP"]
    assert admin_downloads[0]["data"]() == b"complete pdf"
    assert captured == [tuple(selection) if selection else FACTORIES]


def test_pdf_embedding_failure_cleans_images_from_the_failed_attempt(monkeypatch, rendered_batches, export_root):
    def invalid_images(*, fig, file, **kwargs):
        for filename in file:
            Path(filename).write_bytes(b"invalid PNG")

    monkeypatch.setattr(aoi_rs_export.pio, "write_images", invalid_images)
    with pytest.raises(pymupdf.mupdf.FzErrorFormat):
        _build(("M678",), _report)
    assert not list(export_root.rglob("aoi_rs_export_*"))
    assert not list(export_root.rglob("*.png"))


def test_report_styling_does_not_mutate_figures_used_by_the_page(monkeypatch, rendered_batches):
    group = _group()
    originals = [figure.to_json() for _, figure in group.figures]
    monkeypatch.setattr(aoi_rs_export, "iter_aoi_rs_chart_groups", lambda **kwargs: iter([group]))

    _build(("M678",), _report)

    assert [figure.to_json() for _, figure in group.figures] == originals


def test_deferred_download_waits_for_static_pdf_registration_before_returning(monkeypatch):
    downloads = []
    statuses = []
    waiting = Event()
    state = {}
    pdf = b"%PDF-complete report"
    filename = "AOI_RS_2026-08-01_2026-09-22.pdf"
    original_wait = aoi_rs_export._ExportStatus.wait_download_ready

    def wait_for_registration(status):
        waiting.set()
        return original_wait(status)

    def download_button(_label, **kwargs):
        if isinstance(kwargs["data"], bytes):
            status = statuses[0]
            assert status.get()[0] == "busy"
            assert not status._download_ready.is_set()
            assert kwargs["data"] == pdf
            assert kwargs["file_name"] == filename
            assert kwargs["mime"] == "application/pdf"
        downloads.append(kwargs)

    ui = SimpleNamespace(
        query_params={"admin": "true"}, session_state=state,
        expander=lambda *args, **kwargs: nullcontext(),
        multiselect=lambda _label, *, key, **kwargs: state[key],
        caption=lambda *args: None,
        download_button=download_button,
    )
    monkeypatch.setattr(aoi_rs_export, "st", ui)
    monkeypatch.setattr(aoi_rs_export.ConfigLoader, "get_enabled_products", lambda: ["M678"])
    monkeypatch.setattr(aoi_rs_export, "_render_export_status", statuses.append)
    monkeypatch.setattr(aoi_rs_export._ExportStatus, "wait_download_ready", wait_for_registration)
    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", lambda *args, **kwargs: pdf)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )
    status = statuses[0]
    assert status.get_pdf() is None
    assert not status._download_ready.is_set()
    RENDER_EXPORT_STATUS(status)
    assert len(downloads) == 1
    assert not status._download_ready.is_set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        download = executor.submit(downloads[0]["data"])
        try:
            assert waiting.wait(timeout=5)
            prepared = status.get_pdf()
            assert (prepared.data, prepared.filename) == (pdf, filename)
            assert status.get()[0] == "busy"
            assert not download.done()
            with pytest.raises(RuntimeError, match="正在生成"):
                downloads[0]["data"]()
            RENDER_EXPORT_STATUS(status)
            assert len(downloads) == 2
            assert status._download_ready.is_set()
            assert download.result(timeout=5) == pdf
        finally:
            status.mark_download_ready(status.get_pdf())
    assert status.get()[0] == "success"


def test_registration_timeout_retains_pdf_for_static_download(monkeypatch, admin_downloads, export_statuses):
    pdf = b"%PDF-complete report"
    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", lambda *args, **kwargs: pdf)
    monkeypatch.setattr(aoi_rs_export._ExportStatus, "wait_download_ready", lambda self: False)
    monkeypatch.setattr(aoi_rs_export, "logger", SimpleNamespace(exception=lambda *args: None))
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )
    with pytest.raises(RuntimeError, match="报告已生成") as raised:
        admin_downloads[0]["data"]()
    status = export_statuses[0]
    assert status.get() == ("error", str(raised.value))
    prepared = status.get_pdf()
    assert (prepared.data, prepared.filename) == (pdf, admin_downloads[0]["file_name"])
    errors = []
    monkeypatch.setattr(aoi_rs_export.st, "error", errors.append, raising=False)
    RENDER_EXPORT_STATUS(status)
    assert admin_downloads[1]["data"] == pdf
    assert admin_downloads[1]["file_name"] == admin_downloads[0]["file_name"]
    assert admin_downloads[1]["mime"] == "application/pdf"
    assert status._download_ready.is_set()
    assert errors == [str(raised.value)]


def test_old_fragment_cannot_release_new_pdf_download():
    status = aoi_rs_export._ExportStatus()
    assert status.start()
    status.publish(b"old report", "old.pdf")
    old_pdf = status.get_pdf()
    status.mark_download_ready(old_pdf)
    assert status.wait_download_ready()
    status.set("success", "completed")

    assert status.start()
    assert status.get_pdf() is old_pdf
    status.publish(b"new report", "new.pdf")
    new_pdf = status.get_pdf()
    assert new_pdf.ready is not old_pdf.ready
    status.mark_download_ready(old_pdf)
    assert old_pdf.ready.is_set()
    assert not new_pdf.ready.is_set()
    assert status.get()[0] == "busy"
    status.mark_download_ready(new_pdf)
    assert status.wait_download_ready()


def test_failed_rebuild_keeps_last_completed_report_downloadable(monkeypatch, admin_downloads, export_statuses):
    pdf = b"last completed PDF"
    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", lambda *args, **kwargs: pdf)
    aoi_rs_export.render_aoi_rs_export(
        current_product="M678", start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )
    assert admin_downloads[0]["data"]() == pdf
    previous = export_statuses[0].get_pdf()

    def failed_build(*args, **kwargs):
        assert export_statuses[0].get_pdf() is previous
        raise OSError("failed rebuild")

    monkeypatch.setattr(aoi_rs_export, "build_aoi_rs_pdf", failed_build)
    monkeypatch.setattr(aoi_rs_export, "logger", SimpleNamespace(exception=lambda *args: None))
    with pytest.raises(RuntimeError, match="RS 报告导出失败"):
        admin_downloads[0]["data"]()
    assert export_statuses[0].get_pdf() is previous
    monkeypatch.setattr(aoi_rs_export.st, "error", lambda *args: None, raising=False)
    RENDER_EXPORT_STATUS(export_statuses[0])
    assert admin_downloads[1]["data"] == pdf
