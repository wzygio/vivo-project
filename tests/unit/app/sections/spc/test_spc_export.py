"""SPC PDF exports preserve main-chart scope and explicit generation/download steps."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pandas as pd
import plotly.graph_objects as go
import pymupdf
import pytest

from app.sections.inline_domain.spc import spc_export
from src.inline_domain.application.spc.spc_service import SpcReportViewModel

START = date(2026, 8, 1)
END = date(2026, 9, 22)
FACTORIES = ("ARRAY", "OLED", "TP")
FRAME_NAMES = (
    "period_capability_df", "sheet_features_df", "raw_measurements_df", "indicators_df",
)


def _report(product: str, *, empty: bool = False) -> SpcReportViewModel:
    indicators = pd.DataFrame() if empty else pd.DataFrame(
        [{"prod_code": product, "factory": "ARRAY", "step_id": "11629", "param_name": "RS1"}]
    )
    return SpcReportViewModel(
        period_capability_df=indicators.copy(),
        sheet_features_df=indicators.copy(),
        raw_measurements_df=indicators.copy(),
        indicators_df=indicators,
    )


def _group(*, code: str = "RS1", factory: str = "ARRAY") -> SimpleNamespace:
    return SimpleNamespace(
        factory=factory,
        title=f"{code} | 站点 11629",
        figures=tuple(go.Figure(go.Scatter(x=[1, 2], y=[1, 2])) for _ in range(3)),
    )


@pytest.fixture
def export_root(monkeypatch, tmp_path):
    monkeypatch.setattr(spc_export.ConfigLoader, "get_project_root", lambda: tmp_path)
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
        assert kwargs["reference_date"] == END
        if kwargs["period_capability_df"].empty:
            return iter(())
        return iter([_group()])

    monkeypatch.setattr(spc_export.pio, "write_images", write_images)
    monkeypatch.setattr(spc_export, "iter_spc_chart_groups", groups)
    return batches


def _build(products, load_report, *, factories=FACTORIES):
    return spc_export.build_spc_pdf(
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
    assert not list((export_root / "output" / "tmp").glob("spc_export_*"))
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
        spc_export, "iter_spc_chart_groups",
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
        {"prod_code": "M678", "factory": factory, "step_id": "11629", "param_name": "RS1"}
        for factory in FACTORIES
    ])
    report = SpcReportViewModel(**{name: source.copy() for name in FRAME_NAMES})
    calls = []

    def groups(**kwargs):
        for name in FRAME_NAMES[:-1]:
            assert kwargs[name]["factory"].tolist() == ["OLED"]
        return iter([_group(factory="OLED")])

    def load(product):
        calls.append(product)
        return report

    monkeypatch.setattr(spc_export, "iter_spc_chart_groups", groups)
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
        spc_export, "iter_spc_chart_groups",
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

    with pytest.raises(spc_export.SpcExportError):
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
    with pytest.raises(spc_export.SpcExportError):
        _build(products, lambda product: _report(product, empty=True))
    assert not rendered_batches
    assert not list(export_root.rglob("spc_export_*"))


def test_product_load_failure_aborts_whole_export_and_cleans_resources(rendered_batches, export_root):
    def load(product):
        if product == "BROKEN":
            raise RuntimeError("query failed")
        return _report(product)

    with pytest.raises(RuntimeError, match="query failed"):
        _build(("M678", "BROKEN"), load)
    assert not list(export_root.rglob("spc_export_*"))


def test_renderer_failure_does_not_leave_temporary_images_or_partial_report(monkeypatch, rendered_batches, export_root):
    def fail(**kwargs):
        Path(kwargs["file"][0]).write_bytes(b"partial image")
        raise RuntimeError("renderer failed")

    monkeypatch.setattr(spc_export.pio, "write_images", fail)
    with pytest.raises(RuntimeError, match="renderer failed"):
        _build(("M678",), _report)
    assert not list(export_root.rglob("spc_export_*"))
    assert not list(export_root.rglob("*.png"))
    assert not list(export_root.rglob("*.pdf"))


def test_report_size_limit_raises_and_cleans_resources(monkeypatch, rendered_batches, export_root):
    monkeypatch.setattr(spc_export, "MAX_REPORT_BYTES", 1)
    with pytest.raises(spc_export.SpcExportError):
        _build(("M678",), _report)
    assert [len(batch) for batch in rendered_batches] == [3]
    assert not list(export_root.rglob("spc_export_*"))


@pytest.mark.parametrize("query_params", [{}, {"admin": "false"}, {"admin": "True"}])
def test_non_admin_export_returns_before_reading_configuration(monkeypatch, query_params):
    class PublicStreamlit:
        def __init__(self):
            self.query_params = query_params

        def __getattr__(self, name):
            pytest.fail(f"Non-admin export must not access Streamlit {name}")

    def forbidden():
        pytest.fail("Non-admin export must not read product configuration")

    monkeypatch.setattr(spc_export, "st", PublicStreamlit())
    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", forbidden)

    spc_export.render_spc_export(
        start_date=START,
        end_date=END,
        db_manager=object(),
        step_desc_map={},
    )


@pytest.fixture
def admin_ui(monkeypatch):
    buttons, downloads, errors = [], [], []
    state = {}
    ui = SimpleNamespace(
        query_params={"admin": "true"}, session_state=state,
        expander=lambda *args, **kwargs: nullcontext(),
        spinner=lambda *args, **kwargs: nullcontext(),
        multiselect=lambda _label, *, key, **kwargs: state[key],
        caption=lambda *args: None,
        success=lambda *args: None,
        error=errors.append,
        button=lambda label, **kwargs: buttons.append({"label": label, **kwargs}),
        download_button=lambda label, **kwargs: downloads.append({"label": label, **kwargs}),
    )
    monkeypatch.setattr(spc_export, "st", ui)
    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", lambda: ["M999", "M678"])
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_period_sigma_source", lambda: "point_value")
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_period_box_source", lambda: "point_value")
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_line_chart_param_name_contains", list)
    harness = SimpleNamespace(buttons=buttons, downloads=downloads, errors=errors, state=state)
    return harness


def _render_admin(admin_ui):
    spc_export.render_spc_export(
        start_date=START, end_date=END,
        db_manager=object(), step_desc_map={},
    )
    admin_ui.status = admin_ui.state[spc_export.EXPORT_STATUS_KEY]


@pytest.mark.parametrize(
    ("previous", "expected"),
    [(None, ("M999", "M678")), ([], ("M999", "M678")), (["DISABLED", "M678"], ("M678",))],
)
def test_admin_selection_defaults_and_two_step_generation(monkeypatch, admin_ui, previous, expected):
    if previous is not None:
        admin_ui.state[spc_export.PRODUCT_SELECTION_KEY] = previous
    calls = []

    def build(products, **kwargs):
        calls.append((products, kwargs["factories"]))
        return b"complete pdf"

    monkeypatch.setattr(spc_export, "build_spc_pdf", build)
    _render_admin(admin_ui)
    assert calls == []
    assert admin_ui.downloads == []
    assert len(admin_ui.buttons) == 1
    assert admin_ui.buttons[0]["label"] == "生成报告"
    assert admin_ui.state[spc_export.FACTORY_SELECTION_KEY] == ["ARRAY"]
    if previous is None:
        assert admin_ui.state[spc_export.PRODUCT_SELECTION_KEY] == ["M999", "M678"]
    assert "DISABLED" not in admin_ui.state[spc_export.PRODUCT_SELECTION_KEY]
    assert admin_ui.buttons[0]["on_click"]() is None
    assert calls == [(expected, ("ARRAY",))]
    _render_admin(admin_ui)
    download = admin_ui.downloads[0]
    assert download["label"] == "下载报告（PDF）"
    assert download["data"] == b"complete pdf"
    assert download["mime"] == "application/pdf"
    assert download["file_name"].endswith(".pdf")
    assert download["on_click"] == "ignore"
    _render_admin(admin_ui)
    assert calls == [(expected, ("ARRAY",))]


@pytest.mark.parametrize("selection", [["OLED"], ["ARRAY", "TP"], []])
def test_generation_uses_selected_factories(monkeypatch, admin_ui, selection):
    admin_ui.state[spc_export.FACTORY_SELECTION_KEY] = selection
    captured = []

    def build(products, *, factories, **kwargs):
        captured.append(factories)
        return b"complete pdf"

    monkeypatch.setattr(spc_export, "build_spc_pdf", build)
    _render_admin(admin_ui)
    assert admin_ui.buttons[0]["on_click"]() is None
    assert captured == [tuple(selection) if selection else FACTORIES]


def test_stale_factories_are_removed_without_resetting_the_user_selection(admin_ui):
    admin_ui.state[spc_export.FACTORY_SELECTION_KEY] = ["UNKNOWN", "OLED"]

    _render_admin(admin_ui)

    assert admin_ui.state[spc_export.FACTORY_SELECTION_KEY] == ["OLED"]
    assert not admin_ui.downloads


def test_generation_failure_shows_safe_status_without_raising(monkeypatch, admin_ui):
    logged = []

    def fail(*args, **kwargs):
        assert admin_ui.status["kind"] == "busy"
        raise OSError(r"SYNTHETIC_SECRET in C:\private\synthetic-config.yaml")

    monkeypatch.setattr(spc_export, "build_spc_pdf", fail)
    monkeypatch.setattr(spc_export, "logger", SimpleNamespace(exception=lambda *args: logged.append(args)))
    _render_admin(admin_ui)
    assert admin_ui.buttons[0]["on_click"]() is None
    assert len(logged) == 1
    kind, message = admin_ui.status["kind"], admin_ui.status["message"]
    assert kind == "error"
    assert "失败" in message
    assert "SYNTHETIC_SECRET" not in message
    assert "synthetic-config.yaml" not in message
    _render_admin(admin_ui)
    assert admin_ui.errors[-1] == message
    assert admin_ui.downloads == []


def test_generation_empty_data_preserves_specific_safe_message(monkeypatch, admin_ui):
    message = "所选产品和厂别暂无可导出的 SPC 数据。"

    def fail(*args, **kwargs):
        raise spc_export.SpcExportError(message)

    monkeypatch.setattr(spc_export, "build_spc_pdf", fail)
    monkeypatch.setattr(spc_export, "logger", SimpleNamespace(exception=lambda *args: None))
    _render_admin(admin_ui)
    assert admin_ui.buttons[0]["on_click"]() is None
    assert (admin_ui.status["kind"], admin_ui.status["message"]) == ("error", message)


def test_generation_transitions_from_idle_through_busy_to_success(monkeypatch, admin_ui):
    def build(*args, **kwargs):
        kind, message = admin_ui.status["kind"], admin_ui.status["message"]
        assert kind == "busy"
        assert "正在生成" in message
        return b"complete pdf"

    monkeypatch.setattr(spc_export, "build_spc_pdf", build)
    _render_admin(admin_ui)
    assert (admin_ui.status["kind"], admin_ui.status["message"]) == ("idle", "")
    assert admin_ui.buttons[0]["on_click"]() is None
    assert admin_ui.status["kind"] == "success"
    assert admin_ui.status["data"] == b"complete pdf"


def test_generation_reentry_does_not_start_second_renderer(monkeypatch, admin_ui):
    entered, release = Event(), Event()
    builds = []

    def build(products, **kwargs):
        builds.append(products)
        entered.set()
        assert release.wait(timeout=5)
        return b"complete pdf"

    monkeypatch.setattr(spc_export, "build_spc_pdf", build)
    _render_admin(admin_ui)
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(admin_ui.buttons[0]["on_click"])
        try:
            assert entered.wait(timeout=5)
            assert admin_ui.buttons[0]["on_click"]() is None
            assert builds == [("M999", "M678")]
            assert admin_ui.status["kind"] == "busy"
        finally:
            release.set()
        assert first.result(timeout=5) is None
    assert admin_ui.status["kind"] == "success"


def test_failed_rebuild_keeps_last_completed_report_downloadable(monkeypatch, admin_ui):
    monkeypatch.setattr(spc_export, "build_spc_pdf", lambda *args, **kwargs: b"last completed PDF")
    _render_admin(admin_ui)
    assert admin_ui.buttons[0]["on_click"]() is None
    previous = admin_ui.status["data"]

    def fail(*args, **kwargs):
        assert admin_ui.status["data"] is previous
        raise OSError("failed rebuild")

    monkeypatch.setattr(spc_export, "build_spc_pdf", fail)
    monkeypatch.setattr(spc_export, "logger", SimpleNamespace(exception=lambda *args: None))
    assert admin_ui.buttons[0]["on_click"]() is None
    assert admin_ui.status["data"] is previous
    _render_admin(admin_ui)
    assert admin_ui.downloads[-1]["data"] == b"last completed PDF"


def test_disabling_a_product_withdraws_the_completed_report(monkeypatch, admin_ui):
    monkeypatch.setattr(spc_export, "build_spc_pdf", lambda *args, **kwargs: b"previous PDF")
    _render_admin(admin_ui)
    admin_ui.buttons[0]["on_click"]()
    _render_admin(admin_ui)
    assert admin_ui.downloads[-1]["data"] == b"previous PDF"
    admin_ui.downloads.clear()
    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", lambda: ["M678"])

    _render_admin(admin_ui)

    assert admin_ui.state[spc_export.PRODUCT_SELECTION_KEY] == ["M678"]
    assert admin_ui.status["data"] is None
    assert not admin_ui.downloads
    assert admin_ui.status["message"] == "产品范围已变化，请重新生成报告。"
    assert admin_ui.errors[-1] == "产品范围已变化，请重新生成报告。"


def test_stopped_generation_preserves_previous_report_and_allows_retry(monkeypatch, admin_ui):
    from streamlit.runtime.scriptrunner_utils.exceptions import StopException

    monkeypatch.setattr(spc_export, "build_spc_pdf", lambda *args, **kwargs: b"previous PDF")
    _render_admin(admin_ui)
    generate = admin_ui.buttons[0]["on_click"]
    assert generate() is None
    previous = admin_ui.status["data"]

    def stopped_build(*args, **kwargs):
        assert admin_ui.status["kind"] == "busy"
        raise StopException()

    monkeypatch.setattr(spc_export, "build_spc_pdf", stopped_build)
    with pytest.raises(StopException):
        generate()
    assert (admin_ui.status["kind"], admin_ui.status["message"]) == ("error", "报告生成已中断，请重新生成。")
    assert admin_ui.status["data"] is previous
    monkeypatch.setattr(spc_export, "build_spc_pdf", lambda *args, **kwargs: b"replacement PDF")
    assert generate() is None
    assert admin_ui.status["kind"] == "success"
    assert admin_ui.status["data"] == b"replacement PDF"


def _export_app_test():
    from streamlit.testing.v1 import AppTest

    return AppTest.from_string("""
from datetime import date
from app.sections.inline_domain.spc.spc_export import render_spc_export
render_spc_export(
    start_date=date(2026, 8, 1), end_date=date(2026, 9, 22),
    db_manager=None, step_desc_map={},
)
""")


def test_streamlit_two_step_generation_and_multiselect_survive_rerun(monkeypatch):
    calls = []
    pdf = b"%PDF-report fixture"

    def build(products, *, factories, **kwargs):
        calls.append((products, factories))
        return pdf

    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", lambda: ["M999", "M678"])
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_period_sigma_source", lambda: "point_value")
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_period_box_source", lambda: "point_value")
    monkeypatch.setattr(spc_export.ConfigLoader, "get_spc_line_chart_param_name_contains", list)
    monkeypatch.setattr(spc_export, "build_spc_pdf", build)
    app = _export_app_test()
    app.query_params["admin"] = "true"
    app.run()
    assert not app.exception
    assert app.multiselect(key=spc_export.PRODUCT_SELECTION_KEY).value == ["M999", "M678"]
    factories = app.multiselect(key=spc_export.FACTORY_SELECTION_KEY)
    assert factories.value == ["ARRAY"]
    assert factories.options == list(FACTORIES)
    assert len(app.get("download_button")) == 0
    assert calls == []
    factories.set_value(["ARRAY", "OLED"]).run()
    assert not app.exception
    assert calls == []
    app.button[0].click().run()
    assert not app.exception
    assert calls == [(("M999", "M678"), ("ARRAY", "OLED"))]
    downloads = app.get("download_button")
    assert len(downloads) == 1
    assert downloads[0].proto.label == "下载报告（PDF）"
    assert downloads[0].proto.url
    assert app.session_state[spc_export.EXPORT_STATUS_KEY]["data"] == pdf
    app.run()
    assert not app.exception
    assert len(app.get("download_button")) == 1
    assert len(calls) == 1
    app.multiselect(key=spc_export.FACTORY_SELECTION_KEY).set_value(["TP"]).run()
    assert not app.exception
    assert len(calls) == 1
    completed_scope = app.session_state[spc_export.EXPORT_STATUS_KEY]["scope"]
    assert "ARRAY、OLED" in completed_scope
    assert "TP" not in completed_scope
    assert any(caption.value == completed_scope for caption in app.caption)
    app.multiselect(key=spc_export.PRODUCT_SELECTION_KEY).set_value(["M678"]).run()
    assert not app.exception
    assert len(calls) == 1
    assert len(app.get("download_button")) == 1
    assert app.session_state[spc_export.EXPORT_STATUS_KEY]["scope"] == completed_scope


def test_streamlit_public_mode_has_no_export_controls(monkeypatch):
    def forbidden():
        pytest.fail("Public page must not load export configuration")

    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", forbidden)
    app = _export_app_test().run()
    assert not app.exception
    assert not app.button
    assert not app.multiselect
    assert not app.get("download_button")


def test_streamlit_generation_error_renders_safe_message_without_download(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("SYNTHETIC_SECRET")

    monkeypatch.setattr(spc_export.ConfigLoader, "get_enabled_products", lambda: ["M678"])
    monkeypatch.setattr(spc_export, "build_spc_pdf", fail)
    monkeypatch.setattr(spc_export, "logger", SimpleNamespace(exception=lambda *args: None))
    app = _export_app_test()
    app.query_params["admin"] = "true"
    app.run()
    app.button[0].click().run()
    assert not app.exception
    assert len(app.error) == 1
    assert app.error[0].value == "SPC 报告生成失败，请稍后重试。"
    assert not app.get("download_button")


def test_generation_loads_each_product_with_its_own_query_and_cache_signatures(monkeypatch, admin_ui):
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

    def decision(scope, product):
        assert scope == "spc"
        return f"decision:{product}"

    def report_service(**kwargs):
        service_calls.append(kwargs)
        query = json.loads(kwargs["query_config_json"])
        return reports[query["prod_code"]]

    def build(products, *, factories, load_report, start_date, end_date, step_desc_map, **kwargs):
        assert factories == ("ARRAY",)
        assert products == ("M999", "M678")
        assert (start_date, end_date) == (START, END)
        assert step_desc_map == labels
        assert kwargs["period_box_source"] == "point_value"
        assert tuple(kwargs["line_param_name_contains"]) == ()
        returned_reports.extend(load_report(product) for product in products)
        return b"complete pdf"

    monkeypatch.setattr(spc_export, "build_spc_repository", repository)
    monkeypatch.setattr(spc_export, "build_indicator_product_cache_signature", snapshot)
    monkeypatch.setattr(spc_export, "get_scope_decision_signature", decision)
    monkeypatch.setattr(spc_export.SpcReportService, "get_spc_report_data", report_service)
    monkeypatch.setattr(spc_export, "build_spc_pdf", build)

    spc_export.render_spc_export(
        start_date=START, end_date=END,
        db_manager=manager, step_desc_map=labels,
    )

    admin_ui.status = admin_ui.state[spc_export.EXPORT_STATUS_KEY]
    assert repository_calls == service_calls == signature_calls == []
    assert admin_ui.buttons[0]["on_click"]() is None
    assert admin_ui.status["data"] == b"complete pdf"
    assert repository_calls == [(manager, "M999"), (manager, "M678")]
    assert signature_calls == [
        ("spc_database_target_v4", product, ("spc_sheet_oos", "spc_cpk_trend")) for product in ports
    ]
    assert all(actual is reports[product] for actual, product in zip(returned_reports, ports))
    assert len(service_calls) == 2
    for call, product in zip(service_calls, ports):
        assert call["_data_port"] is ports[product]
        assert json.loads(call["query_config_json"]) == {
            "prod_code": product, "start_date": "2026-08-01", "end_date": "2026-09-22",
            "factory": None, "step_id": None, "param_name": None, "data_type_filter": "SPC",
        }
        assert call["snapshot_signature"] == f"snapshot:{product}"
        assert call["product_revision"] == f"snapshot:{product}"
        assert call["decision_signature"] == f"decision:{product}"
        assert call["period_sigma_source"] == "point_value"


def test_pdf_embedding_failure_cleans_images_from_the_failed_attempt(monkeypatch, rendered_batches, export_root):
    def invalid_images(*, fig, file, **kwargs):
        for filename in file:
            Path(filename).write_bytes(b"invalid PNG")

    monkeypatch.setattr(spc_export.pio, "write_images", invalid_images)
    with pytest.raises(pymupdf.mupdf.FzErrorFormat):
        _build(("M678",), _report)
    assert not list(export_root.rglob("spc_export_*"))
    assert not list(export_root.rglob("*.png"))


def test_report_styling_does_not_mutate_figures_used_by_the_page(monkeypatch, rendered_batches):
    group = _group()
    originals = [figure.to_json() for figure in group.figures]
    monkeypatch.setattr(spc_export, "iter_spc_chart_groups", lambda **kwargs: iter([group]))

    _build(("M678",), _report)

    assert [figure.to_json() for figure in group.figures] == originals
