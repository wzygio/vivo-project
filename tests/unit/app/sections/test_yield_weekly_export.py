from datetime import date
from io import BytesIO

import pandas as pd
from PIL import Image
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.util import Pt
from streamlit.testing.v1 import AppTest

from app.charts.yield_domain import weekly_ppt
from app.sections.yield_domain import weekly_export
from src.yield_domain.application.weekly_report import ReportWeek


def test_static_chart_windows_match_dashboard_windows():
    windows = {
        "monthly": ("2026-08", "2026-09", "2026-10", "2026-11"),
        "weekly": ("2026-W45", "2026-W46", "2026-W47", "2026-W48"),
        "daily": tuple(f"2026-11-{day}" for day in range(15, 25)),
    }
    trends = {
        key: pd.DataFrame({
            "time_period": periods, "defect_group": "Group A", "defect_rate": 0.02, "total_panels": 100,
        })
        for key, periods in windows.items()
    }
    month, week, day = weekly_ppt._static_charts(trends)
    for fig, (key, limit) in zip((month, week, day), (("monthly", 3), ("weekly", 3), ("daily", 7))):
        assert tuple(fig.data[0].x) == windows[key][-limit:]
        assert list(fig.data[0].y) == [0.02] * limit
        assert tuple(fig.layout.yaxis.range) == (0, 0.024)


def test_deck_preserves_cover_and_fills_two_product_slots(monkeypatch, tmp_path):
    template = Presentation()
    layout = template.slide_layouts[6]
    cover, body, example = [template.slides.add_slide(layout) for _ in range(3)]
    for slide, label in [(cover, "封面"), (example, "样例")]:
        slide.shapes.add_textbox(Pt(0), Pt(0), Pt(500), Pt(30)).text = label
    for index, (title, summary, chart) in enumerate(weekly_ppt._SLOT_NAMES):
        top = 10 + index * 230
        for name, y, height in [(title, top, 20), (summary, top + 20, 20), (chart, top + 40, 180)]:
            shape = body.shapes.add_textbox(Pt(0), Pt(y), Pt(600), Pt(height))
            shape.name, shape.text = name, "{placeholder}"
    template_bytes = BytesIO()
    template.save(template_bytes)
    png = BytesIO()
    Image.new("RGB", (20, 10), "white").save(png, format="PNG")
    monkeypatch.setattr(weekly_ppt, "_render_charts", lambda figures, _: [png.getvalue()] * len(figures))

    conclusions = {"A": "正常", "B": "CODE-规则", "C": "LOT-CODE超规"}
    result = weekly_ppt.build_weekly_yield_ppt(
        conclusions, dict.fromkeys(conclusions), week_label="2026-W47",
        template_bytes=template_bytes.getvalue(), temp_root=tmp_path,
    )
    slides = Presentation(BytesIO(result)).slides

    assert len(slides) == 3
    assert slides[0]._element.xml == cover._element.xml
    for slide, products in [(slides[1], ("A", "B")), (slides[2], ("C",))]:
        texts = {shape.text for shape in slide.shapes if shape.has_text_frame}
        assert texts == {
            text for product in products
            for text in (f"{product}不良率", f"{product} - 2026-W47不良率，{conclusions[product]}")
        }
        assert sum(shape.shape_type == MSO_SHAPE_TYPE.PICTURE for shape in slide.shapes) == 3 * len(products)


def _export_app():
    from app.sections.yield_domain.weekly_export import render_yield_weekly_export

    render_yield_weekly_export()


def test_download_follows_success_and_is_removed_after_failure(monkeypatch):
    products = ["M678", "Z622"]
    monkeypatch.setattr(weekly_export.ConfigLoader, "get_enabled_products", lambda **_: products)
    monkeypatch.setattr(weekly_export, "_input_revision", lambda _: ("v1",))
    calls = []

    def generate(**kwargs):
        calls.append(kwargs)
        if len(calls) > 1:
            raise ValueError("internal path")
        return weekly_export.WeeklyYieldExport(ReportWeek.previous(), tuple(products), b"pptx", ("v1",))

    monkeypatch.setattr(weekly_export, "generate_weekly_yield_export", generate)
    app = AppTest.from_function(_export_app).run()
    assert not calls
    assert not app.get("download_button")

    app.button(key="yield_weekly_generate").click().run()
    assert len(calls) == 1
    assert len(app.get("download_button")) == 1

    app.button(key="yield_weekly_generate").click().run()
    assert len(calls) == 2
    assert not app.exception
    assert not app.get("download_button")
    assert [error.value for error in app.error] == ["良率周报生成失败，请稍后重试。"]

    products.clear()
    app.run()
    assert [item.value for item in app.info] == ["暂无可导出的产品。"]
    assert not app.button


def test_old_download_is_cleared_when_enabled_scope_changes(monkeypatch):
    products = ["M678", "Z622"]
    monkeypatch.setattr(weekly_export.ConfigLoader, "get_enabled_products", lambda **_: products)
    monkeypatch.setattr(weekly_export, "_input_revision", lambda _: ("v1",))
    monkeypatch.setattr(
        weekly_export, "generate_weekly_yield_export",
        lambda **_: weekly_export.WeeklyYieldExport(ReportWeek.previous(), tuple(products), b"pptx", ("v1",)),
    )
    app = AppTest.from_function(_export_app).run()
    app.button(key="yield_weekly_generate").click().run()
    assert len(app.get("download_button")) == 1

    products.pop()
    app.run()
    assert not app.get("download_button")


def test_old_download_is_cleared_when_a_workbook_changes(monkeypatch):
    products = ("M678", "Z622")
    versions = ["v1"]
    monkeypatch.setattr(weekly_export.ConfigLoader, "get_enabled_products", lambda **_: products)
    monkeypatch.setattr(weekly_export, "_input_revision", lambda _: tuple(versions))
    monkeypatch.setattr(
        weekly_export, "generate_weekly_yield_export",
        lambda **_: weekly_export.WeeklyYieldExport(ReportWeek.previous(), products, b"pptx", tuple(versions)),
    )
    app = AppTest.from_function(_export_app).run()
    app.button(key="yield_weekly_generate").click().run()
    assert len(app.get("download_button")) == 1

    versions[0] = "v2"
    app.run()
    assert not app.get("download_button")
    assert [item.value for item in app.info] == ["周报数据已更新，请重新生成。"]
