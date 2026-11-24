"""Fill the two-product Yield slide template with editable text and static charts."""
from io import BytesIO
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
from unicodedata import east_asian_width

import plotly.graph_objects as go
import plotly.io as pio
from pptx import Presentation
from pptx.enum.text import MSO_AUTO_SIZE
from pptx.util import Pt

from app.charts.ppt_slides import clone_template_slide, drop_slide
from app.charts.yield_domain.mwd_chart import prepare_group_trend_figures

_SLOT_NAMES = (
    ("标题 1", "汇总语句1", "月周天趋势图1"),
    ("标题 2", "汇总语句2", "月周天趋势图2"),
)
_CHART_WIDTHS = (0.29, 0.27, 0.44)


def _static_charts(trends: dict | None) -> tuple[go.Figure, ...]:
    data = trends or {}
    monthly = data.get("monthly")
    groups = sorted(monthly["defect_group"].unique()) if monthly is not None and not monthly.empty else []
    charts = prepare_group_trend_figures(data, groups, {"defect_group": groups})
    figures = []
    for (title, figure), width in zip(charts, _CHART_WIDTHS, strict=True):
        if figure is None:
            figure = go.Figure()
            figure.update_xaxes(visible=False)
            figure.update_yaxes(visible=False)
            figure.add_annotation(text="暂无数据", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
        figure.update_layout(
            template="plotly_white", width=round(1440 * width), height=292,
            title=dict(text=title, x=0.04, y=0.98, font_size=16),
            margin=dict(l=58 if title == "月度趋势" else 18, r=16, t=35, b=25),
            font=dict(family="Microsoft YaHei, Arial, sans-serif", size=13),
            legend=dict(orientation="h", x=0, y=-0.24, yanchor="top", font_size=11, title_text=""),
        )
        figure.update_xaxes(automargin=True)
        figures.append(figure)
    return tuple(figures)


def _render_charts(figures: list[go.Figure], temp_root: Path) -> list[bytes]:
    temp_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="yield_weekly_charts_", dir=temp_root) as directory:
        paths = [Path(directory) / f"{index}.png" for index in range(len(figures))]
        pio.write_images(figures, paths, format="png", scale=2)
        return [path.read_bytes() for path in paths]


def _replace_text(shape, text: str) -> None:
    paragraph = shape.text_frame.paragraphs[0]
    runs = paragraph.runs or (paragraph.add_run(),)
    runs[0].text = text
    for run in runs[1:]:
        run._r.getparent().remove(run._r)


def _slot(slide, names: tuple[str, str, str]) -> tuple:
    placeholders = tuple(tuple(shape for shape in slide.shapes if shape.name == name) for name in names)
    if any(len(matches) != 1 for matches in placeholders):
        raise ValueError(f"Missing or duplicated Yield template shape: {names}")
    title, summary, chart = (matches[0] for matches in placeholders)
    if not title.has_text_frame or not summary.has_text_frame or chart.width <= 0 or chart.height <= 0:
        raise ValueError("Invalid Yield template slot")
    return title, summary, chart


def _fill_slot(slide, shapes: tuple, product: str, summary: str, images: list[bytes], slide_width: int) -> None:
    title, summary_box, chart = shapes
    _replace_text(title, f"{product}不良率")
    _replace_text(summary_box, summary)
    summary_box.text_frame.word_wrap = True
    summary_box.text_frame.auto_size = MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE

    # Allow longer conclusions to wrap by borrowing space from this slot's charts.
    text_width = sum(1 if east_asian_width(char) in {"W", "F"} else 0.6 for char in summary)
    line_capacity = max(1, (summary_box.width - Pt(24)) / Pt(12))
    chart_height = round(chart.height * 0.94)
    extra_height = min(round(chart_height * 0.38), Pt(15) * (ceil(text_width / line_capacity) - 1))
    summary_box.height += extra_height
    left = max(0, chart.left)
    width = min(chart.width, slide_width - left)
    for ratio, image in zip(_CHART_WIDTHS, images, strict=True):
        image_width = int(width * ratio)
        slide.shapes.add_picture(
            BytesIO(image), left, chart.top + extra_height,
            width=image_width, height=chart_height - extra_height,
        )
        left += image_width
    chart.element.getparent().remove(chart.element)


def build_weekly_yield_ppt(
    conclusions: dict[str, str], trends: dict[str, dict | None], *,
    week_label: str, template_bytes: bytes, temp_root: Path,
) -> bytes:
    if not conclusions or conclusions.keys() != trends.keys():
        raise ValueError("Weekly Yield report needs a conclusion and chart input for each product")
    deck = Presentation(BytesIO(template_bytes))
    if len(deck.slides) != 3:
        raise ValueError("Yield template must contain cover, body and example slides")
    source = deck.slides[1]
    for names in _SLOT_NAMES:
        _slot(source, names)
    products = tuple(conclusions)
    figures = [figure for product in products for figure in _static_charts(trends[product])]
    images = _render_charts(figures, temp_root)
    for offset in range(0, len(products), len(_SLOT_NAMES)):
        slide = clone_template_slide(deck, source)
        for index, names in enumerate(_SLOT_NAMES, start=offset):
            shapes = _slot(slide, names)
            if index >= len(products):
                for shape in shapes:
                    shape.element.getparent().remove(shape.element)
                continue
            product = products[index]
            _fill_slot(
                slide, shapes, product, f"{product} - {week_label}不良率，{conclusions[product]}",
                images[index * len(_CHART_WIDTHS):(index + 1) * len(_CHART_WIDTHS)], deck.slide_width,
            )
    drop_slide(deck, 2)
    drop_slide(deck, 1)
    output = BytesIO()
    deck.save(output)
    return output.getvalue()
