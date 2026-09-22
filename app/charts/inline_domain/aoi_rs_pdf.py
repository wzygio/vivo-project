"""Self-contained AOI RS PDF layout: two indicator rows per landscape A3 page."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pymupdf

PAGE_WIDTH, PAGE_HEIGHT = 1191, 842
MARGIN = 32
CHART_GAP = 12
CHART_WIDTH = (PAGE_WIDTH - 2 * MARGIN - 2 * CHART_GAP) / 3
CHART_HEIGHT = CHART_WIDTH * 460 / 600
ROW_HEIGHT = 340
FONT_NAME = "rs-report-font"
INK = (0.18, 0.24, 0.33)
MUTED = (0.42, 0.47, 0.54)


class AoiRsPdfReport:
    """Write embedded images and an embedded CJK font into the supplied document."""

    def __init__(
        self, document: pymupdf.Document, start_date: date, end_date: date
    ) -> None:
        self.document = document
        self.period = f"报表期间：{start_date} ~ {end_date}"
        self.font = pymupdf.Font("cjk")
        self._scope: tuple[str, str] | None = None
        self._page_number: int | None = None
        self._row_count = 0

    def _text(
        self, page: pymupdf.Page, rect: pymupdf.Rect, text: str, size: int = 13
    ) -> None:
        for font_size in range(size, 7, -1):
            remaining = page.insert_textbox(
                rect, text, fontname=FONT_NAME, fontsize=font_size, color=INK
            )
            if remaining >= 0:
                return
        raise ValueError("Report heading does not fit the PDF layout")

    def _new_page(self, subtitle: str) -> pymupdf.Page:
        page = self.document.new_page(width=PAGE_WIDTH, height=PAGE_HEIGHT)
        page.insert_font(fontname=FONT_NAME, fontbuffer=self.font.buffer)
        self._text(
            page,
            pymupdf.Rect(MARGIN, 22, PAGE_WIDTH - MARGIN, 58),
            "AOI_RS监控报表",
            size=24,
        )
        self._text(
            page,
            pymupdf.Rect(MARGIN, 62, PAGE_WIDTH - MARGIN, 92),
            f"{subtitle}    |    {self.period}",
            size=12,
        )
        self._page_number = page.number
        self._row_count = 0
        return page

    def append_indicator(
        self, *, product: str, factory: str, title: str, images: Sequence[Path]
    ) -> None:
        if len(images) != 3:
            raise ValueError("Each RS report row must contain exactly three images")
        scope = (product, factory)
        if scope != self._scope or self._row_count == 2:
            self._new_page(f"产品：{product}    厂别：{factory}")
            self._scope = scope
        page = self.document[self._page_number]
        top = 105 + self._row_count * ROW_HEIGHT
        band = pymupdf.Rect(MARGIN, top, PAGE_WIDTH - MARGIN, top + 31)
        page.draw_rect(band, color=None, fill=(0.94, 0.96, 0.98))
        self._text(page, band + (8, 4, -8, 0), title)
        image_top = top + 38
        for index, path in enumerate(images):
            left = MARGIN + index * (CHART_WIDTH + CHART_GAP)
            rect = pymupdf.Rect(
                left, image_top, left + CHART_WIDTH, image_top + CHART_HEIGHT
            )
            page.insert_image(rect, filename=str(path))
        self._row_count += 1

    def append_empty_products(
        self, products: Sequence[str], factories: Sequence[str]
    ) -> None:
        # Empty products stay visible in the report rather than silently disappearing.
        for offset in range(0, len(products), 18):
            page = self._new_page("数据范围说明")
            self._text(
                page,
                pymupdf.Rect(MARGIN, 110, PAGE_WIDTH - MARGIN, 150),
                f"以下产品在所选厂别（{'、'.join(factories)}）及报表期间内暂无可展示数据：",
            )
            for index, product in enumerate(products[offset : offset + 18]):
                top = 164 + index * 30
                self._text(
                    page,
                    pymupdf.Rect(MARGIN, top, PAGE_WIDTH - MARGIN, top + 28),
                    product,
                )

    def to_bytes(self) -> bytes:
        for page in self.document:
            page.insert_text(
                (MARGIN, PAGE_HEIGHT - 25),
                f"AOI_RS    |    {page.number + 1} / {len(self.document)}",
                fontname=FONT_NAME,
                fontsize=10,
                color=MUTED,
            )
        self.document.set_metadata({"title": "AOI_RS监控报表", "subject": self.period})
        self.document.subset_fonts()
        return self.document.tobytes(garbage=4, deflate=True)
