"""Application composition and public Excel export for the period board."""
from io import BytesIO
import re

import pandas as pd

from src.yield_domain.composition import build_period_trend_service


def load_product_period_trends(product: str):
    return build_period_trend_service().load(product)


def export_period_tables(tables: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={'options': {
        'strings_to_formulas': False, 'strings_to_urls': False}}) as writer:
        for product, table in tables.items():
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,24}', product):
                raise ValueError('Invalid product sheet')
            periods = [c for c in table if re.fullmatch(r'\d{4}-(?:H[12]|Q[1-4]|\d{2})', c)]
            public = table[['指标', 'Defect Group', 'Defect Code', '缺陷名称', *periods]]
            public.to_excel(writer, sheet_name=product, index=False)
            sheet = writer.sheets[product]
            sheet.freeze_panes(1, 4)
            sheet.set_column(0, 3, 20)
            if periods:
                sheet.set_column(4, 3 + len(periods), 13, writer.book.add_format({'num_format': '0.00%'}))
    return buffer.getvalue()
