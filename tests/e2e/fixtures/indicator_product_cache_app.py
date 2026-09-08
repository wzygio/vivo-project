"""Isolated real matrix/cache UI: no database or production workbook writes."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.components import indicator_cache
from app.manager import compliance_manager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_cache, alert_matrix_detail
from app.sections.inline_domain.monitor.alert_matrix_service import AlertMatrixContext, MATRIX_ROWS
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore

st.set_page_config(page_title="指标产品缓存隔离验收", layout="wide")
st.title("全指标预警看板 · 隔离验收")
st.session_state.setdefault("fixture_id", uuid4().hex)
st.session_state.setdefault("counts", {})
fixture_root = ROOT / "output" / "tmp" / "indicator-product-e2e" / st.session_state.fixture_id
fixture_root.mkdir(parents=True, exist_ok=True)
cpk_path = fixture_root / "cpk.xlsx"
compliance_path = fixture_root / "compliance.xlsx"
products = ("M626", "M678")
rows = [{"row_key": r.row_key, "display_name": r.display_name} for r in MATRIX_ROWS]
reference_date = date(2026, 9, 8)


def write_cpk(decorated=False):
    with pd.ExcelWriter(cpk_path) as writer:
        for product in products:
            pd.DataFrame([dict(prod_code=product, factory="ARRAY", step_id="100", param_name="CPK_TEST",
                               period_type="week", period_label="2026-W36", cpk_corrected=.8,
                               flag=decorated if product == "M626" else False)]).to_excel(
                                   writer, sheet_name=product, index=False)


if not cpk_path.exists():
    write_cpk()
if not compliance_path.exists():
    compliance_manager.write_matrix_template(rows, products, compliance_path)
compliance_manager.CONFIG_PATH = compliance_path

revision_dir = fixture_root / "revisions"
original_get = indicator_cache.get_indicator_product_revision
original_bump = indicator_cache.bump_indicator_product_revision


def revision(indicator, product):
    return original_get(indicator, product, revision_dir=revision_dir)


def bump(indicator, product):
    return original_bump(indicator, product, revision_dir=revision_dir)


def count(indicator, product):
    counts = dict(st.session_state.counts)
    key = f"{indicator}|{product}"
    counts[key] = counts.get(key, 0) + 1
    st.session_state.counts = counts


def cpk_loader(product):
    count("spc_cpk_trend", product)
    return CpkLatestExcelStore(cpk_path).read_product(product)


def source_signature(indicator, product):
    if indicator == "spc_cpk_trend":
        return f"{st.session_state.fixture_id}:{CpkLatestExcelStore(cpk_path).source_signature()}"
    return st.session_state.fixture_id


def context_factory():
    return AlertMatrixContext(
        reference_date=reference_date,
        oos_product_loader=lambda *args: SimpleNamespace(decorated_df=pd.DataFrame()),
        spc_cpk_loader=cpk_loader,
        yield_lot_loader=lambda product: count("yield_lot_oos", product),
        yield_trend_loader=lambda product: count("yield_trend_fluctuation", product),
    )


if st.button("Fixture: modify CPK Excel", key="fixture_cpk_edit"):
    write_cpk(decorated=True)
    st.session_state["cpk_edited"] = True
if st.button("Fixture: enable display compliance", key="fixture_compliance_on"):
    frame = pd.read_excel(compliance_path, sheet_name=compliance_manager.SHEET_NAME)
    frame.loc[frame["监控参数"].eq("SPC 趋势波动（CPK）"), "M678"] = True
    frame.to_excel(compliance_path, sheet_name=compliance_manager.SHEET_NAME, index=False)
    st.session_state["compliance_enabled"] = True

real_get_matrix = alert_matrix_cache.get_cached_alert_matrix
alert_matrix.get_cached_alert_matrix = lambda: real_get_matrix(
    products=products, reference_date=reference_date,
    _context_factory=context_factory, _signature_provider=source_signature, _revision_provider=revision,
)
# UI and loader use test-local revision storage, not the shared production markers.
alert_matrix.bump_indicator_product_revision = bump
alert_matrix_cache.get_indicator_product_revision = revision
alert_matrix_detail.cpk_latest_store = lambda: CpkLatestExcelStore(cpk_path)

alert_matrix.render_alert_matrix_board()
for indicator in ("yield_lot_oos", "yield_trend_fluctuation", "spc_cpk_trend"):
    for product in products:
        st.caption(f"count:{indicator}:{product}={st.session_state.counts.get(f'{indicator}|{product}', 0)}")
st.caption(f"CPK edited: {st.session_state.get('cpk_edited', False)}")
st.caption(f"Compliance enabled: {st.session_state.get('compliance_enabled', False)}")
