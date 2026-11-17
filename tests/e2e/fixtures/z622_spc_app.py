"""Real Z622 SPC pipeline over the task's captured PDS facts; no DB connection."""
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import pandas as pd
import pytest
import streamlit as st

from tests.integration.test_z622_spc_pipeline import build_captured_report
from app.sections.inline_domain.spc.spc_dashboard import render_spc_filters, filter_spc_report, render_spc_indicator_sections
from app.sections.inline_domain.spc.scroll_layout import render_spc_page_scroll_layout

st.set_page_config(page_title="Z622 SPC 数据验证", layout="wide")
render_spc_page_scroll_layout()
st.title("Z622 SPC 数据验证")
mode = st.selectbox("验证场景", ["normal", "empty", "failure"], key="z622_fixture_mode")
source = Path(os.environ.get("VIVO_Z622_CAPTURE", str(ROOT / "output/tmp/z622/pds-import.parquet")))
fixture_root = Path(os.environ["VIVO_Z622_FIXTURE_ROOT"])
output = Path(os.environ["VIVO_Z622_BROWSER_OUTPUT"])


@st.cache_resource(max_entries=1)
def patches():
    return pytest.MonkeyPatch()


def load_report(selected_mode):
    report, info, _original = build_captured_report(patches(), fixture_root / selected_mode, source, mode=selected_mode)
    return report, info


try:
    report, info = load_report(mode)
except Exception:
    logging.getLogger(__name__).exception("Captured Z622 fixture pipeline failed")
    st.error("SPC 数据暂时无法读取，请稍后重试。")
    st.stop()

output.mkdir(parents=True, exist_ok=True)
(output / f"pipeline-{mode}.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
if report.raw_measurements_df.empty:
    st.info("当前产品暂无 SPC 数据。")
    st.stop()
choice = report.indicators_df.sort_values(["step_id", "param_name"]).iloc[0]
(output / "selection.json").write_text(json.dumps({"step": str(choice.step_id), "parameter": str(choice.param_name)}), encoding="utf-8")
st.caption(f"Z622 · {len(report.raw_measurements_df)} 条点位 · {len(report.indicators_df)} 个指标")
public_columns = ["prod_code", "factory", "sheet_id", "step_id", "param_name", "site_name", "sheet_start_time", "param_value"]
public = report.raw_measurements_df[public_columns]
st.download_button("下载点位 CSV", public.to_csv(index=False).encode("utf-8-sig"), "Z622-points.csv", "text/csv")


@st.fragment
def interaction():
    factory, params, steps, submitted = render_spc_filters(report.indicators_df)
    if not submitted:
        st.info("当前筛选条件尚未查询。")
        return
    selection = dict(selected_factory=factory, selected_params=params, selected_steps=steps)
    render_spc_indicator_sections(
        filter_spc_report(report.period_capability_df, **selection),
        filter_spc_report(report.sheet_features_df, **selection),
        filter_spc_report(report.raw_measurements_df, **selection),
        reference_date=pd.Timestamp("2026-11-16").date(),
    )


interaction()
