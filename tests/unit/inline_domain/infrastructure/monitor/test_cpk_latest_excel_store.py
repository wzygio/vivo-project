import pandas as pd

from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk


def source():
    return pd.DataFrame([dict(prod_code="M626", factory="ARRAY", step_id="100", param_name="CD",
                             period_type="week", period_label="2026-W36", cpk_corrected=1.2, flag=False)])


def test_product_sheet_only_and_signature_invalidates_cache(tmp_path):
    path = tmp_path / "source.xlsx"
    with pd.ExcelWriter(path) as writer:
        source().to_excel(writer, sheet_name="M626", index=False)
        pd.DataFrame({"invalid": [1]}).to_excel(writer, sheet_name="M626_cpm", index=False)
    store = CpkLatestExcelStore(path)
    before = store.source_signature()
    assert "+08:00" in store.source_updated_at()
    assert len(store.read_product("M626")) == 1
    assert store.read_product("M678") is None
    changed = source().assign(flag=True)
    changed.to_excel(path, sheet_name="M626", index=False)
    assert store.source_signature() != before
    assert store.read_product("M626")["status"].tolist() == ["已修饰或达标"]


def test_transaction_preserves_historical_rows_other_sheets_and_is_idempotent(tmp_path):
    path = tmp_path / "summary.xlsx"
    baseline = pd.DataFrame([
        {"产品": "M626", "周期类型": kind, "时间标签": label, "显示标签": label,
         "监控类型": "SPC", "厂别": "ARRAY", "CPK总项目数": 10, "Cpk≥1.33达标率": .8}
        for kind, label in [("年度", "2026"), ("季度", "2026-Q3"), ("周度", "2026-W36"),
                            ("周度", "2026-W35"), ("月度", "2026-08")]
    ])
    other = pd.DataFrame({"preserve": [123]})
    with pd.ExcelWriter(path) as writer:
        baseline.to_excel(writer, sheet_name="M626 CPK", index=False)
        other.to_excel(writer, sheet_name="M626报警率", index=False)
    store = CpkSummaryWorkbookStore(path)
    old = store.read()
    frame = normalize_latest_cpk(source(), "M626")
    kwargs = dict(products=["M626"], factories=["ARRAY"], factory_key="ARRAY", as_of=pd.Timestamp("2026-09-08"))
    updated, _ = store.refresh_latest(frame, **kwargs)
    preserved = old[old["时间标签"].ne("2026-W36")]
    actual = updated[updated["时间标签"].ne("2026-W36")]
    pd.testing.assert_frame_equal(store._sort(preserved), store._sort(actual))
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="M626报警率"), other)
    assert updated.loc[updated["时间标签"].eq("2026-W36"), "Cpk≥1.33达标率"].iloc[0] == .9
    before = path.read_bytes()
    store.refresh_latest(frame, **kwargs)
    assert path.read_bytes() == before
