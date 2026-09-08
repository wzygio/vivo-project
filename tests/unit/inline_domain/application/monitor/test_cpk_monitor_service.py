from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.monitor import cpk_monitor_service as module
from src.inline_domain.core.monitor.cpk_summary import CPK_SUMMARY_COLUMNS


class Source:
    def __init__(self):
        self.calls = 0
        self.signature = "first"

    def source_signature(self, products, scopes):
        return self.signature

    def read_capability_inputs(self, product, start, end):
        self.calls += 1
        return {"sheet_features_df": pd.DataFrame([
            {"prod_code": product, "factory": factory, "step_id": "1", "param_name": name,
             "sheet_id": f"{factory}{i}", "sheet_start_time": "2026-09-07",
             "sheet_mean": 5 + i / 10, "usl": 10, "lsl": 1}
            for factory in ["ARRAY", "OLED"] for name in ["CD", "EXEMPT"] for i in range(2)
        ])}


class Store:
    def __init__(self):
        self.rows = pd.DataFrame(columns=CPK_SUMMARY_COLUMNS)

    def upsert_current_periods(self, rows, *, as_of):
        self.rows = rows.copy()
        persisted = rows.copy()
        persisted.loc[persisted["周期类型"].eq("年度"), "CPK总项目数"] = 999
        return persisted


def test_cache_signature_filter_exemption_and_workbook_readback(monkeypatch, tmp_path):
    module._cached_cpk_inputs.clear()
    monkeypatch.setattr(module.ConfigLoader, "get_spc_capability_param_exemptions", lambda: ["EXEMPT"])
    monkeypatch.setattr(module, "prepare_capability_decoration", lambda df, *a, **kw: SimpleNamespace(period_capability_df=df))
    source, store = Source(), Store()
    service = module.CpkMonitorService(source, store, product_resource_root=tmp_path)
    kwargs = {"products": ["M626"], "factories": ["ARRAY"], "end_date": "2026-09-07"}
    first = service.build_dashboard(**kwargs)
    service.build_dashboard(**kwargs)
    assert source.calls == 1
    assert first.summary_df.Y26.iloc[0] == 999
    assert set(first.detail_df.factory) == {"ARRAY"}
    assert set(first.detail_df.param_name) == {"CD"}
    source.signature = "configuration-changed"
    service.build_dashboard(**kwargs)
    assert source.calls == 2
    module._cached_cpk_inputs.clear()


def test_empty_selection_does_not_read_or_write():
    source, store = Source(), Store()
    result = module.CpkMonitorService(source, store).build_dashboard(products=[], factories=["ARRAY"])
    assert source.calls == 0
    assert store.rows.empty
    assert result.detail_df.empty
