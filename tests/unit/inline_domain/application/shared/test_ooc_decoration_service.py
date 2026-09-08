from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.shared import ooc_decoration_service as module


def test_ooc_persistence_only_updates_editable_ledger(monkeypatch, tmp_path) -> None:
    captured: list[object] = []
    expected = pd.DataFrame([{"prod_code": "M626", "flag": "False"}])

    def persist(product_dir, detail_df, **kwargs):
        captured.append((product_dir, kwargs["file_name"]))
        return SimpleNamespace(decoration_df=expected)

    monkeypatch.setattr(
        module,
        "persist_sheet_oos_decoration_outcome",
        persist,
    )

    result = module.persist_ooc_facts(
        scope="spc",
        prod_code="M626",
        detail_df=pd.DataFrame(),
        key_columns=["prod_code", "step_id", "param_name", "sheet_id"],
        product_dir=tmp_path / "isolated-resources",
        coverage_start=pd.Timestamp("2026-09-01"),
        coverage_end=pd.Timestamp("2026-09-02"),
    )

    assert captured == [
        (tmp_path / "isolated-resources", module.SCOPE_OOC_DECORATION_FILE_NAME["spc"])
    ]
    pd.testing.assert_frame_equal(result, expected)
    assert not list(tmp_path.rglob("*.parquet"))
