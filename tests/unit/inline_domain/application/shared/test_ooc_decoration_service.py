from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.shared import ooc_decoration_service as module


def test_explicit_resource_override_isolates_ooc_history(monkeypatch, tmp_path) -> None:
    captured: list[object] = []
    fake_history = SimpleNamespace(update_history=lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "build_ooc_history_service",
        lambda resource_dir=None: captured.append(resource_dir) or fake_history,
    )
    monkeypatch.setattr(
        module,
        "scope_resource_dir",
        lambda _scope: tmp_path / "production-resources",
    )

    module.persist_ooc_facts(
        scope="spc",
        prod_code="M626",
        detail_df=pd.DataFrame(),
        key_columns=["prod_code", "step_id", "param_name", "sheet_id"],
        product_dir=tmp_path / "isolated-resources",
        coverage_start=pd.Timestamp("2026-09-01"),
        coverage_end=pd.Timestamp("2026-09-02"),
    )

    assert captured == [tmp_path / "isolated-resources"]
