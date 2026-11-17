"""Captured, read-only PDS facts through the real SPC report and chart pipeline.

Only SQL transport and unavailable process history are replaced. Every resource,
snapshot and publication produced by this harness belongs to its isolated root.
"""

from __future__ import annotations

from contextlib import nullcontext
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CAPTURE = ROOT / "output/tmp/z622/pds-import.parquet"


class CapturedConnection:
    def execution_options(self, **_options):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def begin(self):
        return nullcontext(self)

    def exec_driver_sql(self, sql):
        if not str(sql).upper().startswith("SET LOCAL STATEMENT_TIMEOUT"):
            raise AssertionError("The captured connection permits only a local timeout")


def captured_facts(source: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source)
    frame = frame.rename(columns={
        "glass_id": "sheet_id", "glass_start_time": "start_time",
        "value": "param_value", "sub_equip_id": "unit_id",
    }).copy()
    frame["factory"] = "ARRAY"
    frame["prod_code"] = "Z622"
    frame["lot_id"] = frame["sheet_id"].astype(str).str[:9]
    frame["start_time"] = pd.to_datetime(frame["start_time"])
    return frame


def install_captured_case(monkeypatch, directory: Path, source: Path, *, mode="normal"):
    """Use production policies and monitored pairs without touching their files."""
    from src.shared_kernel.config import ConfigLoader
    from src.inline_domain.infrastructure.shared.measurement_data_loader import RAW_MEASUREMENT_COLUMNS
    from src.inline_domain.infrastructure.spc.spc_repository import build_z622_metadata_repository

    directory.mkdir(parents=True, exist_ok=True)
    config = yaml.safe_load((ROOT / "config/global.yaml").read_text(encoding="utf-8"))
    resources = directory / "resources/inline_domain/spc"
    resources.mkdir(parents=True, exist_ok=True)
    files = config["resources"]["inline_domain"]["files"]
    for key, original in list(files.items()):
        files[key] = str(resources / Path(original).name)
        if key in {"spc_outlier_filters", "spc_z622_monitoring"}:
            original_path = Path(original)
            if not original_path.is_absolute():
                original_path = ROOT / original_path
            shutil.copy2(original_path, files[key])
    if "spc_z622_monitoring" not in files:
        raise AssertionError("Z622 monitoring workbook is not registered")
    config["resources"]["inline_domain"]["dir"] = str(resources.parent)
    config["resources"]["inline_domain"]["directories"] = {}
    config_dir = directory / "config"
    (config_dir / "domain").mkdir(parents=True, exist_ok=True)
    (config_dir / "products").mkdir(exist_ok=True)
    (config_dir / "global.yaml").write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")
    shutil.copy2(ROOT / "config/domain/inline_domain.yaml", config_dir / "domain/inline_domain.yaml")
    product = ROOT / "config/products/Z622.yaml"
    if product.exists():
        shutil.copy2(product, config_dir / "products/Z622.yaml")
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: directory)
    original = captured_facts(source)
    # Use the production reader, including enterprise workbook normalization.
    # No direct pandas Excel read may bypass the captured root/decryption policy.
    pairs = build_z622_metadata_repository().get_parameter_specs("Z622")[["step_id", "param_name"]]
    calls = []

    def read_sql(query, _connection, *, params=None, chunksize=None, **_kwargs):
        sql = str(query).lower()
        assert "array_pds_result_t" in sql, "Unexpected source access in captured pipeline"
        sql_words = " ".join(sql.split())
        assert "p.glass_start_time >= :start_time" in sql_words
        assert "p.glass_start_time < :end_time" in sql_words
        params = params or {}
        assert params.get("lot_prefix") == "L3ZB6BE03%"
        transmitted_pairs = pd.DataFrame([
            {"step_id": value, "param_name": params[f"param_{key.removeprefix('step_')}"]}
            for key, value in params.items() if key.startswith("step_")
        ])
        pd.testing.assert_frame_equal(transmitted_pairs.reset_index(drop=True), pairs.reset_index(drop=True), check_dtype=False)
        calls.append({"sql": str(query), "params": {k: str(v) for k, v in params.items()}})
        if mode == "failure":
            raise OSError("CAPTURED_PDS_TRANSPORT_FAILURE")
        selected = original.merge(transmitted_pairs.drop_duplicates(), on=["step_id", "param_name"], how="inner")
        selected = selected.loc[selected.lot_id.eq("L3ZB6BE03")]
        # Respect every transmitted source-time boundary, independently of its bind name.
        for name, value in params.items():
            lowered = name.lower()
            if any(token in lowered for token in ("start_time", "start_date", "window_start")):
                selected = selected.loc[selected.start_time.ge(pd.Timestamp(value))]
            elif any(token in lowered for token in ("end_time", "end_date", "window_end")):
                selected = selected.loc[selected.start_time.lt(pd.Timestamp(value))]
        selected = selected.reindex(columns=RAW_MEASUREMENT_COLUMNS)
        if mode == "empty":
            selected = selected.iloc[:0]
        if chunksize:
            return (selected.iloc[start:start + chunksize].copy() for start in range(0, max(1, len(selected)), chunksize))
        return selected.copy()

    monkeypatch.setattr(pd, "read_sql", read_sql)
    db = SimpleNamespace(engine=SimpleNamespace(connect=lambda: CapturedConnection()))
    return db, original, calls


def build_captured_report(monkeypatch, directory: Path, source: Path, *, mode="normal"):
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.inline_domain.infrastructure.shared.measurement_preparation import InlineMeasurementPreparationRepository
    from src.inline_domain.infrastructure.spc.spc_repository import (
        SpcRepository, build_z622_measurement_repository, build_z622_metadata_repository,
    )
    from src.shared_kernel.config import ConfigLoader

    db, original, calls = install_captured_case(monkeypatch, directory, source, mode=mode)
    raw = build_z622_measurement_repository(db, snapshot_dir=directory / "data/inline_domain/shared")
    metadata = build_z622_metadata_repository(db)
    history = SimpleNamespace(get_main_process_history=lambda *_args, **_kwargs: pd.DataFrame())
    repository = SpcRepository(InlineMeasurementPreparationRepository(raw, metadata, history))
    query = SpcQueryConfig(prod_code="Z622", start_date="2026-10-01", end_date="2026-11-16")
    report = SpcReportService.get_spc_report_data(
        repository, query.model_dump_json(), snapshot_signature=f"capture:{directory}:{source.stat().st_mtime_ns}",
        product_revision=f"captured-{mode}",
    )
    points = report.raw_measurements_df
    info = {"mode": mode, "source_rows": len(original), "point_rows": len(points),
            "indicator_rows": len(report.indicators_df), "sql_calls": len(calls),
            "enabled": "Z622" in ConfigLoader.get_enabled_products(),
            "capture": str(source), "fixture_root": str(directory)}
    return report, info, original


@pytest.mark.skipif(not CAPTURE.exists(), reason="Requires the task's bounded PDS capture")
def test_captured_pds_reaches_real_report_and_chart_without_capability(tmp_path, monkeypatch):
    from app.charts.inline_domain.spc_charts import iter_spc_chart_groups
    from src.shared_kernel.config import ConfigLoader

    report, info, source = build_captured_report(monkeypatch, tmp_path, CAPTURE)
    assert info["enabled"], "A browser fixture must not invent enabled product availability"
    assert info["sql_calls"] >= 1
    points = report.raw_measurements_df
    assert not points.empty and not report.indicators_df.empty
    assert points.prod_code.eq("Z622").all()
    assert points.factory.eq("ARRAY").all()
    assert points.sheet_id.str.startswith("L3ZB6BE03").all()
    keys = ["prod_code", "step_id", "param_name", "sheet_id", "site_name"]
    assert not points.duplicated(keys).any()
    spec_columns = [column for column in ("usl", "lsl", "ucl", "lcl", "target") if column in points]
    if spec_columns:
        assert points[spec_columns].isna().all().all()
    for metric in ("cpk", "cpm"):
        if metric in report.period_capability_df:
            assert report.period_capability_df[metric].dropna().empty
    offset = pd.Timedelta(days=ConfigLoader.get_data_forward_policy().effective_days)
    expected = source.assign(sheet_start_time=source.start_time + offset)
    expected = ConfigLoader.get_report_cutoff_policy().filter_frame(expected, "sheet_start_time")
    expected = expected.loc[pd.to_numeric(expected.param_value, errors="coerce").notna()]
    latest = expected.groupby(keys, dropna=False).sheet_start_time.max().rename("expected_latest")
    latest_check = points.merge(latest, on=keys, how="left")
    assert latest_check.sheet_start_time.eq(latest_check.expected_latest).all()
    identity = ["sheet_id", "step_id", "param_name", "site_name", "sheet_start_time"]
    compared = points.merge(expected[identity + ["param_value"]].drop_duplicates(),
                            on=identity + ["param_value"], how="left", indicator=True)
    assert compared._merge.eq("both").all(), "No-spec points must retain captured values"
    choice = report.indicators_df.iloc[0]
    selected = points.loc[points.step_id.eq(choice.step_id) & points.param_name.eq(choice.param_name)]
    groups = list(iter_spc_chart_groups(report.period_capability_df, report.sheet_features_df, selected,
                                      reference_date=pd.Timestamp("2026-11-16").date()))
    assert len(groups) == 1 and len(groups[0].figures) == 3
    assert groups[0].figures[0].data, "Historical measurement overview must contain actual traces"
    (tmp_path / "pipeline-evidence.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
