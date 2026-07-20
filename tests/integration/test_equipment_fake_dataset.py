from dataclasses import replace
from pathlib import Path

import pandas as pd

from src.equipment_domain.application import parts_service
from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.infrastructure import data_loader
from src.equipment_domain.infrastructure.data_loader import load_spec_baseline
from src.equipment_domain.infrastructure.fake_data import (
    fabricate_current_snapshot,
    write_fabricated_snapshot,
)


class _OfflineDatabase:
    engine = None


def test_real_baseline_fabrication_is_consumable_by_report_service(
    tmp_path: Path,
    monkeypatch,
) -> None:
    baseline_path = Path("resources/critical_parts_baseline.csv")
    spec_df = load_spec_baseline(baseline_path)
    runtime = get_equipment_runtime_config()
    as_of = pd.Timestamp("2026-07-15 08:30:00")
    result = fabricate_current_snapshot(
        spec_df,
        runtime.fabrication_policy,
        as_of=as_of,
    )
    output_path = write_fabricated_snapshot(
        result.snapshot_df,
        spec_df,
        output_dir=tmp_path,
    )
    test_runtime = replace(runtime, snapshot_dir=tmp_path)
    monkeypatch.setattr(data_loader, "get_equipment_runtime_config", lambda: test_runtime)
    monkeypatch.setattr(parts_service, "get_equipment_runtime_config", lambda: test_runtime)
    parts_service.PartsReportService.fetch_report_payload.clear()

    view_model = parts_service.PartsReportService.get_report_data(
        _db_manager=_OfflineDatabase(),
        baseline_path=str(baseline_path),
        snapshot_signature="task2-opt-integration",
    )

    measured = view_model.report_df[view_model.report_df["测量值"].notna()]
    assert output_path.exists()
    assert result.summary["generated_rows"] == 1685
    assert result.summary["synthetic_param_rows"] == 1519
    assert len(view_model.report_df) == 1781
    assert len(measured) == 1781
    assert not view_model.report_df["测量值"].isna().any()
    assert not view_model.report_df["测量时间"].isna().any()
    assert measured["原始测量值"].notna().all()
    assert measured["是否超规"].any()
    assert (measured["数据修饰"] == "超规修饰").any()
    assert measured["使用进度"].max() <= runtime.alert_policy.display_progress_max_ratio * 100
    assert view_model.last_update == str(as_of)
    assert view_model.normal_count > 0
    assert view_model.warning_count > 0
