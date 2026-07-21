from dataclasses import replace
from pathlib import Path

import pandas as pd

from src.equipment_domain.application import parts_service
from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.infrastructure import data_loader
from src.equipment_domain.infrastructure.data_loader import load_spec_baseline
from src.equipment_domain.infrastructure.fake_data import (
    calculate_spec_signature,
    generate_fabricated_snapshot,
    materialize_param_name,
    write_fabricated_snapshot,
)


class _OfflineDatabase:
    engine = None


def test_report_service_prefers_real_snapshot_and_fills_its_gaps_from_fabrication(
    tmp_path: Path,
    monkeypatch,
) -> None:
    baseline_path = Path("resources/critical_parts_baseline.csv")
    spec_df = load_spec_baseline(baseline_path)
    runtime = get_equipment_runtime_config()
    as_of = pd.Timestamp("2026-07-15 08:30:00")
    result = generate_fabricated_snapshot(
        spec_df,
        runtime.fabrication_policy,
        as_of=as_of,
    )
    output_path = write_fabricated_snapshot(
        result.snapshot_df,
        spec_df,
        output_dir=tmp_path,
    )
    first_spec = spec_df.iloc[0]
    real_value = float(first_spec["寿命规格"]) * 0.42
    real_snapshot = pd.DataFrame({
        "step_id": [str(first_spec["站点"])],
        "sub_equip_id": [str(first_spec["机台号-腔室"])],
        "param_name": [materialize_param_name(
            str(first_spec["参数名称"]),
            str(first_spec["机台号-腔室"]),
        )],
        "value": [real_value],
        "glass_start_time": [as_of],
    })
    real_snapshot.to_parquet(
        tmp_path / f"part_life_snapshot_{calculate_spec_signature(spec_df)}.parquet",
        index=False,
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
    assert view_model.report_df.loc[0, "原始测量值"] == real_value
    assert measured["原始测量值"].notna().all()
    assert measured["使用进度"].max() <= runtime.alert_policy.display_progress_max_ratio * 100
    assert view_model.last_update == str(as_of)
    assert view_model.normal_count > 0
    assert output_path.name.startswith("part_life_fabricated_")
