import importlib
import sys
import threading
from pathlib import Path

import pandas as pd

from src.equipment_domain.application import parts_service
from src.equipment_domain.infrastructure import report_data_adapter


def test_today_cache_key_does_not_pass_end_of_day_as_current_time(monkeypatch) -> None:
    import pytest

    observed = []

    def capture_clock(*args, as_of, **kwargs):
        observed.append(as_of)
        raise RuntimeError("clock captured")

    monkeypatch.setattr(report_data_adapter, "load_spec_baseline", lambda _: pd.DataFrame())
    monkeypatch.setattr(report_data_adapter, "load_report_part_life_snapshots", capture_clock)
    parts_service.PartsReportService.fetch_report_payload.clear()
    with pytest.raises(RuntimeError, match="clock captured"):
        parts_service.PartsReportService.fetch_report_payload(
            object(), "clock-test.csv", "clock-regression",
            as_of_date=pd.Timestamp.now().date().isoformat(),
        )
    assert observed[0] <= pd.Timestamp.now()


def test_parts_report_remains_available_when_service_module_reloads_during_cache_fill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    baseline_path = tmp_path / "critical_parts_baseline.csv"
    baseline_path.write_text(
        """厂别,备件类型,设备类型,膜层,制程,寿命规格,站点,机台号-腔室,参数名称
Array,Target,PVD,MO,Mo DEPO,41000KWH,1K200,3AFS01-SPU-PM5,%TRGTLIFE%_G_MAX
""",
        encoding="utf-8-sig",
    )
    snapshot_df = pd.DataFrame(
        {
            "step_id": ["1K200"],
            "sub_equip_id": ["3AFS01-SPU-PM5"],
            "param_name": ["CH_A_TRGTLIFE_X_G_MAX"],
            "value": [32000.0],
            "glass_start_time": pd.to_datetime(["2026-05-12 08:00:00"]),
        }
    )
    entered_snapshot_load = threading.Event()
    continue_snapshot_load = threading.Event()

    def blocking_snapshot_load(
        _db_manager,
        _spec_df,
        *,
        as_of: pd.Timestamp,
        max_age_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del as_of, max_age_days
        entered_snapshot_load.set()
        assert continue_snapshot_load.wait(timeout=10)
        return snapshot_df, pd.DataFrame()

    original_module = parts_service
    original_service = original_module.PartsReportService
    original_service.fetch_report_payload.clear()
    monkeypatch.setattr(
        report_data_adapter,
        "load_report_part_life_snapshots",
        blocking_snapshot_load,
    )
    outcome: dict[str, object] = {}

    def load_report() -> None:
        try:
            outcome["report"] = original_service.get_report_data(
                _db_manager=object(),
                baseline_path=str(baseline_path),
                snapshot_signature="reload-during-cache-fill",
            )
        except BaseException as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=load_report)
    worker.start()
    assert entered_snapshot_load.wait(timeout=10)

    module_name = original_module.__name__
    try:
        del sys.modules[module_name]
        importlib.import_module(module_name)
        continue_snapshot_load.set()
        worker.join(timeout=15)
    finally:
        continue_snapshot_load.set()
        sys.modules[module_name] = original_module

    assert not worker.is_alive()
    assert "error" not in outcome, repr(outcome.get("error"))
    report = outcome["report"]
    assert isinstance(report, original_module.PartsReportViewModel)
    assert report.total_count == 1
    assert not report.report_df.empty
