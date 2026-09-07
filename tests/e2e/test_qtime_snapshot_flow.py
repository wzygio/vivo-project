"""Isolated CLI-to-shared-application Q-Time snapshot flow."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd

from src.indicator_domain.application.qtime import cached_monitoring
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_shop_monitoring,
)
from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.infrastructure.qtime import repository as repository_module
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository
from tools import refresh_qtime_snapshots


def test_cli_snapshot_is_reused_by_the_shared_application_entry(
    tmp_path,
    monkeypatch,
) -> None:
    database_calls: list[dict[str, object]] = []

    def fake_read_sql(_statement, _engine, params):
        database_calls.append(dict(params))
        shop = str(params["shop"])
        return pd.DataFrame([_source_row(shop)])

    monkeypatch.setattr(repository_module.pd, "read_sql", fake_read_sql)
    writer_service = _service(tmp_path)

    exit_code = refresh_qtime_snapshots.main(
        ["--as-of", "2026-09-02"],
        service_factory=lambda: writer_service,
    )

    assert exit_code == 0
    assert len(database_calls) == 3
    assert sorted(path.name for path in tmp_path.glob("*.parquet")) == [
        "qtime_source_array.parquet",
        "qtime_source_oled.parquet",
        "qtime_source_tp.parquet",
    ]

    monkeypatch.setattr(
        repository_module.pd,
        "read_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("shared read must not access the database")
        ),
    )
    cached_monitoring._cached_monitoring.clear()
    reader_service = _service(tmp_path)
    mtime_ns, size = MISSING_DECISION_FILE_STAT

    first = get_cached_shop_monitoring(
        reader_service,
        shop="ARRAY",
        as_of=date(2026, 9, 2),
        decision_mtime_ns=mtime_ns,
        decision_size=size,
    )
    second = get_cached_shop_monitoring(
        reader_service,
        shop="ARRAY",
        as_of=date(2026, 9, 2),
        decision_mtime_ns=mtime_ns,
        decision_size=size,
    )

    assert first.details["lot_id"].tolist() == ["ARRAY-LOT"]
    pd.testing.assert_frame_equal(second.details, first.details)
    assert len(database_calls) == 3


def _service(snapshot_dir) -> QTimeReportService:
    repository = QTimeRepository(
        SimpleNamespace(engine=object()),
        snapshot_dir=snapshot_dir,
        snapshot_ttl_hours=24,
    )
    return QTimeReportService(repository)


def _source_row(shop: str) -> dict[str, object]:
    step_prefix = {"ARRAY": "1", "OLED": "2", "TP": "3"}[shop]
    return {
        "step_desc": "A->B",
        "lot_id": f"{shop}-LOT",
        "prod_qty": 1,
        "sub_prod_type": "P",
        "f_step": f"{step_prefix}5500",
        "t_step": f"{step_prefix}5600",
        "q_spec": 2.5,
        "wait_time": 0.41,
        "timekey": "20260829010000",
        "shop": shop,
        "prodcode": "M626",
    }
