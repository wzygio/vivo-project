"""Composition root for Inline report outbound adapters."""

from __future__ import annotations

from pathlib import Path

from src.inline_domain.infrastructure.aoi_tt.aoi_tt_repository import AoiTtRepository
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository
from src.inline_domain.infrastructure.ctq.ctq_repository import CtqRepository
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
)
from src.inline_domain.infrastructure.shared.measurement_metadata_loader import (
    InlineMeasurementMetadataRepository,
)
from src.inline_domain.infrastructure.shared.main_process_history_repository import (
    InlineMainProcessHistoryRepository,
)
from src.inline_domain.infrastructure.shared.measurement_preparation import (
    InlineMeasurementPreparationRepository,
)
from src.inline_domain.infrastructure.monitor.monitor_repository import InlineMonitorRepository
from src.inline_domain.infrastructure.monitor.scrap_repository import InlineScrapRepository
from src.inline_domain.infrastructure.spc.spc_repository import SpcRepository
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def build_raw_measurement_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> InlineMeasurementSnapshotRepository:
    return InlineMeasurementSnapshotRepository(
        snapshot_dir=Path("data") / prod_code,
        db_manager=db_manager,
    )


def build_measurement_preparation_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> InlineMeasurementPreparationRepository:
    return InlineMeasurementPreparationRepository(
        raw_measurements=build_raw_measurement_repository(db_manager, prod_code),
        metadata=InlineMeasurementMetadataRepository(db_manager),
        main_process_history=InlineMainProcessHistoryRepository(db_manager),
    )


def build_spc_repository(db_manager: DatabaseManager, prod_code: str) -> SpcRepository:
    return SpcRepository(build_measurement_preparation_repository(db_manager, prod_code))


def build_monitor_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> InlineMonitorRepository:
    return InlineMonitorRepository(
        build_spc_repository(db_manager, prod_code),
        InlineScrapRepository(),
    )


def build_ctq_repository(db_manager: DatabaseManager, prod_code: str) -> CtqRepository:
    return CtqRepository(build_spc_repository(db_manager, prod_code))


def build_aoi_tt_repository(db_manager: DatabaseManager, prod_code: str) -> AoiTtRepository:
    return AoiTtRepository(
        raw_measurements=build_raw_measurement_repository(db_manager, prod_code),
        metadata=InlineMeasurementMetadataRepository(db_manager),
    )


def build_aoi_rs_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> AoiRsSnapshotRepository:
    return AoiRsSnapshotRepository(
        snapshot_dir=Path("data") / prod_code,
        db_manager=db_manager,
    )


def refresh_aoi_rs_snapshots(
    db_manager: DatabaseManager,
    prod_code: str,
    end_date: str,
) -> bool:
    repository = build_aoi_rs_repository(db_manager, prod_code)
    query = AoiRsQueryConfig(
        prod_code=prod_code,
        start_date=end_date,
        end_date=end_date,
    )
    return repository.refresh(query)


def refresh_raw_measurements(
    db_manager: DatabaseManager,
    prod_code: str,
    end_date: str,
) -> bool:
    result = build_raw_measurement_repository(db_manager, prod_code).get_measurements(
        prod_code=prod_code,
        end_date=end_date,
        force_refresh=True,
    )
    return not result.empty
