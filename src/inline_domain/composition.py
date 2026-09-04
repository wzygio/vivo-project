"""Composition root for Inline report outbound adapters."""

from __future__ import annotations

from functools import partial
from pathlib import Path

from src.inline_domain.infrastructure.aoi_tt.aoi_tt_repository import AoiTtRepository
from src.inline_domain.infrastructure.aoi_tt.particle_size_loader import (
    load_particle_size_counts,
)
from src.inline_domain.infrastructure.aoi_tt.particle_size_ratio_loader import (
    load_particle_size_ratios,
)
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
from src.inline_domain.core.shared.measurement_correction import (
    apply_spc_value_corrections,
)
from src.inline_domain.infrastructure.spc.spc_repository import SpcRepository
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.shared_kernel.config import ConfigLoader


def build_raw_measurement_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> InlineMeasurementSnapshotRepository:
    return InlineMeasurementSnapshotRepository(
        snapshot_dir=Path("data") / prod_code,
        db_manager=db_manager,
        measurement_corrector=apply_spc_value_corrections,
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
        particle_size_loader=partial(load_particle_size_counts, db_manager),
        particle_size_ratio_loader=partial(
            load_particle_size_ratios,
            ConfigLoader.get_aoi_tt_particle_size_ratio_spec_path(),
        ),
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
    """页头「刷新数据」L1 handler：仅在真实从数据库刷新成功时返回 True。

    空数据窗口（数据库正常返回空）算成功；数据库失败降级旧快照时返回 False，
    让上层保留 revision 与 L2 缓存（PRD 11.1）。
    """
    result = build_raw_measurement_repository(db_manager, prod_code).refresh_measurements(
        prod_code=prod_code,
        end_date=end_date,
    )
    return result.refreshed_from_db
