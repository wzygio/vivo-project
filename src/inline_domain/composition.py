"""Composition root for Inline report outbound adapters."""

from __future__ import annotations

from functools import lru_cache, partial
from pathlib import Path
from typing import TYPE_CHECKING

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
from src.shared_kernel.snapshot_paths import (
    inline_measurement_directory, aoi_rs_snapshot_directory,
)
from src.inline_domain.infrastructure.shared.resource_paths import (
    monitor_summary_workbook_path,
)

if TYPE_CHECKING:
    from src.inline_domain.application.shared.decoration_ports import DecorationPort


@lru_cache(maxsize=1)
def build_decoration_port() -> DecorationPort:
    """Share one stateless workbook adapter across compatibility call sites."""
    from src.inline_domain.infrastructure.shared.decoration_store import ExcelDecorationStore

    return ExcelDecorationStore()


def build_oos_history_service(resource_dir: Path | None = None):
    """Assemble the shared OOS use case at the composition boundary."""
    resolved_resources = resource_dir or ConfigLoader.get_domain_resource_dir("inline_domain")
    return _build_oos_history_service(
        str(resolved_resources),
        resource_dir is None,
    )


def build_ooc_history_service(resource_dir: Path | None = None):
    """Assemble the shared OOC use case at the composition boundary."""
    resolved_resources = resource_dir or ConfigLoader.get_domain_resource_dir("inline_domain")
    return _build_ooc_history_service(
        str(resolved_resources),
        resource_dir is None,
    )


def build_throughput_history_service(resource_dir: Path | None = None):
    """Assemble the lightweight daily-throughput history boundary."""
    project_root = ConfigLoader.get_project_root()
    snapshot_dir = (
        project_root / "data" / "inline_domain"
        if resource_dir is None
        else Path(resource_dir) / ".throughput_history"
    )
    return _build_throughput_history_service(str(snapshot_dir))


def build_monitor_summary_workbook_service(resource_dir: Path | None = None):
    """Assemble the automatic-warning Excel read model."""
    workbook_path = monitor_summary_workbook_path(resource_dir)
    return _build_monitor_summary_workbook_service(str(workbook_path))


@lru_cache(maxsize=16)
def _build_monitor_summary_workbook_service(workbook_path: str):
    from src.inline_domain.application.monitor.summary_workbook_service import (
        MonitorSummaryWorkbookService,
    )
    from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
        MonitorSummaryWorkbookStore,
    )

    return MonitorSummaryWorkbookService(
        MonitorSummaryWorkbookStore(Path(workbook_path))
    )


@lru_cache(maxsize=16)
def _build_throughput_history_service(snapshot_dir: str):
    from src.inline_domain.application.shared.throughput_history_service import (
        ThroughputHistoryService,
    )
    from src.inline_domain.infrastructure.shared.throughput_history_store import (
        ThroughputHistoryStore,
    )

    return ThroughputHistoryService(ThroughputHistoryStore(Path(snapshot_dir)))


@lru_cache(maxsize=16)
def _build_ooc_history_service(
    resource_dir: str, use_configured_paths: bool
):
    from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
    from src.inline_domain.core.shared.sheet_ooc_decoration import (
        SCOPE_OOC_DECORATION_FILE_NAME,
    )
    from src.inline_domain.infrastructure.shared.resource_paths import decision_workbook_paths

    configured = decision_workbook_paths("ooc") if use_configured_paths else {}

    paths = configured or {
        scope: Path(resource_dir) / scope / name
        for scope, name in SCOPE_OOC_DECORATION_FILE_NAME.items()
    }
    return ExcelAlarmReader(ExcelAlarmStore(paths), "ooc")


@lru_cache(maxsize=16)
def _build_oos_history_service(
    resource_dir: str, use_configured_paths: bool
):
    from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
    from src.inline_domain.application.shared.oos_history_service import SCOPE_DECORATION_FILE_NAME
    from src.inline_domain.infrastructure.shared.resource_paths import decision_workbook_paths

    configured = decision_workbook_paths("oos") if use_configured_paths else {}

    paths = configured or {
        scope: Path(resource_dir) / scope / name
        for scope, name in SCOPE_DECORATION_FILE_NAME.items()
    }
    return ExcelAlarmReader(ExcelAlarmStore(paths), "oos")


def build_live_monitor_source(resource_dir: Path | None = None):
    from src.inline_domain.application.monitor.live_source import LiveMonitorSource
    from src.inline_domain.infrastructure.monitor.input_signature import monitor_input_signature
    from src.inline_domain.infrastructure.shared.resource_paths import scope_resource_dir

    resources = resource_dir or ConfigLoader.get_domain_resource_dir("inline_domain")
    summary_path = monitor_summary_workbook_path(resource_dir)

    def port_factory(scope: str, product: str):
        db = DatabaseManager()
        if scope in {"spc", "ctq"}:
            return build_spc_repository(db, product)
        if scope == "aoi_tt":
            return build_aoi_tt_repository(db, product)
        if scope == "aoi_rs":
            return build_aoi_rs_repository(db, product, require_complete_coverage=True)
        raise ValueError(f"Unsupported Inline scope: {scope}")

    return LiveMonitorSource(
        port_factory=port_factory,
        signature_provider=lambda products: monitor_input_signature(
            ConfigLoader.get_project_root(),
            Path(resources),
            products,
            excluded_paths=(summary_path,),
        ),
        resource_dir_provider=(lambda scope: Path(resource_dir)) if resource_dir else scope_resource_dir,
    )


def build_live_throughput_reader():
    from src.inline_domain.application.monitor.live_source import LiveThroughputReader

    return LiveThroughputReader(build_live_monitor_source())


def build_cpk_monitor_service(resource_dir: Path | None = None):
    from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
    from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
    from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore

    resources = resource_dir or ConfigLoader.get_domain_resource_dir("inline_domain")
    return CpkWorkbookMonitorService(
        CpkSummaryWorkbookStore(monitor_summary_workbook_path(resource_dir)),
        latest_reader=CpkLatestExcelStore(Path(resources) / "spc" / "spc_cpk_cpm_decoration.xlsx"),
    )


def build_raw_measurement_repository(
    db_manager: DatabaseManager,
    prod_code: str,
) -> InlineMeasurementSnapshotRepository:
    return InlineMeasurementSnapshotRepository(
        snapshot_dir=inline_measurement_directory(ConfigLoader.get_project_root() / "data"),
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
    *,
    require_complete_coverage: bool = False,
) -> AoiRsSnapshotRepository:
    return AoiRsSnapshotRepository(
        snapshot_dir=aoi_rs_snapshot_directory(ConfigLoader.get_project_root() / "data"),
        db_manager=db_manager,
        require_complete_coverage=require_complete_coverage,
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
