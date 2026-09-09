from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.yield_domain.application import yield_service
from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.application.yield_service import (
    YieldAnalysisService,
    YieldDataLoadError,
    YieldDataModificationError,
    YieldWarningLinesReadError,
)
from src.shared_kernel.data_health import attach_data_health, make_data_health


def test_empty_health_survives_modified_cache_and_facade(monkeypatch, mock_config):
    health = make_data_health("fresh", source_start="2026-06-01", source_end="2026-09-01")
    monkeypatch.setattr(YieldAnalysisService, "get_raw_panel_details", staticmethod(lambda *args: attach_data_health(pd.DataFrame(), health)))
    YieldAnalysisService.get_modified_panel_details.clear()
    for _ in range(2):
        result = YieldAnalysisService.get_modified_panel_details(mock_config, snapshot_signature="empty-health")
        assert result.empty
        assert result.attrs["data_health"] == health
    assert YieldAnalysisService.get_data_health(mock_config, snapshot_signature="empty-health") == health


def test_source_failure_is_not_cached(monkeypatch, mock_config, tmp_path: Path) -> None:
    attempts = 0

    class FailingDataPort:
        def read_panel(self, query, policy):
            nonlocal attempts
            attempts += 1
            raise ConnectionError("database unavailable")

    query, _ = YieldAnalysisService._build_panel_request(
        mock_config,
        "2026-06-01",
        "2026-09-01",
    )
    monkeypatch.setattr(yield_service, "_resolve_data_port", lambda *args: FailingDataPort())
    YieldAnalysisService.get_raw_panel_details.clear()

    for _ in range(2):
        with pytest.raises(YieldDataLoadError):
            YieldAnalysisService.get_raw_panel_details(
                query.model_dump_json(),
                YieldDataPolicy.from_app_config(mock_config).model_dump_json(),
                snapshot_signature="source-failure",
            )

    assert attempts == 2


def test_modified_panel_cache_key_contains_explicit_analysis_window(
    monkeypatch,
    mock_config,
) -> None:
    queries: list[YieldQueryConfig] = []

    def fake_raw(query_json, *_args, **_kwargs):
        queries.append(YieldQueryConfig.model_validate_json(query_json))
        return pd.DataFrame({"lot_id": ["L1"]})

    YieldAnalysisService.get_modified_panel_details.clear()
    monkeypatch.setattr(YieldAnalysisService, "get_raw_panel_details", staticmethod(fake_raw))
    monkeypatch.setattr(yield_service, "apply_defect_multipliers", lambda frame, _config: frame)

    for end_date in ("2026-08-31", "2026-09-01"):
        YieldAnalysisService.get_modified_panel_details(
            mock_config,
            snapshot_signature="same-snapshot",
            analysis_start_date="2026-06-01",
            analysis_end_date=end_date,
        )

    assert [query.end_date for query in queries] == ["2026-08-31", "2026-09-01"]


def test_modifier_failure_is_not_cached(monkeypatch, mock_config) -> None:
    attempts = 0

    monkeypatch.setattr(
        YieldAnalysisService,
        "get_raw_panel_details",
        staticmethod(lambda *_args, **_kwargs: pd.DataFrame({"lot_id": ["L1"]})),
    )

    def fail_modifier(_frame, _config):
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid multiplier")

    monkeypatch.setattr(yield_service, "apply_defect_multipliers", fail_modifier)
    YieldAnalysisService.get_modified_panel_details.clear()

    for _ in range(2):
        with pytest.raises(YieldDataModificationError):
            YieldAnalysisService.get_modified_panel_details(
                mock_config,
                snapshot_signature="modifier-failure",
                analysis_start_date="2026-06-01",
                analysis_end_date="2026-09-01",
            )

    assert attempts == 2


def test_warning_workbook_failure_is_not_cached(
    monkeypatch,
    mock_config,
    tmp_path: Path,
) -> None:
    warning_resource = mock_config.paths["static_warning_lines"]
    product_dir = tmp_path / "M678"
    warning_path = product_dir.parent / warning_resource.file_name
    warning_path.write_bytes(b"not-an-excel-file")
    attempts = 0

    def fail_read(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise OSError("locked workbook")

    monkeypatch.setattr(pd, "read_excel", fail_read)
    YieldAnalysisService.load_static_warning_lines.clear()

    for _ in range(2):
        with pytest.raises(YieldWarningLinesReadError):
            YieldAnalysisService.load_static_warning_lines(
                mock_config,
                product_dir,
                snapshot_signature="same-snapshot",
                warning_signature="same-warning",
            )

    assert attempts == 2
