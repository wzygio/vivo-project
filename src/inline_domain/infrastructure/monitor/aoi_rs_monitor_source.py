"""Bounded AOI_RS history inputs without creating L1 detail snapshots."""

from __future__ import annotations

from collections.abc import Callable
import hashlib
import json

import pandas as pd

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.application.aoi_rs.decoration_service import prepare_aoi_rs_decoration
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_lot_point_df, build_sheet_point_df
from src.inline_domain.core.aoi_rs.aoi_rs_special_decoration import (
    AOI_RS_DECORATION_POLICY_VERSION, project_factory_scoped_details,
)
from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    load_rs_details, load_pass_through, load_rs_spec_limits,
)
from src.inline_domain.infrastructure.shared.date_exclusion import apply_inline_date_exclusion
from src.shared_kernel.config import ConfigLoader


class AoiRsMonitorSource:
    def __init__(self, db_factory: Callable) -> None:
        self._db_factory = db_factory
        self._specs: dict[str, pd.DataFrame] = {}

    def _read_specs(self, product: str) -> pd.DataFrame:
        if product not in self._specs:
            self._specs[product] = load_rs_spec_limits(self._db_factory(), product, strict=True)
        return self._specs[product].copy()

    def history_signature(self, product: str) -> str:
        # A service instance can be reused by callers; an explicit query must see new specs.
        self._specs[product] = load_rs_spec_limits(self._db_factory(), product, strict=True)
        specs = self._read_specs(product).sort_values(["factory", "step_id", "rs_code", "type_flag"])
        value = self.policy_signature(product) + specs.to_json(orient="split", index=False)
        return hashlib.sha256(value.encode()).hexdigest()

    def policy_signature(self, product: str) -> str:
        domain = ConfigLoader.load_domain_config("inline_domain")
        signature = {
            "version": "aoi-rs-monitor-v1", "decoration_version": AOI_RS_DECORATION_POLICY_VERSION,
            "exclusion": domain.get("data_exclusion"),
            "special": domain.get("aoi_rs", {}).get("special_decoration"),
            "exemptions": ConfigLoader.get_auto_decoration_param_exemptions(),
            "decisions": get_scope_decision_signature("aoi_rs", product),
            "forward": ConfigLoader.get_data_forward_policy().signature,
            "cutoff": ConfigLoader.get_report_cutoff_policy().latest_day_time,
        }
        return hashlib.sha256(json.dumps(signature, sort_keys=True, default=str).encode()).hexdigest()

    def read_projected(self, product: str, factory: str, start: str, end: str) -> dict:
        if factory not in {"ARRAY", "OLED", "TP"}:
            raise ValueError("Invalid AOI_RS factory")
        db = self._db_factory()
        forward = ConfigLoader.get_data_forward_policy()
        source_start, source_end = forward.to_source_window(pd.Timestamp(start), pd.Timestamp(end))
        details, passed = [], []
        specs = self._read_specs(product)
        specs = specs.loc[specs["factory"].eq(factory)].copy()
        cursor = source_start
        # Every source read is limited to one product/factory/calendar-month window.
        while cursor <= source_end:
            tail = min(cursor + pd.offsets.MonthEnd(), source_end)
            query = AoiRsQueryConfig(prod_code=product, factory=factory,
                                     start_date=str(cursor.date()), end_date=str(tail.date()))
            details.append(load_rs_details(db, query, strict=True))
            cursor = tail + pd.Timedelta(days=1)
        rs = pd.concat(details, ignore_index=True)
        steps = tuple(sorted(set(specs["step_id"].astype(str)) | set(rs["step_id"].astype(str))))
        cursor = source_start
        while cursor <= source_end:
            tail = min(cursor + pd.offsets.MonthEnd(), source_end)
            query = AoiRsQueryConfig(prod_code=product, factory=factory,
                                     start_date=str(cursor.date()), end_date=str(tail.date()))
            passed.append(load_pass_through(db, query, strict=True, step_ids=steps))
            cursor = tail + pd.Timedelta(days=1)
        throughput = pd.concat(passed, ignore_index=True)

        def project(frame: pd.DataFrame) -> pd.DataFrame:
            display = forward.shift_frame(frame, ("start_time",))
            display = ConfigLoader.get_report_cutoff_policy().filter_frame(display, "start_time")
            display = apply_inline_date_exclusion(display)
            return display.loc[display["start_time"].ge(pd.Timestamp(start))
                               & display["start_time"].lt(pd.Timestamp(end) + pd.Timedelta(days=1))].copy()

        rs, throughput = project(rs), project(throughput)
        factories = tuple(ConfigLoader.get_aoi_rs_special_decoration_factories())
        window = ConfigLoader.get_aoi_rs_special_decoration_window()
        if factory in factories and not rs.empty:
            path = ConfigLoader.get_domain_resource_path("inline_domain", "aoi_rs_sheet_oos_decoration")
            decoration = prepare_aoi_rs_decoration(
                build_lot_point_df(rs, throughput), build_sheet_point_df(rs), specs,
                path.parent, product, persist=False, workbook_path=path,
                exempt_param_name_contains=ConfigLoader.get_auto_decoration_param_exemptions(),
                rs_details_df=rs, special_factories=factories, pass_through_df=throughput,
                decoration_window=window,
            )
            projected = project_factory_scoped_details(rs, decoration.sheet_points_df, factories, decoration_window=window)
        else:
            projected = rs.copy()
        keys = ["prod_code", "factory", "step_id", "rs_code"]
        universe = pd.concat([rs[keys], specs[keys]], ignore_index=True).drop_duplicates()
        descriptions = specs[[*keys, "code_desc"]].drop_duplicates(keys)
        universe = universe.merge(descriptions, on=keys, how="left")
        return {"rs_details_df": projected, "pass_through_df": throughput, "indicators_df": universe,
                "special_factories": factories, "decoration_window": window}
