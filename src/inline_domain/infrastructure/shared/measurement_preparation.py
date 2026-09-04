"""Shared inline measurement preparation pipeline.

Owns the cross-module preparation logic previously hosted by ``SpcRepository``:
clean/coercion/dedup, LOSS exclusion, whitelist merge + data_type classification,
outlier filtering, time/dimension filters, main-process trace, and spec-limit
YAML overrides. SPC/CTQ/monitor consume this through thin projection adapters.
"""

from __future__ import annotations

import logging

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.inline_domain.core.monitor.monitor_param_classifier import classify_param_type
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.ports.measurement_snapshot import (
    MainProcessHistoryPort,
    MeasurementMetadataPort,
    MeasurementSnapshotPort,
)
from src.inline_domain.infrastructure.shared.main_process_trace import (
    apply_main_process_history,
    attach_main_process_spec,
)
from src.inline_domain.infrastructure.shared.measurement_preprocessor import (
    filter_excluded_param_names,
)
from src.inline_domain.infrastructure.shared.outlier_filter_rules import (
    apply_outlier_filter_rules,
    load_outlier_filter_rules,
)
from src.shared_kernel.config import ConfigLoader


class InlineMeasurementPreparationRepository:
    """Prepare the shared inline measurement projection from the three data ports."""

    def __init__(
        self,
        raw_measurements: MeasurementSnapshotPort,
        metadata: MeasurementMetadataPort,
        main_process_history: MainProcessHistoryPort,
    ) -> None:
        if raw_measurements is None or metadata is None or main_process_history is None:
            raise ValueError(
                "Measurement preparation repository requires raw measurement, "
                "metadata, and history ports"
            )
        self.raw_measurements = raw_measurements
        self.metadata = metadata
        self.main_process_history = main_process_history

    # ==========================================
    # 规格线数据拉取代理
    # ==========================================
    def get_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """
        提取产品管控规格线，并应用 YAML 配置覆盖。
        职责代理：让 Service 层彻底与 data_loader 解耦。
        （由于规格表数据量极小且变动不频繁，此处可选择直接透传或后期加入轻量级缓存）
        """
        logging.info(f"[SpcRepo] 代理拉取 {prod_code} 规格基准线...")
        # 1. 从数据库获取原始规格
        spec_df = self.metadata.get_parameter_specs(prod_code)

        # 2. 从 YAML 配置读取规格覆盖项
        spec_overrides = self._load_spec_overrides_from_yaml(prod_code)

        # 3. 应用覆盖
        if spec_overrides and not spec_df.empty:
            spec_df = self._apply_spec_overrides(spec_df, spec_overrides, prod_code)

        return spec_df

    def _load_spec_overrides_from_yaml(self, prod_code: str) -> list:
        """
        从产品的 YAML 配置文件中读取 spc_spec_override 配置。

        Returns:
            list: 规格覆盖配置列表，每个元素为 dict 包含 step_id, param_name, ucl/lcl/usl/lsl 等
        """
        try:
            project_root = ConfigLoader.get_project_root()
            product_yaml_path = project_root / "config" / "products" / f"{prod_code}.yaml"

            if not product_yaml_path.exists():
                return []

            yaml_config = ConfigLoader._load_yaml(product_yaml_path)
            overrides = yaml_config.get('spc_spec_override', [])

            if overrides:
                logging.info(f"[SpcRepo] 从 YAML 加载到 {len(overrides)} 条规格覆盖配置")

            return overrides if isinstance(overrides, list) else []

        except Exception as e:
            logging.warning(f"[SpcRepo] 读取规格覆盖配置失败: {e}")
            return []

    def _apply_spec_overrides(
        self,
        spec_df: pd.DataFrame,
        overrides: list,
        prod_code: str
    ) -> pd.DataFrame:
        """
        将 YAML 中的规格覆盖应用到数据库规格数据框。
        匹配条件：prod_code + step_id + param_name 三者必须完全匹配

        Args:
            spec_df: 原始规格数据框
            overrides: YAML 中的覆盖配置列表
            prod_code: 产品代码

        Returns:
            pd.DataFrame: 应用覆盖后的规格数据框
        """
        df = spec_df.copy()
        applied_count = 0

        for override in overrides:
            # 三重匹配条件：prod_code + step_id + param_name
            override_prod = override.get('prod_code')
            step_id = override.get('step_id')
            param_name = override.get('param_name')

            # 1. 检查 prod_code 是否匹配（如果配置了的话）
            if override_prod and override_prod != prod_code:
                continue

            # 2. 构建精确匹配条件（step_id 和 param_name 必须同时指定）
            if not step_id or not param_name:
                logging.warning(
                    f"[SpcRepo] 规格覆盖配置不完整，跳过: prod_code={override_prod or prod_code}, "
                    f"step_id={step_id}, param_name={param_name}"
                )
                continue

            # 3. 三重匹配：prod_code（已验证）+ step_id + param_name
            mask = (df['step_id'] == step_id) & (df['param_name'] == param_name)

            # 4. 应用覆盖值
            if mask.any():
                for col in ['ucl', 'lcl', 'usl', 'lsl', 'target']:
                    if col in override and override[col] is not None:
                        old_val = df.loc[mask, col].iloc[0] if mask.sum() > 0 else None
                        df.loc[mask, col] = override[col]
                        logging.info(
                            f"[SpcRepo] 规格覆盖: {prod_code}-{step_id}-{param_name} {col}: "
                            f"{old_val} → {override[col]}"
                        )
                applied_count += 1
            else:
                logging.warning(
                    f"[SpcRepo] 未找到匹配规格: {prod_code}-{step_id}-{param_name}，跳过覆盖"
                )

        if applied_count > 0:
            logging.info(f"[SpcRepo] 共成功应用 {applied_count} 条规格覆盖配置")

        return df

    # ==========================================
    # 量测明细制备 (强制 3 个月看板逻辑)
    # ==========================================
    def get_prepared_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        """Return the prepared measurement projection derived from the shared ports."""
        raw = self.raw_measurements.get_measurements(
            config.prod_code,
            config.end_date,
            force_refresh,
        )
        return self._prepare_shared_measurements(raw, config)

    def _prepare_shared_measurements(
        self, raw: pd.DataFrame, config: SpcQueryConfig
    ) -> pd.DataFrame:
        if raw.empty:
            return raw.copy()
        prepared = raw.rename(columns={"start_time": "sheet_start_time"}).copy()
        prepared["sheet_start_time"] = pd.to_datetime(
            prepared["sheet_start_time"], errors="coerce"
        )
        prepared["param_value"] = pd.to_numeric(
            prepared["param_value"], errors="coerce"
        )
        prepared = prepared.dropna(subset=["sheet_start_time", "param_value"])
        prepared = filter_excluded_param_names(prepared)
        prepared = prepared.sort_values("sheet_start_time").drop_duplicates(
            subset=["prod_code", "factory", "sheet_id", "step_id", "param_name", "site_name"],
            keep="last",
        )
        catalog = self.metadata.get_parameter_catalog(config.prod_code)
        if catalog is None:
            prepared["data_type"] = "UNKNOWN"
        elif catalog.empty:
            return prepared.iloc[0:0]
        else:
            typed = catalog.copy()
            typed["data_type"] = typed["data_type"].apply(classify_param_type)
            target = (config.data_type_filter or "ALL").upper()
            if target != "ALL":
                typed = typed[typed["data_type"].eq(target)]
            prepared["param_name_upper"] = prepared["param_name"].astype(str).str.upper()
            prepared = prepared.merge(
                typed,
                left_on="param_name_upper",
                right_on="ref_param_name",
                how="inner",
            ).drop(columns=["param_name_upper", "ref_param_name"])
        prepared = self._apply_outlier_filters(prepared, config.prod_code)
        start = pd.Timestamp(config.start_date)
        end = pd.Timestamp(config.end_date) + pd.Timedelta(days=1)
        prepared = prepared[
            prepared["sheet_start_time"].ge(start) & prepared["sheet_start_time"].lt(end)
        ].copy()
        if config.factory:
            prepared = prepared[prepared["factory"].eq(config.factory.upper())]
        if config.step_id:
            prepared = prepared[prepared["step_id"].eq(config.step_id)]
        if config.param_name:
            prepared = prepared[prepared["param_name"].eq(config.param_name)]
        if prepared.empty:
            return prepared.reset_index(drop=True)
        specs = self.metadata.get_parameter_specs(config.prod_code)
        routed = attach_main_process_spec(prepared.reset_index(drop=True), specs)
        history = self.main_process_history.get_main_process_history(
            routed,
            history_start=start - relativedelta(months=1),
            history_end=pd.Timestamp(config.end_date),
        )
        return apply_main_process_history(routed, history)

    def _apply_outlier_filters(self, df: pd.DataFrame, prod_code: str) -> pd.DataFrame:
        """Load mandatory rules and physically remove matching measurements."""
        if df.empty:
            return df.copy()
        project_root = ConfigLoader.get_project_root()
        rule_file = ConfigLoader.get_domain_resource_path(
            "inline_domain",
            "spc_outlier_filters",
            "spc_outlier_filters.xlsx",
        )
        rules = load_outlier_filter_rules(
            rule_file,
            project_root / "output" / "decrypted_files",
        )
        return apply_outlier_filter_rules(df, prod_code, rules)
