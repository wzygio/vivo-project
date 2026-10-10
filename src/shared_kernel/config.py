# src\shared_kernel\config.py
import yaml
import logging
from pathlib import Path
from datetime import date
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# 引入我们定义的 Pydantic 模型
from src.shared_kernel.config_model import AppConfig
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy

class ConfigLoader:
    """
    [配置工厂]
    纯静态工具类，负责按需加载配置。
    不持有任何状态，不创建全局单例。
    """

    @classmethod
    def get_enabled_products(cls) -> List[str]:
        """
        [新增] 从 global.yaml 读取启用的产品列表。
        这成为了系统产品列表的唯一真理来源。
        """
        root_dir = cls.get_project_root()
        global_yaml_path = root_dir / "config" / "global.yaml"
        
        try:
            global_conf = cls._load_yaml(global_yaml_path)
            
            # 读取 product_registry.enabled_products
            registry = global_conf.get('product_registry', {})
            products = registry.get('enabled_products', [])
            
            if not products:
                logging.warning(f"⚠️ global.yaml 中未找到有效的 enabled_products 列表，将回退到默认 ['M678']。")
                return ["M678"]
            return products
            
        except Exception as e:
            logging.error(f"❌ 读取全局产品列表失败: {e}")
            return ["M678"] # 最后的防线

    @classmethod
    def get_product_annotations(cls) -> Dict[str, str]:
        """Read optional product annotations for presentation only."""
        yaml_path = cls.get_project_root() / "config" / "global.yaml"
        registry = cls._load_yaml(yaml_path).get("product_registry", {})
        if not isinstance(registry, dict):
            raise ValueError("product_registry must be a mapping")
        annotations = registry.get("product_annotations")
        if annotations is None:
            return {}
        if not isinstance(annotations, dict) or any(
            not isinstance(code, str)
            or (annotation is not None and not isinstance(annotation, str))
            for code, annotation in annotations.items()
        ):
            raise ValueError("product_annotations must map product codes to text")
        return {
            code: annotation.strip()
            for code, annotation in annotations.items()
            if annotation is not None and annotation.strip()
        }

    @classmethod
    def get_work_order_types(cls) -> List[str]:
        """Return the global work-order allowlist used by report queries."""
        yaml_path = cls.get_project_root() / "config" / "global.yaml"
        global_conf = cls._load_yaml(yaml_path)
        data_source = global_conf.get("data_source", {})
        if not isinstance(data_source, dict):
            raise ValueError("global.yaml: 'data_source' must be a mapping")
        configured = data_source.get("work_order_types", [])
        if not isinstance(configured, list):
            raise ValueError(
                "global.yaml: 'data_source.work_order_types' must be a list"
            )
        normalized = tuple(
            dict.fromkeys(
                str(value).strip()
                for value in configured
                if value is not None and str(value).strip()
            )
        )
        if not normalized:
            raise ValueError(
                "global.yaml: 'data_source.work_order_types' must not be empty"
            )
        return list(normalized)
        
    @staticmethod
    def get_project_root() -> Path:
        """健壮的动态计算项目根目录"""
        current_dir = Path(__file__).resolve().parent
        for parent in [current_dir] + list(current_dir.parents):
            if (parent / "pyproject.toml").exists():
                return parent
        return Path.cwd() # Fallback

    @staticmethod
    def _load_yaml(file_path: Path) -> Dict[str, Any]:
        """内部辅助：安全加载 YAML"""
        if not file_path.exists():
            logging.warning(f"配置文件未找到: {file_path}")
            return {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logging.error(f"解析 YAML 失败 ({file_path}): {e}")
            return {}

    @staticmethod
    def _deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """
        内部辅助：递归合并字典。
        优先使用 update 中的值覆盖 base。
        """
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigLoader._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    @classmethod
    def load_config(cls, product_code: str) -> AppConfig:
        """
        [核心入口] 加载指定产品的完整配置对象。
        
        Args:
            product_code (str): 产品代码，如 "M678"。这将决定加载哪个 YAML 文件。
            
        Returns:
            AppConfig: 校验通过的 Pydantic 配置对象。
        """
        root_dir = cls.get_project_root()
        config_dir = root_dir / "config"
        
        # 1. 路径组装
        global_yaml_path = config_dir / "global.yaml"
        product_yaml_path = config_dir / "products" / f"{product_code}.yaml"
        env_path = root_dir / ".env"

        logging.info(f"正在构建配置对象 (Product: {product_code})...")

        # 2. 加载 .env 环境变量 (如有)
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)

        # 3. 加载 YAML
        global_conf = cls._load_resource_global_config()
        product_conf = cls._load_yaml(product_yaml_path)

        if not global_conf and not product_conf:
            msg = f"未找到任何有效配置！请检查路径: {config_dir}"
            logging.error(msg)
            # 在这一步抛出异常是合理的，因为没有配置程序无法运行
            raise FileNotFoundError(msg)

        # 4. 深度合并 (Global < Product)
        merged_conf = cls._deep_merge(global_conf, product_conf)
        # Resource locations are global; product configuration owns sheet selection.
        if cls._global_resource_config("yield_domain", global_conf) is not None:
            paths = dict(merged_conf.get("paths", {}))
            for alias, resource_key in {
                "static_warning_lines": "static_warning_lines",
                "rate_override_config": "override_rates",
                "yield_modifier_config": "yield_modifier_table",
            }.items():
                metadata = dict(paths.get(alias, {}))
                metadata["file_name"] = str(cls.get_domain_resource_path("yield_domain", resource_key))
                paths[alias] = metadata
            merged_conf["paths"] = paths

        # 5. 数据源一致性强制覆盖
        # 即使 YAML 里写错了 product_code，也以传入参数为准
        if 'data_source' not in merged_conf:
            merged_conf['data_source'] = {}
        merged_conf['data_source']['product_code'] = product_code

        # 6. Pydantic 实例化与校验
        try:
            config_obj = AppConfig.model_validate(merged_conf)
            logging.info(f"✅ 配置加载完成: {product_code}")
            return config_obj
        except Exception as e:
            logging.error(f"❌ 配置数据校验失败: {e}")
            raise ValueError(f"配置不符合 Schema 定义: {e}") from e

    @classmethod
    def get_snapshot_ttl_hours(cls) -> int:
        """兼容快照仓储调用；TTL 同样服从全局应用缓存配置。"""
        return cls.get_cache_ttl_seconds() // 3600

    @classmethod
    def get_incremental_refresh_days(cls) -> int:
        """Load the shared source-snapshot overlap; older configs default to 7 days."""
        yaml_path = cls.get_project_root() / "config" / "global.yaml"
        application = cls._load_yaml(yaml_path).get("application", {})
        if not isinstance(application, dict):
            raise ValueError("global.yaml: 'application' must be a mapping")
        days = application.get("incremental_refresh_days", 7)
        if isinstance(days, bool) or not isinstance(days, int) or days <= 0:
            raise ValueError(
                "global.yaml: 'application.incremental_refresh_days' must be a positive integer"
            )
        return days

    @classmethod
    def get_data_forward_policy(cls) -> DataForwardPolicy:
        """Load the global manufacturing source-time display policy."""
        yaml_path = cls.get_project_root() / "config" / "global.yaml"
        data_forward = cls._load_yaml(yaml_path).get("data_forward", {})
        if not isinstance(data_forward, dict):
            raise ValueError("global.yaml: 'data_forward' must be a mapping")
        return DataForwardPolicy(
            enabled=data_forward.get("enabled", False),
            offset_days=data_forward.get("offset_days", 4),
        )

    @classmethod
    def get_report_cutoff_policy(cls) -> ReportCutoffPolicy:
        """Read the global latest-day cutoff; never derive it from UI dates."""
        from src.shared_kernel.report_cutoff_config import load_report_cutoff_policy

        return load_report_cutoff_policy(cls)

    @classmethod
    def get_cache_ttl_seconds(cls) -> int:
        """
        从 global.yaml 读取应用数据缓存的唯一 TTL，返回秒。

        ``application.cache_ttl_hours`` 是所有项目自有 ``st.cache_data`` 的
        单一事实源。配置缺失或非法时直接报错，避免服务在不受控的回退 TTL 下运行。
        """
        yaml_path = cls.get_project_root() / "config" / "global.yaml"
        application = cls._load_yaml(yaml_path).get("application", {})
        if not isinstance(application, dict):
            raise ValueError("global.yaml: 'application' must be a mapping")
        try:
            ttl_hours = int(application["cache_ttl_hours"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "global.yaml: 'application.cache_ttl_hours' must be a positive integer"
            ) from exc
        if ttl_hours <= 0:
            raise ValueError(
                "global.yaml: 'application.cache_ttl_hours' must be a positive integer"
            )
        return ttl_hours * 3600

    @classmethod
    def get_service_cache_ttl_seconds(cls, cache_name: str, default_hours: int) -> int:
        """兼容旧调用；服务名和默认值不再影响 TTL。"""
        del cache_name, default_hours
        return cls.get_cache_ttl_seconds()

    @classmethod
    def get_domain_config_path(cls, domain: str) -> Path:
        """
        [路由] 解析 domain 配置文件路径。
        优先读取 global.yaml 的 domain_config 段；未配置时回退到
        config/domain/<domain>.yaml 约定路径。
        """
        root_dir = cls.get_project_root()
        router = cls._load_yaml(root_dir / "config" / "global.yaml").get("domain_config", {})
        configured = router.get(domain) if isinstance(router, dict) else None
        if configured:
            path = Path(str(configured))
            return path if path.is_absolute() else root_dir / path
        return root_dir / "config" / "domain" / f"{domain}.yaml"

    @classmethod
    def load_domain_config(cls, domain: str) -> Dict[str, Any]:
        """加载指定 domain 的 YAML 配置（路径由 global.yaml 路由决定）。"""
        return cls._load_yaml(cls.get_domain_config_path(domain))

    @classmethod
    def _load_resource_global_config(cls) -> Dict[str, Any]:
        """A broken global config must never select a different maintained file."""
        path = cls.get_project_root() / "config" / "global.yaml"
        try:
            with path.open(encoding="utf-8") as stream:
                config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError("global.yaml cannot be parsed for resource resolution") from exc
        if not isinstance(config, dict) or not config:
            raise ValueError("global.yaml must contain a non-empty mapping")
        return config

    @classmethod
    def _global_resource_config(
        cls, domain: str, global_conf: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        """None supports older isolated configs; an active registry is strict."""
        if global_conf is None:
            global_conf = cls._load_resource_global_config()
        if "resources" not in global_conf:
            return None
        registry = global_conf["resources"]
        if not isinstance(registry, dict):
            raise ValueError("global.yaml: 'resources' must be a mapping")
        values = registry[domain]
        if not isinstance(values, dict):
            raise ValueError(f"global.yaml: resources.{domain} must be a mapping")
        return values

    @classmethod
    def _resolve_registered_path(cls, value: object, setting: str) -> Path:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"global.yaml: {setting} must be a non-empty path string")
        path = Path(value.strip())
        return path if path.is_absolute() else cls.get_project_root() / path

    @classmethod
    def get_domain_resource_directory(cls, domain: str, key: str) -> Path:
        """Resolve an explicitly registered collection directory, including uploads."""
        resources = cls._global_resource_config(domain)
        if resources is None:
            raise KeyError(f"Global resource directories are not configured: {domain}/{key}")
        directories = resources.get("directories", {})
        if not isinstance(directories, dict):
            raise ValueError(f"global.yaml: resources.{domain}.directories must be a mapping")
        return cls._resolve_registered_path(
            directories[key], f"resources.{domain}.directories.{key}",
        )

    @classmethod
    def get_domain_resource_dir(cls, domain: str) -> Path:
        """
        Resolve the domain root from global resources.<domain>.dir.
        Valid older isolated configs without a registry retain legacy resolution.
        """
        global_resources = cls._global_resource_config(domain)
        if global_resources is not None:
            return cls._resolve_registered_path(global_resources["dir"], f"resources.{domain}.dir")
        root_dir = cls.get_project_root()
        resources_conf = cls.load_domain_config(domain).get("resources", {})
        configured = resources_conf.get("dir") if isinstance(resources_conf, dict) else None
        if configured:
            path = Path(str(configured))
            return path if path.is_absolute() else root_dir / path
        return root_dir / "resources" / domain

    @classmethod
    def get_domain_resource_path(cls, domain: str, key: str, default_name: Optional[str] = None) -> Path:
        """
        Resolve a full relative/absolute file path from the global registry.
        Active registries reject missing keys; legacy defaults apply only to valid
        older isolated configs without the resources section.
        """
        global_resources = cls._global_resource_config(domain)
        if global_resources is not None:
            files = global_resources.get("files", {})
            if not isinstance(files, dict):
                raise ValueError(f"global.yaml: resources.{domain}.files must be a mapping")
            return cls._resolve_registered_path(files[key], f"resources.{domain}.files.{key}")
        root_dir = cls.get_project_root()
        resources_conf = cls.load_domain_config(domain).get("resources", {})
        files = resources_conf.get("files", {}) if isinstance(resources_conf, dict) else {}
        configured = files.get(key) if isinstance(files, dict) else None
        if configured:
            path = Path(str(configured))
            if path.is_absolute():
                return path
            if len(path.parts) > 1:
                return root_dir / path
            return cls.get_domain_resource_dir(domain) / path
        if default_name:
            return cls.get_domain_resource_dir(domain) / default_name
        raise KeyError(f"{domain} 配置的 resources.files 中未找到资源键: {key}")

    @classmethod
    def get_compliance_config_path(cls) -> Path:
        """Return the single workbook used by the manager and runtime engine."""
        return cls.get_domain_resource_path("inline_domain", "compliance_config", "compliance_config.xlsx")

    @classmethod
    def get_compliance_config(cls) -> dict:
        """旧后台四维修饰已停用；新矩阵配置只由前端管理器读取。"""
        return {"rules": []}

    @classmethod
    def get_spc_period_sigma_source(cls) -> str:
        """Read the SPC capability period sigma source from the inline domain config."""
        try:
            spc_conf = cls.load_domain_config("inline_domain").get("spc", {})
            report_conf = spc_conf.get("spc_cpk", {})
            return str(report_conf.get("period_sigma_source", "sheet_mean")).strip().lower()
        except Exception as e:
            logging.error(f"❌ 读取 SPC 周期能力口径配置失败: {e}")
            return "sheet_mean"

    @classmethod
    def get_spc_point_decoration_policy(
        cls, *, today: date | None = None,
    ) -> tuple[float, str, str] | None:
        """Resolve the optional central specification band and its display dates."""
        report = cls.load_domain_config("inline_domain").get("spc", {})
        if not isinstance(report, dict):
            raise ValueError("spc must be a mapping")
        section = report.get("point_decoration", {})
        if not isinstance(section, dict):
            raise ValueError("spc.point_decoration must be a mapping")
        enabled = section.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("spc.point_decoration.enabled must be a boolean")
        if not enabled:
            return None
        configured = section.get("central_fraction", .5)
        if isinstance(configured, bool):
            raise ValueError("spc.point_decoration.central_fraction must be in (0, 1]")
        try:
            fraction = float(configured)
        except (TypeError, ValueError) as exc:
            raise ValueError("spc.point_decoration.central_fraction must be in (0, 1]") from exc
        if not 0 < fraction <= 1:
            raise ValueError("spc.point_decoration.central_fraction must be in (0, 1]")
        start, end = cls._decoration_date_window(
            section, default_start="2026-10-05", path="spc.point_decoration", today=today,
        )
        return fraction, start, end

    @classmethod
    def get_spc_period_box_source(cls) -> str:
        """Read the SPC capability period boxplot sample source from the inline domain config."""
        try:
            spc_conf = cls.load_domain_config("inline_domain").get("spc", {})
            report_conf = spc_conf.get("spc_cpk", {})
            source = str(report_conf.get("period_box_source", "point_value")).strip().lower()
            return source if source in {"sheet_mean", "point_value"} else "point_value"
        except Exception as e:
            logging.error(f"❌ 读取 SPC 周期箱线图数据源配置失败: {e}")
            return "point_value"

    @classmethod
    def get_spc_capability_param_exemptions(cls) -> list[str]:
        """Read parameter-name tokens excluded from both SPC capability metrics."""
        try:
            spc_conf = cls.load_domain_config("inline_domain").get("spc", {})
            capability_conf = spc_conf.get("spc_cpk", {})
            configured_values = capability_conf.get("exempt_param_name_contains", [])
            if not isinstance(configured_values, list):
                return []
            return [
                str(value).strip()
                for value in configured_values
                if value is not None and str(value).strip()
            ]
        except Exception as exc:
            logging.error("❌ 读取 SPC 能力指标参数豁免配置失败: %s", exc)
            return []

    @classmethod
    def get_spc_line_chart_param_name_contains(cls) -> list[str]:
        """Read parameter-name tokens rendered as SPC Sheet point line charts."""
        try:
            spc_conf = cls.load_domain_config("inline_domain").get("spc", {})
            chart_conf = spc_conf.get("chart", {})
            configured_values = chart_conf.get("line_param_name_contains", [])
            if not isinstance(configured_values, list):
                return []
            return [
                str(value).strip()
                for value in configured_values
                if value is not None and str(value).strip()
            ]
        except Exception as exc:
            logging.error("❌ 读取 SPC 前端折线图参数配置失败: %s", exc)
            return []

    @classmethod
    def get_auto_decoration_param_exemptions(cls) -> list[str]:
        """Read parameter-name tokens that bypass automatic value clipping."""
        try:
            decoration_conf = cls.load_domain_config("inline_domain").get("auto_decoration", {})
            configured_values = decoration_conf.get(
                "exempt_param_name_contains",
                [],
            )
            if not isinstance(configured_values, list):
                return []
            return [
                str(value).strip()
                for value in configured_values
                if value is not None and str(value).strip()
            ]
        except Exception as exc:
            logging.error("❌ 读取自动修饰参数豁免配置失败: %s", exc)
            return []

    @classmethod
    def get_inline_data_exclusion(
        cls, *, today: date | None = None,
    ) -> tuple[tuple[str, ...], str, str] | None:
        """Resolve the domain-wide projection exclusion, including legacy AOI config."""
        domain = cls.load_domain_config("inline_domain")
        section = domain.get("data_exclusion", domain.get("aoi_data_exclusion", {}))
        if not isinstance(section, dict):
            raise ValueError("data_exclusion must be a mapping")
        factories = section.get("factories", [])
        if not isinstance(factories, list) or any(
            not isinstance(value, str) or not value.strip() for value in factories
        ):
            raise ValueError("data_exclusion.factories must be a list of factory names")
        if not factories:
            return None
        try:
            start = date.fromisoformat(str(section["start_date"]))
            configured_end = section["end_date"]
            end = (
                (today or date.today())
                if configured_end == "today"
                else date.fromisoformat(str(configured_end))
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "data_exclusion requires ISO start_date and ISO end_date or 'today'"
            ) from exc
        if start > end:
            raise ValueError("data_exclusion.start_date must not exceed end_date")
        names = tuple(dict.fromkeys(value.strip().upper() for value in factories))
        return names, start.isoformat(), end.isoformat()

    @classmethod
    def get_aoi_data_exclusion(
        cls, *, today: date | None = None,
    ) -> tuple[tuple[str, ...], str, str] | None:
        """Compatibility entry; all Inline modules now share one policy."""
        return cls.get_inline_data_exclusion(today=today)

    @classmethod
    def get_aoi_rs_special_decoration_factories(cls) -> list[str]:
        """Read the AOI_RS special-rule allowlist; an empty list disables it."""
        report = cls.load_domain_config("inline_domain").get("aoi_rs", {})
        if not isinstance(report, dict):
            raise ValueError("aoi_rs must be a mapping")
        special = report.get("special_decoration", {})
        if not isinstance(special, dict):
            raise ValueError("aoi_rs.special_decoration must be a mapping")
        factories = special.get("factories", ["OLED"])
        if not isinstance(factories, list) or any(
            not isinstance(value, str) or not value.strip() for value in factories
        ):
            raise ValueError("aoi_rs.special_decoration.factories must be a list of factory names")
        return list(dict.fromkeys(value.strip().upper() for value in factories))

    @classmethod
    def get_aoi_rs_special_decoration_window(
        cls, *, today: date | None = None,
    ) -> tuple[str, str]:
        """Resolve the inclusive display dates for AOI_RS special rules."""
        report = cls.load_domain_config("inline_domain").get("aoi_rs", {})
        if not isinstance(report, dict):
            raise ValueError("aoi_rs must be a mapping")
        section = report.get("special_decoration", {})
        if not isinstance(section, dict):
            raise ValueError("aoi_rs.special_decoration must be a mapping")
        return cls._decoration_date_window(
            section, default_start="2026-09-21", path="aoi_rs.special_decoration", today=today,
        )

    @staticmethod
    def _decoration_date_window(
        section: dict, *, default_start: str, path: str, today: date | None = None,
    ) -> tuple[str, str]:
        try:
            start = date.fromisoformat(str(section.get("start_date", default_start)))
            configured_end = section.get("end_date", "today")
            end = (today or date.today()) if configured_end == "today" else date.fromisoformat(str(configured_end))
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{path} requires ISO start_date and ISO end_date or 'today'") from exc
        if start > end:
            raise ValueError(f"{path}.start_date must not exceed end_date")
        return start.isoformat(), end.isoformat()

    @classmethod
    def get_aoi_tt_particle_size_generation_enabled(cls) -> bool:
        """Return whether AOI_TT Particle Size values are generated from ratios."""
        try:
            report_conf = cls.load_domain_config("inline_domain").get("aoi_tt", {})
            particle_conf = report_conf.get("particle_size", {})
            return bool(particle_conf.get("generate_from_ratios", True))
        except Exception as exc:
            logging.error("❌ 读取 AOI_TT Particle Size 模式失败: %s", exc)
            return True

    @classmethod
    def get_aoi_tt_particle_size_ratio_jitter(cls) -> float:
        """Return the bounded relative jitter used by stable ratio generation."""
        try:
            report_conf = cls.load_domain_config("inline_domain").get("aoi_tt", {})
            particle_conf = report_conf.get("particle_size", {})
            configured = float(particle_conf.get("ratio_jitter", 0.1))
            return min(max(configured, 0.0), 1.0)
        except Exception as exc:
            logging.error("❌ 读取 AOI_TT Particle Size 扰动比例失败: %s", exc)
            return 0.1

    @classmethod
    def get_aoi_tt_particle_size_ratio_spec_path(cls) -> Path:
        """Resolve the AOI_TT station-level Particle Size ratio workbook."""
        return cls.get_domain_resource_path(
            "inline_domain",
            "aoi_tt_particle_size_ratio_spec",
            "AOI_TT-比例规格表.xlsx",
        )

    @classmethod
    def get_scrap_factory_mapping(cls) -> dict:
        """
        [新增] 获取报废站点 → 厂别映射配置
        """
        root_dir = cls.get_project_root()
        yaml_path = root_dir / "config" / "scrap_factory_mapping.yaml"
        
        try:
            if yaml_path.exists():
                result = cls._load_yaml(yaml_path)
                # 防御：确保 mappings 不为 None（YAML 空节点解析为 None）
                if result.get('mappings') is None:
                    result['mappings'] = {}
                return result
        except Exception as e:
            logging.error(f"❌ 读取 scrap_factory_mapping.yaml 失败: {e}")
            
        return {"default_prefix_rules": {}, "mappings": {}}

    @classmethod
    def get_equipment_config(cls) -> dict[str, Any]:
        """Load the critical-parts configuration from the equipment domain config."""
        config = cls.load_domain_config("equipment_domain")
        equipment = config.get("equipment", {})
        if not isinstance(equipment, dict):
            raise ValueError("equipment_domain.yaml: 'equipment' must be a mapping")
        if not equipment:
            raise ValueError(
                "equipment_domain.yaml is missing required 'equipment' settings: "
                f"{cls.get_domain_config_path('equipment_domain')}"
            )
        resources = cls._global_resource_config("equipment_domain")
        if resources is not None:
            equipment = cls._deep_merge(equipment, {"baseline": {
                "source_excel_path": str(cls.get_domain_resource_path("equipment_domain", "baseline_source_excel")),
            }})
        return equipment
