# src\shared_kernel\config.py
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# 引入我们定义的 Pydantic 模型
from src.shared_kernel.config_model import AppConfig
from src.shared_kernel.compliance_config_excel import load_compliance_config_from_xlsx

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
        global_conf = cls._load_yaml(global_yaml_path)
        product_conf = cls._load_yaml(product_yaml_path)

        if not global_conf and not product_conf:
            msg = f"未找到任何有效配置！请检查路径: {config_dir}"
            logging.error(msg)
            # 在这一步抛出异常是合理的，因为没有配置程序无法运行
            raise FileNotFoundError(msg)

        # 4. 深度合并 (Global < Product)
        merged_conf = cls._deep_merge(global_conf, product_conf)

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
    def get_compliance_config_path(cls) -> Path:
        """Return the single workbook used by the manager and runtime engine."""
        return cls.get_project_root() / "resources" / "compliance_config.xlsx"

    @classmethod
    def get_compliance_config(cls) -> dict:
        """
        获取厂别-产品型号-监控类型-月份四维修饰配置。
        """
        xlsx_path = cls.get_compliance_config_path()
        try:
            if xlsx_path.exists():
                return load_compliance_config_from_xlsx(xlsx_path)
        except Exception as e:
            logging.error(f"❌ 读取 compliance 配置失败: {e}")

        return {"rules": []}

    @classmethod
    def get_spc_period_sigma_source(cls) -> str:
        """Read the SPC capability period sigma source from inline_config.yaml."""
        root_dir = cls.get_project_root()
        global_yaml_path = root_dir / "config" / "inline_config.yaml"

        try:
            global_conf = cls._load_yaml(global_yaml_path)
            spc_conf = global_conf.get("spc", {})
            report_conf = spc_conf.get("spc_cpk", {})
            return str(report_conf.get("period_sigma_source", "sheet_mean")).strip().lower()
        except Exception as e:
            logging.error(f"❌ 读取 SPC 周期能力口径配置失败: {e}")
            return "sheet_mean"

    @classmethod
    def get_spc_period_box_source(cls) -> str:
        """Read the SPC capability period boxplot sample source from inline_config.yaml."""
        yaml_path = cls.get_project_root() / "config" / "inline_config.yaml"
        try:
            spc_conf = cls._load_yaml(yaml_path).get("spc", {})
            report_conf = spc_conf.get("spc_cpk", {})
            source = str(report_conf.get("period_box_source", "point_value")).strip().lower()
            return source if source in {"sheet_mean", "point_value"} else "point_value"
        except Exception as e:
            logging.error(f"❌ 读取 SPC 周期箱线图数据源配置失败: {e}")
            return "point_value"

    @classmethod
    def get_spc_sheet_oos_clip_rules(cls) -> list[dict[str, object]]:
        """Read normalized parameter-specific Sheet OOS clipping rules."""
        yaml_path = cls.get_project_root() / "config" / "inline_config.yaml"
        try:
            spc_conf = cls._load_yaml(yaml_path).get("spc", {})
            decoration_conf = spc_conf.get("sheet_oos_decoration", {})
            configured_rules = decoration_conf.get("param_clip_rules", [])
            if not isinstance(configured_rules, list):
                return []

            normalized_rules: list[dict[str, object]] = []
            for rule in configured_rules:
                if not isinstance(rule, dict):
                    continue
                needle = str(rule.get("param_name_contains", "")).strip()
                if not needle:
                    continue
                try:
                    lower_offset = float(rule.get("lower_offset", 0.0))
                    upper_offset = float(rule.get("upper_offset", 0.0))
                except (TypeError, ValueError):
                    continue
                normalized_rules.append(
                    {
                        "param_name_contains": needle,
                        "lower_offset": lower_offset,
                        "upper_offset": upper_offset,
                    }
                )
            return normalized_rules
        except Exception as e:
            logging.error(f"❌ 读取 SPC Sheet OOS 截断规则失败: {e}")
            return []

    @classmethod
    def get_auto_decoration_param_exemptions(cls) -> list[str]:
        """Read parameter-name tokens that bypass automatic value clipping."""
        yaml_path = cls.get_project_root() / "config" / "inline_config.yaml"
        try:
            spc_conf = cls._load_yaml(yaml_path).get("spc", {})
            decoration_conf = spc_conf.get("auto_decoration", {})
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
        """Load the critical-parts configuration from equipment_config.yaml."""
        yaml_path = cls.get_project_root() / "config" / "equipment_config.yaml"
        config = cls._load_yaml(yaml_path)
        equipment = config.get("equipment", {})
        if not isinstance(equipment, dict):
            raise ValueError("equipment_config.yaml: 'equipment' must be a mapping")
        if not equipment:
            raise ValueError(
                f"equipment_config.yaml is missing required 'equipment' settings: {yaml_path}"
            )
        return equipment
