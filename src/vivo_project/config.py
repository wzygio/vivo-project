# src/vivo_project/config.py
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# 引入我们定义的 Pydantic 模型
from vivo_project.config_model import AppConfig

class ConfigLoader:
    """
    [配置工厂]
    纯静态工具类，负责按需加载配置。
    不持有任何状态，不创建全局单例。
    """

    @staticmethod
    def get_project_root() -> Path:
        """
        动态计算项目根目录。
        策略：以当前文件 (src/vivo_project/config.py) 为基准向上回溯。
        """
        try:
            # 当前文件: src/vivo_project/config.py
            # parent: src/vivo_project
            # parent.parent: src
            # parent.parent.parent: 项目根目录 (包含 .env, config/, src/)
            return Path(__file__).resolve().parent.parent.parent
        except NameError:
            # Fallback for some obscure environments
            return Path.cwd()

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
